# Extracts the Data from Client Database/ Source

import argparse
import os
from typing import List, Callable, Tuple
import logging
import glob
import asyncio
from contextlib import asynccontextmanager

from dotenv import load_dotenv
import pandas as pd
import polars as pl
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
import asyncpg

from . import common


load_dotenv()

logger = logging.getLogger(__name__)


tables_dir = os.getenv("tables_dir")

if tables_dir:
    transformed_tables_dir = os.path.join(
        os.path.dirname(tables_dir), 
        "transformed_tables"
    )


from typing import Protocol

class Extract(Protocol):
    """Base class for any Extraction Task.
    
    This could be a connector to the client database.  
    Be it postgres, clickhouse, or even a csv file.
    Any source of truth which the client provides.  
    
    And from where the data is to be extracted for transformation and loading.
    """

    async def get_data(self, *args, **kwargs):
        """Get the data from the data source. 
        This method has to be implemented for every Extraction task.
        """
        pass


class ExtractFromPostgres:
    """This class should be initialized only once.
    
    This will keep a track of the tables that have been successfuly extracted."""
    def  __init__(
            self,
            host: str,
            user: str,
            password: str,
            database: str,
            table_name: str | None = None,
            chunk_size: int | None = None,
            schema_name: str | None = None,
            table_dir: str | None = None,
            port: int |  None = 5432,
    ):
        self._host = host
        self._user = user
        self.__password = password
        self._database = database
        self._table_name = table_name
        self._port = port
        self._schema_name = schema_name
        self._table_dir = table_dir

        # ensuring we have a chunk size
        try:
            self._chunk_size = (
                self._calculate_optimum_chunk_size()
                if not chunk_size 
                else chunk_size
            )
        except NotImplementedError:
            logger.warning(f"No chunk size provided. Using chunk size=50000")
            self._chunk_size = 50_000

        self.tables = []    # holds names of the tables which have been successfuly extracted

    @property
    def host(self):
        return self._host
    
    @property
    def database(self):
        return self._database
    
    @property
    def schema_name(self):
        return self._schema_name
    
    @property
    def password(self):
        return "***" + self.__password[-3:] if len(self.__password) > 4 else "***" + self.__password[-1]
    
    @property
    def table_dir(self):
        return self._table_dir
    
    @table_dir.setter
    def table_dir(self, value: str):
        if not os.path.isdir(value):
            raise ValueError("Table directory has to be a valid directory")
        self._table_dir = value


    @schema_name.setter
    def schema_name(self, value: str):
        if type(value) is not str:
            raise ValueError("Schema name should be a string")
        self._schema_name = value
    
    # TODO:
    def _calculate_optimum_chunk_size(self) -> int:
        """Calculates the most optimum chunk size of data to download."""
        raise NotImplementedError()

    def __repr__(self) -> str:
        """Return a nice string"""
        return f"""{self.__class__.__name__()}(host={self.host}, 
                                               user={self._user},
                                               database={self.database}, 
                                               password={self.password}, 
                                               port={self._port})"""

    async def _run_unit_query(self, cursor, query: str):
        """It is generator. Runs a query and generates responses. Row-wise."""
        
        cursor.execute(query)
        ans = cursor.fetchall()

        if type(cursor) is psycopg2.extras.DictCursor:
            for row in ans:
                yield dict(row)
        elif type(cursor) is psycopg2.extras.NamedTupleCursor:
            for row in ans:
                yield row

    # async def _run_unit_query(self, connection, query: str):
    #     """Overloaded for asyncpg"""
    #     async for record in connection.cursor(query, prefetch=self._chunk_size):
    #         yield record

    async def _get_all_table_names_in_database(self):
        assert self.schema_name is not None, "Schema Name is None."

        _all_data = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='{self.schema_name}';
        """
        async with self._conn.transaction():
            async for row in self._conn.cursor(_all_data):
                yield dict(row)["table_name"]



    async def get_connection(self, as_dict: bool=False):
        """`NamedTuple` is more memory efficient so use `as_dict=False` which is the default choice,
        unless you have a very good reason to have dictionary.

        The memory consumption is > 2X for `dict` than `NamedTuple`
        """
        # OLD:
        # conn = psycopg2.connect(
        #     host=self.host, 
        #     database=self.database, 
        #     user=self._user, 
        #     port=self._port, 
        #     password=self.__password, 
        # )
        logger.debug("Connecting to the database")
        self._conn = await asyncpg.connect(
            host=self.host, 
            database=self.database, 
            user=self._user, 
            port=self._port, 
            password=self.__password,           
        )

        # if as_dict: 
        #     cursor_factory = psycopg2.extras.DictCursor
        # else: 
        #     cursor_factory = psycopg2.extras.NamedTupleCursor
        
        # return conn.cursor(cursor_factory=cursor_factory)
        

    async def _fetch_chunks_of_data_from_db(self, table_name: str):
        """Gives chunks of data from the database.
        Each chunk will be saved as a single parquet file.
        """

        if self.schema_name: 
            query = f"SELECT * FROM {self.schema_name}.{table_name}"
        else:
            query = f"SELECT * FROM {table_name}"

        logger.info("Fetching chunks from table=%s", table_name)

        _table_path = os.path.join(self.table_dir, table_name)
        os.makedirs(_table_path, exist_ok=True)  # ensure the directory for the table exists

        _buffer = []    # will hold all the chunks until they are enough

        if self._conn:
            chunk_idx = 0

            # using asyncpg
            async with self._conn.transaction():
                async for row in self._conn.cursor(query, prefetch=self._chunk_size):
                    _buffer.append(dict(row))
                    logger.debug("Row appended=%s", row)
                    if len(_buffer) >= self._chunk_size:
                        logger.debug("Saving Chunk=%s for Table=%s", chunk_idx, table_name)
                        table = pa.Table.from_pylist(_buffer)
                        table_path = os.path.join(_table_path, f"{table_name}_chunk_{chunk_idx}.parquet")
                        # write the chunk
                        pq.write_table(table, table_path)
                        chunk_idx += 1
                        
                        _buffer = []    # resetting buffer as empty list
            
            if _buffer:
                logger.info("Saving Final Chunk=%s for Table=%s", chunk_idx, table_name)
                table = pa.Table.from_pylist(_buffer)
                file_path = os.path.join(_table_path, f"{table_name}_chunk_{chunk_idx}.parquet")
                pq.write_table(table, file_path)        
        else:
            raise Exception("No connection!")
                
        # write for psycopg2
        # if cursor


    async def _get_data_one_table(self, table_name: str | None = None):

        if table_name and self._table_name:
            # use this table
            logger.warning(
                "Object initialization table name=%s does not match table name now given=%s. Using the latter.", 
                self._table_name, table_name
            )
            self._table_name = table_name
        if not table_name:
            if self._table_name:
                table_name = self._table_name
            else:
                raise ValueError("Provide Table Name")

        # establish connection
        await self.get_connection()

        await self._fetch_chunks_of_data_from_db(table_name)

    async def get_data(self, table_names: List[str] | None = None):
        await self.get_connection()

        print(common.Ui.heading_divider)
        print("{:^10}".format("Extracting Data from Postgres"), end=" | ")
        print("{:>10}".format(self.database), end=" | ")
        print(common.Ui.heading_divider)
        
        # making a list of tables to be downloaded
        # if table names is not given then
        # get all the tables
        _list_tables = table_names or self._get_all_table_names_in_database()

        # this should ideally have a separate thread for each table
        async for table in _list_tables:
            logger.debug("Fetching Table=%s", table)
            await self._get_data_one_table(table)
    
        await self._conn.close()
        


