# Extracts the Data from Client Database/ Source

import os
from typing import List, Callable, Tuple, Optional, Dict, Any, Union
import logging
import glob
import asyncio
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from functools import wraps
from enum import Enum
from pathlib import Path
from dataclasses import dataclass

from dotenv import load_dotenv
import pandas as pd
import polars as pl
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
import asyncpg

from . import common
from .metadata_handler import MetadataHandler, MetadataTypes


load_dotenv()

logger = logging.getLogger(__name__)


tables_dir = os.getenv("tables_dir")

if tables_dir:
    transformed_tables_dir = os.path.join(
        os.path.dirname(tables_dir), 
        "transformed_tables"
    )


from typing import Protocol

from .executable import _ExecutableMeta
from . import exceptions

# class Extract(Protocol):
#     """Base class for any Extraction Task.
    
#     This could be a connector to the client database.  
#     Be it postgres, clickhouse, or even a csv file.
#     Any source of truth which the client provides.  
    
#     And from where the data is to be extracted for transformation and loading.
#     """

#     async def get_data(self, *args, **kwargs):
#         """Get the data from the data source. 
#         This method has to be implemented for every Extraction task.
#         """
#         pass

class ExtractOutputOptions(Enum):
    STREAMING = 1
    '''When the data is getting streamed'''

    PERSIST = 2
    '''When the data is written/persisted on the storage'''

class Extract(metaclass=_ExecutableMeta):
    _registry_ = {}


    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        name = kwargs.get("name", cls.__name__)
        # setting the output_option of the subclass as default
        # default = PERSIST
        setattr(cls, "output_option", ExtractOutputOptions.PERSIST)
        
        # wrapping the run of the subclass
        Extract._wrap_subclass_run(cls)

        logger.debug(f"Registering {name} as an extractor.")
        Extract._registry_[name] = cls
    
    @classmethod
    def get_extractor(mcls, name: str):
        if name not in mcls._registry_:
            raise exceptions.FunctionalityNotFoundError(
                f"Extractor {name} not found"
            )
        return mcls._registry_.get(name)

    def _wrap_subclass_run(cls):
        # wrap the run of subclass

        # subclass run
        original_run = cls.run
        if asyncio.iscoroutinefunction(original_run):
            @wraps(original_run)
            async def _extended_run(self, x, *args, **kwargs):
                try:
                    res = await original_run(self, x, *args, **kwargs)
                    if self.output_option == ExtractOutputOptions.PERSIST:
                        # data is being persisted permanently
                        if isinstance(self, ExtractFromPostgres):
                            logger.debug("%s completed. Data saved at %s", type(self), res)
                        return res
                    logger.debug("%s completed", self.__class__.__name__)
                    return res
                except Exception as exc:
                    logger.exception("Exception while executing %s. %s", self.__class__.__name__, exc)
            
            cls.run = _extended_run
            return 
        
        @wraps(original_run)
        def _extended_run(self, x, *args, **kwargs):
            try:
                res = original_run(self, x, *args, **kwargs)
                if self.output_option == ExtractOutputOptions.PERSIST:
                    # data is being persisted permanently
                    if isinstance(self, ExtractFromPostgres):
                        logger.debug("%s completed. Data saved at %s", type(self), res)
                    return res
                logger.debug("%s completed", self.__class__.__name__)
                return res
            except Exception as exc:
                logger.exception("Exception while executing %s. %s", self.__class__.__name__, exc)

        cls.run = _extended_run

# class ExtractFromPostgres:
#     """This class should be initialized only once.
    
#     This will keep a track of the tables that have been successfuly extracted."""
#     def  __init__(
#             self,
#             host: str,
#             user: str,
#             password: str,
#             database: str,
#             table_name: str | None = None,
#             chunk_size: int | None = None,
#             schema_name: str | None = None,
#             table_dir: str | None = None,
#             port: int |  None = 5432,
#     ):
#         self._host = host
#         self._user = user
#         self.__password = password
#         self._database = database
#         self._table_name = table_name
#         self._port = port
#         self._schema_name = schema_name
#         self._table_dir = table_dir

#         # ensuring we have a chunk size
#         try:
#             self._chunk_size = (
#                 self._calculate_optimum_chunk_size()
#                 if not chunk_size 
#                 else chunk_size
#             )
#         except NotImplementedError:
#             logger.warning(f"No chunk size provided. Using chunk size=50000")
#             self._chunk_size = 50_000

#         self.tables = []    # holds names of the tables which have been successfuly extracted

#     @property
#     def host(self):
#         return self._host
    
#     @property
#     def database(self):
#         return self._database
    
#     @property
#     def schema_name(self):
#         return self._schema_name
    
#     @property
#     def password(self):
#         return "***" + self.__password[-3:] if len(self.__password) > 4 else "***" + self.__password[-1]
    
#     @property
#     def table_dir(self):
#         return self._table_dir
    
#     @table_dir.setter
#     def table_dir(self, value: str):
#         if not os.path.isdir(value):
#             raise ValueError("Table directory has to be a valid directory")
#         self._table_dir = value


#     @schema_name.setter
#     def schema_name(self, value: str):
#         if type(value) is not str:
#             raise ValueError("Schema name should be a string")
#         self._schema_name = value
    
#     # TODO:
#     def _calculate_optimum_chunk_size(self) -> int:
#         """Calculates the most optimum chunk size of data to download."""
#         raise NotImplementedError()

#     def __repr__(self) -> str:
#         """Return a nice string"""
#         return f"""{self.__class__.__name__()}(host={self.host}, 
#                                                user={self._user},
#                                                database={self.database}, 
#                                                password={self.password}, 
#                                                port={self._port})"""

#     async def _run_unit_query(self, cursor, query: str):
#         """It is generator. Runs a query and generates responses. Row-wise."""
        
#         cursor.execute(query)
#         ans = cursor.fetchall()

#         if type(cursor) is psycopg2.extras.DictCursor:
#             for row in ans:
#                 yield dict(row)
#         elif type(cursor) is psycopg2.extras.NamedTupleCursor:
#             for row in ans:
#                 yield row

#     # async def _run_unit_query(self, connection, query: str):
#     #     """Overloaded for asyncpg"""
#     #     async for record in connection.cursor(query, prefetch=self._chunk_size):
#     #         yield record

#     async def _get_all_table_names_in_database(self):
#         assert self.schema_name is not None, "Schema Name is None."

#         _all_data = f"""
#         SELECT table_name
#         FROM information_schema.tables
#         WHERE table_schema='{self.schema_name}';
#         """
#         async with self._conn.transaction():
#             async for row in self._conn.cursor(_all_data):
#                 yield dict(row)["table_name"]



#     async def get_connection(self, as_dict: bool=False):
#         """`NamedTuple` is more memory efficient so use `as_dict=False` which is the default choice,
#         unless you have a very good reason to have dictionary.

#         The memory consumption is > 2X for `dict` than `NamedTuple`
#         """
#         # OLD:
#         # conn = psycopg2.connect(
#         #     host=self.host, 
#         #     database=self.database, 
#         #     user=self._user, 
#         #     port=self._port, 
#         #     password=self.__password, 
#         # )
#         logger.debug("Connecting to the database")
#         self._conn = await asyncpg.connect(
#             host=self.host, 
#             database=self.database, 
#             user=self._user, 
#             port=self._port, 
#             password=self.__password,           
#         )

#         # if as_dict: 
#         #     cursor_factory = psycopg2.extras.DictCursor
#         # else: 
#         #     cursor_factory = psycopg2.extras.NamedTupleCursor
        
#         # return conn.cursor(cursor_factory=cursor_factory)
        

#     async def _fetch_chunks_of_data_from_db(self, table_name: str):
#         """Gives chunks of data from the database.
#         Each chunk will be saved as a single parquet file.
#         """

#         if self.schema_name: 
#             query = f"SELECT * FROM {self.schema_name}.{table_name}"
#         else:
#             query = f"SELECT * FROM {table_name}"

#         logger.info("Fetching chunks from table=%s", table_name)

#         _table_path = os.path.join(self.table_dir, table_name)
#         os.makedirs(_table_path, exist_ok=True)  # ensure the directory for the table exists

#         _buffer = []    # will hold all the chunks until they are enough

#         if self._conn:
#             chunk_idx = 0

#             # using asyncpg
#             async with self._conn.transaction():
#                 async for row in self._conn.cursor(query, prefetch=self._chunk_size):
#                     _buffer.append(dict(row))
#                     logger.debug("Row appended=%s", row)
#                     if len(_buffer) >= self._chunk_size:
#                         logger.debug("Saving Chunk=%s for Table=%s", chunk_idx, table_name)
#                         table = pa.Table.from_pylist(_buffer)
#                         table_path = os.path.join(_table_path, f"{table_name}_chunk_{chunk_idx}.parquet")
#                         # write the chunk
#                         pq.write_table(table, table_path)
#                         chunk_idx += 1
                        
#                         _buffer = []    # resetting buffer as empty list
            
#             if _buffer:
#                 logger.info("Saving Final Chunk=%s for Table=%s", chunk_idx, table_name)
#                 table = pa.Table.from_pylist(_buffer)
#                 file_path = os.path.join(_table_path, f"{table_name}_chunk_{chunk_idx}.parquet")
#                 pq.write_table(table, file_path)        
#         else:
#             raise Exception("No connection!")
                
#         # write for psycopg2
#         # if cursor


#     async def _get_data_one_table(self, table_name: str | None = None):

#         if table_name and self._table_name:
#             # use this table
#             logger.warning(
#                 "Object initialization table name=%s does not match table name now given=%s. Using the latter.", 
#                 self._table_name, table_name
#             )
#             self._table_name = table_name
#         if not table_name:
#             if self._table_name:
#                 table_name = self._table_name
#             else:
#                 raise ValueError("Provide Table Name")

#         # establish connection
#         await self.get_connection()

#         await self._fetch_chunks_of_data_from_db(table_name)

#     async def get_data(self, table_names: List[str] | None = None):
#         await self.get_connection()

#         print(common.Ui.heading_divider)
#         print("{:^10}".format("Extracting Data from Postgres"), end=" | ")
#         print("{:>10}".format(self.database), end=" | ")
#         print(common.Ui.heading_divider)
        
#         # making a list of tables to be downloaded
#         # if table names is not given then
#         # get all the tables
#         _list_tables = table_names or self._get_all_table_names_in_database()

#         # this should ideally have a separate thread for each table
#         async for table in _list_tables:
#             logger.debug("Fetching Table=%s", table)
#             await self._get_data_one_table(table)
    
#         await self._conn.close()
        

# class IncrementalExtractFromPostgres(ExtractFromPostgres):
#     """
#     Extended PostgreSQL extractor for incremental loads.
#     - Fetches only new data based on fromDate/toDate columns
#     - Appends new chunks without overwriting existing ones
#     - Maintains state file to track last extraction
#     """
    
#     def __init__(
#         self,
#         host: str,
#         user: str,
#         password: str,
#         database: str,
#         table_name: Optional[str] = None,
#         chunk_size: Optional[int] = None,
#         schema_name: Optional[str] = None,
#         table_dir: Optional[str] = None,
#         port: Optional[int] = 5432,
#         state_file: str = ".last_extract.txt",
#         date_column_from: str = "fromDate",
#         date_column_to: str = "toDate"
#     ):
#         """Initialize incremental extractor with date tracking."""
#         super().__init__(
#             host=host,
#             user=user,
#             password=password,
#             database=database,
#             table_name=table_name,
#             chunk_size=chunk_size,
#             schema_name=schema_name,
#             table_dir=table_dir,
#             port=port
#         )
        
#         self.state_file = state_file
#         self.date_column_from = date_column_from
#         self.date_column_to = date_column_to
#         self.last_extract_date = self._load_last_extract_date()
    
#     def _load_last_extract_date(self) -> datetime:
#         """Load the last extraction date from state file."""
#         if os.path.exists(self.state_file):
#             try:
#                 with open(self.state_file, 'r') as f:
#                     date_str = f.read().strip()
#                     last_date = datetime.fromisoformat(date_str)
#                     logger.info("Loaded last extract date: %s", last_date)
#                     return last_date
#             except (ValueError, IOError) as e:
#                 logger.warning("Failed to load state file: %s, using default", e)
        
#         # Default: 7 days ago
#         default_date = datetime.now() - timedelta(days=7)
#         logger.info("Using default extraction date: %s", default_date)
#         return default_date
    
#     def _save_last_extract_date(self, date: datetime):
#         """Save the extraction date to state file."""
#         try:
#             with open(self.state_file, 'w') as f:
#                 f.write(date.isoformat())
#             logger.info("State saved: last extract date = %s", date.isoformat())
#         except IOError as e:
#             logger.error("Failed to save state file: %s", e)
    
#     def _get_next_chunk_index(self, table_path: str, table_name: str) -> int:
#         """
#         Get the next available chunk index by scanning existing files.
#         This prevents overwriting existing chunks during incremental loads.
        
#         Parameters
#         ----------
#         table_path : str
#             Directory containing table chunks
#         table_name : str
#             Name of the table
            
#         Returns
#         -------
#         int
#             Next available chunk index
#         """
#         if not os.path.exists(table_path):
#             return 0
        
#         # Find all existing chunk files
#         existing_files = [f for f in os.listdir(table_path) if f.endswith('.parquet')]
        
#         if not existing_files:
#             return 0
        
#         # Extract chunk indices from filenames
#         # Format: {table_name}_chunk_{idx}.parquet
#         max_idx = -1
#         for filename in existing_files:
#             try:
#                 # Extract the chunk index from filename
#                 parts = filename.replace('.parquet', '').split('_chunk_')
#                 if len(parts) == 2:
#                     idx = int(parts[1])
#                     max_idx = max(max_idx, idx)
#             except (ValueError, IndexError):
#                 continue
        
#         next_idx = max_idx + 1
#         logger.info("Table %s: Found %d existing chunks, starting at chunk %d", 
#                    table_name, max_idx + 1, next_idx)
#         return next_idx
    
#     async def _fetch_chunks_of_data_from_db(self, table_name: str):
#         """
#         Override to fetch only incremental data based on date range.
#         Appends new chunks without overwriting existing ones.
#         """
#         current_date = datetime.now()
#         from_date = self.last_extract_date.date()
#         to_date = current_date.date()
        
#         logger.info(
#             "Extracting incremental data: table=%s, date_range=[%s to %s]", 
#             table_name, from_date, to_date
#         )
        
#         # Build incremental query
#         if self.schema_name:
#             base_query = f'SELECT * FROM {self.schema_name}."{table_name}"'
#         else:
#             base_query = f'SELECT * FROM "{table_name}"'
        
#         incremental_query = f"""
#             {base_query}
#             WHERE (
#                 ("{self.date_column_from}" >= '{from_date}' AND "{self.date_column_from}" < '{to_date}')
#                 OR ("{self.date_column_to}" >= '{from_date}' AND "{self.date_column_to}" < '{to_date}')
#                 OR ("{self.date_column_from}" < '{from_date}' AND "{self.date_column_to}" >= '{to_date}')
#             )
#         """
        
#         _table_path = os.path.join(self.table_dir, table_name)
#         os.makedirs(_table_path, exist_ok=True)
        
#         # Get next available chunk index to avoid overwriting
#         chunk_idx = self._get_next_chunk_index(_table_path, table_name)
#         starting_chunk_idx = chunk_idx
        
#         _buffer = []
#         total_rows = 0
        
#         if self._conn:
#             import pyarrow as pa
#             import pyarrow.parquet as pq
            
#             async with self._conn.transaction():
#                 async for row in self._conn.cursor(incremental_query, prefetch=self._chunk_size):
#                     _buffer.append(dict(row))
#                     total_rows += 1
                    
#                     if len(_buffer) >= self._chunk_size:
#                         logger.debug("Saving chunk=%s for table=%s", chunk_idx, table_name)
#                         table = pa.Table.from_pylist(_buffer)
#                         table_path = os.path.join(_table_path, f"{table_name}_chunk_{chunk_idx}.parquet")
#                         pq.write_table(table, table_path)
#                         chunk_idx += 1
#                         _buffer = []
            
#             # Save final chunk if there's remaining data
#             if _buffer:
#                 logger.info("Saving final chunk=%s for table=%s", chunk_idx, table_name)
#                 table = pa.Table.from_pylist(_buffer)
#                 file_path = os.path.join(_table_path, f"{table_name}_chunk_{chunk_idx}.parquet")
#                 pq.write_table(table, file_path)
#                 chunk_idx += 1
            
#             logger.info("Extracted %d new rows from table=%s (chunks: %d-%d)", 
#                        total_rows, table_name, starting_chunk_idx, chunk_idx - 1)
            
#             # Track successfully extracted table
#             if table_name not in self.tables:
#                 self.tables.append(table_name)
#         else:
#             raise Exception("No database connection established!")
    
#     async def get_data(self, table_names: Optional[List[str]] = None):
#         """
#         Extract incremental data from specified tables.
#         Updates the state file after successful extraction.
        
#         Parameters
#         ----------
#         table_names : Optional[List[str]]
#             List of table names to extract
#         """
#         extraction_start_time = datetime.now()
        
#         # Call parent's get_data to handle connection and extraction
#         await super().get_data(table_names=table_names)
        
#         # Update state file after successful extraction
#         self._save_last_extract_date(extraction_start_time)
        
#         logger.info("Incremental extraction completed for tables: %s", self.tables)




class ExtractFromPostgres(Extract):
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
            port: int |  None = 5432
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
        return (
            f"{self.__class__.__name__} "
                f"(host={self.host}, " 
                f"user={self._user}, " 
                f"database={self.database}, " 
                f"password={self.password}, " 
                f"port={self._port}), "
                f"output_option={self.output_option}"
        )

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

    async def _table_shape_(self, table_name: str):
        schema_name = self.schema_name or "public"
        _query_for_shape = f"""
        WITH cols AS (
            SELECT COUNT(*) AS column_count
            FROM information_schema.columns
            WHERE table_schema='{self.schema_name}'
                AND table_name = '{table_name}'
        ),

        row AS (
            SELECT COUNT(*) AS row_count
            FROM {table_name}
        )
        SELECT row_count, column_count
        FROM rows, cols;
        """

        async with self._conn.transaction():
            return self._conn.cursor(_query_for_shape)

    async def _get_all_table_names_in_database(self):
        assert self.schema_name is not None, "Schema Name is not provided."

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

        
    async def _fetch_chunks_of_data_from_db(
        self, 
        table_name: str, 
        start_row: int = None, 
        end_row: int = None
    ):
        """Gives chunks of data from the database.
        Each chunk will be saved as a single parquet file.
        
        Args:
            table_name: Name of the table to fetch data from
            start_row: Optional starting row number (1-indexed, inclusive)
            end_row: Optional ending row number (1-indexed, inclusive)
        """

        # Build the base query
        if self.schema_name: 
            base_query = f"SELECT * FROM {self.schema_name}.{table_name}"
        else:
            base_query = f"SELECT * FROM {table_name}"
        
        # Add LIMIT and OFFSET if start_row or end_row is provided
        if start_row is not None or end_row is not None:
            offset = (start_row - 1) if start_row else 0
            
            if start_row and end_row:
                limit = end_row - start_row + 1
            elif end_row:
                limit = end_row
            else:
                limit = None
            
            if limit is not None:
                query = f"{base_query} LIMIT {limit} OFFSET {offset}"
            else:
                query = f"{base_query} OFFSET {offset}"
            
            logger.info(
                "Fetching chunks from table=%s with start_row=%s, end_row=%s (LIMIT=%s, OFFSET=%s)", 
                table_name, start_row, end_row, limit, offset
            )
        else:
            query = base_query
            logger.info("Fetching all chunks from table=%s", table_name)

        _table_path = os.path.join(self.table_dir, table_name)
        os.makedirs(_table_path, exist_ok=True)  # ensure the directory for the table exists

        _buffer = []    # will hold all the chunks until they are enough

        if self._conn:
            chunk_idx = 0
            _row_count = 0

            # using asyncpg for PostgreSQL
            async with self._conn.transaction():
                async for row in self._conn.cursor(query, prefetch=self._chunk_size):
                    _buffer.append(dict(row))
                    
                    if (_row_count+1) % int(self._chunk_size / 10) == 0:
                        logger.debug("Row appended=%s", row)

                    if len(_buffer) >= self._chunk_size:
                        logger.debug("Saving Chunk=%s for Table=%s", chunk_idx, table_name)
                        table = pa.Table.from_pylist(_buffer)
                        table_path = os.path.join(_table_path, f"{table_name}_chunk_{chunk_idx}.parquet")
                        # write the chunk
                        pq.write_table(table, table_path)
                        chunk_idx += 1
                        
                        _buffer = []    # resetting buffer as empty list
                    _row_count += 1

            if _buffer:
                logger.info("Saving Final Chunk=%s for Table=%s", chunk_idx, table_name)
                table = pa.Table.from_pylist(_buffer)
                file_path = os.path.join(_table_path, f"{table_name}_chunk_{chunk_idx}.parquet")
                pq.write_table(table, file_path)  
        else:
            raise Exception("No connection!")
                
        # write for psycopg2
        # if cursor

    async def _get_data_one_table(self, table_name: str | None = None, *args, **kwargs):

        if (table_name and self._table_name) and (table_name != self._table_name):
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

        await self._fetch_chunks_of_data_from_db(table_name, *args, **kwargs)

    async def _table_names_list_to_generator_convertor(self, table_names: List[str]):
        for table in table_names:
            yield table

    async def arun(self, *args, **kwargs):
        table_names = args[0]
        logger.debug("Table Names in ExtractFromPostgres.arun=%s", table_names)
        try:
            await self.get_connection()
            logger.info("Database connected.")
        except Exception as exc:
            raise
        print(common.Ui.heading_divider)
        print("{:^10}".format("Extracting Data from Postgres"), end=" | ")
        print("{:>10}".format(self.database))
        print(common.Ui.heading_divider)
        
        # making a list of tables to be downloaded
        # if table names is not given then
        # get all the tables
        if table_names:
            _list_tables = self._table_names_list_to_generator_convertor(table_names)   # creates a generator 
        else:
            _list_tables = self._get_all_table_names_in_database(*args, **kwargs)

        # this should ideally have a separate thread for each table
        async for table in _list_tables:    # _list_tables should be a generator
            logger.debug("Fetching Table=%s", table)
            await self._get_data_one_table(table)
    
        await self._conn.close()

        return os.path.join(self._table_dir, table_names[0])

    async def run(self, *args, **kwargs) -> str:
        logger.warning("%s only has asynchronous run. " \
        "So, running this asynchronously", self.__class__.__name__)
        table_names = args[0]
        if type(table_names) is not list:
            raise TypeError(f"Arguments provided={args}." 
                            f"`table_names = {args[0]}`"
                            "`table_names` should be list of table names.")
        await self.arun(table_names=table_names, *args, **kwargs)
        return os.path.join(self._table_dir, table_names[0])    # TODO: Generalize this for more tables 

class ExtractFromFile(Extract):
    """
    Generic file extractor supporting Excel, CSV, and Parquet formats.

    Returns a Polars DataFrame or list of DataFrames if chunking is enabled.
    """

    __name__ = "extract_from_file"

    def __init__(
        self,
        file_path: str,
        sheet_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        chunk_size: Optional[int] = None,
        has_header: bool = True,
        delimiter: str = ",",
        encoding: str = "utf8",
    ):
        """
        Args:
            file_path: Path to the Excel, CSV, or Parquet file.
            sheet_name: (Excel only) Sheet to extract. Defaults to the first sheet.
            columns: List of column names to select.
            filters: Dictionary of column filters, e.g. {"country": "US"}.
            chunk_size: If specified, splits the DataFrame into chunks of given size.
            has_header: For CSV files, whether the first row is a header.
            delimiter: CSV delimiter.
            encoding: Encoding for text-based files.
        """
        self.file_path = file_path
        self.sheet_name = sheet_name
        self.columns = columns
        self.filters = filters or {}
        self.chunk_size = chunk_size
        self.has_header = has_header
        self.delimiter = delimiter
        self.encoding = encoding


    def _validate_file(self):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        self._ext = os.path.splitext(self.file_path)[1].lower()

        valid_extensions = {
            ".xlsx": "Excel",
            ".xls": "Excel",
            ".csv": "CSV",
            ".parquet": "Parquet",
        }

        if self._ext not in valid_extensions:
            raise ValueError(
                f"Invalid file type for {self.file_path}. "
                f"Supported types are: {', '.join(valid_extensions.keys())}"
            )

        self._file_type = valid_extensions[self._ext]
        logger.debug(f"Validated {self._file_type} file: {self.file_path}")


    def _read_file(self) -> pl.DataFrame:
        if self._ext in (".xlsx", ".xls"):
            return pl.read_excel(source=self.file_path, sheet_name=self.sheet_name)

        elif self._ext == ".csv":
            return pl.read_csv(
                self.file_path,
                has_header=self.has_header,
                separator=self.delimiter,
                encoding=self.encoding,
            )

        elif self._ext == ".parquet":
            return pl.read_parquet(self.file_path)

        raise ValueError(f"Unsupported file type: {self._ext}")


    def _apply_filters(self, df: pl.DataFrame) -> pl.DataFrame:
        for col, val in self.filters.items():
            if col in df.columns:
                df = df.filter(pl.col(col) == val)
            else:
                logger.warning(f"Filter column {col} not found in file {self.file_path}")
        return df


    async def run(self, x=None, *args, **kwargs) -> Union[pl.DataFrame, List[pl.DataFrame]]:
        """
        Extracts data from the specified file into a Polars DataFrame.
        Supports Excel, CSV, and Parquet formats.
        """
        self._validate_file()
        logger.debug(f"Starting extraction: {self.file_path}")

        # Read the file asynchronously
        df = await asyncio.to_thread(self._read_file)

        # Column selection
        if self.columns:
            missing = [c for c in self.columns if c not in df.columns]
            if missing:
                logger.warning(f"Columns {missing} not found in {self.file_path}")
            df = df.select([c for c in self.columns if c in df.columns])

        # Apply filters
        if self.filters:
            df = self._apply_filters(df)

        # Return chunks if requested
        if self.chunk_size:
            total_rows = df.height
            chunks = [
                df[i : i + self.chunk_size] for i in range(0, total_rows, self.chunk_size)
            ]
            logger.debug(f"Returning {len(chunks)} chunks from {self.file_path}")
            return chunks

        logger.debug(
            f"Extraction complete for {self.file_path} "
            f"(rows={df.height}, cols={len(df.columns)})"
        )
        return df

class IncrementalExtractFromPostgres(ExtractFromPostgres):
    """
    Extended PostgreSQL extractor for incremental loads.
    - Fetches only new data based on fromDate/toDate columns
    - Appends new chunks without overwriting existing ones
    - Maintains state file to track last extraction
    """
    
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        table_name: Optional[str] = None,
        chunk_size: Optional[int] = None,
        schema_name: Optional[str] = None,
        table_dir: Optional[str] = None,
        port: Optional[int] = 5432,
        state_file: str = ".last_extract.txt",
        date_column_from: str = "fromDate",
        date_column_to: str = "toDate"
    ):
        """Initialize incremental extractor with date tracking."""
        super().__init__(
            host=host,
            user=user,
            password=password,
            database=database,
            table_name=table_name,
            chunk_size=chunk_size,
            schema_name=schema_name,
            table_dir=table_dir,
            port=port
        )
        
        self.state_file = state_file
        self.date_column_from = date_column_from
        self.date_column_to = date_column_to
        self.last_extract_date = self._load_last_extract_date()
    
    def _load_last_extract_date(self) -> datetime:
        """Load the last extraction date from state file."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    date_str = f.read().strip()
                    last_date = datetime.fromisoformat(date_str)
                    logger.info("Loaded last extract date: %s", last_date)
                    return last_date
            except (ValueError, IOError) as e:
                logger.warning("Failed to load state file: %s, using default", e)
        
        # Default: 7 days ago
        default_date = datetime.now() - timedelta(days=7)
        logger.info("Using default extraction date: %s", default_date)
        return default_date
    
    def _save_last_extract_date(self, date: datetime):
        """Save the extraction date to state file."""
        try:
            with open(self.state_file, 'w') as f:
                f.write(date.isoformat())
            logger.info("State saved: last extract date = %s", date.isoformat())
        except IOError as e:
            logger.error("Failed to save state file: %s", e)
    
    def _get_next_chunk_index(self, table_path: str, table_name: str) -> int:
        """
        Get the next available chunk index by scanning existing files.
        This prevents overwriting existing chunks during incremental loads.
        
        Parameters
        ----------
        table_path : str
            Directory containing table chunks
        table_name : str
            Name of the table
            
        Returns
        -------
        int
            Next available chunk index
        """
        if not os.path.exists(table_path):
            return 0
        
        # Find all existing chunk files
        existing_files = [f for f in os.listdir(table_path) if f.endswith('.parquet')]
        
        if not existing_files:
            return 0
        
        # Extract chunk indices from filenames
        # Format: {table_name}_chunk_{idx}.parquet
        max_idx = -1
        for filename in existing_files:
            try:
                # Extract the chunk index from filename
                parts = filename.replace('.parquet', '').split('_chunk_')
                if len(parts) == 2:
                    idx = int(parts[1])
                    max_idx = max(max_idx, idx)
            except (ValueError, IndexError):
                continue
        
        next_idx = max_idx + 1
        logger.info("Table %s: Found %d existing chunks, starting at chunk %d", 
                   table_name, max_idx + 1, next_idx)
        return next_idx
    
    async def _fetch_chunks_of_data_from_db(self, table_name: str):
        """
        Override to fetch only incremental data based on date range.
        Appends new chunks without overwriting existing ones.
        """
        current_date = datetime.now()
        from_date = self.last_extract_date.date()
        to_date = current_date.date()
        
        logger.info(
            "Extracting incremental data: table=%s, date_range=[%s to %s]", 
            table_name, from_date, to_date
        )
        
        # Build incremental query
        if self.schema_name:
            base_query = f'SELECT * FROM {self.schema_name}."{table_name}"'
        else:
            base_query = f'SELECT * FROM "{table_name}"'
        
        incremental_query = f"""
            {base_query}
            WHERE (
                ("{self.date_column_from}" >= '{from_date}' AND "{self.date_column_from}" < '{to_date}')
                OR ("{self.date_column_to}" >= '{from_date}' AND "{self.date_column_to}" < '{to_date}')
                OR ("{self.date_column_from}" < '{from_date}' AND "{self.date_column_to}" >= '{to_date}')
            )
        """
        
        _table_path = os.path.join(self.table_dir, table_name)
        os.makedirs(_table_path, exist_ok=True)
        
        # Get next available chunk index to avoid overwriting
        chunk_idx = self._get_next_chunk_index(_table_path, table_name)
        starting_chunk_idx = chunk_idx
        
        _buffer = []
        total_rows = 0
        
        if self._conn:
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            async with self._conn.transaction():
                async for row in self._conn.cursor(incremental_query, prefetch=self._chunk_size):
                    _buffer.append(dict(row))
                    total_rows += 1
                    
                    if len(_buffer) >= self._chunk_size:
                        logger.debug("Saving chunk=%s for table=%s", chunk_idx, table_name)
                        table = pa.Table.from_pylist(_buffer)
                        table_path = os.path.join(_table_path, f"{table_name}_chunk_{chunk_idx}.parquet")
                        pq.write_table(table, table_path)
                        chunk_idx += 1
                        _buffer = []
            
            # Save final chunk if there's remaining data
            if _buffer:
                logger.info("Saving final chunk=%s for table=%s", chunk_idx, table_name)
                table = pa.Table.from_pylist(_buffer)
                file_path = os.path.join(_table_path, f"{table_name}_chunk_{chunk_idx}.parquet")
                pq.write_table(table, file_path)
                chunk_idx += 1
            
            logger.info("Extracted %d new rows from table=%s (chunks: %d-%d)", 
                       total_rows, table_name, starting_chunk_idx, chunk_idx - 1)
            
            # Track successfully extracted table
            if table_name not in self.tables:
                self.tables.append(table_name)
        else:
            raise Exception("No database connection established!")
    
    async def arun(self, *args, **kwargs):
        """
        Extract incremental data from specified tables.
        Updates the state file after successful extraction.
        
        Parameters
        ----------
        table_names : Optional[List[str]]
            List of table names to extract
        """
        table_names = kwargs.get("table_names", None) or kwargs.get("x", None)
        extraction_start_time = datetime.now()
        
        # Call parent's get_data to handle connection and extraction
        await super().get_data(table_names=table_names)
        
        # Update state file after successful extraction
        self._save_last_extract_date(extraction_start_time)
        
        logger.info("Incremental extraction completed for tables: %s", self.tables)

    def run(self, *args, **kwargs):
        table_names = kwargs.get("table_names", None) or kwargs.get("x", None)
        logger.warning(f"{self.__class__.__name__} does not have synchronous run.")
        return asyncio.run(self.arun(table_names=table_names))
        # raise NotImplementedError(f"{self.__class__.__name__} does not have synchronous run.")

class IncrementalExtractFromPostgresRowWise(ExtractFromPostgres):
    """The incremental extraction occurs row wise.
    So, this expects a row number to take the data from 
    and an ending row till which the data is to be taken."""
    def __init__(
            self, 
            row_start: int = 0,
            row_end: int = -1,
            n_rows: Optional[int] = -1,
            data_metadata: Union[str, Path] = ".data.metadata",
            **kwargs
    ):
        """
        _summary_

        Parameters
        ----------
        row_start : int, optional
            Extraction should begin at , by default 0
        row_end : int, optional
            Data to be extracted upto this row number, by default -1, or gets all the rows
        n_rows : Optional[int], optional
            Number of rows to extract. Only to be provided when `end_row` is not provided, by default -1
        data_metadata : Union[str, Path], optional
            _description_, by default ".data.metadata"
        """
        # initialize postgres funcitonality
        super().__init__(**kwargs)

        # inititalize starting row
        self._starting_row = row_start

        # load the last extracted row
        # expects `.data.metadata` file
        self._metadata_path = Path(data_metadata)

        self.metadata_handler = MetadataHandler(data_metadata)
        
        try:
            self._last_extracted = self.metadata_handler._metadata[-1]
        except IndexError:
            self._last_extracted = None

        if self._last_extracted:
            if not self._last_extracted.get("data_type") == MetadataTypes.TABULAR:
                raise Exception("Last saved metadata is not of a table")

            # if last from the prev stored metadata is extracted
            # update the starting row as the ending row + 1 of the last extracted data 
            self._starting_row = self._last_extracted["content"]["row_end"] + 1 # last row of the last data
        
        # initializing ending row according to the starting row
        self._ending_row = max(self._starting_row + n_rows, row_end)
        if self._ending_row == -1:
            logger.warning("All rows of the data will be extracted")
    
    async def _fetch_chunks_of_data_from_db(self, table_name):
        return await super()._fetch_chunks_of_data_from_db(table_name, self._starting_row, self._ending_row) 
    
    async def arun(self, *args, **kwargs):
        kwargs.update({"start_row": self._starting_row})
        kwargs.update({"end_row": self._ending_row})
        return await super().arun(*args, **kwargs)
    
    async def run(self, *args, **kwargs):
        return await super().run(*args, **kwargs)