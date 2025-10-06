import os
from typing import List, Tuple, Protocol, Optional
import logging
import asyncio
import uuid

import asyncpg
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def _get_postgres_compatible_schema(schema: pl.Schema) -> List[Tuple[str, str]]:

    columns: List[Tuple] = []

    for colname, dtype in schema.items():
        if type(dtype) in (pl.Float64, pl.Float32):
            _type = "DOUBLE PRECISION"
        elif type(dtype) in (pl.Int16, pl.Int64, pl.Int32, pl.Int128, pl.Int8):
            _type = "BIGINT"
        elif type(dtype) is pl.String:
            _type = "TEXT"
        elif type(dtype) in (pl.Date,):
            _type = "DATE"
        elif type(dtype) in (pl.Datetime,):
            _type = "TIMESTAMP"
        elif dtype in (pl.Time,):
            _type =  "TIME"
        elif type(dtype) is pl.Boolean:
            _type = "BOOLEAN"
        elif dtype in (pl.Object,):
            _type =  "JSONB"
        else:
            _type = "TEXT"
        columns.append((colname, _type))
    return columns


class ChunkData:
    @staticmethod
    def read_from_path(path: str) -> "ChunkData":
        return ChunkData(pl.read_parquet(path))
    
    def __init__(self, data: pl.DataFrame):
        self._data = data

    @property
    def data(self):
        return self._data
    
    @property
    def shape(self):
        return self._data.shape


class Load(Protocol):

    async def aload_data(self, *args, **kwargs): ...

    def load_data(self, *args, **kwargs): ...

class _LoadParquetInMemory:
    _path_is_directory_parquet_files = False
    _path_is_single_parquet_file = False

    def __init__(self, path: str):
        """
        _summary_

        Parameters
        ----------
        path : str
            Could be a path to a single parquet file. 
            Or a directory containing many parquet files
        """
        self._path = path   
        if os.path.isfile(self._path):
            self._path_is_single_parquet_file = True
        elif os.path.isdir(self._path):
            self._path_is_directory_parquet_files = True     

    async def aload_data(self):
        """Is a generator. Loads data into memory.
        Data chunk will be used by downstream tasks like loading the data into postgres db."""
        if self._path_is_directory_parquet_files:
            # we have a directory containing many parquet files
            # we shall read and load them one by one
            for chunk_file in os.listdir(self._path):
                _chunk = self._read_one_chunk_file(os.path.join(self._path, chunk_file))
                yield _chunk
        elif self._path_is_single_parquet_file:
            _chunk = self._read_one_chunk_file(self._path)
            yield _chunk
    
    def _read_one_chunk_file(self, chunk_file_path: str) -> ChunkData:
        return ChunkData.read_from_path(chunk_file_path)
    
    def load_data(self):
        """Is a generator. Loads data into memory.
        Data chunk will be used by downstream tasks like loading the data into postgres db."""
        if self._path_is_directory_parquet_files:
            # we have a directory containing many parquet files
            # we shall read and load them one by one
            for chunk_file in os.listdir(self._path):
                _chunk = self._read_one_chunk_file(os.path.join(self._path, chunk_file))
                yield _chunk
        elif self._path_is_single_parquet_file:
            _chunk = self._read_one_chunk_file(self._path)
            yield _chunk

class LoadToPostgres:
    def __init__(
            self,
            username: str,
            password: str,
            database: str,
            url: str = "localhost",
            table: Optional[str] = None,
            port: Optional[int] = 5432
    ):
        self._username = username
        self.__password = password
        self._database = database
        self._url = url
        self._port = port
        if table:
            self._table = table


    def _ensure_table(self, conn, sample_data: pl.DataFrame):
        # synchronous only

        cur = conn.cursor()

        if not hasattr(self, "_table") or not self._table:
            # make a table
            # with arbitrary name
            self._table = f"table_{uuid.uuid4().hex[:8]}"
            logger.info("Table name was not given, Creating one with name=%s", self._table)

        # Check if table exists
        cur.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s);",
            (self._table,),
        )
        table_exists = cur.fetchone()[0]

        if table_exists:
            logger.info("Table '%s' already exists.", self._table)
            cur.close()
            conn.close()
            return  

        # Infer schema from Polars DataFrame
        col_defs = _get_postgres_compatible_schema(sample_data.schema)

        create_table_sql = f"CREATE TABLE {self._table} ({', '.join(col_defs)});"
        logger.info("Creating table '%s' with schema: %s", self._table, create_table_sql)

        try:
            cur.execute(create_table_sql)
            conn.commit()
            logger.info("Table '%s' created successfully.", self._table)
        except Exception as e:
            conn.rollback()
            logger.exception("Failed to create table: %s", e)
            raise
        finally:
            cur.close()
            conn.close()
            
    async def _aensure_table(self, conn, sample_data: pl.DataFrame):
            # asynchronous
            if not hasattr(self, "_table") or not self._table:
                self._table = f"table_{uuid.uuid4().hex[:8]}"
                logger.info("Table name was not given, creating one with name=%s", self._table)

            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables WHERE table_name = $1
                );
            """
            table_exists = await conn.fetchval(query, self._table)

            if table_exists:
                logger.info("Table '%s' already exists.", self._table)
                return

            col_defs = _get_postgres_compatible_schema(sample_data.schema)
            create_table_sql = f"CREATE TABLE {self._table} ({', '.join([f'"{col}" {pg_type}' for col, pg_type in col_defs])});"
            logger.info("Creating table '%s' with schema: %s", self._table, create_table_sql)

            try:
                await conn.execute(create_table_sql)
                logger.info("Table '%s' created successfully (async).", self._table)
            except Exception as e:
                logger.exception("Failed to create table asynchronously: %s", e)
                raise            

    def load_data(
            self, 
            path: str,
            table_name: Optional[str] = None,
            batch_size: Optional[int] = 10_000
    ):
        # this path could be that of a single parquet file
        # or a whole directory containing many parquet files

        self._table = self._table or table_name
        
        # loading parquet file into the memory
        _parquet_loader = _LoadParquetInMemory(path)
        
        conn = None

        try:
            conn = psycopg2.connect(
                dbname=self._database,
                user=self._username,
                password=self.__password,
                host=self._url,
                port=self._port
            )
            cursor = conn.cursor()

            _chunk_idx = 1

            # generating chunks
            for chunk in _parquet_loader.load_data():
                logger.debug("Chunk no. %s ", _chunk_idx)
                _columns = chunk.data.columns

                # when the first chunk, check the table
                if _chunk_idx == 1:
                    self._ensure_table(conn, chunk.data.head(5))

                insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                    sql.Identifier(self._table),
                    sql.SQL(", ").join(map(sql.Identifier, _columns)),
                )

                # Batched iteration
                for batch in chunk.data.iter_slices(batch_size):
                    rows = list(batch.iter_rows())
                    execute_values(cursor, insert_sql, rows, page_size=batch_size)
                _chunk_idx += 1

            conn.commit()
        
        except Exception as exc:
            if conn:
                conn.rollback()
            logger.exception("Failed to load data: %s", exc)
            raise
        
        finally:
            if conn:
                conn.close()

    async def aload_data(
            self, 
            path: str,
            table_name: Optional[str] = None,
            batch_size: Optional[int] = 10_000
    ):
        # this path could be that of a single parquet file
        # or a whole directory containing many parquet files

        if table_name:
            self._table = table_name
                    
        # loading parquet file into the memory
        _parquet_loader = _LoadParquetInMemory(path)
        
        try:
            conn = await asyncpg.connect(
                user=self._username,
                password=self.__password,
                database=self._database,
                host=self._url,
                port=self._port,                
            )
            _chunk_idx = 1

            # generating chunks
            async for chunk in _parquet_loader.aload_data():
                logger.debug("Chunk no. %s ", _chunk_idx)
                if _chunk_idx == 1:
                    # check the table
                    await self._aensure_table(conn, chunk.data.head(5))

                _columns = chunk.data.columns
                column_list = ", ".join(f'"{col}"' for col in _columns)

                for batch in chunk.data.iter_slices(batch_size):
                    rows = [tuple(row) for row in batch.iter_rows()]
                    placeholders = ", ".join(f"${i+1}" for i in range(len(_columns)))
                    query = f"INSERT INTO {self._table} ({column_list}) VALUES ({placeholders})"

                    # batch insert manually
                    await conn.executemany(query, rows)
                
                _chunk_idx += 1

        except Exception as exc:
            logger.exception("Ascync load failed: %s", exc)
            raise
        finally:
            if conn:
                await conn.close()


        
