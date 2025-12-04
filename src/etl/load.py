import os
from typing import (
    List, 
    Tuple, 
    Protocol, 
    Optional, 
    Literal, 
    Union
)
import logging
import asyncio
import uuid
from functools import wraps

import asyncpg
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

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


# class Load(Protocol):

#     async def aload_data(self, *args, **kwargs): ...

#     def load_data(self, *args, **kwargs): ...

from . import executable
from . import exceptions

class Load(metaclass=executable._ExecutableMeta):
    _registry_ = {}
    
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        name = kwargs.get("name", cls.__name__)
        
        cls._can_be_applied_to_chunk_ = True

        # wrap the run() of subclasses, since the input could be a dataframe
        # or a table path
        Load._wrap_subclass_run(cls)

        logger.info(f"Registering {name} as an Loading Method.")
        Load._registry_[name] = cls
    
    @classmethod
    def get_transform(mcls, name: str):
        if name not in mcls._registry_:
            raise exceptions.FunctionalityNotFoundError(f"Load Method {name} not found")
        return mcls._registry_.get(name)

    def _wrap_subclass_run(cls):
        # to wrap the run of subclass
        original_run = cls.run

        @wraps(original_run)
        def _extended_run(self, x: Union[pl.DataFrame, str, pd.DataFrame], *args, **kwargs):
            df = Load._check_input(x)
            return original_run(self, df, *args, **kwargs)
        
        cls.run = _extended_run
    
    def _check_input(x: Union[pl.DataFrame, str, pd.DataFrame]) -> pl.DataFrame:
        if isinstance(x, pd.DataFrame): 
            return pl.DataFrame(x)
        elif isinstance(x, pl.DataFrame):
            return x
        elif isinstance(x, str):
            return pl.read_parquet(x)
        raise TypeError("Transform input should be either table path or table itself")
    

class _LoadParquetFromPath:
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


class _LoadParquetFromBuffer:
    _is_chunks_of_parquet_files = False
    _is_single_parquet_file = False

    def __init__(self, parquet_in_buffer: pl.DataFrame):
        """
        _summary_

        Parameters
        ----------
        path : str
            Could be a path to a single parquet file. 
            Or a directory containing many parquet files
        """
        self._parquet_in_buffer = parquet_in_buffer # could be pl.DataFrame

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
        # TODO: loading all at once, highly undesirable, will change in coming iterations
        yield ChunkData(self._parquet_in_buffer )  



# class LoadToPostgres(Load):
#     def __init__(
#             self,
#             username: str,
#             password: str,
#             database: str,
#             url: str = "localhost",
#             table: Optional[str] = None,
#             port: Optional[int] = 5432
#     ):
#         self._username = username
#         self.__password = password
#         self._database = database
#         self._url = url
#         self._port = port
#         if table:
#             self._table = table


#     def _ensure_table(self, conn, sample_data: pl.DataFrame):
#         # synchronous only

#         cur = conn.cursor()

#         if not hasattr(self, "_table") or not self._table:
#             # make a table
#             # with arbitrary name
#             self._table = f"table_{uuid.uuid4().hex[:8]}"
#             logger.info("Table name was not given, Creating one with name=%s", self._table)

#         # Check if table exists
#         cur.execute(
#             "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s);",
#             (self._table,),
#         )
#         table_exists = cur.fetchone()[0]

#         if table_exists:
#             logger.info("Table '%s' already exists.", self._table)
#             cur.close()
#             conn.close()
#             return  

#         # Infer schema from Polars DataFrame
#         col_defs = _get_postgres_compatible_schema(sample_data.schema)

#         create_table_sql = f"CREATE TABLE {self._table} ({', '.join(col_defs)});"
#         logger.info("Creating table '%s' with schema: %s", self._table, create_table_sql)

#         try:
#             cur.execute(create_table_sql)
#             conn.commit()
#             logger.info("Table '%s' created successfully.", self._table)
#         except Exception as e:
#             conn.rollback()
#             logger.exception("Failed to create table: %s", e)
#             raise
#         finally:
#             cur.close()
#             conn.close()
            
#     async def _aensure_table(self, conn, sample_data: pl.DataFrame):
#             # asynchronous
#             if not hasattr(self, "_table") or not self._table:
#                 self._table = f"table_{uuid.uuid4().hex[:8]}"
#                 logger.info("Table name was not given, creating one with name=%s", self._table)

#             query = """
#                 SELECT EXISTS (
#                     SELECT FROM information_schema.tables WHERE table_name = $1
#                 );
#             """
#             table_exists = await conn.fetchval(query, self._table)

#             if table_exists:
#                 logger.info("Table '%s' already exists.", self._table)
#                 return

#             col_defs = _get_postgres_compatible_schema(sample_data.schema)
#             create_table_sql = f"CREATE TABLE {self._table} ({', '.join([f'"{col}" {pg_type}' for col, pg_type in col_defs])});"
#             logger.info("Creating table '%s' with schema: %s", self._table, create_table_sql)

#             try:
#                 await conn.execute(create_table_sql)
#                 logger.info("Table '%s' created successfully (async).", self._table)
#             except Exception as e:
#                 logger.exception("Failed to create table asynchronously: %s", e)
#                 raise            

#     def load_data(
#             self, 
#             path: str,
#             table_name: Optional[str] = None,
#             batch_size: Optional[int] = 10_000
#     ):
#         # this path could be that of a single parquet file
#         # or a whole directory containing many parquet files

#         self._table = self._table or table_name
        
#         # loading parquet file into the memory
#         _parquet_loader = _LoadParquetInMemory(path)
        
#         conn = None

#         try:
#             conn = psycopg2.connect(
#                 dbname=self._database,
#                 user=self._username,
#                 password=self.__password,
#                 host=self._url,
#                 port=self._port
#             )
#             cursor = conn.cursor()

#             _chunk_idx = 1

#             # generating chunks
#             for chunk in _parquet_loader.load_data():
#                 logger.debug("Chunk no. %s ", _chunk_idx)
#                 _columns = chunk.data.columns

#                 # when the first chunk, check the table
#                 if _chunk_idx == 1:
#                     self._ensure_table(conn, chunk.data.head(5))

#                 insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
#                     sql.Identifier(self._table),
#                     sql.SQL(", ").join(map(sql.Identifier, _columns)),
#                 )

#                 # Batched iteration
#                 for batch in chunk.data.iter_slices(batch_size):
#                     rows = list(batch.iter_rows())
#                     execute_values(cursor, insert_sql, rows, page_size=batch_size)
#                 _chunk_idx += 1

#             conn.commit()
        
#         except Exception as exc:
#             if conn:
#                 conn.rollback()
#             logger.exception("Failed to load data: %s", exc)
#             raise
        
#         finally:
#             if conn:
#                 conn.close()

#     async def aload_data(
#             self, 
#             path: str,
#             table_name: Optional[str] = None,
#             batch_size: Optional[int] = 10_000
#     ):
#         # this path could be that of a single parquet file
#         # or a whole directory containing many parquet files

#         if table_name:
#             self._table = table_name
                    
#         # loading parquet file into the memory
#         _parquet_loader = _LoadParquetInMemory(path)
        
#         try:
#             conn = await asyncpg.connect(
#                 user=self._username,
#                 password=self.__password,
#                 database=self._database,
#                 host=self._url,
#                 port=self._port,                
#             )
#             _chunk_idx = 1

#             # generating chunks
#             async for chunk in _parquet_loader.aload_data():
#                 logger.debug("Chunk no. %s ", _chunk_idx)
#                 if _chunk_idx == 1:
#                     # check the table
#                     await self._aensure_table(conn, chunk.data.head(5))

#                 _columns = chunk.data.columns
#                 column_list = ", ".join(f'"{col}"' for col in _columns)

#                 for batch in chunk.data.iter_slices(batch_size):
#                     rows = [tuple(row) for row in batch.iter_rows()]
#                     placeholders = ", ".join(f"${i+1}" for i in range(len(_columns)))
#                     query = f"INSERT INTO {self._table} ({column_list}) VALUES ({placeholders})"

#                     # batch insert manually
#                     await conn.executemany(query, rows)
                
#                 _chunk_idx += 1

#         except Exception as exc:
#             logger.exception("Ascync load failed: %s", exc)
#             raise
#         finally:
#             if conn:
#                 await conn.close()
class LoadToExcel(Load):
    """
    Saves a Polars DataFrame to disk as Parquet, Excel, or CSV.

    The output path is defined during initialization, so at runtime you only
    need to call `.run(df)` without specifying any path.
    """

    def __init__(
        self,
        output_path: str,
        save_type: Literal["parquet", "excel", "csv"] = "parquet",
        overwrite: bool = False,
    ):
        """
        Args:
            output_path: Full path (without extension) or directory path where data should be saved.
            save_type: One of 'parquet', 'excel', 'csv'.
            overwrite: Whether to overwrite an existing file.
        """
        self._save_type = save_type.lower()
        self._output_path = output_path
        self._overwrite = overwrite

        match self._save_type:
            case "excel":
                self._extension = ".xlsx"
            case "csv":
                self._extension = ".csv"
            case "parquet":
                self._extension = ".parquet"
            case _:
                raise ValueError(f"Unsupported save type: {save_type}")

        # ensure directory exists
        os.makedirs(os.path.dirname(self._output_path), exist_ok=True)

    def _get_dt_for_name(self) -> str:
        from datetime import datetime
        return datetime.now().strftime("%d-%m-%Y_%H-%M-%S")

    def _get_full_output_path(self) -> str:
        """Constructs the final file path with extension."""
        if not self._output_path.endswith(self._extension):
            full_path = self._output_path + self._extension
        else:
            full_path = self._output_path

        if os.path.exists(full_path) and not self._overwrite:
            logger.info(f"File already exists: {full_path}")
            full_path = f"{self._output_path}_{self._get_dt_for_name()}{self._extension}"
            
        return full_path

    def run(
        self,
        df: pl.DataFrame,
        batch_size: Optional[int] = None,
    ) -> str:
        """
        Saves the given DataFrame to the configured output path.
        """
        output_path = self._get_full_output_path()

        logger.debug(
            f"Saving DataFrame ({df.height} rows, {len(df.columns)} cols) "
            f"to {output_path} as {self._save_type.upper()}"
        )

        if self._save_type == "parquet":
            df.write_parquet(output_path)

        elif self._save_type == "csv":
            df.write_csv(output_path)

        elif self._save_type == "excel":
            # Polars doesn’t support Excel export natively → use pandas
            df.to_pandas().to_excel(output_path, index=False)

        logger.info(f"Data successfully saved at: {output_path}")
        return output_path

class LoadToPostgres(Load):
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
            return  

        # Infer schema from Polars DataFrame
        _col_defs = _get_postgres_compatible_schema(sample_data.schema) # List[Tuple[str, str]]

        # getting the list of strings
        col_defs = [
            f'"{name}" {dtype}'
            for name, dtype in _col_defs
        ]

        create_table_sql = f'CREATE TABLE "{self._table}" (\n  {",\n  ".join(col_defs)}\n);'
        logger.info("Creating table '%s' with schema: %s", self._table, create_table_sql)

        try:
            cur.execute(create_table_sql)
            conn.commit()
            logger.info("Table '%s' created successfully.", self._table)
        except Exception as e:
            conn.rollback()
            logger.exception("Failed to create table: %s", e)
            raise
        # finally:
        #     cur.close()
        #     conn.close()
            
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

    def run(
            self, 
            x: Union[pl.DataFrame | str] = None, # used when table is already stored somewhere
            table_name: Optional[str] = None,
            batch_size: Optional[int] = 10_000
    ):
        # this path could be that of a single parquet file
        # or a whole directory containing many parquet files

        self._table = self._table or table_name
        
        if type(x) is str:
            # loading parquet file into the memory
            _parquet_loader = _LoadParquetFromPath(x)
            
        elif type(x) is pl.DataFrame:
            _parquet_loader = _LoadParquetFromBuffer(x)
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

    async def arun(
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
        _parquet_loader = _LoadParquetFromPath(path)
        
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

class LoadToGCP(Load):
    """
    Loads a dataframe (or table path) into a GCP Cloud Storage bucket.
    Automatically converts DF to parquet before upload.
    """

    def __init__(self, bucket: str, destination_path: str, *, project: str = None):
        try:
            from google.cloud import storage
        except ImportError:
            logger.exception("google-cloud package is not installed.")
            raise ImportError("Please install google-cloud-storage using with - `pip install google-cloud-storage`")
        
        self.bucket_name = bucket
        self.destination_path = destination_path
        self.project = project
        self.client = storage.Client(project=self.project)
        self.bucket = self.client.bucket(self.bucket_name)

    def _df_to_parquet(self, df: pl.DataFrame) -> str:
        """Convert DF to temp parquet file before upload."""
        local_path = "/tmp/_gcp_upload.parquet"
        df.write_parquet(local_path)
        return local_path

    def _upload(self, local_path: str):
        """Upload the file to GCP Storage."""
        blob = self.bucket.blob(self.destination_path)
        blob.upload_from_filename(local_path)

    # Sync run()
    def run(self, df: pl.DataFrame, *args, **kwargs):
        local_path = self._df_to_parquet(df)
        self._upload(local_path)
        return {"status": "uploaded", "gcp_path": self.destination_path}

    # Async arun()
    async def arun(self, df: pl.DataFrame, *args, **kwargs):
        # GCP client isn't async, so we run in threadpool
        import asyncio

        local_path = self._df_to_parquet(df)

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._upload, local_path)

        return {"status": "uploaded", "gcp_path": self.destination_path}



  
