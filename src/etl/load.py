import os
from typing import List, Tuple, Protocol

import polars as pl

def _get_postgres_compatible_schema(schema: pl.Schema):

    columns: List[Tuple] = []

    for colname, dtype in schema.items():
        if type(dtype) in (pl.Float64, pl.Float32):
            _type = "FLOAT"
        elif type(dtype) in (pl.Int16, pl.Int64, pl.Int32, pl.Int128, pl.Int8):
            _type = "INT"
        elif type(dtype) is pl.String:
            _type = "TEXT"
        elif type(dtype) in (pl.Date, pl.Datetime):
            _type = "DATE"
        elif type(dtype) is pl.Boolean:
            _type = "BOOLEAN"
        else:
            _type = None
        columns.append((colname, _type))
    return columns

class Load(Protocol):

    async def aload_data(self, *args, **kwargs): ...

    def load_data(self, *args, **kwargs): ...


class LoadToPostgres:
    def __init__(self):
        pass