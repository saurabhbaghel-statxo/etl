import os
import asyncio
import glob
from typing import (
    Protocol, 
    List, 
    Dict, 
    Optional, 
    Tuple, 
    Any, 
    Callable,
    Literal,
    Iterable,
    DefaultDict,
    Set,
    Union
)
from enum import Enum, EnumMeta
from collections import defaultdict, deque
from functools import wraps
from types import FunctionType
import logging
from uuid import uuid4
from dataclasses import dataclass, field
import json

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
import pandas as pd
from datetime import datetime

from . import exceptions
from . import executable
from . import _utils


logger = logging.getLogger(__name__)


__all__ = [
    "TransformPipe",
    "FlattenDictColumn",
    "RenameColumns",
    "DropColumns",
    "ValueMapping",
    "FilterByUniqueGroupCount",
    "ExtractMonthNumber",
    "CastToDatetime",
    "ExtractDatePart",
    "ExtractHour",
    "ExtractDayOfWeek",
    "JoinDataFrames",
    "MapElements",
    "StringJoinTwoColumns",
    "SelectColumns"
]

__HASHES_INDEX__: Set[int] = set()

type Table = Union[pd.DataFrame, pl.DataFrame]
type Number = Union[int, float]

class Arity(Enum):
    '''Arity of the transformation.'''
    none = 0
    '''No operands. For Terminal Transform Nodes'''

    unary = 1
    '''Single Operand'''

    binary = 2
    '''Two Operands'''

    ternary = 3
    '''Three Operands'''


class MetaEnum(EnumMeta):
    def __contains__(cls, item):
        return item in cls.__members__.keys()

class BaseEnum(Enum, metaclass=MetaEnum):
    pass

class Status(Enum):
    READY = 1
    '''Initial state of the node'''

    WAITING = 2
    '''State when the node is queued to be processed'''

    PROCESSED = 3
    '''State when the node is completely processed'''
    
    def __add__(self, other: int):
        """
        Adds an integer to the state to return the state after the other (int) + state.
        
        Example
        -------
        ```python    
        state = State.READY

        # when the state needs to be bumped up
        # we can add 1 to the state
        state += 1

        print(state)    # <State.WAITING:2>

        ```

        """
        members = list(self.__class__)
        new_index = (members.index(self) + other) % len(members)
        return members[new_index]
    
# Different statuses of the node
statuses = [
    "READY",    # initial state of the node
    "WAITING",  # state when the node is queued to be processed
    "PROCESSED" # state when it is processed
]


def flatten_dictionary(unflattened_dict: dict, basekey=None) -> dict:
    '''Flattens a dictionary whose values may be list or dictionaries themselves'''

    if basekey: 
        unflattened_dict = {f"{basekey}_{key}": value for key, value in unflattened_dict.items()}

    curr_dict = unflattened_dict.copy()
        
    for key, value in unflattened_dict.items():
        if type(value) is dict:
            # it is a dictionary itself
            # flatten it
            value = curr_dict.pop(key)
            curr_dict.update(flatten_dictionary(value, basekey=key))

        if type(value) is list:
            value = curr_dict.pop(key)
            for idx, v in enumerate(value):
                curr_dict.update(flatten_dictionary(v, basekey=f"{key}_{idx}"))

    return curr_dict

def _get_pl_dtype(value):
    """Gives the polars compatible datatypes"""
    _m = {
        str: pl.String,
        int: pl.Int32,
        float: pl.Float32,
        datetime: pl.Datetime,
        bool: pl.Boolean,
        list: pl.List,
        dict: pl.Struct
    }
    return _m[type(value)]


# class Transform(Protocol):

#     async def atransform_data(self, *args, **kwargs): ...

#     def transform_data(self, *args, **kwargs) -> pl.DataFrame: ...

#     @property
#     def next_transform(self) -> Optional["Transform"]: ...
    
#     @property
#     def prev_transform(self) -> Optional["Transform"]: ...
    
#     @next_transform.setter
#     def next_transform(self, transform: "Transform") -> None: ...
    
#     @prev_transform.setter
#     def prev_transform(self, transform: "Transform") -> None: ...


# class TransformPipe:
#     """These Transforms form a graph, where one transform is connected to another."""
#     @staticmethod
#     def from_list_of_transforms(transforms: List[Transform]) -> "TransformPipe":
#         return TransformPipe(transforms)

#     def __init__(
#             self, 
#             df: pl.DataFrame | str,
#             transforms: Optional[List[Transform]] = [],
#             init_transform: Optional[Transform] = None,
#             save_as: Literal["parquet_table"] = "parquet_table",
#             table_path: Optional[str] = None,
#             write_chunk_size: Optional[int] = None
#         ):
#         if type(df) is str:
#             try:
#                 self._df = self._read_chunks_from_dir(df)
#                 self._df_exists_at_init = True

#             except:
#                 # flag indicating that the df does not
#                 # exist yet, but will exist after intermediate
#                 # tasks
#                 self._df_exists_at_init = False
#                 # as the file is expected to be at the 
#                 # given path after the intermediate jobs
#                 self._df_to_be_expected_at = df
#         else:
#             self._df = df

#         if save_as == "parquet_table":
#             assert table_path, "When saving as a parquet table, provide table path"
#             self._write_chunk_size = write_chunk_size or 50_000
#             self._table_path = table_path
        
#         if transforms:
#             self._all_transforms_: List[Transform] = transforms
#             self._first_transform = self._all_transforms_[0]
#             # Makes a graph from the list of unconnected transforms
#             self._make_graph_from_list()
            
#         elif init_transform:
#             self._first_transform = init_transform
#             # Makes a graph from the already connected transforms

#     def _make_graph(self):
#         """Makes graph of the given """
#         pass

#     def _make_graph_from_list(self):
#         if not self._all_transforms_:
#             logger.error("No Transforms available")
#             exit(0) # exit the code
        
#         self._first_transform = _transform = self._all_transforms_[0]
#         i: int = 1
#         logger.info("Total Transformations=%s", len(self._all_transforms_))
        
#         logger.info("%s: %s - %s", i, _transform.__class__.__name__, _transform.__class__.__doc__.strip("\n"))
#         while i <= len(self._all_transforms_) - 1:
#             _transform.next_transform = self._all_transforms_[i]
#             _transform = _transform.next_transform
#             logger.info("%s: %s - %s", i+1, _transform.__class__.__name__, _transform.__class__.__doc__.strip("\n"))
#             i += 1
        
#     def _read_chunks_from_dir(self, dir: str):
#         table = pq.read_table(dir)
#         table = table.combine_chunks()
#         return pl.from_arrow(table)
    
#     def transform_data(self) -> pl.DataFrame:
#         # start with the first transform
#         # _transform = self._all_transforms_[0]
#         if not self._df_exists_at_init:
#             # try again
#             try:
#                 self._df = self._read_chunks_from_dir(self._df_to_be_expected_at)
#                 logger.info("Table loaded for transformation=%s", self._df_to_be_expected_at)
#             except Exception as exc:
#                 logger.exception("Table is not found at=%s", self._df_to_be_expected_at)
#                 raise

#         _transform = self._first_transform
#         while _transform:
#             logger.info("Transform=%s", _transform.__class__.__name__, exc_info=True)
#             self._df = _transform.transform_data(self._df)
#             _transform = _transform.next_transform
        
#         if self._table_path:
#             os.makedirs(self._table_path, exist_ok=True)
#             total_rows = self._df.height
#             logger.info("Saving transformed data in %s dir in chunks of %s rows", self._table_path, self._write_chunk_size)

#             _table_name = os.path.basename(self._table_path)

#             for start in range(0, total_rows, self._write_chunk_size):
#                 end = min(start + self._write_chunk_size, total_rows)
#                 chunk = self._df[start:end]
#                 chunk_file = os.path.join(self._table_path, f"{_table_name}_chunk_{start}_{end}.parquet")
#                 chunk.write_parquet(chunk_file)
#                 logger.debug("Saved chunk: %s rows -> %s", end-start, chunk_file)

#         return self._df


# class Linkable:
#     def __init__(
#             self, 
#             next: Transform | None = None, 
#             prev: Transform | None = None
#         ):
#         self._next = next
#         self._prev = prev

#     @property
#     def next(self):
#         return self._next

#     @property
#     def prev(self):
#         return self._prev
    
#     @next.setter
#     def next(self, transform: Transform):
#         self._next = transform

#     @prev.setter
#     def prev(self, transform: Transform):
#         self._prev = transform


# class RenameColumns:
#     """
#     Renames the columns with the given names.
#     """
#     @staticmethod
#     def rename_columns(columns_mapping: Dict[str, str]):
#         return RenameColumns(columns_mapping=columns_mapping)

#     def __init__(
#             self,
#             columns_mapping: Dict[str, str] 
#     ):
#         self._columns_mapping = columns_mapping
#         self._linkable = Linkable()

#     async def transform_data(self, df: pl.DataFrame):
#         df = df.rename(self._columns_mapping)
#         return df
    
#     def transform_data(self, df: pl.DataFrame):
#         df = df.rename(self._columns_mapping)
#         return df
    
#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next
    
#     @property
#     def prev_transform(self) -> Optional[Transform]: 
#         return self._linkable.prev
    
#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None: 
#         self._linkable.next = transform
    
#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None: 
#         self._linkable.prev = transform
    

# class DropColumns:
#     """
#     Drops list of columns from the dataframe.
#     """
#     @staticmethod
#     def drop_columns(df: pl.DataFrame):
#         return DropColumns(df)

#     def __init__(
#             self, 
#             columns: List[str]
#     ):
#         """

#         Parameters
#         ----------
#         columns : List[str]
#             List of columns to be dropped/deleted
#         """
#         self._columns = columns
#         self._linkable = Linkable()

#     async def atransform_data(self, df: pl.DataFrame):
#         df = df.drop(self._columns)
#         return df

#     def transform_data(self, df: pl.DataFrame):
#         df = df.drop(self._columns)
#         return df
    
#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next
    
#     @property
#     def prev_transform(self) -> Optional[Transform]: 
#         return self._linkable.prev
    
#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None: 
#         self._linkable.next = transform
    
#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None: 
#         self._linkable.prev = transform
    

# class ValueMapping:
#     """Maps values of the columns to some other values.
#     Typically used in bucketing values."""
#     @staticmethod
#     def value_mapping(df: pl.DataFrame):
#         return ValueMapping(df)

#     def __init__(
#             self, 
#             column: str,
#             mapping: dict,
#             default: Any,
#             alias: str
#     ):
#         # self._df = dataframe
#         self._column = column
#         self._mapping = mapping
#         self._default = default
#         self._alias = alias
#         self._linkable = Linkable()

#     async def atransform_data(
#             self, 
#             df: pl.DataFrame
#     ):
#         df = df.with_columns(
#             pl.col(self._column).map_dict(self._mapping, dafault=self._default).alias(self._alias)
#             # I think it should be map_element()
#         )
#         return df
    
#     def transform_data(
#             self, 
#             df: pl.DataFrame
#     ):
#         df = df.with_columns(
#             pl.col(self._column).map_dict(self._mapping, dafault=self._default).alias(self._alias)
#             # I think it should be map_element()
#         )
#         return df
    
#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next
    
#     @property
#     def prev_transform(self) -> Optional[Transform]: 
#         return self._linkable.prev
    
#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None: 
#         self._linkable.next = transform
    
#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None: 
#         self._linkable.prev = transform


# class FlattenDictColumn:
#     """Flattens Dictionary values in a column."""

#     @staticmethod
#     def flatten_dict_column(df: pl.DataFrame):
#         return FlattenDictColumn(df)
    
#     def __init__(
#             self, 
#             column: str,
#             schema: Dict,
#             columns: Optional[List[str]] = None,
#         ):
#         """
#         Flattens and expands the column containing dictionary values.


#         Parameters
#         ----------
#         column : str
#             _description_
#         schema : Dict
#             _description_, by default None
#         columns : Optional[List[str]], optional
#             _description_, by default None
#         """
#         self._column = column
#         self._schema = schema
#         self._columns = columns
#         self._linkable = Linkable()

#     async def atransform_data(self, df: pl.DataFrame):
#         if df[self._column].dtype.is_(pl.String):
#             # first convert this into a Struct column
#             df = df.with_columns(
#                 pl.col("data").str.json_decode()
#             )

#         if self._schema:
#             all_keys = flatten_dictionary(self._schema)
        
#         elif self._columns:
#             all_keys = self._columns
        
#         df = df.with_columns(
#             pl.col(self._column)
#             .struct
#             .rename_fields(all_keys)
#         ).unnest(self._column)
#         return df
    
#     def transform_data(self, df: pl.DataFrame):
#         if df[self._column].dtype.is_(pl.String):
#             _dtype = pl.Struct(
#                 [
#                     pl.Field(field, _get_pl_dtype(value))
#                     for field, value in self._schema.items()
#                 ]
#             )
#             # first convert this into a Struct column
#             df = df.with_columns(
#                 pl.col("data").str.json_decode(dtype=_dtype)
#             )

#         if self._schema:
#             all_keys = list(flatten_dictionary(self._schema).keys())
#             # print(all_keys)
#             logger.debug("All Keys=%s", all_keys)

#         # elif self._columns:
#         #     all_keys = self._columns
        
#         df = df.with_columns(
#             pl.col(self._column)
#             .struct
#             .rename_fields(all_keys)
#         ).unnest(self._column)
#         return df
    
#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next
    
#     @property
#     def prev_transform(self) -> Optional[Transform]: 
#         return self._linkable.prev
    
#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None: 
#         self._linkable.next = transform
    
#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None: 
#         self._linkable.prev = transform
    

# class FilterByUniqueGroupCount:
#     """
#     Keeps rows where `id_column` is associated with a single unique value of `group_column`.
#     After filtering, aggregates specified metrics by (id_column, group_column, optional status columns).
#     """

#     def __init__(
#         self,
#         id_column: str,
#         group_column: str,
#         status_columns: list[str] | None = None,
#         agg_columns: dict[str, str] | None = None,
#     ):
#         """
#         Parameters
#         ----------
#         id_column : str
#             Column used as the main identifier.
#         group_column : str
#             Column checked for uniqueness.
#         status_columns : list[str], optional
#             Extra columns to preserve in the grouping.
#         agg_columns : dict[str, str], optional
#             Mapping of column -> aggregation function ("sum", "mean", etc.).
#         """
#         self._id_column = id_column
#         self._group_column = group_column
#         self._status_columns = status_columns or []
#         self._agg_columns = agg_columns or {}
#         self._linkable = Linkable()

#     async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         # 1: find IDs with exactly one unique group value
#         valid_ids = (
#             df.select([self._id_column, self._group_column])
#               .unique()
#               .group_by(self._id_column)
#               .agg(pl.col(self._group_column).n_unique().alias("groupCount"))
#               .filter(pl.col("groupCount") == 1)[self._id_column]
#               .to_list()
#         )

#         # 2: filter df
#         df = df.filter(pl.col(self._id_column).is_in(valid_ids))

#         # 3: group and aggregate
#         group_keys = [self._id_column, self._group_column] + self._status_columns
#         aggregations = []
#         for col, agg_func in self._agg_columns.items():
#             if agg_func == "sum":
#                 aggregations.append(pl.col(col).sum())
#             elif agg_func == "mean":
#                 aggregations.append(pl.col(col).mean())
#             elif agg_func == "max":
#                 aggregations.append(pl.col(col).max())
#             elif agg_func == "min":
#                 aggregations.append(pl.col(col).min())
#             else:
#                 raise ValueError(f"Unsupported aggregation: {agg_func}")

#         df = df.unique().group_by(group_keys).agg(aggregations)
#         return df
    
#     def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         # 1: find IDs with exactly one unique group value
#         valid_ids = (
#             df.select([self._id_column, self._group_column])
#               .unique()
#               .group_by(self._id_column)
#               .agg(pl.col(self._group_column).n_unique().alias("groupCount"))
#               .filter(pl.col("groupCount") == 1)[self._id_column]
#               .to_list()
#         )

#         # 2: filter df
#         df = df.filter(pl.col(self._id_column).is_in(valid_ids))

#         # 3: group and aggregate
#         group_keys = [self._id_column, self._group_column] + self._status_columns
#         aggregations = []
#         for col, agg_func in self._agg_columns.items():
#             if agg_func == "sum":
#                 aggregations.append(pl.col(col).sum())
#             elif agg_func == "mean":
#                 aggregations.append(pl.col(col).mean())
#             elif agg_func == "max":
#                 aggregations.append(pl.col(col).max())
#             elif agg_func == "min":
#                 aggregations.append(pl.col(col).min())
#             else:
#                 raise ValueError(f"Unsupported aggregation: {agg_func}")

#         df = df.unique().group_by(group_keys).agg(aggregations)
#         return df

#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next

#     @property
#     def prev_transform(self) -> Optional[Transform]:
#         return self._linkable.prev

#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None:
#         self._linkable.next = transform

#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None:
#         self._linkable.prev = transform


# class ExtractMonthNumber:
#     """
#     Extracts month number from a datetime column.
#     """

#     def __init__(self, column: str, alias: str | None = None):
#         self._column = column
#         self._alias = alias or f"{column}MonthNum"
#         self._linkable = Linkable()

#     async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         return df.with_columns(
#             pl.col(self._column).dt.month().alias(self._alias)
#         )

#     def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         return df.with_columns(
#             pl.col(self._column).dt.month().alias(self._alias)
#         )

#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next

#     @property
#     def prev_transform(self) -> Optional[Transform]:
#         return self._linkable.prev

#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None:
#         self._linkable.next = transform

#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None:
#         self._linkable.prev = transform


# class CastToDatetime:
#     """
#     Casts a string column into a proper Datetime type.
#     """

#     def __init__(self, column: str, date_format: str | None = None, alias: str | None = None):
#         self._column = column
#         self._date_format = date_format
#         self._alias = alias or column
#         self._linkable = Linkable()

#     async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         return df.with_columns(
#             pl.col(self._column).str.strptime(pl.Datetime, format=self._date_format).alias(self._alias)
#         )
    

#     def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         return df.with_columns(
#             pl.col(self._column).str.strptime(pl.Datetime, format=self._date_format).alias(self._alias)
#         )

#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next

#     @property
#     def prev_transform(self) -> Optional[Transform]:
#         return self._linkable.prev

#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None:
#         self._linkable.next = transform

#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None:
#         self._linkable.prev = transform


# class ExtractDatePart:
#     """
#     Extracts the date (YYYY-MM-DD) from a datetime column.
#     """

#     def __init__(self, column: str, alias: str | None = None):
#         self._column = column
#         self._alias = alias or f"{column}Date"
#         self._linkable = Linkable()

#     async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         return df.with_columns(
#             pl.col(self._column).dt.date().alias(self._alias)
#         )

#     def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         return df.with_columns(
#             pl.col(self._column).dt.date().alias(self._alias)
#         )

#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next

#     @property
#     def prev_transform(self) -> Optional[Transform]:
#         return self._linkable.prev

#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None:
#         self._linkable.next = transform

#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None:
#         self._linkable.prev = transform


# class ExtractHour:
#     """
#     Extracts the hour (0–23) from a datetime column.
#     """

#     def __init__(self, column: str, alias: str | None = None):
#         self._column = column
#         self._alias = alias or f"{column}Hour"
#         self._linkable = Linkable()

#     async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         return df.with_columns(
#             pl.col(self._column).dt.hour().alias(self._alias)
#         )
    
#     def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         return df.with_columns(
#             pl.col(self._column).dt.hour().alias(self._alias)
#         )

#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next

#     @property
#     def prev_transform(self) -> Optional[Transform]:
#         return self._linkable.prev

#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None:
#         self._linkable.next = transform

#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None:
#         self._linkable.prev = transform


# class ExtractDayOfWeek:
#     """
#     Extracts day of the week from a datetime column.
#     Can return either day name (e.g., "Monday") or sort order (0 = Monday, ...).
#     """

#     def __init__(self, column: str, as_string: bool = True, alias: str | None = None):
#         self._column = column
#         self._as_string = as_string
#         if alias:
#             self._alias = alias
#         else:
#             self._alias = f"{column}Day" if as_string else f"{column}DaySortOrder"
#         self._linkable = Linkable()

#     async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         if self._as_string:
#             return df.with_columns(
#                 pl.col(self._column).dt.strftime("%A").alias(self._alias)
#             )
#         else:
#             return df.with_columns(
#                 pl.col(self._column).dt.weekday().alias(self._alias)
#             )
        
#     def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         if self._as_string:
#             return df.with_columns(
#                 pl.col(self._column).dt.strftime("%A").alias(self._alias)
#             )
#         else:
#             return df.with_columns(
#                 pl.col(self._column).dt.weekday().alias(self._alias)
#             )

#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next

#     @property
#     def prev_transform(self) -> Optional[Transform]:
#         return self._linkable.prev

#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None:
#         self._linkable.next = transform

#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None:
#         self._linkable.prev = transform


# class JoinDataFrames:
#     """
#     Performs a join between two DataFrames on given keys.

#     Parameters
#     ----------
#     right_df : pl.DataFrame
#         The right DataFrame to join with.
#     on : str | list[str]
#         Column(s) to join on.
#     how : str, default "inner"
#         Join type: "inner", "left", "outer", "semi", "anti", "cross".
#     """

#     def __init__(self, on: str | list[str], how: str = "inner"):
#         self._on = on
#         self._how = how
#         self._linkable = Linkable()

#     async def atransform_data(self, right_df: pl.DataFrame, left_df: pl.DataFrame) -> pl.DataFrame:
#         return right_df.join(left_df, on=self._on, how=self._how)
    
#     def transform_data(self, right_df: pl.DataFrame, left_df: pl.DataFrame) -> pl.DataFrame:
#         return right_df.join(left_df, on=self._on, how=self._how)

#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next

#     @property
#     def prev_transform(self) -> Optional[Transform]:
#         return self._linkable.prev

#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None:
#         self._linkable.next = transform

#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None:
#         self._linkable.prev = transform


# class MapElements:
#     """
#     Maps each row of a column to the given function.
#     """

#     def __init__(
#             self, 
#             column: str, 
#             func: Callable, 
#             return_dtype: Optional[Literal["int", "str"]] = None,
#             alias: str | None = None
#     ):
#         self._column = column
#         self._func = func
#         if return_dtype:
#             if return_dtype == "int":
#                 self._return_dtype = pl.Int64
#             elif return_dtype == "str":
#                 self._return_dtype = pl.Utf8
#         if alias:
#             self._alias = alias
#         self._linkable = Linkable()

#     async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         if self._return_dtype:
#             return df.with_columns(
#                 pl.col(self._column)
#                 .map_elements(self._func, return_dtype=self._return_dtype)
#                 .alias(self._alias)
#             )
#         else: 
#             return df.with_columns(
#                 pl.col(self._column)
#                 .map_elements(self._func)
#                 .alias(self._alias)
#             )

#     def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         if self._return_dtype:
#             return df.with_columns(
#                 pl.col(self._column)
#                 .map_elements(self._func, return_dtype=self._return_dtype)
#                 .alias(self._alias)
#             )
#         else: 
#             return df.with_columns(
#                 pl.col(self._column)
#                 .map_elements(self._func)
#                 .alias(self._alias)
#             )

#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next

#     @property
#     def prev_transform(self) -> Optional[Transform]:
#         return self._linkable.prev

#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None:
#         self._linkable.next = transform

#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None:
#         self._linkable.prev = transform


# class StringJoinTwoColumns:
#     """
#     String Concatenates two string Columns
#     """

#     def __init__(
#             self, 
#             left_column: str,
#             right_column: str, 
#             joined_by: str, # string which comes in middle 
#             alias: str | None = None
#     ):
#         self._left_column = left_column
#         self._right_column = right_column
#         self._joined_by = joined_by
#         if alias:
#             self._alias = alias
#         self._linkable = Linkable()

#     async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         if self._alias:
#             return df.with_columns(
#                 (pl.col(self._left_column) 
#                  + self._joined_by 
#                  + pl.col(self._right_column)).alias(self._alias)
#             )
#         else:
#             return df.with_columns(
#                 (pl.col(self._left_column) 
#                  + self._joined_by 
#                  + pl.col(self._right_column))
#             )

#     def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         if self._alias:
#             return df.with_columns(
#                 (pl.col(self._left_column) 
#                  + self._joined_by 
#                  + pl.col(self._right_column)).alias(self._alias)
#             )
#         else:
#             return df.with_columns(
#                 (pl.col(self._left_column) 
#                  + self._joined_by 
#                  + pl.col(self._right_column))
#             )
#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next

#     @property
#     def prev_transform(self) -> Optional[Transform]:
#         return self._linkable.prev

#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None:
#         self._linkable.next = transform

#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None:
#         self._linkable.prev = transform


# class SelectColumns:
#     """
#     Returns the selected columns in the table
#     """

#     def __init__(
#             self, 
#             columns: List[str], 
#             alias: str | None = None
#     ):
#         self._columns = columns

#         if alias:
#             self._alias = alias
#         self._linkable = Linkable()

#     async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         return df.select(self._columns)
    

#     def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         return df.select(self._columns)

#     @property
#     def next_transform(self) -> Optional[Transform]:
#         return self._linkable.next

#     @property
#     def prev_transform(self) -> Optional[Transform]:
#         return self._linkable.prev

#     @next_transform.setter
#     def next_transform(self, transform: Transform) -> None:
#         self._linkable.next = transform

#     @prev_transform.setter
#     def prev_transform(self, transform: Transform) -> None:
#         self._linkable.prev = transform


# class ConditionalColumnTransform:
#     """
#     Creates a new column based on multiple conditions using Polars when-then-otherwise logic.
    
#     This transform allows you to create conditional columns similar to SQL CASE WHEN statements.
#     """
    
#     def __init__(
#             self,
#             conditions: List[pl.Expr],
#             then_values: List[Any],
#             otherwise_value: Any = None,
#             alias: str = "conditional_column"
#     ):
#         """
#         Initialize the conditional column transform.
        
#         Parameters
#         ----------
#         conditions : List[pl.Expr]
#             List of Polars expressions representing conditions to evaluate
#         then_values : List[Any]
#             List of values/expressions to return when each condition is True
#         otherwise_value : Any, optional
#             Value to return when no conditions match, by default None
#         alias : str, optional
#             Name for the new column, by default "conditional_column"
            
#         Examples
#         --------
#         ```
#         transform = ConditionalColumnTransform(
#             conditions=[pl.col("age") > 18],
#             then_values=["Adult"],
#             otherwise_value="Minor",
#             alias="age_group"
#         )
#         ```

        

#         ```
#         transform = ConditionalColumnTransform(
#             conditions=[
#                 pl.col("score") >= 90,
#                 pl.col("score") >= 80,
#                 pl.col("score") >= 70
#             ],
#             then_values=["A", "B", "C"],
#             otherwise_value="F",
#             alias="grade"
#         )
#         ```
#         """
#         if len(conditions) != len(then_values):
#             raise ValueError("conditions and then_values must have the same length")
        
#         self._conditions = conditions
#         self._then_values = then_values
#         self._otherwise_value = otherwise_value
#         self._alias = alias
#         self._linkable = Linkable()
    
#     def _build_conditional_expression(self) -> pl.Expr:
#         """Build the chained when-then-otherwise expression."""
#         if not self._conditions:
#             raise ValueError("At least one condition must be provided")
        
#         # Start with the first condition
#         expr = pl.when(self._conditions[0]).then(self._then_values[0])
        
#         # Chain additional when-then clauses
#         for condition, value in zip(self._conditions[1:], self._then_values[1:]):
#             expr = expr.when(condition).then(value)
        
#         # Add otherwise clause
#         expr = expr.otherwise(self._otherwise_value)
        
#         return expr.alias(self._alias)
    
#     async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         """Asynchronously transform the dataframe."""
#         expr = self._build_conditional_expression()
#         return df.with_columns(expr)
    
#     def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
#         """Transform the dataframe."""
#         expr = self._build_conditional_expression()
#         return df.with_columns(expr)
    
#     @property
#     def next_transform(self) -> Optional["Transform"]:
#         return self._linkable.next
    
#     @property
#     def prev_transform(self) -> Optional["Transform"]:
#         return self._linkable.prev
    
#     @next_transform.setter
#     def next_transform(self, transform: "Transform") -> None:
#         self._linkable.next = transform
    
#     @prev_transform.setter
#     def prev_transform(self, transform: "Transform") -> None:
#         self._linkable.prev = transform


# class TransformNode:
#     def __init__(
#             self,
#             transform: Transform,
#             input: Optional[Table] = None,
#             is_entry_point: bool = False,
#             *args, **kwargs
#     ):
        
#         self._transform: Transform = transform
#         '''Actual Transformation functionality'''

#         self._is_entry_point: bool = is_entry_point
#         '''Tells whether the node is an entry point to the subsequent graph'''

#         self._input: Optional[Table] = input
#         # NOTE: Single input for a single node
#         # TODO: Might have multiple inputs for a single node, 
#         # better to have a tuple of tables
#         '''Actual Table which will be transformed'''

#         self._status: Status = Status.READY
#         '''Tells the status of the Node. Whether it is to be invoked, is in progress, failed or was successful.'''

#         self._next_nodes: List["TransformNode"] = [] 
#         # NOTE: Deprecated
#         '''A list of all the nodes which come next to the current node'''

#         self._name = kwargs.get("name", self._generate_name())
#         '''A unique name of the Node'''

#         self._arity = kwargs.get("arity", None)
#         '''Arity of the Transform'''

#         self.output: Optional[Table] = None
#         '''Actual output table after application of transformation'''

#     def _generate_name(self):
#         return f"{self._transform.__class__.__name__}_{uuid4().hex[:8]}"

#     @property
#     def is_entry_point(self) -> bool:
#         """Tells whether the node is an entry point"""
#         return self._is_entry_point

#     @is_entry_point.setter
#     def is_entry_point(self, val: bool):
#         self._is_entry_point = val

#     @property
#     def name(self) -> str:
#         """Symbolic name of the Node"""
#         return self._name

#     @property
#     def arity(self) -> Optional[Arity]:
#         return self._arity

#     @property
#     def next(self) -> List["TransformNode"]: ...
    
#     @property
#     def prev(self) -> List["TransformNode"]: ...
    
#     @next.setter
#     def next(self, node: "TransformNode"): ...

#     @prev.setter
#     def prev(self, node: "TransformNode"): ...

#     @property
#     def status(self) -> Status:
#         return self._status
    
#     def transform(self, x: Optional[Iterable[Table]], *args, **kwargs) -> Optional[Table]:
#         """Transforms table. 
#         Applies the transformations to the table. 
#         Ensure that the length of `x` is equal to the arity of the operation."""
#         if self.arity:
#             assert self.arity == len(x), f"Needs {self._arity} operands, given {len(x)}"

#         self.output = self._transform.transform_data(x)
#         return self.output
    
#     async def atransform(self, x: Optional[Iterable[Table]], *args, **kwargs) -> Optional[Table]:
#         """Asynchronously Transform the table"""
#         if self.arity:
#             assert self.arity == len(x), f"Needs {self._arity} operands, given {len(x)}"

#         self.output = await self._transform.atransform_data(x)
#         return self.output          

#     def __hash__(self) -> int:
#         if len(__HASHES_INDEX__) != 0:
#             return __HASHES_INDEX__[-1] + 1
#         __HASHES_INDEX__.add(0)
#         return 0

# class RenameColumnsNode(TransformNode):
#     def __init__(self, columns_mapping: Dict[str, str]):
#         self._transform = RenameColumns(columns_mapping)
#         self._name = "RenameColumns"
#         self._arity = Arity.UNARY

#     @property
#     def name(self) -> str:
#         return self._name
    
#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(df)


# class DropColumnsNode(TransformNode):
#     def __init__(self, columns: List[str]):
#         self._transform = DropColumns(columns)
#         self._name = "DropColumns"
#         self._arity = Arity.UNARY

#     @property
#     def name(self) -> str:
#         return self._name
    
#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(df)


# class ValueMappingNode(TransformNode):
#     def __init__(self, column: str, mapping: dict, default: Any, alias: str):
#         self._transform = ValueMapping(column, mapping, default, alias)
#         self._name = "ValueMapping"
#         self._arity = Arity.UNARY

#     @property
#     def name(self) -> str:
#         return self._name
    
#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(df)


# class FlattenDictColumnNode(TransformNode):
#     def __init__(
#             self, 
#             column: str, 
#             schema: Dict, 
#             columns: Optional[List[str]] = None,
#             is_entry_point: bool = False
#     ):
#         super().__init__(
#             transform=FlattenDictColumn(column=column, schema=schema, columns=columns),
#             is_entry_point=is_entry_point,
#             arity=Arity.unary
#         )

#     @property
#     def name(self) -> str:
#         return self._name
    
#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(df)


# class FilterByUniqueGroupCountNode(TransformNode):
#     def __init__(
#         self,
#         id_column: str,
#         group_column: str,
#         status_columns: list[str] | None = None,
#         agg_columns: dict[str, str] | None = None,
#     ):
#         self._transform = FilterByUniqueGroupCount(id_column, group_column, status_columns, agg_columns)
#         self._name = "FilterByUniqueGroupCount"
#         self._arity = Arity.UNARY

#     @property
#     def name(self) -> str:
#         return self._name
    
#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(df)


# class ExtractMonthNumberNode(TransformNode):
#     def __init__(self, column: str, alias: str | None = None):
#         self._transform = ExtractMonthNumber(column, alias)
#         self._name = "ExtractMonthNumber"
#         self._arity = Arity.UNARY

#     @property
#     def name(self) -> str:
#         return self._name

#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(df)


# class CastToDatetimeNode(TransformNode):
#     def __init__(self, column: str, date_format: str | None = None, alias: str | None = None):
#         self._transform = CastToDatetime(column, date_format, alias)
#         self._name = "CastToDatetime"
#         self._arity = Arity.UNARY

#     @property
#     def name(self) -> str:
#         return self._name

#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(df)


# class ExtractDatePartNode(TransformNode):
#     def __init__(self, column: str, alias: str | None = None):
#         self._transform = ExtractDatePart(column, alias)
#         self._name = "ExtractDatePart"
#         self._arity = Arity.UNARY

#     @property
#     def name(self) -> str:
#         return self._name

#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(df)


# class ExtractHourNode(TransformNode):
#     def __init__(self, column: str, alias: str | None = None):
#         self._transform = ExtractHour(column, alias)
#         self._name = "ExtractHour"
#         self._arity = Arity.UNARY

#     @property
#     def name(self) -> str:
#         return self._name

#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(df)


# class ExtractDayOfWeekNode(TransformNode):
#     def __init__(self, column: str, as_string: bool = True, alias: str | None = None):
#         self._transform = ExtractDayOfWeek(column, as_string, alias)
#         self._name = "ExtractDayOfWeek"
#         self._arity = Arity.UNARY

#     @property
#     def name(self) -> str:
#         return self._name

#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(df)


# class JoinDataFramesNode(TransformNode):
#     def __init__(self, on: str | list[str], how: str = "inner"):
#         self._transform = JoinDataFrames(on, how)
#         self._name = "JoinDataFrames"
#         self._arity = Arity.BINARY  # joins need 2 inputs

#     @property
#     def name(self) -> str:
#         return self._name

#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, left_df: pl.DataFrame, right_df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(left_df, right_df)


# class MapElementsNode(TransformNode):
#     def __init__(self, column: str, func: Callable, return_dtype: Optional[Literal["int", "str"]] = None, alias: str | None = None):
#         self._transform = MapElements(column, func, return_dtype, alias)
#         self._name = "MapElements"
#         self._arity = Arity.UNARY

#     @property
#     def name(self) -> str:
#         return self._name

#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(df)


# class StringJoinTwoColumnsNode(TransformNode):
#     def __init__(self, left_column: str, right_column: str, joined_by: str, alias: str | None = None):
#         self._transform = StringJoinTwoColumns(left_column, right_column, joined_by, alias)
#         self._name = "StringJoinTwoColumns"
#         self._arity = Arity.UNARY

#     @property
#     def name(self) -> str:
#         return self._name

#     @property
#     def arity(self) -> Arity:
#         return self._arity

#     def transform(self, df: pl.DataFrame) -> pl.DataFrame:
#         return self._transform.transform_data(df)


# class ConditionalColumnNode(TransformNode):
#     def __init__(
#             self,
#             conditions: List[pl.Expr],
#             then_values: List[Any],
#             otherwise_value: Any = None,
#             alias: str = "conditional_column",
#             is_entry_point: bool=False,
            
#     ):
#         super().__init__(
#             transform=ConditionalColumnTransform(
#                 conditions=conditions,
#                 then_values=then_values,
#                 otherwise_value=otherwise_value,
#                 alias=alias
#             ),
#             is_entry_point=is_entry_point,
#             arity = Arity.unary
#         )


# class SelectColumnsNode(TransformNode):
#     def __init__(
#         self,
#         columns: List[str], 
#         is_entry_point = False
#     ):
#         super().__init__(
#             transform=SelectColumns(columns=columns), 
#             is_entry_point=is_entry_point,
#             arity=Arity.unary 
#         )
    

# class TransformGraph:
#     def __init__(
#             self, 
#             edges: Optional[List[Tuple[TransformNode, "TransformGraph"]]] = None,
#             inputs: Optional[Tuple[Table]] = None
#     ):
#         """
#         Usage
#         ------

#         ```python
#         transform_graph = TransformGraph(
#                 edges = [
#                     (
#                         RenameColumns(*args, **kwargs),
#                         DeleteColumns(*args, **kwargs)
#                     )
#                 ]
#             )
        
#         ```
#         """  
#         # init adjacency
#         self._adjacency: Dict[TransformNode, List[TransformNode]] = defaultdict(list)

#         if edges:
#             for src, dest in edges:
#                 # if isinstance(dest, TransformNode):
#                 self._adjacency[src].append(dest)
        
#         self._inputs = inputs or ()

#         # --- all nodes ---
#         self._nodes: Set[TransformNode] = set(self._adjacency.keys()) | {
#             node for dests in self._adjacency.values() for node in dests
#         }

#         # --- entry points ---
#         self._entry_points = [
#             node for node in self._nodes 
#             if all(node not in dests for dests in self._adjacency.values())
#         ]

#     @property
#     def inputs(self) -> Optional[Tuple[Table]]:
#         return self._inputs
    
#     @inputs.setter
#     def inputs(self, x: Table):
#         self._inputs = (*self._inputs, x)   # appends x to the inputs

#     def _contains_cycle(self) -> bool:
#         visited, stack = set(), set()

#         def dfs(node):
#             if node in stack:
#                 return True
#             if node in visited:
#                 return False
#             stack.add(node)
            
#             for nxt in self._adjacency[node]:
#                 if dfs(nxt):
#                     return True
            
#             stack.remove(node)
#             visited.add(node)
#             return False
        
#         return any(dfs(n) for n in self._nodes)
    
#     def topological_order(self) -> List[TransformNode]:
#         # NOTE: this will create a queue of the transformations
        
#         # TODO: a single queue for a single stream of transforms 
#         # for one table.
#         # Different queues for different (independent) tables should run
#         # concurrently/parallely (whatever is optimized.)

#         # getting the indegrees of the nodes
#         self.indegree = {n: 0 for n in self._nodes}

#         for dests in self._adjacency.values():
#             # tells that the node is reached by other nodes
#             for d in dests:
#                 self.indegree[d] += 1
        
#         # making a queue for nodes which cannot be reached from other nodes
#         _q = [] # intermediary queue
#         for node, deg in self.indegree.items():
#             if deg == 0:
#                 # starting node
#                 # mark it as an entry point
#                 node.is_entry_point = True

#                 # add it to the intermediary queue
#                 _q.append(node)
#         queue = deque(_q)
        
#         order = []
#         while queue:
#             node = queue.popleft()
#             order.append(node)
#             for nxt in self._adjacency[node]:
#                 self.indegree[nxt] -= 1  
#                 # in a way, removing the edge (node -> nxt) 
#                 # from the graph

#                 if self.indegree[nxt] == 0:

#                     # if the indegree of the node is 0
#                     # this means that the node is not reached
#                     # by other nodes
#                     queue.append(nxt)

#         if len(order) != len(self._nodes):
#             raise ValueError("Cycle detected in graph!")
#         return order

#     def traverse(
#             self, 
#             _inputs: Tuple[Table], 
#             ordered_list_transforms: List[TransformNode]
#     ) -> Union[Table]:    
#         # NOTE: keeping return type as Table for now
        
#         # TODO: return type should be more general, not just table
        
#         # ensure we have ordered list of transformations
#         if not ordered_list_transforms:
#             raise ValueError("Order transformations first.")
        
#         ordered_queue = deque(ordered_list_transforms)
#         input_tables = deque(_inputs)    
#         transformed_tables_buffer = deque([])

#         while ordered_queue:
#             # _node <- ordered_queue.pop()
#             _transform = ordered_queue.popleft()
            
#             # --- input block ---
#             # get the number of inputs for the node
#             # equal to its indegree 
#             if self._indegree[_transform] == 0:
#                 # unreachable node
#                 # needs inputs as many as its arity
#                 x = [input_tables.popleft() for _ in range(_transform.arity)]

#             else:
#                 # reached by multiple nodes
#                 # get the output from adjacent nodes
#                 x = []
#                 for src, adj in self._adjacency.items():
#                     if _transform in adj:
#                         x.append(src.output)
#                         # delete the output from predecessor nodes
#                         del src.output  # TODO: Check this deletion works as expected or not  

#             # perform the transformation on the current input
#             result = _transform.transform(x)
            
            
#             # add the transformed table 
#             # to the transformed tables buffer
#             # transformed_tables_buffer.append(_transform.transform(x))
            

#             # when the input_tables queue gets empty
#             # it is then we should replace it with transformed_tables_buffer
#             # transformed tables will now become the input for subsequent transformations
#             # input_tables = transformed_tables_buffer

#         return result




            
                




#         # map the inputs to the entry points
#         # counter_entry_point: int = 0
#         # for _transform in queue:
#         #     _transform.status += 1  
#         #     # expecting the status to be READY
#         #     # its status becomes WAITING 
#         #     if _transform.is_entry_point:
#         #         x = _inputs[counter_entry_point]
#         #         counter_entry_point += 1
#         #     # it is not possible to have no entry points
#         #     x = _transform.transform([x])
#         #     _transform.status += 1  
#         #     # bump up the status again to reset status to READY
#         # return x

#     def transform(self):
#         """Traverses the graph and performs the transformations.
#         The order in which the transformations will be performed will be decided by topological sorting.
#         """
#         # checking existence of inputs
#         assert self._inputs, "Inputs not yet provided."
#         # ensure #(inputs) = #(entry points). One input for one entry point
#         assert (
#             len(self._entry_points) == len(self._inputs), 
#             f"Number of inputs expected={len(self._entry_points)}, provided={len(self._inputs)}"
#         )

#         # order the transformations
#         ordered_transformations = self.topological_order()

#         # traverse
#         return self.traverse(self._inputs, ordered_transformations)


class Transform(metaclass=executable._ExecutableMeta):
    _registry_ = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        name = kwargs.get("name", cls.__name__)
        
        # add a flag indicating if the transform could be applied to 
        # data chunk, this will be used to do transformations in concurrently
        # and apply the transform when the chunk is downloaded 
        # and waiting for next chunk to be downloaded
        cls._can_be_applied_to_chunk_ = True

        # wrap the run() of subclasses, since the input could be a dataframe
        # or a table path
        Transform._wrap_subclass_run(cls)

        logger.info(f"Registering {name} as an Transformations.")
        Transform._registry_[name] = cls
    
    @classmethod
    def get_transform(mcls, name: str):
        if name not in mcls._registry_:
            raise exceptions.FunctionalityNotFoundError(f"Transform {name} not found")
        return mcls._registry_.get(name)

    def _wrap_subclass_run(cls):
        # to wrap the run of subclass
        original_run = cls.run

        @wraps(original_run)
        def _extended_run(self, x: Union[pl.DataFrame, str, pd.DataFrame], *args, **kwargs):
            df = Transform._check_input(x)
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
    
class RenameColumns(Transform):
    """
    Renames the columns with the given names.
    """
    
    __name__ = "rename_columns"
    
    def __init__(
            self,
            columns_mapping: Dict[str, str] 
    ):
        self._columns_mapping = columns_mapping

    async def arun(self, df: pl.DataFrame):
        df = df.rename(self._columns_mapping)
        return df
    
    def run(self, df: pl.DataFrame):
        df = df.rename(self._columns_mapping)
        return df


class DropColumns(Transform):
    """
    Drops list of columns from the dataframe.
    """
    
    __name__ = "drop_columns"
    
    def __init__(self, columns: List[str]):
        """
        Parameters
        ----------
        columns : List[str]
            List of columns to be dropped/deleted
        """
        self._columns = columns

    async def arun(self, df: pl.DataFrame):
        return df.drop(self._columns)
    
    def run(self, df: pl.DataFrame):
        return df.drop(self._columns)


class ValueMapping(Transform):
    """Maps values of the columns to some other values.
    Typically used in bucketing values."""
    
    __name__ = "value_mapping"

    def __init__(
            self, 
            column: str,
            mapping: dict,
            default: Any,
            alias: str
    ):
        self._column = column
        self._mapping = mapping
        self._default = default
        self._alias = alias

    async def arun(self, df: pl.DataFrame):
        return df.with_columns(
            pl.col(self._column).map_dict(self._mapping, default=self._default).alias(self._alias)
        )
    
    def run(self, df: pl.DataFrame):
        return df.with_columns(
            pl.col(self._column).map_dict(self._mapping, default=self._default).alias(self._alias)
        )


class FlattenDictColumn(Transform):
    """Flattens Dictionary values in a column."""
    
    __name__ = "flatten_dict_column"
    
    def __init__(
            self, 
            column: str,
            schema: Optional[Dict] = None,
            columns: Optional[List[str]] = None,
        ):
        """
        Flattens and expands the column containing dictionary values.

        Parameters
        ----------
        column : str
            Column name containing dictionaries
        schema : Dict
            Schema for the dictionary, by default None
        columns : Optional[List[str]], optional
            List of column names, by default None
        """
        self._column = column
        self._schema = schema
        self._columns = columns

    def infer_schema(self, data: pl.DataFrame | pl.Struct | str | Dict) -> Dict[str, pl.DataType]:
        if isinstance(data, pl.DataFrame):
            return data.schema
        
        elif isinstance(data, pl.Struct):
            return data.to_schema()

        elif isinstance(data, str):
            # it is a json string
            _dict_flattened = _utils.basic_dict_string_flattener(data)
            return {col: _get_pl_dtype(val) for col, val in _dict_flattened.items()}

        elif isinstance(data, dict):
            _dict_flattened = _utils.flatten_dictionary(data)
            return {col: _get_pl_dtype(val) for col, val in _dict_flattened.items()}

        # return {key: _get_pl_dtype(val) for key, val in data_dict.items()}        

    async def arun(self, df: pl.DataFrame):
        # convert the data column to a Struct column if it was String
        if df[self._column].dtype.is_(pl.String):
            df = df.with_columns(
                pl.col(self._column).str.json_decode()
            )
        
        self._schema = self._schema or self.infer_schema(flatten_dictionary(df))

        if self._schema:
            all_keys = list(flatten_dictionary(self._schema).keys())
        elif self._columns:
            all_keys = self._columns
        
        return df.with_columns(
            pl.col(self._column).struct.rename_fields(all_keys)
        ).unnest(self._column)
    
    def run(self, df: pl.DataFrame):

        # self._schema_data_dict_ = self._schema or self.infer_schema(df[self._column][0])
        self._schema_data_dict_ = self.infer_schema(self._schema)

        # converting the data column from string dtype -> struct dtype
        if df[self._column].dtype.is_(pl.String):
            _dtype = pl.Struct(
                [
                    pl.Field(field, pl_datatype)  # field is column name
                    for field, pl_datatype in self._schema_data_dict_.items()
                ]
            )
            df = df.with_columns(
                pl.col(self._column).str.json_decode(dtype=_dtype)
            )

        logger.debug("Before Flattening table looks like= ", df.head(2))

        # update the schema again 
        # with the updated columns
        # self._schema_data_dict_ = self.infer_schema(df[self._column][0])

        if self._schema_data_dict_:
            _keys_for_flat_dict = list(flatten_dictionary(self._schema_data_dict_).keys())

        try:
            res = df.with_columns(
                pl.col(self._column).struct.rename_fields(_keys_for_flat_dict)
            ).unnest(self._column)

            logger.debug("Flattened Columns=%s", str(res.columns))
            
            return res
        
        except pl.exceptions.DuplicateError as exc:
            # this means that there is a conflict with the incoming 
            # columns from flattening and the columns already present

            logger.warning("Conflict in columns, renaming the old columns, %s", exc)
            common_keys = set(_keys_for_flat_dict).intersection(set(df.columns))
            _renaming_mapping = {ckey: f"{ckey}_old" for ckey in common_keys}
            df = df.rename(_renaming_mapping)
            logger.info("Renamed old keys which were conflicting with new: %s", str(_renaming_mapping))
            
            res = df.with_columns(
                pl.col(self._column).struct.rename_fields(_keys_for_flat_dict)
            ).unnest(self._column)

            logger.debug("Flattened Columns=%s", str(res.columns))

            return res
            


class FilterByUniqueGroupCount(Transform):
    """
    Keeps rows where `id_column` is associated with a single unique value of `group_column`.
    After filtering, aggregates specified metrics by (id_column, group_column, optional status columns).
    """
    _can_be_applied_to_chunk_ = False   # this transform cannot be applied to chunk
    __name__ = "filter_by_unique_group_count"

    def __init__(
        self,
        id_column: str,
        group_column: str,
        status_columns: list[str] | None = None,
        agg_columns: dict[str, str] | None = None,
    ):
        """
        Parameters
        ----------
        id_column : str
            Column used as the main identifier.
        group_column : str
            Column checked for uniqueness.
        status_columns : list[str], optional
            Extra columns to preserve in the grouping.
        agg_columns : dict[str, str], optional
            Mapping of column -> aggregation function ("sum", "mean", etc.).
        """
        self._id_column = id_column
        self._group_column = group_column
        self._status_columns = status_columns or []
        self._agg_columns = agg_columns or {}

    async def arun(self, df: pl.DataFrame) -> pl.DataFrame:
        return self._apply_transform(df)
    
    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        return self._apply_transform(df)
    
    def _apply_transform(self, df: pl.DataFrame) -> pl.DataFrame:
        valid_ids = (
            df.select([self._id_column, self._group_column])
              .unique()
              .group_by(self._id_column)
              .agg(pl.col(self._group_column).n_unique().alias("groupCount"))
              .filter(pl.col("groupCount") == 1)[self._id_column]
              .to_list()
        )

        df = df.filter(pl.col(self._id_column).is_in(valid_ids))

        group_keys = [self._id_column, self._group_column] + self._status_columns
        aggregations = []
        for col, agg_func in self._agg_columns.items():
            if agg_func == "sum":
                aggregations.append(pl.col(col).sum())
            elif agg_func == "mean":
                aggregations.append(pl.col(col).mean())
            elif agg_func == "max":
                aggregations.append(pl.col(col).max())
            elif agg_func == "min":
                aggregations.append(pl.col(col).min())
            else:
                raise ValueError(f"Unsupported aggregation: {agg_func}")

        return df.unique().group_by(group_keys).agg(aggregations)


class ExtractMonthNumber(Transform):
    """
    Extracts month number from a datetime column.
    """

    __name__ = "extract_month_number"

    def __init__(self, column: str, alias: str | None = None):
        self._column = column
        self._alias = alias or f"{column}MonthNum"

    async def arun(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).dt.month().alias(self._alias)
        )

    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).dt.month().alias(self._alias)
        )


class CastToDatetime(Transform):
    """
    Casts a string column into a proper Datetime type.
    """

    __name__ = "cast_to_datetime"

    def __init__(self, column: str, date_format: str | None = None, alias: str | None = None):
        self._column = column
        self._date_format = date_format
        self._alias = alias or column

    async def arun(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).str.strptime(pl.Datetime, format=self._date_format).alias(self._alias)
        )

    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).str.strptime(pl.Datetime, format=self._date_format).alias(self._alias)
        )


class ExtractDatePart(Transform):
    """
    Extracts the date (YYYY-MM-DD) from a datetime column.
    """

    __name__ = "extract_date_part"

    def __init__(self, column: str, alias: str | None = None):
        self._column = column
        self._alias = alias or f"{column}Date"

    async def arun(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).dt.date().alias(self._alias)
        )

    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).dt.date().alias(self._alias)
        )


class ExtractHour(Transform):
    """
    Extracts the hour (0–23) from a datetime column.
    """

    __name__ = "extract_hour"

    def __init__(self, column: str, alias: str | None = None):
        self._column = column
        self._alias = alias or f"{column}Hour"

    async def arun(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).dt.hour().alias(self._alias)
        )
    
    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).dt.hour().alias(self._alias)
        )


class ExtractDayOfWeek(Transform):
    """
    Extracts day of the week from a datetime column.
    Can return either day name (e.g., "Monday") or sort order (0 = Monday, ...).
    """

    __name__ = "extract_day_of_week"

    def __init__(self, column: str, as_string: bool = True, alias: str | None = None):
        self._column = column
        self._as_string = as_string
        if alias:
            self._alias = alias
        else:
            self._alias = f"{column}Day" if as_string else f"{column}DaySortOrder"

    async def arun(self, df: pl.DataFrame) -> pl.DataFrame:
        if self._as_string:
            return df.with_columns(
                pl.col(self._column).dt.strftime("%A").alias(self._alias)
            )
        else:
            return df.with_columns(
                pl.col(self._column).dt.weekday().alias(self._alias)
            )
        
    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        if self._as_string:
            return df.with_columns(
                pl.col(self._column).dt.strftime("%A").alias(self._alias)
            )
        else:
            return df.with_columns(
                pl.col(self._column).dt.weekday().alias(self._alias)
            )


class JoinDataFrames(Transform):
    
    _can_be_applied_to_chunk_ = False   # join cannot be applied to chunk
    __name__ = "join_dataframes"

    def __init__(self, on: str | list[str], how: str = "inner"):
        """
        Performs a join between two DataFrames on given keys.

        Parameters
        ----------
        on : str | list[str]
            Column(s) to join on.
        how : str, default "inner"
            Join type: "inner", "left", "outer", "semi", "anti", "cross".
        """

        self._on = on
        self._how = how

    async def arun(self, left_df: pl.DataFrame, right_df: pl.DataFrame) -> pl.DataFrame:
        return left_df.join(right_df, on=self._on, how=self._how)
    
    def run(self, left_df: pl.DataFrame, right_df: pl.DataFrame) -> pl.DataFrame:
        return left_df.join(right_df, on=self._on, how=self._how)


class MapElements(Transform):
    """
    Maps each row of a column to the given function.
    """

    __name__ = "map_elements"

    def __init__(
            self, 
            column: str, 
            func: Callable, 
            return_dtype: Optional[Literal["int", "str"]] = None,
            alias: str | None = None
    ):
        self._column = column
        self._func = func
        if return_dtype:
            if return_dtype == "int":
                self._return_dtype = pl.Int64
            elif return_dtype == "str":
                self._return_dtype = pl.Utf8
        else:
            self._return_dtype = None
        self._alias = alias or column

    async def arun(self, df: pl.DataFrame) -> pl.DataFrame:
        if self._return_dtype:
            return df.with_columns(
                pl.col(self._column)
                .map_elements(self._func, return_dtype=self._return_dtype)
                .alias(self._alias)
            )
        else: 
            return df.with_columns(
                pl.col(self._column)
                .map_elements(self._func)
                .alias(self._alias)
            )

    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        if self._return_dtype:
            return df.with_columns(
                pl.col(self._column)
                .map_elements(self._func, return_dtype=self._return_dtype)
                .alias(self._alias)
            )
        else: 
            return df.with_columns(
                pl.col(self._column)
                .map_elements(self._func)
                .alias(self._alias)
            )


class StringJoinTwoColumns(Transform):
    """
    String concatenates two string columns.
    """

    __name__ = "string_join_two_columns"

    def __init__(
            self, 
            left_column: str,
            right_column: str, 
            joined_by: str,
            alias: str | None = None
    ):
        self._left_column = left_column
        self._right_column = right_column
        self._joined_by = joined_by
        self._alias = alias or f"{left_column}_{right_column}"

    async def arun(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            (pl.col(self._left_column) 
             + self._joined_by 
             + pl.col(self._right_column)).alias(self._alias)
        )

    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            (pl.col(self._left_column) 
             + self._joined_by 
             + pl.col(self._right_column)).alias(self._alias)
        )


class SelectColumns(Transform):
    """
    Returns the selected columns in the table.
    """

    __name__ = "select_columns"

    def __init__(self, columns: List[str]):
        self._columns = columns

    async def arun(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.select(self._columns)

    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.select(self._columns)


class ConditionalColumnTransform(Transform):
    """
    Creates a new column based on multiple conditions using Polars when-then-otherwise logic.
    
    This transform allows you to create conditional columns similar to SQL CASE WHEN statements.
    """
    _can_be_applied_to_chunk_ = False   # sometime it can be applied, but for now, assume negative
    __name__ = "conditional_column_transform"

    def __init__(
            self,
            conditions: List[pl.Expr],
            then_values: List[Any],
            otherwise_value: Any = None,
            alias: str = "conditional_column"
    ):
        """
        Initialize the conditional column transform.
        
        Parameters
        ----------
        conditions : List[pl.Expr]
            List of Polars expressions representing conditions to evaluate
        then_values : List[Any]
            List of values/expressions to return when each condition is True
        otherwise_value : Any, optional
            Value to return when no conditions match, by default None
        alias : str, optional
            Name for the new column, by default "conditional_column"
            
        Examples
        --------
        ```
        transform = ConditionalColumnTransform(
            conditions=[pl.col("age") > 18],
            then_values=["Adult"],
            otherwise_value="Minor",
            alias="age_group"
        )
        ```

        ```
        transform = ConditionalColumnTransform(
            conditions=[
                pl.col("score") >= 90,
                pl.col("score") >= 80,
                pl.col("score") >= 70
            ],
            then_values=["A", "B", "C"],
            otherwise_value="F",
            alias="grade"
        )
        ```
        """
        if len(conditions) != len(then_values):
            raise ValueError("conditions and then_values must have the same length")
        
        self._conditions = conditions
        self._then_values = then_values
        self._otherwise_value = otherwise_value
        self._alias = alias
    
    def _build_conditional_expression(self) -> pl.Expr:
        """Build the chained when-then-otherwise expression."""
        if not self._conditions:
            raise ValueError("At least one condition must be provided")
        
        expr = pl.when(self._conditions[0]).then(self._then_values[0])
        
        for condition, value in zip(self._conditions[1:], self._then_values[1:]):
            expr = expr.when(condition).then(value)
        
        if self._otherwise_value:
            expr = expr.otherwise(self._otherwise_value)
        
        logger.debug("Conditional column transform expression=%s", str(expr))
        print(f"Conditional column transform expression={str(expr)}")
        return expr.alias(self._alias)
    
    async def arun(self, df: pl.DataFrame) -> pl.DataFrame:
        expr = self._build_conditional_expression()
        return df.with_columns(expr)
    
    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        expr = self._build_conditional_expression()
        return df.with_columns(expr)


# class TransformParquet:
#     def __init__(
#             self, 
#             file: str | None = None,
#             files_dir: str | None = None,
#             load_sequentially: bool = False
#         ):
#         self._file = file
#         self._files_dir = files_dir
#         self._load_sequentially = load_sequentially

#     async def _load(self):
#         if self._files_dir:
#             # load all the files in the directory
#             # files will be present chunk wise
#             assert (os.path.isdir(self._files_dir),
#                     "Path does not direct to a valid directory.")
            
#             # listing all the parquet files in the directory
#             all_parquet_files_in_dir = sorted(
#                 glob.glob(
#                     pathname=os.path.join(self._files_dir, "*.parquet")
#                 )
#             )

#             if len(all_parquet_files_in_dir) == 0:
#                 # no parquet files present in the directory
#                 logger.warning("No parquet files present in the directory.")
#                 return

#             for file in all_parquet_files_in_dir:
#                 _table_chunk = pq.read_table(file)

def flatten_dict_columns(column, schema, columns):
    return 
