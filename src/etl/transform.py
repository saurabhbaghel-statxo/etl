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
    Literal
)
import logging

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from datetime import datetime



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
    "JoinDataFrames"
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


class Transform(Protocol):

    async def atransform_data(self, *args, **kwargs): ...

    def transform_data(self, *args, **kwargs): ...

    @property
    def next_transform(self) -> Optional["Transform"]: ...
    
    @property
    def prev_transform(self) -> Optional["Transform"]: ...
    
    @next_transform.setter
    def next_transform(self, transform: "Transform") -> None: ...
    
    @prev_transform.setter
    def prev_transform(self, transform: "Transform") -> None: ...


class TransformPipe:
    """These Transforms form a graph, where one transform is connected to another."""
    @staticmethod
    def from_list_of_transforms(transforms: List[Transform]) -> "TransformPipe":
        return TransformPipe(transforms)

    def __init__(
            self, 
            transforms: Optional[List[Transform]] = [],
            init_transform: Optional[Transform] = None
        ):
        if transforms:
            self._all_transforms_: List[Transform] = transforms
            self._first_transform = self._all_transforms_[0]
            # Makes a graph from the list of unconnected transforms
            self._make_graph_from_list()
            
        elif init_transform:
            self._first_transform = init_transform
            # Makes a graph from the already connected transforms
    
    def _make_graph(self):
        """Makes graph of the given """
        pass

    def _make_graph_from_list(self):
        if not self._all_transforms_:
            logger.error("No Transforms available")
            exit(0) # exit the code
        
        self._first_transform = _transform = self._all_transforms_[0]
        i: int = 1
        logger.info("Total Transformations=%s", len(self._all_transforms_))
        
        logger.info("%s: %s - %s", i, _transform.__class__.__name__, _transform.__class__.__doc__.strip("\n"))
        while i <= len(self._all_transforms_) - 1:
            _transform.next_transform = self._all_transforms_[i]
            _transform = _transform.next_transform
            logger.info("%s: %s - %s", i+1, _transform.__class__.__name__, _transform.__class__.__doc__.strip("\n"))
            i += 1
        
    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        # start with the first transform
        # _transform = self._all_transforms_[0]
        _transform = self._first_transform
        while _transform:
            logger.info("Transform=%s", _transform.__class__.__name__, exc_info=True)
            df = _transform.transform_data(df)
            _transform = _transform.next_transform
        
        return df


class Linkable:
    def __init__(
            self, 
            next: Transform | None = None, 
            prev: Transform | None = None
        ):
        self._next = next
        self._prev = prev

    @property
    def next(self):
        return self._next

    @property
    def prev(self):
        return self._prev
    
    @next.setter
    def next(self, transform: Transform):
        self._next = transform

    @prev.setter
    def prev(self, transform: Transform):
        self._prev = transform


class RenameColumns:
    """
    Renames the columns with the given names.
    """
    @staticmethod
    def rename_columns(df: pl.DataFrame):
        return RenameColumns(df)

    def __init__(
            self,
            columns_mapping: Dict[str, str] 
    ):
        self._columns_mapping = columns_mapping
        self._linkable = Linkable()

    async def transform_data(self, df: pl.DataFrame):
        df = df.rename(self._columns_mapping)
        return df
    
    def transform_data(self, df: pl.DataFrame):
        df = df.rename(self._columns_mapping)
        return df
    
    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next
    
    @property
    def prev_transform(self) -> Optional[Transform]: 
        return self._linkable.prev
    
    @next_transform.setter
    def next_transform(self, transform: Transform) -> None: 
        self._linkable.next = transform
    
    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None: 
        self._linkable.prev = transform
    

class DropColumns:
    """
    Drops list of columns from the dataframe.
    """
    @staticmethod
    def drop_columns(df: pl.DataFrame):
        return DropColumns(df)

    def __init__(
            self, 
            columns: List[str]
    ):
        """

        Parameters
        ----------
        columns : List[str]
            List of columns to be dropped/deleted
        """
        self._columns = columns
        self._linkable = Linkable()

    async def atransform_data(self, df: pl.DataFrame):
        df = df.drop(self._columns)
        return df

    def transform_data(self, df: pl.DataFrame):
        df = df.drop(self._columns)
        return df
    
    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next
    
    @property
    def prev_transform(self) -> Optional[Transform]: 
        return self._linkable.prev
    
    @next_transform.setter
    def next_transform(self, transform: Transform) -> None: 
        self._linkable.next = transform
    
    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None: 
        self._linkable.prev = transform
    

class ValueMapping:
    """Maps values of the columns to some other values.
    Typically used in bucketing values."""
    @staticmethod
    def value_mapping(df: pl.DataFrame):
        return ValueMapping(df)

    def __init__(
            self, 
            column: str,
            mapping: dict,
            default: Any,
            alias: str
    ):
        # self._df = dataframe
        self._column = column
        self._mapping = mapping
        self._default = default
        self._alias = alias
        self._linkable = Linkable()

    async def atransform_data(
            self, 
            df: pl.DataFrame
    ):
        df = df.with_columns(
            pl.col(self._column).map_dict(self._mapping, dafault=self._default).alias(self._alias)
            # I think it should be map_element()
        )
        return df
    
    def transform_data(
            self, 
            df: pl.DataFrame
    ):
        df = df.with_columns(
            pl.col(self._column).map_dict(self._mapping, dafault=self._default).alias(self._alias)
            # I think it should be map_element()
        )
        return df
    
    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next
    
    @property
    def prev_transform(self) -> Optional[Transform]: 
        return self._linkable.prev
    
    @next_transform.setter
    def next_transform(self, transform: Transform) -> None: 
        self._linkable.next = transform
    
    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None: 
        self._linkable.prev = transform


class FlattenDictColumn:
    """Flattens Dictionary values in a column."""

    @staticmethod
    def flatten_dict_column(df: pl.DataFrame):
        return FlattenDictColumn(df)
    
    def __init__(
            self, 
            column: str,
            schema: Dict,
            columns: Optional[List[str]] = None,
        ):
        """
        Flattens and expands the column containing dictionary values.


        Parameters
        ----------
        column : str
            _description_
        schema : Dict
            _description_, by default None
        columns : Optional[List[str]], optional
            _description_, by default None
        """
        self._column = column
        self._schema = schema
        self._columns = columns
        self._linkable = Linkable()

    async def atransform_data(self, df: pl.DataFrame):
        if df[self._column].dtype.is_(pl.String):
            # first convert this into a Struct column
            df = df.with_columns(
                pl.col("data").str.json_decode()
            )

        if self._schema:
            all_keys = flatten_dictionary(self._schema)
        
        elif self._columns:
            all_keys = self._columns
        
        df = df.with_columns(
            pl.col(self._column)
            .struct
            .rename_fields(all_keys)
        ).unnest(self._column)
        return df
    
    def transform_data(self, df: pl.DataFrame):
        if df[self._column].dtype.is_(pl.String):
            _dtype = pl.Struct(
                [
                    pl.Field(field, _get_pl_dtype(value))
                    for field, value in self._schema.items()
                ]
            )
            # first convert this into a Struct column
            df = df.with_columns(
                pl.col("data").str.json_decode(dtype=_dtype)
            )

        if self._schema:
            all_keys = list(flatten_dictionary(self._schema).keys())
            # print(all_keys)
            logger.debug("All Keys=%s", all_keys)

        # elif self._columns:
        #     all_keys = self._columns
        
        df = df.with_columns(
            pl.col(self._column)
            .struct
            .rename_fields(all_keys)
        ).unnest(self._column)
        return df
    
    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next
    
    @property
    def prev_transform(self) -> Optional[Transform]: 
        return self._linkable.prev
    
    @next_transform.setter
    def next_transform(self, transform: Transform) -> None: 
        self._linkable.next = transform
    
    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None: 
        self._linkable.prev = transform
    

class FilterByUniqueGroupCount:
    """
    Keeps rows where `id_column` is associated with a single unique value of `group_column`.
    After filtering, aggregates specified metrics by (id_column, group_column, optional status columns).
    """

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
        self._linkable = Linkable()

    async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        # 1: find IDs with exactly one unique group value
        valid_ids = (
            df.select([self._id_column, self._group_column])
              .unique()
              .group_by(self._id_column)
              .agg(pl.col(self._group_column).n_unique().alias("groupCount"))
              .filter(pl.col("groupCount") == 1)[self._id_column]
              .to_list()
        )

        # 2: filter df
        df = df.filter(pl.col(self._id_column).is_in(valid_ids))

        # 3: group and aggregate
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

        df = df.unique().group_by(group_keys).agg(aggregations)
        return df
    
    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        # 1: find IDs with exactly one unique group value
        valid_ids = (
            df.select([self._id_column, self._group_column])
              .unique()
              .group_by(self._id_column)
              .agg(pl.col(self._group_column).n_unique().alias("groupCount"))
              .filter(pl.col("groupCount") == 1)[self._id_column]
              .to_list()
        )

        # 2: filter df
        df = df.filter(pl.col(self._id_column).is_in(valid_ids))

        # 3: group and aggregate
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

        df = df.unique().group_by(group_keys).agg(aggregations)
        return df

    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next

    @property
    def prev_transform(self) -> Optional[Transform]:
        return self._linkable.prev

    @next_transform.setter
    def next_transform(self, transform: Transform) -> None:
        self._linkable.next = transform

    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None:
        self._linkable.prev = transform


class ExtractMonthNumber:
    """
    Extracts month number from a datetime column.
    """

    def __init__(self, column: str, alias: str | None = None):
        self._column = column
        self._alias = alias or f"{column}MonthNum"
        self._linkable = Linkable()

    async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).dt.month().alias(self._alias)
        )

    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).dt.month().alias(self._alias)
        )

    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next

    @property
    def prev_transform(self) -> Optional[Transform]:
        return self._linkable.prev

    @next_transform.setter
    def next_transform(self, transform: Transform) -> None:
        self._linkable.next = transform

    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None:
        self._linkable.prev = transform


class CastToDatetime:
    """
    Casts a string column into a proper Datetime type.
    """

    def __init__(self, column: str, date_format: str | None = None, alias: str | None = None):
        self._column = column
        self._date_format = date_format
        self._alias = alias or column
        self._linkable = Linkable()

    async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).str.strptime(pl.Datetime, format=self._date_format).alias(self._alias)
        )
    

    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).str.strptime(pl.Datetime, format=self._date_format).alias(self._alias)
        )

    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next

    @property
    def prev_transform(self) -> Optional[Transform]:
        return self._linkable.prev

    @next_transform.setter
    def next_transform(self, transform: Transform) -> None:
        self._linkable.next = transform

    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None:
        self._linkable.prev = transform


class ExtractDatePart:
    """
    Extracts the date (YYYY-MM-DD) from a datetime column.
    """

    def __init__(self, column: str, alias: str | None = None):
        self._column = column
        self._alias = alias or f"{column}Date"
        self._linkable = Linkable()

    async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).dt.date().alias(self._alias)
        )

    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).dt.date().alias(self._alias)
        )

    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next

    @property
    def prev_transform(self) -> Optional[Transform]:
        return self._linkable.prev

    @next_transform.setter
    def next_transform(self, transform: Transform) -> None:
        self._linkable.next = transform

    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None:
        self._linkable.prev = transform


class ExtractHour:
    """
    Extracts the hour (0–23) from a datetime column.
    """

    def __init__(self, column: str, alias: str | None = None):
        self._column = column
        self._alias = alias or f"{column}Hour"
        self._linkable = Linkable()

    async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).dt.hour().alias(self._alias)
        )
    
    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col(self._column).dt.hour().alias(self._alias)
        )

    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next

    @property
    def prev_transform(self) -> Optional[Transform]:
        return self._linkable.prev

    @next_transform.setter
    def next_transform(self, transform: Transform) -> None:
        self._linkable.next = transform

    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None:
        self._linkable.prev = transform


class ExtractDayOfWeek:
    """
    Extracts day of the week from a datetime column.
    Can return either day name (e.g., "Monday") or sort order (0 = Monday, ...).
    """

    def __init__(self, column: str, as_string: bool = True, alias: str | None = None):
        self._column = column
        self._as_string = as_string
        if alias:
            self._alias = alias
        else:
            self._alias = f"{column}Day" if as_string else f"{column}DaySortOrder"
        self._linkable = Linkable()

    async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        if self._as_string:
            return df.with_columns(
                pl.col(self._column).dt.strftime("%A").alias(self._alias)
            )
        else:
            return df.with_columns(
                pl.col(self._column).dt.weekday().alias(self._alias)
            )
        
    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        if self._as_string:
            return df.with_columns(
                pl.col(self._column).dt.strftime("%A").alias(self._alias)
            )
        else:
            return df.with_columns(
                pl.col(self._column).dt.weekday().alias(self._alias)
            )

    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next

    @property
    def prev_transform(self) -> Optional[Transform]:
        return self._linkable.prev

    @next_transform.setter
    def next_transform(self, transform: Transform) -> None:
        self._linkable.next = transform

    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None:
        self._linkable.prev = transform


class JoinDataFrames:
    """
    Performs a join between two DataFrames on given keys.

    Parameters
    ----------
    right_df : pl.DataFrame
        The right DataFrame to join with.
    on : str | list[str]
        Column(s) to join on.
    how : str, default "inner"
        Join type: "inner", "left", "outer", "semi", "anti", "cross".
    """

    def __init__(self, on: str | list[str], how: str = "inner"):
        self._on = on
        self._how = how
        self._linkable = Linkable()

    async def atransform_data(self, right_df: pl.DataFrame, left_df: pl.DataFrame) -> pl.DataFrame:
        return right_df.join(left_df, on=self._on, how=self._how)
    
    def transform_data(self, right_df: pl.DataFrame, left_df: pl.DataFrame) -> pl.DataFrame:
        return right_df.join(left_df, on=self._on, how=self._how)

    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next

    @property
    def prev_transform(self) -> Optional[Transform]:
        return self._linkable.prev

    @next_transform.setter
    def next_transform(self, transform: Transform) -> None:
        self._linkable.next = transform

    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None:
        self._linkable.prev = transform


class MapElements:
    """
    Maps each row of a column to the given function.
    """

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
        if alias:
            self._alias = alias
        self._linkable = Linkable()

    async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
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

    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
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

    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next

    @property
    def prev_transform(self) -> Optional[Transform]:
        return self._linkable.prev

    @next_transform.setter
    def next_transform(self, transform: Transform) -> None:
        self._linkable.next = transform

    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None:
        self._linkable.prev = transform


class StringJoinTwoColumns:
    """
    String Concatenates two string Columns
    """

    def __init__(
            self, 
            left_column: str,
            right_column: str, 
            joined_by: str, # string which comes in middle 
            alias: str | None = None
    ):
        self._left_column = left_column
        self._right_column = right_column
        self._joined_by = joined_by
        if alias:
            self._alias = alias
        self._linkable = Linkable()

    async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        if self._alias:
            return df.with_columns(
                (pl.col(self._left_column) 
                 + self._joined_by 
                 + pl.col(self._right_column)).alias(self._alias)
            )
        else:
            return df.with_columns(
                (pl.col(self._left_column) 
                 + self._joined_by 
                 + pl.col(self._right_column))
            )

    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        if self._alias:
            return df.with_columns(
                (pl.col(self._left_column) 
                 + self._joined_by 
                 + pl.col(self._right_column)).alias(self._alias)
            )
        else:
            return df.with_columns(
                (pl.col(self._left_column) 
                 + self._joined_by 
                 + pl.col(self._right_column))
            )
    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next

    @property
    def prev_transform(self) -> Optional[Transform]:
        return self._linkable.prev

    @next_transform.setter
    def next_transform(self, transform: Transform) -> None:
        self._linkable.next = transform

    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None:
        self._linkable.prev = transform


class SelectColumns:
    """
    Returns the selected columns in the table
    """

    def __init__(
            self, 
            columns: List[str], 
            alias: str | None = None
    ):
        self._columns = columns

        if alias:
            self._alias = alias
        self._linkable = Linkable()

    async def atransform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.select(self._columns)
    

    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
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

    @property
    def next_transform(self) -> Optional[Transform]:
        return self._linkable.next

    @property
    def prev_transform(self) -> Optional[Transform]:
        return self._linkable.prev

    @next_transform.setter
    def next_transform(self, transform: Transform) -> None:
        self._linkable.next = transform

    @prev_transform.setter
    def prev_transform(self, transform: Transform) -> None:
        self._linkable.prev = transform


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

