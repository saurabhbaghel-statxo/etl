from typing import Tuple, List, Union

import polars as pl
import pandas as pd

type Dataframe = Union[pl.DataFrame, pd.DataFrame]
type 