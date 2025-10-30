# Changelog

## [Unreleased]

## [0.1.0] - 29-10-2025

### Added
- Executable meta class
- Runnable derived from Executable meta class
- Runner derived from Executable meta class
- Executor
- Policy
- Scheduler
- Extraction base class derived from Executable meta class
- Extraction from Postgres inheriting Extraction base class
- Transform Base class derived from Executable meta class
- Rename column transform
- Drop column transform
- Value mapping transform
- Flatten Dictionary transform
- Filtering by unique group count transform
- Extracting month number transform
- Extract date from datetime transform
- Extract hour part from datetime transform
- Extract day of week from date transform
- Casting string of datetime to datetime object transform
- Joining two tables transform
- Mapping elements transform
- String concatenation transform
- Select columns transform
- Conditional column transform
- Load base class
- Load to postgres


## [0.1.1] - 30-10-2025

### Changed
- Add thin wrapper to base load class to enable load methods handle both path of table and dataframe alike
- Rename `_LoadParquetInMemory()` to `_LoadParquetFromPath`


### Added 
- Loading method from parquet already present in buffer instead of getting it path - `_LoadParquetFromBuffer()`
- Extraction method from excel files (.xlsx, .csv, .parquet) - `ExtractFromFile()`
- Add duplicate row remover trasnform - `DropDuplicates()`