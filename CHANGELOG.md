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


## [0.1.2] - 17-11-2025

### Changed
- Made the Extraction row wise. Now we can select the starting and ending row within which the data is to be extracted.


### Added
- A metadata handler `MetadataHandler`, this will read already saved metadata and write 
- An enum of data types `MetadataTypes`
- A tabular data type content `TabularData` for metadata 

## [0.1.2] - 18-11-2025

### Added
- `table_name` in data metadata

## [0.1.3] - 20-11-2025

### Changed
- Removed the first step invokation out of scheduler to solve (ETL-9)
- Getting the last metadata for current table only after checking the table name
- Adding datetime to final table name after loading for `.csv`s. Eg. - `revenues_new_19-11-2025_15-47.csv`
 

### Added
- Can now schedule the whole etl pipeline to be repeated after fixed time (ETL-8)


## [0.1.6.1] - 02-12-2025

## [0.1.7] - 04-12-2025

### Added
- Can now load the data to GCP cloud storage

### Changed
- Metadata will be stored as a sqlite3 db as well as json