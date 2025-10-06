import os
import logging
from typing import Optional, Any, Dict, List, Union
import asyncio

import polars as pl

from . import extract
from . import transform
from . import load

logger = logging.getLogger(__name__)


class Etl:
    """High-level ETL orchestrator that coordinates Extract, Transform, and Load operations.
    
    Supports both single-table and multi-table ETL with table-specific transformations.
    """
    
    def __init__(
            self,
            extract: Optional[extract.Extract] = None,
            transform_pipe: Optional[Union[transform.TransformPipe, Dict[str, transform.TransformPipe]]] = None,
            load: Optional[load.Load] = None
    ):
        """Initialize the ETL pipeline with optional Extract, Transform, and Load components.
        
        Parameters
        ----------
        extract : Optional[extract.Extract]
            The extraction component to fetch data from source
        transform : Optional[Union[transform.TransformPipe, Dict[str, transform.TransformPipe]]]
            Either a single TransformPipe or a dictionary mapping table names to their TransformPipes
        load : Optional[load.Load]
            The load component to write data to destination
        """
        self._extract_subpipe = extract
        self._load_subpipe = load
        
        # Handle both single transform and multi-table transforms
        if isinstance(transform_pipe, dict):
            self._transform_subpipes: Dict[str, transform.TransformPipe] = transform_pipe
            self._multi_table_mode = True
        else:
            self._transform_subpipes = {"default": transform_pipe} if transform_pipe else {}
            self._multi_table_mode = False
        
        # Track the data paths and intermediate results
        self._extracted_data_paths: Dict[str, str] = {}
        self._transformed_data_paths: Dict[str, str] = {}
        self._dataframes: Dict[str, pl.DataFrame] = {}
        
        # Validate and build the pipeline
        self._validate_pipeline()
        self._make_pipe()

    def _validate_pipeline(self):
        """Validate that at least one pipeline component is provided."""
        has_transform = bool(self._transform_subpipes)
        if not any([self._extract_subpipe, has_transform, self._load_subpipe]):
            raise ValueError("At least one of Extract, Transform, or Load must be provided")
        
        logger.info("ETL Pipeline initialized with: Extract=%s, Transform=%s (tables: %s), Load=%s",
                   bool(self._extract_subpipe),
                   has_transform,
                   list(self._transform_subpipes.keys()) if has_transform else [],
                   bool(self._load_subpipe))

    def _make_pipe(self):
        """Build the ETL pipeline by connecting the components."""
        components = []
        
        if self._extract_subpipe:
            components.append("Extract")
        if self._transform_subpipes:
            if self._multi_table_mode:
                components.append(f"Transform[{len(self._transform_subpipes)} tables]")
            else:
                components.append("Transform")
        if self._load_subpipe:
            components.append("Load")
        
        self._pipe = " -> ".join(components)
        logger.info("ETL Pipeline created: %s", self._pipe)

    def run(
            self, 
            extract_params: Optional[dict] = None,
            transform_params: Optional[Union[dict, Dict[str, dict]]] = None,
            load_params: Optional[Union[dict, Dict[str, dict]]] = None,
            tables: Optional[List[str]] = None
    ) -> Dict[str, pl.DataFrame]:
        """Execute the ETL pipeline synchronously.
        
        Parameters
        ----------
        extract_params : Optional[dict]
            Parameters to pass to the extract.get_data() method
        transform_params : Optional[Union[dict, Dict[str, dict]]]
            Either a single dict for all tables, or table-specific dicts
        load_params : Optional[Union[dict, Dict[str, dict]]]
            Either a single dict for all tables, or table-specific dicts
        tables : Optional[List[str]]
            List of table names to process. If None, processes all available
            
        Returns
        -------
        Dict[str, pl.DataFrame]
            Dictionary mapping table names to their final DataFrames
        """
        extract_params = extract_params or {}
        transform_params = transform_params or {}
        load_params = load_params or {}
        
        logger.info("Starting ETL pipeline execution: %s", self._pipe)
        
        try:
            # Step 1: Extract
            if self._extract_subpipe:
                logger.info("Step 1: Extracting data")
                if hasattr(self._extract_subpipe, 'get_data'):
                    result = self._extract_subpipe.get_data(**extract_params)
                    if asyncio.iscoroutine(result):
                        asyncio.run(result)
                    
                # Build extracted data paths for each table
                table_dir = extract_params.get('table_dir') or getattr(self._extract_subpipe, 'table_dir', None)
                extracted_tables = extract_params.get('table_names', tables or [])
                
                if table_dir and extracted_tables:
                    for table in extracted_tables:
                        self._extracted_data_paths[table] = os.path.join(table_dir, table)
                
                logger.info("Data extracted for tables: %s", list(self._extracted_data_paths.keys()))
            
            # Determine which tables to process
            tables_to_process = tables or list(self._transform_subpipes.keys())
            if not tables_to_process and self._extracted_data_paths:
                tables_to_process = list(self._extracted_data_paths.keys())
            
            # Step 2: Transform (per table)
            if self._transform_subpipes:
                logger.info("Step 2: Transforming data for %d tables", len(tables_to_process))
                
                for table_name in tables_to_process:
                    # Get table-specific transform pipe
                    transform_pipe = self._transform_subpipes.get(table_name) or self._transform_subpipes.get("default")
                    
                    if not transform_pipe:
                        logger.warning("No transform pipeline found for table: %s, skipping", table_name)
                        continue
                    
                    logger.info("Transforming table: %s", table_name)
                    
                    # Get table-specific transform params
                    if isinstance(transform_params, dict) and table_name in transform_params:
                        table_transform_params = transform_params[table_name]
                    else:
                        table_transform_params = transform_params if not isinstance(transform_params, dict) or not any(k in self._transform_subpipes for k in transform_params.keys()) else {}
                    
                    # Execute transformation
                    self._dataframes[table_name] = transform_pipe.transform_data()
                    
                    # Track transformed data path
                    if hasattr(transform_pipe, '_table_path'):
                        self._transformed_data_paths[table_name] = transform_pipe._table_path
                        logger.info("Transformed data for %s saved to: %s", table_name, transform_pipe._table_path)
            
            # Step 3: Load (per table)
            if self._load_subpipe:
                logger.info("Step 3: Loading data for %d tables", len(tables_to_process))

                for table_name in tables_to_process:
                    # Determine source path for this table
                    if isinstance(load_params, dict) and table_name in load_params:
                        table_load_params = load_params[table_name].copy()
                    else:
                        table_load_params = load_params.copy() if isinstance(load_params, dict) else {}
                    
                    # Set path if not provided
                    if 'path' not in table_load_params:
                        table_load_params['path'] = (
                            self._transformed_data_paths.get(table_name) or 
                            self._extracted_data_paths.get(table_name)
                        )
                    
                    if not table_load_params.get('path'):
                        logger.warning("No data path available for loading table: %s, skipping", table_name)
                        continue
                    
                    # Set table name if not provided
                    if 'table_name' not in table_load_params:
                        table_load_params['table_name'] = table_name
                    
                    logger.info("Loading table: %s", table_name)
                    self._load_subpipe.load_data(**table_load_params)
            
            logger.info("ETL pipeline completed successfully")
            return self._dataframes
            
        except Exception as e:
            logger.exception("ETL pipeline failed: %s", e)
            raise

    async def arun(
            self, 
            extract_params: Optional[dict] = None,
            transform_params: Optional[Union[dict, Dict[str, dict]]] = None,
            load_params: Optional[Union[dict, Dict[str, dict]]] = None,
            tables: Optional[List[str]] = None
    ) -> Dict[str, pl.DataFrame]:
        """Execute the ETL pipeline asynchronously.
        
        Parameters
        ----------
        extract_params : Optional[dict]
            Parameters to pass to the extract.get_data() method
        transform_params : Optional[Union[dict, Dict[str, dict]]]
            Either a single dict for all tables, or table-specific dicts
        load_params : Optional[Union[dict, Dict[str, dict]]]
            Either a single dict for all tables, or table-specific dicts
        tables : Optional[List[str]]
            List of table names to process. If None, processes all available
            
        Returns
        -------
        Dict[str, pl.DataFrame]
            Dictionary mapping table names to their final DataFrames
        """
        extract_params = extract_params or {}
        transform_params = transform_params or {}
        load_params = load_params or {}
        
        logger.info("Starting async ETL pipeline execution: %s", self._pipe)
        
        try:
            # Step 1: Extract
            if self._extract_subpipe:
                logger.info("Step 1: Extracting data (async)")
                await self._extract_subpipe.get_data(**extract_params)
                
                # Build extracted data paths for each table
                table_dir = extract_params.get('table_dir') or getattr(self._extract_subpipe, 'table_dir', None)
                extracted_tables = extract_params.get('table_names', tables or [])
                
                if table_dir and extracted_tables:
                    for table in extracted_tables:
                        self._extracted_data_paths[table] = os.path.join(table_dir, table)
                
                logger.info("Data extracted for tables: %s", list(self._extracted_data_paths.keys()))
            
            # Determine which tables to process
            tables_to_process = tables or list(self._transform_subpipes.keys())
            if not tables_to_process and self._extracted_data_paths:
                tables_to_process = list(self._extracted_data_paths.keys())
            
            # Step 2: Transform (per table)
            if self._transform_subpipes:
                logger.info("Step 2: Transforming data for %d tables", len(tables_to_process))
                
                for table_name in tables_to_process:
                    transform_pipe = self._transform_subpipes.get(table_name) or self._transform_subpipes.get("default")
                    
                    if not transform_pipe:
                        logger.warning("No transform pipeline found for table: %s, skipping", table_name)
                        continue
                    
                    logger.info("Transforming table: %s", table_name)
                    self._dataframes[table_name] = transform_pipe.transform_data()
                    
                    if hasattr(transform_pipe, '_table_path'):
                        self._transformed_data_paths[table_name] = transform_pipe._table_path
                        logger.info("Transformed data for %s saved to: %s", table_name, transform_pipe._table_path)
            
            # Step 3: Load (per table)
            if self._load_subpipe:
                logger.info("Step 3: Loading data (async) for %d tables", len(tables_to_process))
                
                for table_name in tables_to_process:
                    if isinstance(load_params, dict) and table_name in load_params:
                        table_load_params = load_params[table_name].copy()
                    else:
                        table_load_params = load_params.copy() if isinstance(load_params, dict) else {}
                    
                    if 'path' not in table_load_params:
                        table_load_params['path'] = (
                            self._transformed_data_paths.get(table_name) or 
                            self._extracted_data_paths.get(table_name)
                        )
                    
                    if not table_load_params.get('path'):
                        logger.warning("No data path available for loading table: %s, skipping", table_name)
                        continue
                    
                    if 'table_name' not in table_load_params:
                        table_load_params['table_name'] = table_name
                    
                    logger.info("Loading table: %s (async)", table_name)
                    await self._load_subpipe.aload_data(**table_load_params)
            
            logger.info("Async ETL pipeline completed successfully")
            return self._dataframes
            
        except Exception as e:
            logger.exception("Async ETL pipeline failed: %s", e)
            raise

    def get_extracted_data_path(self, table_name: Optional[str] = None) -> Union[str, Dict[str, str]]:
        """Get the path(s) where extracted data was saved."""
        if table_name:
            return self._extracted_data_paths.get(table_name)
        return self._extracted_data_paths

    def get_transformed_data_path(self, table_name: Optional[str] = None) -> Union[str, Dict[str, str]]:
        """Get the path(s) where transformed data was saved."""
        if table_name:
            return self._transformed_data_paths.get(table_name)
        return self._transformed_data_paths

    def get_dataframe(self, table_name: Optional[str] = None) -> Union[pl.DataFrame, Dict[str, pl.DataFrame]]:
        """Get the in-memory DataFrame(s) from the transformation step."""
        if table_name:
            return self._dataframes.get(table_name)
        return self._dataframes

    @property
    def pipeline(self) -> str:
        """Get a string representation of the pipeline flow."""
        return self._pipe

    @property
    def processed_tables(self) -> List[str]:
        """Get list of tables that were processed."""
        return list(self._dataframes.keys())

    def __repr__(self) -> str:
        """String representation of the ETL pipeline."""
        return f"Etl(pipeline={self._pipe}, tables={self.processed_tables})"