#!/usr/bin/env python3
"""
Unified Supabase client for both database and storage operations
Handles data reading, writing, and file storage
"""

# Standard library imports
import io
import logging
from typing import Dict, Optional, List, Any

# Third-party imports
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from supabase import create_client, Client

# Local imports
import configuration as CONFIG

# Module-level logger
logger = logging.getLogger(__name__)

class SupabaseClient:
    """
    Unified Supabase client for both database and storage operations
    """
    
    def __init__(self):
        """Initialize Supabase client with user authentication"""
        try:
            # Create client with anon key first
            self.client: Client = create_client(
                CONFIG.SUPABASE_URL, 
                CONFIG.SUPABASE_ANON_KEY
            )
            
            # Sign in with user credentials
            auth_response = self.client.auth.sign_in_with_password({
                "email": CONFIG.TEST_USER_EMAIL,
                "password": CONFIG.TEST_USER_PASSWORD
            })
            
            if auth_response.user:
                logger.info("Successfully authenticated with user credentials")
            else:
                logger.error("Failed to authenticate with user credentials")
                raise Exception("User authentication failed")
            
            # Storage configuration
            self.bucket_name = CONFIG.STORAGE_CONFIG["BUCKET_NAME"]
            self.processed_path = CONFIG.STORAGE_CONFIG["PROCESSED_FEATURES_PATH"]
            self.forecasts_path = CONFIG.STORAGE_CONFIG["INCREMENTAL_FORECASTS_PATH"]
            
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            raise

    def write_to_table(self, table_name: str, data: DataFrame, spark: SparkSession) -> bool:
        """
        Write DataFrame to Supabase table
        
        Args:
            table_name: Name of the table to write to
            data: Spark DataFrame to write
            spark: SparkSession instance
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert Spark DataFrame to Pandas
            pandas_df = data.toPandas()
            
            # Convert to records format
            records = pandas_df.to_dict('records')
            
            # Insert data
            result = self.client.table(table_name).insert(records).execute()
            
            if result.data:
                logger.info(f"Successfully wrote {len(result.data)} records to {table_name}")
                return True
            else:
                logger.error(f"No data was written to {table_name}")
                return False
            
        except Exception as e:
            logger.error(f"Failed to write to table {table_name}: {e}")
            return False

    def batch_read_table(self, table_name: str, spark: SparkSession, 
                    filters: Optional[Dict] = None, batch_size: int = 5000) -> DataFrame:
        """
        Read all data from a table using pagination
        
        Args:
            table_name: Name of the table to read
            spark: SparkSession instance
            filters: Optional filters to apply
            batch_size: Number of records per batch
        
        Returns:
            pyspark.sql.DataFrame: All table data
        """
        try:
            all_data = []
            offset = 0
            
            while True:
                # Build query with pagination
                query = self.client.table(table_name).select("*")
                
                # Apply filters
                if filters:
                    for key, value in filters.items():
                        if key.endswith('_gte'):
                            column = key.replace('_gte', '')
                            query = query.gte(column, value.replace('gte.', ''))
                        elif key.endswith('_lte'):
                            column = key.replace('_lte', '')
                            query = query.lte(column, value.replace('lte.', ''))
                        elif key.endswith('_eq'):
                            column = key.replace('_eq', '')
                            query = query.eq(column, value.replace('eq.', ''))
                        else:
                            query = query.eq(key, value)
                
                # Add pagination
                query = query.range(offset, offset + batch_size - 1)
                
                # Execute query
                result = query.execute()
                
                if not result.data:
                    break
                    
                all_data.extend(result.data)
                logger.info(f"Read batch: {len(result.data)} records (total: {len(all_data)})")
                
                # If we got fewer records than batch_size, we've reached the end
                if len(result.data) < batch_size:
                    break
                    
                offset += batch_size
            
            # Convert to Spark DataFrame
            if all_data:
                pandas_df = pd.DataFrame(all_data)
                spark_df = spark.createDataFrame(pandas_df)
                # Ensure no caching
                spark_df.unpersist()
                logger.info(f"Successfully read {len(all_data)} records from {table_name}")
                return spark_df
            else:
                logger.warning(f"No data found in table {table_name}")
                return spark.createDataFrame([], StructType([]))
        
        except Exception as e:
            logger.error(f"Failed to read table {table_name}: {e}")
            raise

    def read_sales_data(self, start_date: str, spark: SparkSession, client: "SupabaseClient" = None) -> DataFrame:
        """Read sales data and return Spark DataFrame"""
        if client is None:
            client = self
        
        # Apply date filter to get data from start_date onwards (no end date filter)
        logger.info(f"Reading sales data from {start_date} onwards (no end date filter)")
        filters = {
            "day_gte": f"gte.{start_date}"
        }
        
        return self.batch_read_table(CONFIG.TABLES["sales_movement"], spark, filters=filters)

    def read_products(self, spark: SparkSession, client: "SupabaseClient" = None) -> DataFrame:
        """Read products data and return Spark DataFrame"""
        if client is None:
            client = self
        
        # No filtering temporarily to find MW1592 shopify_product_id
        logger.info("No product filter applied - processing all products temporarily")
        return self.batch_read_table(CONFIG.TABLES["products"], spark)

    def read_calendar_effects(self, spark: SparkSession, client: "SupabaseClient" = None) -> DataFrame:
        """Read calendar effects data and return Spark DataFrame"""
        if client is None:
            client = self
        
        logger.info("Reading calendar effects data")
        return self.batch_read_table(CONFIG.TABLES["calendar_effects"], spark)

    def upload_processed_features(self, data: pd.DataFrame) -> bool:
        """
        Upload processed features to Supabase Storage (OVERWRITE mode)
        Deletes old processed features files and uploads new one
        
        Args:
            data: Pandas DataFrame with processed features
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # First, clean up all existing processed features files
            try:
                existing_files = self.client.storage.from_(self.bucket_name).list(path=self.processed_path)
                if existing_files:
                    # Filter out .emptyFolderPlaceholder and only delete .parquet files
                    parquet_files = [f for f in existing_files if f['name'].endswith('.parquet')]
                    if parquet_files:
                        logger.info(f"Deleting {len(parquet_files)} existing processed features files")
                        for file in parquet_files:
                            file_path = f"{self.processed_path}{file['name']}"
                            try:
                                self.client.storage.from_(self.bucket_name).remove([file_path])
                                logger.info(f"Deleted existing file: {file['name']}")
                            except Exception as e:
                                logger.warning(f"Could not delete file {file['name']}: {e}")
            except Exception as e:
                logger.warning(f"Could not list existing files for cleanup: {e}")
            
            # Use consistent filename for true overwriting
            filename = "processed_features.parquet"
            file_path = f"{self.processed_path}{filename}"
            
            # Convert DataFrame to Parquet bytes
            parquet_buffer = io.BytesIO()
            data.to_parquet(parquet_buffer, index=False)
            parquet_bytes = parquet_buffer.getvalue()
            
            # Upload new file (try upload first, then update if exists)
            try:
                result = self.client.storage.from_(self.bucket_name).upload(
                    file_path, 
                    parquet_bytes,
                    file_options={"content-type": "application/octet-stream"}
                )
            except Exception as e:
                if "409" in str(e) or "Duplicate" in str(e):
                    # File exists, use update instead
                    logger.info("File exists, updating instead of uploading...")
                    result = self.client.storage.from_(self.bucket_name).update(
                        file_path, 
                        parquet_bytes,
                        file_options={"content-type": "application/octet-stream"}
                    )
                else:
                    raise e
            
            if result:
                logger.info(f"Successfully uploaded processed features to {file_path} (overwrite mode)")
                return True
            else:
                logger.error("Failed to upload processed features")
                return False
                
        except Exception as e:
            logger.error(f"Failed to upload processed features: {e}")
            return False
    
    def download_processed_features(self, spark: SparkSession) -> Optional[DataFrame]:
        """
        Download processed features from Supabase Storage
        
        Args:
            spark: SparkSession instance
            
        Returns:
            Optional[DataFrame]: Spark DataFrame with processed features or None if not found
        """
        try:
            # List files in processed features path
            files = self.client.storage.from_(self.bucket_name).list(path=self.processed_path)
            
            # Debug: Log available fields in file objects
            if files:
                logger.info(f"Available fields in file objects: {list(files[0].keys())}")
                logger.info(f"Sample file object: {files[0]}")
            
            # Find the most recent parquet file
            parquet_files = [f for f in files if f['name'].endswith('.parquet')]
            if not parquet_files:
                logger.warning("No processed features files found in storage")
                return None
            
            # Get the most recent file - try different timestamp fields
            def get_timestamp(file_obj):
                # Try different possible timestamp field names
                for field in ['updated_at', 'created_at', 'last_modified', 'metadata']:
                    if field in file_obj:
                        if field == 'metadata' and isinstance(file_obj[field], dict):
                            # If metadata is a dict, look for timestamp fields within it
                            for meta_field in ['updated_at', 'created_at', 'last_modified']:
                                if meta_field in file_obj[field]:
                                    return file_obj[field][meta_field]
                        else:
                            return file_obj[field]
                # If no timestamp found, use name as fallback (lexicographic sorting)
                return file_obj['name']
            
            latest_file = max(parquet_files, key=get_timestamp)
            file_path = f"{self.processed_path}{latest_file['name']}"
            
            # Download file
            file_data = self.client.storage.from_(self.bucket_name).download(file_path)
            
            # Convert to Pandas DataFrame
            pandas_df = pd.read_parquet(io.BytesIO(file_data))
            
            # Convert to Spark DataFrame
            spark_df = spark.createDataFrame(pandas_df)
            
            logger.info(f"Successfully downloaded processed features from {latest_file['name']}")
            return spark_df
            
        except Exception as e:
            logger.error(f"Failed to download processed features: {e}")
            return None

    def upload_forecast_run(self, data: pd.DataFrame, run_id: int) -> bool:
        """
        Upload forecast run to Supabase Storage (APPEND mode)
        Keeps all historical forecast runs - no overwriting
        
        Args:
            data: Pandas DataFrame with forecast data
            run_id: Unique run identifier
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Generate filename with timestamp and run ID
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_run_{run_id}.parquet"
            file_path = f"{self.forecasts_path}{filename}"
            
            # Convert DataFrame to Parquet bytes
            parquet_buffer = io.BytesIO()
            data.to_parquet(parquet_buffer, index=False)
            parquet_bytes = parquet_buffer.getvalue()
            
            # Upload to Supabase Storage (append mode - no overwriting)
            result = self.client.storage.from_(self.bucket_name).upload(
                file_path, 
                parquet_bytes,
                file_options={"content-type": "application/octet-stream"}
            )
            
            if result:
                logger.info(f"Successfully uploaded forecast run {run_id} to {file_path} (append mode)")
                logger.info(f"File size: {len(parquet_bytes)} bytes")
                return True
            else:
                logger.error(f"Failed to upload forecast run {run_id}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to upload forecast run {run_id}: {e}")
            return False

    def download_forecast_run(self, run_id: int, spark: SparkSession) -> Optional[DataFrame]:
        """
        Download a specific forecast run from Supabase Storage
        
        Args:
            run_id: Run ID to download
            spark: SparkSession instance
            
        Returns:
            Optional[DataFrame]: Spark DataFrame with forecast data or None if not found
        """
        try:
            # List files in forecasts path
            files = self.client.storage.from_(self.bucket_name).list(path=self.forecasts_path)
            
            # Find file with matching run_id
            target_file = None
            for file in files:
                if f"_run_{run_id}.parquet" in file['name']:
                    target_file = file
                    break
            
            if not target_file:
                logger.error(f"Forecast run {run_id} not found")
                return None
            
            # Download file
            file_path = f"{self.forecasts_path}{target_file['name']}"
            file_data = self.client.storage.from_(self.bucket_name).download(file_path)
            
            # Convert to Pandas DataFrame
            pandas_df = pd.read_parquet(io.BytesIO(file_data))
            
            # Convert to Spark DataFrame
            spark_df = spark.createDataFrame(pandas_df)
            
            logger.info(f"Successfully downloaded forecast run {run_id} from {target_file['name']}")
            return spark_df
            
        except Exception as e:
            logger.error(f"Failed to download forecast run {run_id}: {e}")
            return None

    def list_forecast_runs(self) -> List[Dict[str, Any]]:
        """
        List all available forecast runs from storage
        
        Returns:
            List[Dict]: List of forecast run information
        """
        try:
            # List files in forecasts path
            files = self.client.storage.from_(self.bucket_name).list(path=self.forecasts_path)
            
            # Filter for forecast run files and extract run IDs
            forecast_runs = []
            for file in files:
                if "_run_" in file['name'] and file['name'].endswith('.parquet'):
                    # Extract run ID from filename
                    try:
                        run_id = int(file['name'].split('_run_')[1].split('.')[0])
                        forecast_runs.append({
                            'run_id': run_id,
                            'filename': file['name'],
                            'size': file.get('size', 'unknown'),
                            'last_modified': file.get('last_modified', 'unknown')
                        })
                    except (ValueError, IndexError):
                        logger.warning(f"Could not parse run ID from filename: {file['name']}")
                        continue
            
            # Sort by run ID
            forecast_runs.sort(key=lambda x: x['run_id'])
            
            logger.info(f"Found {len(forecast_runs)} forecast runs in storage")
            return forecast_runs
            
        except Exception as e:
            logger.error(f"Failed to list forecast runs: {e}")
            return []
