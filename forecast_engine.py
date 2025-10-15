#!/usr/bin/env python3
"""
Main script for Aya Forecast Engine
Simple wrapper that calls process and forecast modules serially
"""

# Standard library imports
import sys
import time

# PySpark imports
from pyspark.sql import SparkSession

# Local imports
from functions_library.logger_configuration import setup_logger
import configuration as CONFIG
from functions_library.data_validation import validate_configuration
from functions_library.data_processing import process_data
from functions_library.forecast_generation import forecast_demand
from functions_library.supabase_connection import SupabaseClient

def get_next_run_id(storage_client):
    """
    Get the next incremental run ID by checking existing runs
    
    Args:
        storage_client: SupabaseClient instance
        
    Returns:
        int: Next run ID (1, 2, 3, etc.)
    """
    try:
        # Get existing runs
        existing_runs = storage_client.list_forecast_runs()
        print(f"DEBUG: Found {len(existing_runs)} existing runs: {existing_runs}")
        
        if not existing_runs:
            # No existing runs, start with 1
            print("DEBUG: No existing runs found, starting with run ID 1")
            return 1
        
        # Find the highest run_id and add 1
        max_run_id = max(run['run_id'] for run in existing_runs)
        next_run_id = max_run_id + 1
        print(f"DEBUG: Max existing run ID: {max_run_id}, next run ID: {next_run_id}")
        return next_run_id
        
    except Exception as e:
        # If there's an error, fall back to timestamp-based ID
        print(f"Warning: Could not get incremental run ID, using timestamp: {e}")
        return int(time.time())

def main():
    """Main execution function - simple wrapper"""
    # Use configuration defaults directly
    log_level = 'INFO'
    
    # Get run_id early for logging
    try:
        storage_client = SupabaseClient()
        run_id = get_next_run_id(storage_client)
    except Exception as e:
        # If we can't get run_id, continue without it
        run_id = None
        print(f"Warning: Could not get run_id for logging: {e}")
    
    # Setup logging with run_id
    logger = setup_logger("aya_forecast", log_level, run_id)
    logger.info("Aya Forecast Engine started")
    
    # Initialize Spark session - simple configuration
    spark_start = time.time()
    
    spark = (SparkSession.builder
        .appName("AyaForecastEngine")
        .master("local[1]")  # Use single core to avoid worker issues
        .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")  # Disable partition coalescing
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Disable Arrow optimization
        .getOrCreate())
    
    # Set Spark log level to WARN to suppress INFO messages
    spark.sparkContext.setLogLevel("WARN")
    
    # Suppress cmdstanpy verbose output by setting environment variable
    import os
    os.environ["CMDSTANPY_LOGGING_LEVEL"] = "ERROR"
    
    # Suppress warnings
    import warnings
    warnings.filterwarnings("ignore")
    
    spark_time = time.time() - spark_start
    logger.info(f"Spark initialization: {spark_time:.2f} seconds")
    
    try:
        # Step 0: Configuration Validation
        logger.info("=" * 60)
        logger.info("STEP 0: CONFIGURATION VALIDATION")
        logger.info("=" * 60)
        step0_start = time.time()
        validate_configuration(logger)
        step0_time = time.time() - step0_start
        logger.info(f"[OK] Configuration validation complete")
        logger.info(f"[TIME] Step 0 Total Time: {step0_time:.2f} seconds")
        logger.info("")
        
        # Step 1: Process data
        logger.info("=" * 60)
        logger.info("STEP 1: DATA PROCESSING")
        logger.info("=" * 60)
        step1_start = time.time()
        
        # Process data using Spark session (date range comes from CONFIG inside processing module)
        processed_df = process_data(spark)
        
        # Clear any caching and force fresh evaluation
        processed_df.unpersist()
        
        # Clear all caches after data processing to free memory
        spark.catalog.clearCache()
        
        step1_time = time.time() - step1_start
        logger.info(f"[OK] Processing complete: {processed_df.count()} rows")
        logger.info(f"[TIME] Step 1 Total Time: {step1_time:.2f} seconds")
        
        logger.info("")
        
        # Step 2: Upload processed features to Supabase Storage
        logger.info("=" * 60)
        logger.info("STEP 2: UPLOAD PROCESSED FEATURES TO STORAGE")
        logger.info("=" * 60)
        step2_start = time.time()
        
        try:
            # Initialize storage client
            storage_client = SupabaseClient()
            
            # Convert Spark DataFrame to Pandas for upload
            processed_pandas = processed_df.toPandas()
            
            # Upload processed features (overwrite)
            upload_success = storage_client.upload_processed_features(processed_pandas)
            if upload_success:
                logger.info("[OK] Processed features uploaded to Supabase Storage")
            else:
                logger.error("[ERROR] Failed to upload processed features to storage - check logs above for details")
                
        except Exception as e:
            logger.error(f"[ERROR] Storage upload failed: {e}")
            logger.warning("Continuing with pipeline despite storage failure")
        
        step2_time = time.time() - step2_start
        logger.info(f"[TIME] Step 2 Total Time: {step2_time:.2f} seconds")
        logger.info("")
        
        # Step 3: Run forecasting
        logger.info("=" * 60)
        logger.info("STEP 3: DEMAND FORECASTING")
        logger.info("=" * 60)
        step3_start = time.time()
        
        # Use the configured forecast start date
        logger.info(f"[INFO] Historical data period: {CONFIG.HISTORY_START_DATE} to {CONFIG.HISTORY_END_DATE}")
        logger.info(f"[INFO] Forecast will start on: {CONFIG.FORECAST_CONFIG['FORECAST_START_DATE']}")
        
        try:
            forecast_results = forecast_demand(
                processed_data=processed_df,
                spark=spark,
                run_id=run_id
            )
            
            # Clear caches after forecasting to free memory
            spark.catalog.clearCache()
            
            step3_time = time.time() - step3_start
            logger.info(f"[OK] Forecasting complete: {forecast_results.count()} forecast records")
            logger.info(f"[TIME] Step 3 Total Time: {step3_time:.2f} seconds")
            logger.info("")
        except Exception as e:
            logger.error(f"[ERROR] Forecasting failed: {e}")
            logger.error("Pipeline terminated due to forecast failure.")
            step3_time = time.time() - step3_start
            logger.error(f"[ERROR] Step 3 Total Time: {step3_time:.2f} seconds")
            logger.error("=" * 60)
            logger.error("FAILURE: PIPELINE TERMINATED DUE TO FORECAST ERROR")
            logger.error("=" * 60)
            return 1
        
        # Step 4: Upload incremental forecast to Supabase Storage
        logger.info("=" * 60)
        logger.info("STEP 4: UPLOAD INCREMENTAL FORECAST TO STORAGE")
        logger.info("=" * 60)
        step4_start = time.time()
        
        try:
            # Convert Spark DataFrame to Pandas for upload
            forecast_pandas = forecast_results.toPandas()
            
            # Use the run_id we got earlier
            
            # Upload forecast run
            success = storage_client.upload_forecast_run(forecast_pandas, run_id)
            
            if success:
                logger.info(f"[OK] Forecast run {run_id} uploaded to Supabase Storage")
                
                # List all forecast runs for summary
                runs = storage_client.list_forecast_runs()
                logger.info(f"[INFO] Total forecast runs in storage: {len(runs)}")
                if runs:
                    latest_run = max(runs, key=lambda x: x['run_id'])
                    logger.info(f"[INFO] Latest run: {latest_run['run_id']} ({latest_run.get('last_modified', 'N/A')})")
            else:
                logger.warning("[WARN] Failed to upload incremental forecast to storage")
                
        except Exception as e:
            logger.error(f"[ERROR] Forecast storage upload failed: {e}")
            logger.warning("Continuing with pipeline despite storage failure")
        
        step4_time = time.time() - step4_start
        logger.info(f"[TIME] Step 4 Total Time: {step4_time:.2f} seconds")
        logger.info("")
        
        # Final Summary
        total_time = step0_time + step1_time + step2_time + step3_time + step4_time
        logger.info("=" * 60)
        logger.info("SUCCESS: PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"[DATA] Total Records Processed: {processed_df.count()}")
        logger.info(f"[FORECAST] Total Forecasts Generated: {forecast_results.count()}")
        logger.info(f"[TIME] Total Execution Time: {total_time:.2f} seconds")
        logger.info(f"   |-- Step 0 (Validation): {step0_time:.2f}s ({step0_time/total_time*100:.1f}%)")
        logger.info(f"   |-- Step 1 (Processing): {step1_time:.2f}s ({step1_time/total_time*100:.1f}%)")
        logger.info(f"   |-- Step 2 (Storage - Processed): {step2_time:.2f}s ({step2_time/total_time*100:.1f}%)")
        logger.info(f"   |-- Step 3 (Forecasting): {step3_time:.2f}s ({step3_time/total_time*100:.1f}%)")
        logger.info(f"   `-- Step 4 (Storage - Forecasts): {step4_time:.2f}s ({step4_time/total_time*100:.1f}%)")
        logger.info("=" * 60)
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    finally:
        # Clean up Spark session and clear all caches
        logger.info("Cleaning up Spark resources...")
        try:
            # Clear all cached DataFrames and RDDs
            spark.catalog.clearCache()
            logger.info("Cleared all cached DataFrames and RDDs")
        except Exception as e:
            logger.warning(f"Could not clear cache: {e}")
        
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    sys.exit(main())
