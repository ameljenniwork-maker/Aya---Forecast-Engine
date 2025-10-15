#!/usr/bin/env python3
"""
Temporary script to export processed data for a specific style as CSV
"""

import os
import sys
from pyspark.sql import SparkSession
from functions_library.supabase_connection import SupabaseClient
from functions_library.data_processing import process_data
from functions_library.logger_configuration import setup_logger

def export_style_data(style="MW1341"):
    """Export processed data for a specific style as CSV"""
    
    # Setup
    logger = setup_logger("export_style", "INFO")
    logger.info(f"Exporting processed data for style: {style}")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ExportStyleData") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    
    try:
        # Initialize Supabase client
        storage_client = SupabaseClient()
        
        # Process data
        logger.info("Processing data...")
        processed_df = process_data(storage_client, spark, logger)
        
        # Filter for specific style
        logger.info(f"Filtering for style: {style}")
        style_data = processed_df.filter(processed_df.style == style)
        
        # Convert to Pandas and export
        logger.info("Converting to Pandas and exporting...")
        style_pandas = style_data.toPandas()
        
        # Create output filename
        output_file = f"processed_data_{style}.csv"
        
        # Export to CSV
        style_pandas.to_csv(output_file, index=False)
        
        logger.info(f"✅ Exported {len(style_pandas)} rows to {output_file}")
        logger.info(f"Columns: {list(style_pandas.columns)}")
        logger.info(f"Date range: {style_pandas['date'].min()} to {style_pandas['date'].max()}")
        
        # Show sample data
        print(f"\n📊 Sample data for {style}:")
        print(style_pandas[['date', 'product_id', 'sales_units', 'age_category', 'sales_category', 'day_in_stock', 'recent_sales_units']].head(10))
        
        return output_file
        
    except Exception as e:
        logger.error(f"❌ Export failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    style = sys.argv[1] if len(sys.argv) > 1 else "MW1341"
    export_style_data(style)
