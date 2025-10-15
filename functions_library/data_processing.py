#!/usr/bin/env python3
"""
Data Processing Module

This module handles all data processing operations for the forecast pipeline.
"""

import time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import configuration as CONFIG
from functions_library.supabase_connection import SupabaseClient
from functions_library.logger_configuration import get_logger

# Module-level logger - use the main logger to ensure it writes to the same file
logger = get_logger("aya_forecast")


def read_data(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Read all required data from Supabase
    
    Args:
        spark: SparkSession instance
        
    Returns:
        tuple: (sales_df, products_df, calendar_effects_df)
    """
    
    logger.info("=" * 50)
    logger.info("FUNCTION: read_data - STARTED")
    logger.info("=" * 50)
    start_time = time.time()
    
    try:
        # Create Supabase client
        logger.info("    Creating Supabase client...")
        client = SupabaseClient()
        
        # Read sales data (all data from HISTORY_START_DATE onwards)
        logger.info(f"    Reading sales data from {CONFIG.HISTORY_START_DATE} onwards...")
        sales_df = client.read_sales_data(CONFIG.HISTORY_START_DATE, spark)
        
        sales_count = sales_df.count()
        logger.info(f"    [OK] Sales data: {sales_count} records")
        # Log min/max date to validate full range
        try:
            # sales_movement uses 'day' as the date column
            date_min_max = sales_df.select(F.min("day"), F.max("day")).collect()[0]
            logger.info(f"    [DATE RANGE] Sales data span: {date_min_max[0]} to {date_min_max[1]}")
        except Exception as e:
            logger.warning(f"    [DATE RANGE] Could not compute sales date range: {e}")
        
        # Read products data
        logger.info("    Reading products data...")
        products_df = client.read_products(spark)
        products_count = products_df.count() if products_df else 0
        logger.info(f"    [OK] Products data: {products_count} records")
        
        # Read calendar effects data
        logger.info("    Reading calendar effects data...")
        calendar_effects_df = client.read_calendar_effects(spark)
        calendar_count = calendar_effects_df.count() if calendar_effects_df else 0
        logger.info(f"    [OK] Calendar effects data: {calendar_count} records")
        
        # Check if we have data from database
        if sales_count == 0:
            logger.error("No sales data found in database - pipeline cannot continue")
            raise ValueError("No sales data available from database")
        
        duration = time.time() - start_time
        logger.info(f"[TIMING] read_data completed in {duration:.2f} seconds")
        logger.info(f"[DATA COUNT] Sales DF after read_data: {sales_count} rows")
        logger.info("=" * 50)
        logger.info("FUNCTION: read_data - COMPLETED")
        logger.info("=" * 50)
        
        return sales_df, products_df, calendar_effects_df
        
    except Exception as e:
        logger.error(f"read_data failed: {e}")
        raise


def generate_categories(sales_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Generate age and sales categories for the data using configuration
    
    Args:
        sales_df: Sales data DataFrame
        products_df: Products data DataFrame
        
    Returns:
        DataFrame: Sales data with categories added
    """
    
    logger.info("=" * 50)
    logger.info("FUNCTION: generate_categories - STARTED")
    logger.info("=" * 50)
    start_time = time.time()
    
    try:
        # Step 1: Basic column mapping and renaming
        logger.info("    Mapping and renaming columns...")
        logger.debug(f"    [DEBUG] Input sales_df count before mapping: {sales_df.count()}")
        sales_df = sales_df.select(
            F.col("id").alias("id"),
            F.col("product_id").alias("product_id"),
            F.col("shopify_product_id").alias("shopify_product_id"),
            F.col("day").alias("date"),
            F.col("units_sold").alias("sales_units"),
            F.col("sales_amount").alias("sales_amount")
        )
        logger.debug(f"    [DEBUG] Sales_df count after mapping: {sales_df.count()}")
        logger.info("    [OK] Column mapping completed")
        
        # Step 2: Create day_in_stock as row number per product
        logger.info("    Creating day_in_stock column...")
        logger.debug(f"    [DEBUG] Sales_df count before day_in_stock: {sales_df.count()}")
        window_spec = Window.partitionBy("product_id").orderBy("date")
        sales_df = sales_df.withColumn("day_in_stock", F.row_number().over(window_spec))
        logger.debug(f"    [DEBUG] Sales_df count after day_in_stock: {sales_df.count()}")
        logger.info("    [OK] Day in stock calculation completed")
        
        # Step 3: Create sales categories using configuration
        logger.info("    Creating recent sales units window...")
        logger.debug(f"    [DEBUG] Sales_df count before recent_sales_units: {sales_df.count()}")
        window_spec = Window.partitionBy("product_id").orderBy("date").rowsBetween(-(CONFIG.RECENT_SALES_UNITS_WINDOW-1), 0)
        sales_df = sales_df.withColumn(
            "recent_sales_units", F.sum("sales_units").over(window_spec)
        )
        logger.debug(f"    [DEBUG] Sales_df count after recent_sales_units: {sales_df.count()}")
        logger.info("    [OK] Recent sales units calculated")
        
        # Step 4: Create age categories using simple logic
        logger.info("    Creating age categories...")
        logger.debug(f"    [DEBUG] Sales_df count before age categories: {sales_df.count()}")
        
        # Simple age category logic
        sales_df = sales_df.withColumn("age_category",
            F.when(F.col("day_in_stock") == 1, "00| Draft")
            .when(F.col("day_in_stock") <= 8, "01| New")
            .when(F.col("day_in_stock") <= 15, "02| Launch")
            .when(F.col("day_in_stock") <= 31, "03| Growth")
            .otherwise("04| Mature")
        )
        logger.debug(f"    [DEBUG] Sales_df count after age categories: {sales_df.count()}")
        logger.info("    [OK] Age categories created using simple logic")
        
        # Create sales categories using configuration
        logger.info("    Creating cumulative sales and first sales date...")
        logger.debug(f"    [DEBUG] Sales_df count before cumulative sales: {sales_df.count()}")
        cumulative_sales_window = Window.partitionBy("product_id").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
        sales_df = sales_df.withColumn(
            "cumulative_sales", F.sum("sales_units").over(cumulative_sales_window)
        )
        
        first_sales_window = Window.partitionBy("product_id")
        sales_df = sales_df.withColumn(
            "first_positive_sales_date",
            F.min(F.when(F.col("cumulative_sales") > 0, F.col("date"))).over(first_sales_window)
        )
        logger.debug(f"    [DEBUG] Sales_df count after cumulative sales: {sales_df.count()}")
        logger.info("    [OK] Cumulative sales and first sales date calculated")
        
        # Create sales categories using simple logic
        logger.info("    Creating sales categories using simple logic...")
        logger.debug(f"    [DEBUG] Sales_df count before sales categories: {sales_df.count()}")
        
        # Simple sales category logic
        sales_df = sales_df.withColumn("sales_category",
            F.when(F.col("day_in_stock") == 1, "00| Draft")
            .when((F.col("day_in_stock") > 1) & (F.col("recent_sales_units") == 0), "01| Dead")
            .when(F.col("recent_sales_units") < 14, "02| Very Low")
            .when(F.col("recent_sales_units") < 28, "03| Low")
            .when(F.col("recent_sales_units") < 56, "04| Alive")
            .when(F.col("recent_sales_units") < 84, "05| Medium")
            .when(F.col("recent_sales_units") < 140, "06| Winning")
            .otherwise("07| High Winning")
        )
        logger.debug(f"    [DEBUG] Sales_df count after sales categories: {sales_df.count()}")
        logger.info("    [OK] Sales categories created using simple logic")
        
        # Create summarized sales categories by concatenating age and sales categories
        logger.info("    Creating summarized sales categories...")
        sales_df = sales_df.withColumn(
            "summarized_sales_category",
            F.concat_ws("_", F.col("age_category"), F.col("sales_category"))
        )
        logger.info("    [OK] Summarized sales categories created")
        
        # Create combined age_sales_category
        logger.info("    Creating combined age_sales_category...")
        sales_df = sales_df.withColumn(
            "age_sales_category",
            F.concat(F.col("age_category"), F.lit("_"), F.col("summarized_sales_category"))
        )
        logger.info("    [OK] Combined age_sales_category created")
        
        # Clean up temporary columns
        logger.info("    Cleaning up temporary columns...")
        sales_df = sales_df.drop("first_positive_sales_date", "cumulative_sales")
        logger.info("    [OK] Temporary columns cleaned up")
        
        duration = time.time() - start_time
        logger.info(f"[TIMING] generate_categories completed in {duration:.2f} seconds")
        
        # Log sales dataframe count after categories generation
        sales_count = sales_df.count()
        logger.debug(f"[DATA COUNT] Sales DF after generate_categories: {sales_count} rows")
        logger.info("=" * 50)
        logger.info("FUNCTION: generate_categories - COMPLETED")
        logger.info("=" * 50)
        
        return sales_df
        
    except Exception as e:
        logger.error(f"generate_categories failed: {e}")
        raise


def generate_salary_period(sales_df: DataFrame) -> DataFrame:
    """
    Generate salary period flag using configuration
    
    Args:
        sales_df: Sales data DataFrame
        
    Returns:
        DataFrame: Sales data with salary period flag added
    """
    
    logger.info("=" * 50)
    logger.info("FUNCTION: generate_salary_period - STARTED")
    logger.info("=" * 50)
    start_time = time.time()
    
    try:
        # Add salary period flag using configuration
        logger.info("    Creating salary period flag...")
        sales_df = sales_df.withColumn(
            "SALARY_PERIOD",
            F.when(
                (F.dayofmonth("date") >= CONFIG.SALARY_START_DATE) | 
                (F.dayofmonth("date") <= CONFIG.SALARY_END_DATE), 
                1
            ).otherwise(0)
        )
        logger.info("    [OK] Salary period flag created")
        
        duration = time.time() - start_time
        logger.info(f"[TIMING] generate_salary_period completed in {duration:.2f} seconds")
        
        # Log sales dataframe count after salary period generation
        sales_count = sales_df.count()
        logger.debug(f"[DATA COUNT] Sales DF after generate_salary_period: {sales_count} rows")
        logger.info("=" * 50)
        logger.info("FUNCTION: generate_salary_period - COMPLETED")
        logger.info("=" * 50)
        
        return sales_df
        
    except Exception as e:
        logger.error(f"generate_salary_period failed: {e}")
        raise


def process_calendar_effects(calendar_effects_df: DataFrame, sales_df: DataFrame) -> DataFrame:
    """Process calendar effects data and return pivoted DataFrame with all sales dates"""
    
    start_time = time.time()
    logger.info("Starting process_calendar_effects")
    
    try:
        if calendar_effects_df is not None and calendar_effects_df.count() > 0:
            logger.info("    Processing calendar effects data...")
            logger.info(f"    [DEBUG] Calendar effects input count: {calendar_effects_df.count()}")
            
            # Select and rename columns, exclude salary effects (handled separately)
            calendar_effects_df = calendar_effects_df.select(
                F.col("calendar_effect").alias("calendar_effect"),
                F.col("calendar_effect_desc").alias("calendar_effect_desc"),
                F.col("calendar_effect_type").alias("calendar_effect_type"),
                F.col("date").alias("date"),
                F.col("start_date").alias("start_date"),
                F.col("end_date").alias("end_date")
            )
            logger.info(f"    [DEBUG] Calendar effects after column selection: {calendar_effects_df.count()}")
            
            # Explode date ranges for calendar effects
            logger.info("    Exploding date ranges for calendar effects...")
            calendar_effects_df = calendar_effects_df.withColumn(
                "effect_date",
                F.explode(F.expr("sequence(cast(start_date as date), cast(end_date as date), interval 1 day)"))
            ).select(
                F.col("calendar_effect"),
                F.col("calendar_effect_type"),
                F.col("date").alias("actual_effect_date"),  # Keep the actual effect date
                F.col("effect_date").alias("date")
            )
            logger.info(f"    [DEBUG] Calendar effects after date explosion: {calendar_effects_df.count()}")
            
            # Calculate day offset from actual effect date
            logger.info("    Calculating day offsets...")
            calendar_effects_df = calendar_effects_df.withColumn(
                "day_offset",
                F.datediff(F.col("date"), F.col("actual_effect_date"))
            )
            logger.info(f"    [DEBUG] Calendar effects after day offset calculation: {calendar_effects_df.count()}")
            
            # Create day column names based on offset
            logger.info("    Creating calendar effect day column names...")
            calendar_effects_df = calendar_effects_df.withColumn(
                "calendar_effect_day",
                F.when(F.col("day_offset") == 0, F.concat(F.col("calendar_effect_type"), F.lit("_DAY_0")))
                .when(F.col("day_offset") > 0, F.concat(F.col("calendar_effect_type"), F.lit("_DAY_PLUS_"), F.col("day_offset")))
                .when(F.col("day_offset") < 0, F.concat(F.col("calendar_effect_type"), F.lit("_DAY_MINUS_"), F.abs(F.col("day_offset"))))
            )
            logger.info(f"    [DEBUG] Calendar effects after day column creation: {calendar_effects_df.count()}")
            
            # Pivot calendar effects to individual day columns
            logger.info("    Processing calendar effects pivot...")
            try:
                # Get all unique dates from sales data to ensure we don't lose any dates
                # Ensure date column is properly formatted
                all_dates = sales_df.select("date").distinct()
                all_dates = all_dates.filter(F.col("date").isNotNull())  # Remove null dates
                logger.info(f"    [DEBUG] All unique sales dates: {all_dates.count()}")
                
                # Ensure calendar effects date column is properly formatted
                calendar_effects_df = calendar_effects_df.filter(F.col("date").isNotNull())  # Remove null dates
                
                # Pivot calendar effects
                calendar_effects_pivot = calendar_effects_df.groupBy("date").pivot("calendar_effect_day").agg(F.lit(1))
                calendar_effects_pivot = calendar_effects_pivot.fillna(0)
                logger.info(f"    [DEBUG] Calendar effects after pivot: {calendar_effects_pivot.count()}")
                
                # Join with all sales dates to ensure we have all dates (even those without calendar effects)
                # Use left join to preserve all sales dates
                calendar_effects_df = all_dates.join(calendar_effects_pivot, "date", "left")
                calendar_effects_df = calendar_effects_df.fillna(0)
                logger.info(f"    [DEBUG] Calendar effects after join with all dates: {calendar_effects_df.count()}")
                
                # Verify we didn't lose any dates
                if calendar_effects_df.count() != all_dates.count():
                    logger.warning(f"    [WARNING] Date count mismatch: expected {all_dates.count()}, got {calendar_effects_df.count()}")
                    # Log missing dates for debugging
                    missing_dates = all_dates.subtract(calendar_effects_df.select("date"))
                    missing_count = missing_dates.count()
                    if missing_count > 0:
                        logger.warning(f"    [WARNING] {missing_count} dates are missing from calendar effects")
                        # Show first few missing dates
                        missing_sample = missing_dates.limit(5).collect()
                        for row in missing_sample:
                            logger.warning(f"    [WARNING] Missing date: {row.date}")
                
                logger.info("    [OK] Calendar effects pivot completed")
            except Exception as e:
                logger.warning(f"    Calendar effects pivot failed: {e}")
                logger.warning("    Skipping calendar effects processing due to pivot error")
                calendar_effects_df = None
        else:
            logger.info("    No calendar effects data available, skipping calendar effects processing")
            calendar_effects_df = None
        
        duration = time.time() - start_time
        logger.info(f"process_calendar_effects completed in {duration:.2f} seconds")
        
        return calendar_effects_df
        
    except Exception as e:
        logger.error(f"process_calendar_effects failed: {e}")
        raise


def integrate_data(sales_df: DataFrame, products_df: DataFrame, calendar_effects_df: DataFrame) -> DataFrame:
    """
    Integrate all data by joining sales to products and processed calendar effects
    
    Args:
        sales_df: Sales data DataFrame with categories
        products_df: Products data DataFrame
        calendar_effects_df: Calendar effects data DataFrame
        
    Returns:
        DataFrame: Fully integrated sales data
    """
    
    logger.info("=" * 50)
    logger.info("FUNCTION: integrate_data - STARTED")
    logger.info("=" * 50)
    start_time = time.time()
    
    try:
        # Join with products data
        logger.info("    Joining with products data...")
        logger.debug(f"    [DEBUG] Sales_df count before products join: {sales_df.count()}")
        products_selected = products_df.select(
            F.col("id").alias("product_id"),
            F.col("product_number").alias("style")
        )
        sales_df = sales_df.join(products_selected, "product_id", "left")
        logger.debug(f"    [DEBUG] Sales_df count after products join: {sales_df.count()}")
        logger.info("    [OK] Products data joined")
        
        # Process calendar effects
        logger.info("    Processing calendar effects...")
        processed_calendar_effects = process_calendar_effects(calendar_effects_df, sales_df)
        
        # Join calendar effects with sales data
        if processed_calendar_effects is not None:
            logger.info("    Joining calendar effects with sales data...")
            logger.debug(f"    [DEBUG] Sales_df count before calendar effects join: {sales_df.count()}")
            sales_df = sales_df.join(processed_calendar_effects, "date", "left")
            logger.debug(f"    [DEBUG] Sales_df count after calendar effects join: {sales_df.count()}")
            
            # Fill null values for all calendar effect columns with 0
            logger.info("    Filling null values for calendar effect columns...")
            calendar_columns = [col for col in sales_df.columns if col not in ["SALARY_PERIOD"] and col.endswith("_DAY")]
            if calendar_columns:
                for col in calendar_columns:
                    sales_df = sales_df.fillna({col: 0})
                logger.info(f"    [OK] Processed {len(calendar_columns)} calendar effect columns")
            else:
                logger.info("    [OK] No calendar effect columns found to fill")
        else:
            logger.info("    No processed calendar effects available, skipping calendar effects join")
        
        duration = time.time() - start_time
        logger.info(f"[TIMING] integrate_data completed in {duration:.2f} seconds")
        
        # Log sales dataframe count after data integration
        sales_count = sales_df.count()
        logger.debug(f"[DATA COUNT] Sales DF after integrate_data: {sales_count} rows")
        logger.info("=" * 50)
        logger.info("FUNCTION: integrate_data - COMPLETED")
        logger.info("=" * 50)
        
        return sales_df
        
    except Exception as e:
        logger.error(f"integrate_data failed: {e}")
        raise


def process_data(spark: SparkSession) -> DataFrame:
    """
    Main processing function that orchestrates all data processing steps
    
    Uses configured parameters from CONFIG - processes all data from HISTORY_START_DATE to today
    
    Args:
        spark: SparkSession instance
        
    Returns:
        DataFrame: Fully processed sales data
    """
    logger.info("Starting process_data")
    start_time = time.time()
    
    try:
        # Step 1: Read all data
        logger.info("  [1/4] Reading data from database...")
        read_start = time.time()
        sales_df, products_df, calendar_effects_df = read_data(spark)
        read_duration = time.time() - read_start
        logger.info(f"  [1/4] [OK] Data reading completed in {read_duration:.2f}s")
        logger.debug(f"  [1/4] [DEBUG] Sales DF after read_data: {sales_df.count()} rows")
        
        # Step 2: Generate categories
        logger.info("  [2/4] Generating categories...")
        categories_start = time.time()
        sales_df = generate_categories(sales_df, products_df)
        categories_duration = time.time() - categories_start
        logger.info(f"  [2/4] [OK] Categories generated in {categories_duration:.2f}s")
        logger.debug(f"  [2/4] [DEBUG] Sales DF after generate_categories: {sales_df.count()} rows")
        
        # Step 3: Generate salary period
        logger.info("  [3/4] Generating salary periods...")
        salary_start = time.time()
        sales_df = generate_salary_period(sales_df)
        salary_duration = time.time() - salary_start
        logger.info(f"  [3/4] [OK] Salary periods generated in {salary_duration:.2f}s")
        logger.debug(f"  [3/4] [DEBUG] Sales DF after generate_salary_period: {sales_df.count()} rows")
        
        # Step 4: Integrate all data (join with products and calendar effects)
        logger.info("  [4/4] Integrating all data...")
        integrate_start = time.time()
        sales_df = integrate_data(sales_df, products_df, calendar_effects_df)
        integrate_duration = time.time() - integrate_start
        logger.info(f"  [4/4] [OK] Data integration completed in {integrate_duration:.2f}s")
        logger.debug(f"  [4/4] [DEBUG] Sales DF after integrate_data: {sales_df.count()} rows")
        
        # Get final summary
        total_records = sales_df.count()
        total_sales = sales_df.agg(F.sum("sales_units")).collect()[0][0] or 0
        logger.info(f"Processing summary: {total_records} records, {total_sales} total sales units")
        
        # Clear any implicit caching
        sales_df.unpersist()
        
        duration = time.time() - start_time
        logger.info(f"process_data completed in {duration:.2f} seconds")
        
        return sales_df
        
    except Exception as e:
        logger.error(f"process_data failed: {e}")
        raise