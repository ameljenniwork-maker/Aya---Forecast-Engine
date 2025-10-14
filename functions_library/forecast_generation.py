#!/usr/bin/env python3
"""
Demand Forecasting Module for Aya Forecast Engine
Modular wrapper for Prophet-based demand forecasting with lifecycle-aware configurations
"""

# Standard library imports
import time
from typing import Dict, Any

# Third-party imports
import pandas as pd
import numpy as np

# PySpark imports
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Local imports
from functions_library.supabase_connection import SupabaseClient
import configuration as CONFIG
from functions_library.logger_configuration import get_logger

# Module-level logger
logger = get_logger(__name__)

def forecast_demand(processed_data: DataFrame, spark: SparkSession = None, run_id: int = None) -> DataFrame:
    """
    Main forecasting pipeline - takes processed data as input
    
    Args:
        processed_data: Processed DataFrame from process_data module
        spark: SparkSession instance (optional, will use processed_data.sparkSession if not provided)
        
    Returns:
        DataFrame: Forecast results
        
    Uses configured parameters from CONFIG:
    - FORECAST_START_DATE: When to start forecasting
    - FORECAST_HORIZON: How many days to forecast ahead
    """
    # Use module-level logger (no need for parameter)
    
    # Use provided spark session or fall back to processed_data.sparkSession
    if spark is None:
        spark = processed_data.sparkSession
    
    logger.info("  [START] Starting demand forecasting pipeline")
    total_start_time = time.time()
    
    try:
        # Spark configuration is handled in main.py - no need to configure here
        logger.info("Using Spark session configured in main.py")
        
        # Use configured parameters
        forecast_start_date = CONFIG.FORECAST_CONFIG["FORECAST_START_DATE"]
        forecast_horizon = CONFIG.FORECAST_CONFIG["FORECAST_HORIZON"]
        logger.info(f"Using configured parameters: start_date={forecast_start_date}, horizon={forecast_horizon}")
        
        # Step 1: Filter non-eligible categories
        logger.info("  [STEP 1] Filtering non-eligible products...")
        filtering_start = time.time()
        qualified_data = filter_non_eligible_categories(processed_data, forecast_start_date)
        filtering_time = time.time() - filtering_start
        logger.info(f"    [OK] Non-eligible category filtering: {filtering_time:.2f} seconds")
        
        # Step 2: Use provided run_id or generate one
        logger.info("  [STEP 2] Using run ID...")
        run_id_start = time.time()
        if run_id is None:
            run_id = int(time.time())
            logger.info(f"    [INFO] No run_id provided, using timestamp: {run_id}")
        else:
            logger.info(f"    [INFO] Using provided run_id: {run_id}")
        run_id_time = time.time() - run_id_start
        logger.info(f"    [OK] Run ID setup: {run_id_time:.2f} seconds")
        
        # Step 3: Store forecast configuration (commented out for now)
        logger.info("  [STEP 3] Storing configuration...")
        config_start = time.time()
        # write_forecast_configuration(run_id, forecast_start_date, forecast_horizon)
        config_time = time.time() - config_start
        logger.info(f"    [OK] Configuration storage: {config_time:.2f} seconds")
        
        # Step 4: Run Prophet forecasts
        logger.info("  [STEP 4] Running forecasting algorithms...")
        forecast_start = time.time()
        # Use configured parameters
        forecast_params = get_forecast_params()
        forecast_params["FORECAST_START_DATE"] = forecast_start_date
        forecast_params["FORECAST_HORIZON"] = forecast_horizon
        forecast_data = run_prophet_forecast(qualified_data, forecast_params)
        forecast_time = time.time() - forecast_start
        logger.info(f"    [OK] Prophet forecasting: {forecast_time:.2f} seconds")
        
        # Step 5: Write forecast movement (commented out for now)
        logger.info("  [STEP 5] Writing forecast results...")
        write_start = time.time()
        # write_forecast_movement(forecast_data, run_id, None)
        write_time = time.time() - write_start
        logger.info(f"    [OK] Forecast movement writing: {write_time:.2f} seconds")
        
        total_time = time.time() - total_start_time
        logger.info(f"  [TIME] Total forecasting time: {total_time:.2f} seconds")
        logger.info("  [SUCCESS] Forecast pipeline completed successfully")
        return forecast_data
        
    except Exception as e:
        logger.error(f"Forecast pipeline failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise


def filter_non_eligible_categories(processed_data: DataFrame, forecast_start_date: str) -> DataFrame:
    """
    Filter out products that are not eligible for forecasting based on configuration
    
    Non-eligible categories:
    - Age categories: 00| Draft (products that haven't been launched yet)
    - Sales categories: 01| Dead (products with no recent sales)
    """
    logger.info("Filtering non-eligible categories for forecasting")
    filtering_start = time.time()
    
    # Get non-eligible categories from configuration
    non_eligible_age = CONFIG.NON_ELIGIBLE_CATEGORIES["age_categories"]
    non_eligible_sales = CONFIG.NON_ELIGIBLE_CATEGORIES["sales_categories"]
    
    logger.info(f"Non-eligible age categories: {non_eligible_age}")
    logger.info(f"Non-eligible sales categories: {non_eligible_sales}")
    
    # Step 1: Select only needed columns from processed data
    column_selection_start = time.time()
    required_columns = [
        "product_id", "date", "sales_units", "age_category", "sales_category", "age_sales_category", "style"
    ]
    
    # Filter to only include required columns
    processed_subset = processed_data.select(*required_columns)
    column_selection_time = time.time() - column_selection_start
    logger.info(f"Column selection: {column_selection_time:.2f} seconds")
    
    # Step 2: Filter to historical data only (up to forecast_start_date - 1)
    date_filtering_start = time.time()
    history_end_date = F.date_sub(F.lit(forecast_start_date), 1)
    logger.info(f"Configured history end date: {history_end_date}")
    
    # Use configured history_end_date directly
    logger.info(f"Using configured history end date: {CONFIG.HISTORY_END_DATE}")
    processed_subset = processed_subset.filter(F.col("date") <= F.lit(CONFIG.HISTORY_END_DATE))
    date_filtering_time = time.time() - date_filtering_start
    logger.info(f"Date filtering: {date_filtering_time:.2f} seconds")
    
    # Step 3: Filter out non-eligible products based on their categories on history end date
    product_filtering_start = time.time()
    logger.info(f"FILTERING: Starting product filtering for forecast_start_date: {forecast_start_date}")
    
    # Debug: Check data size before filtering - use cache to avoid recomputation
    total_records_start = time.time()
    # No caching - process data directly
    total_records = processed_subset.count()
    total_records_time = time.time() - total_records_start
    logger.info(f"FILTERING: Total records in processed_subset: {total_records} (took {total_records_time:.2f}s)")
    
    # Debug: Check unique products
    unique_products_start = time.time()
    unique_products_count = processed_subset.select("product_id").distinct().count()
    unique_products_time = time.time() - unique_products_start
    logger.info(f"FILTERING: Unique products: {unique_products_count} (took {unique_products_time:.2f}s)")
    
    # Debug: Check date range
    date_range_start = time.time()
    date_range = processed_subset.select(F.min("date"), F.max("date")).collect()[0]
    date_range_time = time.time() - date_range_start
    logger.info(f"FILTERING: Date range: {date_range[0]} to {date_range[1]} (took {date_range_time:.2f}s)")
    
    # Simple filtering: get products on the configured history end date with eligible categories
    logger.info(f"FILTERING: Starting on_forecast_qualification filtering...")
    qualification_start = time.time()
    
    # Use configured history end date for category filtering
    logger.info(f"FILTERING: Using configured history end date: {CONFIG.HISTORY_END_DATE}")
    
    on_forecast_qualification = (processed_subset
        .filter(F.col("date") == F.lit(CONFIG.HISTORY_END_DATE))
        .filter(~F.col("age_category").isin(non_eligible_age))
        .filter(~F.col("sales_category").isin(non_eligible_sales))
        .select("product_id", "age_sales_category", "sales_category", "age_category", "style")
        .withColumnRenamed("age_sales_category", "on_forecast_age_sales_category")
        .withColumnRenamed("sales_category", "on_forecast_sales_category") 
        .withColumnRenamed("age_category", "on_forecast_age_category")
    )
    
    # No caching - process data directly
    
    # Debug: Check qualification results
    qualification_count_start = time.time()
    qualification_count = on_forecast_qualification.count()
    qualification_count_time = time.time() - qualification_count_start
    qualification_time = time.time() - qualification_start
    logger.info(f"FILTERING: Qualified products: {qualification_count} (took {qualification_time:.2f}s, count took {qualification_count_time:.2f}s)")
    
    # Join back to get all historical data for qualified products
    logger.info(f"FILTERING: Starting join operation...")
    join_start = time.time()
    qualified_products = processed_subset.join(on_forecast_qualification, on="product_id", how="inner")
    join_time = time.time() - join_start
    logger.info(f"FILTERING: Join completed (took {join_time:.2f}s)")
    
    # Debug: Check if on_forecast columns are present
    columns_after_join = qualified_products.columns
    logger.info(f"FILTERING: Columns after join: {columns_after_join}")
    on_forecast_cols = [col for col in columns_after_join if col.startswith('on_forecast_')]
    logger.info(f"FILTERING: On_forecast columns found: {on_forecast_cols}")
    
    # No caching - process data directly
    
    qualified_count = qualified_products.count()
    logger.info(f"Qualified products after all filters: {qualified_count}")
    
    if qualified_count == 0:
        logger.warning("No qualified products found! Debugging filters:")
        logger.warning(f"  Date filter: date == {CONFIG.HISTORY_END_DATE} (configured history end date)")
        logger.warning(f"  Age filter: age_category NOT IN {non_eligible_age}")
        logger.warning(f"  Sales filter: sales_category NOT IN {non_eligible_sales}")
        
        # Check what categories exist in the data on configured history end date
        last_date_data = processed_subset.filter(F.col("date") == F.lit(CONFIG.HISTORY_END_DATE))
        last_date_count = last_date_data.count()
        if last_date_count > 0:
            age_categories = last_date_data.select("age_category").distinct().collect()
            sales_categories = last_date_data.select("sales_category").distinct().collect()
            logger.warning(f"  Available age categories: {[row.age_category for row in age_categories]}")
            logger.warning(f"  Available sales categories: {[row.sales_category for row in sales_categories]}")
    product_filtering_time = time.time() - product_filtering_start
    logger.info(f"Product filtering: {product_filtering_time:.2f} seconds")
    
    # Step 3: Join back to subset to get all historical data for qualified products
    joining_start = time.time()
    # Use qualified_products (which already has all the data) instead of joining back
    qualified_data = qualified_products
    joining_time = time.time() - joining_start
    logger.info(f"Data joining: {joining_time:.2f} seconds")
    
    # Cache the final qualified data
    # No caching - process data directly
    
    # Step 4: Count qualified products
    counting_start = time.time()
    qualified_count = qualified_data.select('product_id').distinct().count()
    counting_time = time.time() - counting_start
    logger.info(f"Product counting: {counting_time:.2f} seconds")
    
    total_filtering_time = time.time() - filtering_start
    logger.info(f"Found {qualified_count} qualified products for forecasting.")
    logger.info(f"Total filtering time: {total_filtering_time:.2f} seconds")
    
    return qualified_data

def get_forecast_params() -> Dict[str, Any]:
    """
    Get forecast parameters from configuration
    """
    return CONFIG.FORECAST_CONFIG

def croston_sba(y: np.ndarray, n_periods: int, alpha: float = 0.4) -> np.ndarray:
    """
    Croston's SBA method for intermittent demand forecasting
    """
    if len(y) == 0:
        return np.zeros(n_periods)
    
    # Separate demand and intervals
    demand = y[y > 0]
    intervals = []
    
    if len(demand) == 0:
        return np.zeros(n_periods)
    
    # Calculate intervals between non-zero demands
    last_demand_idx = 0
    for i in range(1, len(y)):
        if y[i] > 0:
            intervals.append(i - last_demand_idx)
            last_demand_idx = i
    
    if len(intervals) == 0:
        return np.zeros(n_periods)
    
    # Initial estimates
    if len(demand) == 1:
        avg_demand = demand[0]
        avg_interval = len(y)
    else:
        avg_demand = np.mean(demand)
        avg_interval = np.mean(intervals)
    
    # Croston's method
    forecast = avg_demand / avg_interval
    
    # SBA bias correction
    forecast = forecast * (1 - alpha / 2)
    
    return np.full(n_periods, forecast)

def extrapolate_regressors(pdf: pd.DataFrame, future: pd.DataFrame, regressors: list) -> pd.DataFrame:
    """
    Extrapolate regressors into the forecast horizon
    """
    for reg in regressors:
        if reg in pdf.columns:
            # Use the last known value for categorical regressors
            last_value = pdf[reg].iloc[-1] if len(pdf) > 0 else 0
            future[reg] = last_value
        else:
            future[reg] = 0
    return future

def run_prophet_forecast(qualified_data: DataFrame, params: Dict) -> DataFrame:
    """
    Run Prophet forecasting for all qualified products with lifecycle-aware configurations
    """
    logger.info("Running Prophet forecasts")
    forecast_start_time = time.time()
    
    FORECAST_START_DATE = params["FORECAST_START_DATE"]
    FORECAST_HORIZON = params["FORECAST_HORIZON"]
    MIN_OBS_FOR_PROPHET = params.get("MIN_OBS_FOR_PROPHET", 8)
    AGE_SALES_CATEGORY_CONFIG = params.get("AGE_SALES_CATEGORY_CONFIG", {})
    
    # Default configuration
    DEFAULT_CONFIG = {
        "LOOK_BACK_HORIZON": 60,
        "n_changepoints": 1,
        "changepoint_prior_scale": 0.05,
        "OVER_FORECAST_FACTOR": 1.1,
        "NUMERIC_REGRESSORS": [],
        "CATEGORICAL_REGRESSORS": [],
    }
    
    # Step 1: Get unique products to process in batches
    logger.info("  [PROPHET] Starting Prophet forecasting pipeline")
    unique_products_start = time.time()
    logger.info("  [PROPHET] Getting unique product IDs")
    unique_products = qualified_data.select("product_id").distinct().collect()
    unique_products_time = time.time() - unique_products_start
    product_ids = [row.product_id for row in unique_products]
    logger.info(f"  [PROPHET] Found {len(product_ids)} unique products (took {unique_products_time:.2f}s)")
    
    # Debug: Check qualified data size
    qualified_data_count_start = time.time()
    logger.info("  [PROPHET] Counting qualified data records")
    qualified_data_count = qualified_data.count()
    qualified_data_count_time = time.time() - qualified_data_count_start
    logger.info(f"  [PROPHET] Qualified data records: {qualified_data_count} (took {qualified_data_count_time:.2f}s)")
    
    logger.info(f"  [PROPHET] Processing {len(product_ids)} products using batch processing...")
    
    # Process all products in parallel using Spark's mapPartitions
    # This eliminates batching and uses all CPU cores efficiently
    logger.info("  [PROPHET] Using batch processing for Prophet models")
    
    # Initialize variables for the current batching approach (to be replaced)
    all_forecasts = []
    batch_size = 20  # Reduced batch size for better memory management
    
    # TODO: Replace this batching approach with parallel processing using mapPartitions
    # Current approach: Sequential processing in batches (SLOW)
    # Better approach: Use Spark's mapPartitions for true parallel processing (FAST)
    for i in range(0, len(product_ids), batch_size):
        batch_start_time = time.time()
        batch_products = product_ids[i:i + batch_size]
        batch_num = i//batch_size + 1
        total_batches = (len(product_ids) + batch_size - 1)//batch_size
        logger.info(f"PROPHET: Starting batch {batch_num}/{total_batches} with {len(batch_products)} products")
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_products)} products)")
        
        # Filter to current batch
        batch_filter_start = time.time()
        batch_data = qualified_data.filter(F.col("product_id").isin(batch_products))
        batch_filter_time = time.time() - batch_filter_start
        logger.info(f"PROPHET: Batch {batch_num} filtering took {batch_filter_time:.2f}s")
        
        # Convert to pandas for Prophet processing
        pandas_start = time.time()
        df = batch_data.toPandas()
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.rename(columns={'date': 'ds', 'sales_units': 'sales_units'})
        pandas_time = time.time() - pandas_start
        logger.info(f"PROPHET: Batch {batch_num} pandas conversion took {pandas_time:.2f}s")
        logger.info(f"  Batch data conversion to pandas: {pandas_time:.2f} seconds")
        
        # Step 2: Ensure regressors are numeric
        regressor_start = time.time()
        all_regressors = []
        for config in AGE_SALES_CATEGORY_CONFIG.values():
            all_regressors.extend(config.get("NUMERIC_REGRESSORS", []))
            all_regressors.extend(config.get("CATEGORICAL_REGRESSORS", []))
        
        for col in set(all_regressors):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        regressor_time = time.time() - regressor_start
        logger.info(f"  Batch regressor preparation: {regressor_time:.2f} seconds")
        
        # Step 3: Process each product group in this batch
        processing_start = time.time()
        
        # Debug: Check available columns
        logger.info(f"  Available columns in batch df: {list(df.columns)}")
        logger.info(f"  Batch DataFrame shape: {df.shape}")
        
        # Check for missing required columns
        required_cols = ['product_id', 'on_forecast_age_sales_category', 'on_forecast_age_category', 'on_forecast_sales_category', 'style']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"  Missing required columns: {missing_cols}")
        
        # Group by product_id only first to see what we have
        product_groups = df.groupby('product_id')
        total_products = len(product_groups)
        processed_products = 0
        skipped_products = 0
        
        logger.info(f"  Processing {total_products} products in this batch...")
        
        # Group by product_id only
        for product_id, pdf in product_groups:
            product_start_time = time.time()
            processed_products += 1
            logger.info(f"PROPHET: Batch {batch_num} - Processing product {processed_products}/{total_products}: {product_id}")
            
            # Log progress every 10 products
            if processed_products % 10 == 0:
                logger.info(f"    Processed {processed_products}/{total_products} product groups ({processed_products/total_products*100:.1f}%)")
        
            pdf = pdf.sort_values('ds').reset_index(drop=True)
            
            # Keep extra identifiers - use on_forecast categories (from history end date)
            age_cat = pdf['on_forecast_age_category'].iloc[-1]  # Last record (history end date)
            sales_cat = pdf['on_forecast_sales_category'].iloc[-1]
            age_sales_cat = pdf['on_forecast_age_sales_category'].iloc[-1]
            style = pdf['style'].iloc[-1] if 'style' in pdf.columns else None
        
            # Ensure we have string values, not Series
            if hasattr(age_cat, 'iloc'):
                age_cat = age_cat.iloc[0] if len(age_cat) > 0 else str(age_cat)
            if hasattr(sales_cat, 'iloc'):
                sales_cat = sales_cat.iloc[0] if len(sales_cat) > 0 else str(sales_cat)
            if hasattr(age_sales_cat, 'iloc'):
                age_sales_cat = age_sales_cat.iloc[0] if len(age_sales_cat) > 0 else str(age_sales_cat)
            if hasattr(style, 'iloc'):
                style = style.iloc[0] if len(style) > 0 else str(style)
            
            # Log product details
            logger.info(f"    Processing product {product_id}: age={age_cat}, sales={sales_cat}, age_sales={age_sales_cat}, style={style}, records={len(pdf)}")
            
            # Merge defaults with specific config
            cfg = {**DEFAULT_CONFIG, **AGE_SALES_CATEGORY_CONFIG.get(age_sales_cat, {})}
            
            # Regressors for this category
            NUMERIC_REGRESSORS = cfg.get("NUMERIC_REGRESSORS", [])
            CATEGORICAL_REGRESSORS = cfg.get("CATEGORICAL_REGRESSORS", [])
            ALL_REGRESSORS = NUMERIC_REGRESSORS + CATEGORICAL_REGRESSORS
            
            # Training window - exclude forecast start date to avoid data leakage
            train = pdf[pdf['ds'] < pd.to_datetime(FORECAST_START_DATE)].tail(cfg["LOOK_BACK_HORIZON"]).copy()
            train = train.rename(columns={"sales_units": "y"})
            
            logger.info(f"      Training data: {len(train)} records (forecast_start={FORECAST_START_DATE}, look_back={cfg['LOOK_BACK_HORIZON']})")
            logger.info(f"      Training columns: {list(train.columns)}")
            logger.info(f"      Training sales sum: {train['y'].sum()}, mean: {train['y'].mean()}")
            logger.info(f"      Training date range: {train['ds'].min()} to {train['ds'].max()}")
            
            if len(train) < MIN_OBS_FOR_PROPHET:
                logger.warning(f"      Insufficient data for {product_id} ({len(train)} obs < {MIN_OBS_FOR_PROPHET} required), skipping.")
                skipped_products += 1
                continue
                
            if "y" in train.columns:
                zeros_ratio = (train["y"].to_numpy() == 0).mean()
                logger.info(f"      [DATA] Zero ratio in training: {zeros_ratio:.2f}")
                logger.info(f"      [DATA] Training data sample: {train[['ds', 'y']].head(3).to_dict()}")
                logger.info(f"      [DATA] Training data shape: {train.shape}")
                logger.info(f"      [DATA] Training columns: {list(train.columns)}")
            else:
                zeros_ratio = 0
                logger.warning(f"      [ERROR] No 'y' column in training data!")
            
            # Use Croston for high zero ratio OR insufficient data (faster than Prophet)
            croston_threshold = CONFIG.FORECAST_CONFIG["CORSTON_ZEROS_RATIO"]
            if zeros_ratio >= croston_threshold:
                logger.info(f"      [CROSTON] Using Croston method for {product_id} (zero ratio: {zeros_ratio:.2f}, data points: {len(train)})")
                fc = croston_sba(train['y'].values, FORECAST_HORIZON)
                fc = fc * cfg.get("OVER_FORECAST_FACTOR", 1.0)
                df_res = pd.DataFrame({
                    "product_id": product_id,
                    "age_sales_category": age_sales_cat,
                    "age_category": age_cat,
                    "sales_category": sales_cat,
                    "style": style,
                    "forecast_method": "croston",
                    "date": pd.date_range(start=FORECAST_START_DATE, periods=FORECAST_HORIZON).strftime('%Y-%m-%d'),
                    "forecast": fc
                })
                all_forecasts.append(df_res)
                logger.info(f"      [CROSTON] Added Croston forecast for {product_id}")
                continue
            
            # Future dataframe
            future_dates = pd.date_range(start=FORECAST_START_DATE, periods=FORECAST_HORIZON)
            future = pd.DataFrame({"ds": future_dates})
            
            # Extrapolate regressors into the forecast horizon
            future = extrapolate_regressors(pdf, future, ALL_REGRESSORS)
            
            # Try Prophet first, fallback to Croston if it fails
            logger.info(f"      [PROPHET] Starting Prophet attempt for {product_id}...")
            logger.info(f"      [PROPHET] Training data shape: {train.shape}")
            logger.info(f"      [PROPHET] Training data types: {train.dtypes.to_dict()}")
            logger.info(f"      [PROPHET] Zero ratio: {zeros_ratio:.2f} (threshold: {croston_threshold})")
            
            try:
                from prophet import Prophet
                
                # Clean training data - remove duplicate columns and ensure proper format
                train_clean = train[['ds', 'y'] + [r for r in ALL_REGRESSORS if r in train.columns]].copy()
                train_clean = train_clean.loc[:, ~train_clean.columns.duplicated()]
                
                # Ensure all regressors are numeric
                for col in train_clean.columns:
                    if col not in ['ds', 'y']:
                        train_clean[col] = pd.to_numeric(train_clean[col], errors='coerce').fillna(0)
                
                logger.info(f"    Clean training data shape: {train_clean.shape}")
                logger.info(f"    Clean training columns: {list(train_clean.columns)}")
                
                # Configure Prophet model (matching working notebook implementation)
                model = Prophet(
                    growth="linear",
                    seasonality_mode="multiplicative",
                    daily_seasonality=False,
                    interval_width=0.80,
                    uncertainty_samples=0,  # Disable uncertainty sampling to avoid broadcasting issues
                    changepoint_prior_scale=cfg.get("changepoint_prior_scale", 0.05)
                )
                
                # Add regressors
                for reg in ALL_REGRESSORS:
                    if reg in train_clean.columns and reg not in ['ds', 'y']:
                        logger.info(f"    Adding regressor: {reg}")
                        model.add_regressor(reg)
                
                # Fit the model
                logger.info(f"    Fitting Prophet model...")
                model.fit(train_clean)
                logger.info(f"    Prophet model fitted successfully")
                
                # Prepare future data
                future_clean = future[['ds'] + [r for r in ALL_REGRESSORS if r in future.columns]].copy()
                future_clean = future_clean.loc[:, ~future_clean.columns.duplicated()]
                
                # Ensure all regressors are numeric
                for col in future_clean.columns:
                    if col != 'ds':
                        future_clean[col] = pd.to_numeric(future_clean[col], errors='coerce').fillna(0)
                
                logger.info(f"    Future data shape: {future_clean.shape}")
                logger.info(f"    Future columns: {list(future_clean.columns)}")
                
                # Make predictions
                logger.info(f"    Making predictions...")
                forecast = model.predict(future_clean)
                yhat = forecast['yhat'].values
                logger.info(f"    Predictions made: {len(yhat)} values, range: {yhat.min():.2f} to {yhat.max():.2f}")
                logger.info(f"    Forecast DataFrame shape: {forecast.shape}")
                logger.info(f"    Forecast columns: {list(forecast.columns)}")
                logger.info(f"    Sample forecast values: {yhat[:5]}")
                logger.info(f"    Contains NaN: {np.isnan(yhat).any()}")
                
                # Apply over-forecast factor and ensure non-negative
                yhat = np.array(yhat) * cfg.get("OVER_FORECAST_FACTOR", 1.0)
                yhat = np.maximum(yhat, 0)  # Ensure non-negative values
                
                df_res = pd.DataFrame({
                    "product_id": product_id,
                    "age_sales_category": age_sales_cat,
                    "age_category": age_cat,
                    "sales_category": sales_cat,
                    "style": style,
                    "forecast_method": "prophet",
                    "date": future_clean["ds"].dt.strftime('%Y-%m-%d'),
                    "forecast": yhat
                })
                
                logger.info(f"      [PROPHET] SUCCESS: Using Prophet method for {product_id}")
                logger.info(f"      [PROPHET] Forecast values: {yhat[:5]}...")
                
            except Exception as e:
                logger.warning(f"      [PROPHET] FAILED for {product_id}: {str(e)}")
                logger.warning(f"      [PROPHET] Error type: {type(e).__name__}")
                import traceback
                logger.warning(f"      [PROPHET] Traceback: {traceback.format_exc()}")
                
                # Fallback to Croston
                logger.info(f"      [FALLBACK] Using Croston fallback for {product_id}")
                fc = croston_sba(train['y'].values, FORECAST_HORIZON)
                fc = fc * cfg.get("OVER_FORECAST_FACTOR", 1.0)
                
                df_res = pd.DataFrame({
                    "product_id": product_id,
                    "age_sales_category": age_sales_cat,
                    "age_category": age_cat,
                    "sales_category": sales_cat,
                    "style": style,
                    "forecast_method": "croston",
                    "date": pd.date_range(start=FORECAST_START_DATE, periods=FORECAST_HORIZON).strftime('%Y-%m-%d'),
                    "forecast": fc
                })
                logger.info(f"      [FALLBACK] Added Croston forecast for {product_id}")
            
            all_forecasts.append(df_res)
        
        processing_time = time.time() - processing_start
        batch_total_time = time.time() - batch_start_time
        logger.info(f"PROPHET: Batch {batch_num} completed - Processing: {processing_time:.2f}s, Total: {batch_total_time:.2f}s")
        logger.info(f"  Batch processing: {processing_time:.2f} seconds")
        logger.info(f"  Processed {len(batch_products)} products in this batch")
    
    logger.info(f"Generated {len(all_forecasts)} total forecast groups")
    
    if len(all_forecasts) == 0:
        logger.warning("No forecasts generated! Possible reasons:")
        logger.warning(f"  - Total products processed: {qualified_data.count()}")
        logger.warning(f"  - Check if products have sufficient training data (min {MIN_OBS_FOR_PROPHET} obs)")
        logger.warning(f"  - Check if forecast start date {FORECAST_START_DATE} is correct")
        logger.warning(f"  - Check if products are being filtered out by non-eligible categories")
    
    # Step 4: Convert results back to Spark DataFrame
    conversion_start = time.time()
    if not all_forecasts:
        logger.warning("No forecasts generated.")
        return qualified_data.sparkSession.createDataFrame([], schema="""
            product_id string, 
            age_sales_category string, 
            age_category string, 
            sales_category string, 
            style string,
            forecast_method string,
            date string, 
            forecast double
        """)
    
    # Convert back to Spark DataFrame
    final_spark_df = qualified_data.sparkSession.createDataFrame(pd.concat(all_forecasts, ignore_index=True))
    conversion_time = time.time() - conversion_start
    logger.info(f"Result conversion to Spark: {conversion_time:.2f} seconds")
    
    total_forecast_time = time.time() - forecast_start_time
    logger.info(f"Total Prophet forecasting time: {total_forecast_time:.2f} seconds")
    
    return final_spark_df

def write_forecast_movement(forecast_data: DataFrame, run_id: int, client: SupabaseClient):
    """
    Write forecast results (commented out for now)
    """
    logger.info(f"Writing forecast movement for run_id: {run_id}")
    
    # TODO: Implement when database tables are available
    # Convert to pandas for writing
    # pandas_df = forecast_data.toPandas()
    # 
    # # Map to forecast_movement table schema
    # forecast_df = pandas_df.rename(columns={
    #     'forecast_units': 'forecast',
    #     'forecast_date': 'date'
    # })
    # 
    # # Add required columns
    # forecast_df['id'] = range(len(forecast_df))
    # forecast_df['created_at'] = pd.Timestamp.now()
    # 
    # # Write to forecast_movement table
    # client.write_table(forecast_df, "forecast_movement", if_exists="append")
    
if __name__ == "__main__":
    print("=== Forecast Demand Module ===")
    print("This module provides demand forecasting functionality.")
    print("Database write operations are currently commented out.")
    print("Run forecast_demand() to start the forecasting process.")
