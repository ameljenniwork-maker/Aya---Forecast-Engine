"""
Centralized configuration for Aya Forecast Engine
"""

from datetime import datetime, timedelta

# =============================================================================
# SUPABASE CONFIGURATION
# =============================================================================
# NOTE: In production, replace these hardcoded values with environment variables or secrets service
SUPABASE_URL = "https://ulyuxtudmapnyeskjzmb.supabase.co"
SUPABASE_ANON_KEY = "sb_publishable_WktOhme4rqgmKB0Xc40l0g_VUWkNZ6T"
# Using Supabase client with anon key - no direct DB connection needed

# Test user credentials
TEST_USER_EMAIL = "test@mawsim.app"
TEST_USER_PASSWORD = "test123"

# =============================================================================
# SUPABASE STORAGE CONFIGURATION
# =============================================================================
STORAGE_CONFIG = {
    "BUCKET_NAME": "demand-forecast",
    "PROCESSED_FEATURES_PATH": "processed_features/",  # Will be appended with datetime
    "INCREMENTAL_FORECASTS_PATH": "incremental_forecasts/"
}

# =============================================================================
# DATABASE TABLE NAMES
# =============================================================================
TABLES = {
    "products": "products",
    "sales_movement": "sales_movement", 
    "inventory_movement": "inventory_movement",
    "calendar_effects": "calendar_effects",
    "calendar_hierarchy": "calendar_hierarchy",
}

# =============================================================================
# DATA PROCESSING PARAMETERS
# =============================================================================
# Date ranges for data processing
HISTORY_START_DATE = "2025-04-21"  # Start of sales data
HISTORY_END_DATE = "2025-08-29"    # End of historical data (actual data availability)
# Compute forecast start date as the day after HISTORY_END_DATE (keep as string)
_history_end_dt = datetime.strptime(HISTORY_END_DATE, "%Y-%m-%d").date()
FORECAST_START_DATE = (_history_end_dt + timedelta(days=1)).strftime("%Y-%m-%d")


# Salary period configuration
SALARY_START_DATE = 25  # 25th of month
SALARY_END_DATE = 4     # 4th of next month


# Rolling sales window configuration
RECENT_SALES_UNITS_WINDOW = 14
# Sales category configuration - simple multipliers based on RECENT_SALES_UNITS_WINDOW
# Categories are assigned based on recent_sales_units (sum of sales over the window period)
SALES_CATEGORY_MULTIPLIERS = {
    "00| Draft": 0,          # Before first sales (day_in_stock = 0 or no sales yet)
    "01| Dead": 0,           # <= 0 sales (no recent sales)
    "02| Very Low": 1,       # < 1 * RECENT_SALES_UNITS_WINDOW (e.g., < 14 sales)
    "03| Low": 2,            # < 2 * RECENT_SALES_UNITS_WINDOW (e.g., < 28 sales)
    "04| Alive": 4,          # < 4 * RECENT_SALES_UNITS_WINDOW (e.g., < 56 sales)
    "05| Medium": 6,         # < 6 * RECENT_SALES_UNITS_WINDOW (e.g., < 84 sales)
    "06| Winning": 10,       # < 10 * RECENT_SALES_UNITS_WINDOW (e.g., < 140 sales)
    "07| High Winning": float('inf')  # >= 10 * RECENT_SALES_UNITS_WINDOW (e.g., >= 140 sales)
}


# Age category configuration - simple day ranges based on day_in_stock
# Categories are assigned based on how many days the product has been in stock
AGE_CATEGORY_DAYS = {
    "00| Draft": 0,          # Before first sales (day_in_stock = 0 or no sales yet)
    "01| New": 7,            # <= 7 days in stock
    "02| Launch": 14,        # <= 14 days in stock
    "03| Growth": 30,        # <= 30 days in stock
    "04| Mature": float('inf')  # > 30 days in stock
}



# Sales category summarization
SALES_CATEGORY_SUMMARY = {
    "01| Low": ["01| Dead", "02| Very Low", "03| Low"],
    "02| Medium": ["04| Alive", "05| Medium"],
    "03| High": ["06| Winning", "07| High Winning"]
}


# =============================================================================
# FORECAST ELIGIBILITY CONFIGURATION
# =============================================================================
# Categories that are NOT eligible for forecasting
NON_ELIGIBLE_CATEGORIES = {
    "age_categories": [
        "00| Draft",  # Products in draft state
        "01| New"     # Products in their first week
    ],
    "sales_categories": [
        "01| Dead"    # Products with no recent sales
    ]
}

# =============================================================================
# FORECASTING CONFIGURATION
# =============================================================================




FORECAST_CONFIG = {
    "FORECAST_START_DATE": FORECAST_START_DATE,  # Automatically calculated as HISTORY_END_DATE + 1
    "FORECAST_HORIZON": 7,
    "MIN_OBS_FOR_PROPHET": 8,
    "CORSTON_ZEROS_RATIO": 0.6,  # Threshold for using Croston method instead of Prophet
    "AGE_SALES_CATEGORY_CONFIG": {
        # --- LAUNCH --- 7-14 days
        "02| LAUNCH_01| LOW": {
            "LOOK_BACK_HORIZON": 10,
            "OVER_FORECAST_FACTOR": 1.0,
            "NUMERIC_REGRESSORS": [],
            "CATEGORICAL_REGRESSORS": [],
            "changepoint_prior_scale": 0.001
        },
        "02| LAUNCH_02| MEDIUM": {
            "LOOK_BACK_HORIZON": 10,
            "OVER_FORECAST_FACTOR": 1.15,
            "NUMERIC_REGRESSORS": [],
            "CATEGORICAL_REGRESSORS": [],
            "changepoint_prior_scale": 0.08
        },
        "02| LAUNCH_03| HIGH": {
            "LOOK_BACK_HORIZON": 7,
            "OVER_FORECAST_FACTOR": 1.2,
            "NUMERIC_REGRESSORS": [],
            "CATEGORICAL_REGRESSORS": [],
            "changepoint_prior_scale": 0.1
        },
        # --- GROWTH ---
        "03| GROWTH_01| LOW": {
            "LOOK_BACK_HORIZON": 10,
            "OVER_FORECAST_FACTOR": 1.0,
            "NUMERIC_REGRESSORS": [],
            "CATEGORICAL_REGRESSORS": [],
            "changepoint_prior_scale": 0.001
        },
        "03| GROWTH_02| MEDIUM": {
            "LOOK_BACK_HORIZON": 30,
            "OVER_FORECAST_FACTOR": 1.3,
            "NUMERIC_REGRESSORS": [],
            "CATEGORICAL_REGRESSORS": [
                "EID_ADHA_DAY_0", "EID_ADHA_DAY_PLUS_1",
                "EID_FITR_DAY_0", "EID_FITR_DAY_MINUS_1",
                "ISLAMIC_NEW_YEAR_DAY_0", "ISLAMIC_NEW_YEAR_DAY_PLUS_1",
                "BLACK_FRIDAY_DAY_0",
                "NATIONAL_DAY_DAY_0",
                "ARAFAT_DAY_DAY_0",
                "WHITE_FRIDAY_DAY_0"
            ],
            "changepoint_prior_scale": 0.1
        },
        "03| GROWTH_03| HIGH": {
            "LOOK_BACK_HORIZON": 30,
            "OVER_FORECAST_FACTOR": 1.3,
            "NUMERIC_REGRESSORS": [],
            "CATEGORICAL_REGRESSORS": [
                "EID_ADHA_DAY_0", "EID_ADHA_DAY_PLUS_1",
                "EID_FITR_DAY_0", "EID_FITR_DAY_MINUS_1",
                "ISLAMIC_NEW_YEAR_DAY_0", "ISLAMIC_NEW_YEAR_DAY_PLUS_1",
                "BLACK_FRIDAY_DAY_0",
                "NATIONAL_DAY_DAY_0",
                "ARAFAT_DAY_DAY_0",
                "WHITE_FRIDAY_DAY_0"
            ],
            "changepoint_prior_scale": 0.2
        },
        # --- MATURE ---
        "04| MATURE_01| LOW": {
            "LOOK_BACK_HORIZON": 60,
            "OVER_FORECAST_FACTOR": 1.0,
            "NUMERIC_REGRESSORS": [],
            "CATEGORICAL_REGRESSORS": [
                "EID_ADHA_DAY_0", "EID_ADHA_DAY_PLUS_1",
                "EID_FITR_DAY_0", "EID_FITR_DAY_MINUS_1",
                "ISLAMIC_NEW_YEAR_DAY_0", "ISLAMIC_NEW_YEAR_DAY_PLUS_1",
                "BLACK_FRIDAY_DAY_0",
                "NATIONAL_DAY_DAY_0",
                "ARAFAT_DAY_DAY_0",
                "WHITE_FRIDAY_DAY_0"
            ],
            "changepoint_prior_scale": 0.005
        },
        "04| MATURE_02| MEDIUM": {
            "LOOK_BACK_HORIZON": 60,
            "OVER_FORECAST_FACTOR": 1.3,
            "NUMERIC_REGRESSORS": [],
            "CATEGORICAL_REGRESSORS": [
                "SALARY_PERIOD",
                "EID_ADHA_DAY_0", "EID_ADHA_DAY_PLUS_1",
                "EID_FITR_DAY_0", "EID_FITR_DAY_MINUS_1",
                "ISLAMIC_NEW_YEAR_DAY_0", "ISLAMIC_NEW_YEAR_DAY_PLUS_1",
                "BLACK_FRIDAY_DAY_0",
                "NATIONAL_DAY_DAY_0",
                "ARAFAT_DAY_DAY_0",
                "WHITE_FRIDAY_DAY_0"
            ],
            "n_changepoints": 4,
            "changepoint_prior_scale": 0.15
        },
        "04| MATURE_03| HIGH": {
            "LOOK_BACK_HORIZON": 60,
            "OVER_FORECAST_FACTOR": 1.3,
            "NUMERIC_REGRESSORS": [],
            "CATEGORICAL_REGRESSORS": [
                "SALARY_PERIOD",
                "EID_ADHA_DAY_0", "EID_ADHA_DAY_PLUS_1",
                "EID_FITR_DAY_0", "EID_FITR_DAY_MINUS_1",
                "ISLAMIC_NEW_YEAR_DAY_0", "ISLAMIC_NEW_YEAR_DAY_PLUS_1",
                "BLACK_FRIDAY_DAY_0",
                "NATIONAL_DAY_DAY_0",
                "ARAFAT_DAY_DAY_0",
                "WHITE_FRIDAY_DAY_0"
            ],
            "changepoint_prior_scale": 0.2
        }
    }
}

