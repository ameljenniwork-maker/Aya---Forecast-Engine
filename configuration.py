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
HISTORY_START_DATE = "2025-04-20"  # Start of sales data
HISTORY_END_DATE = "2025-09-30"    # End of historical data (actual data availability)
# Compute forecast start date as the day after HISTORY_END_DATE (keep as string)
_history_end_dt = datetime.strptime(HISTORY_END_DATE, "%Y-%m-%d").date()
FORECAST_START_DATE = (_history_end_dt + timedelta(days=1)).strftime("%Y-%m-%d")



# Rolling sales window configuration
RECENT_SALES_UNITS_WINDOW = 14

# Salary period configuration
SALARY_START_DATE = 25  # 25th of month
SALARY_END_DATE = 4     # 4th of next month

# Age category configuration
AGE_CATEGORY_BINS = {
    "00| Draft": {"special": "before_first_sales"},
    "01| New": {"max_days": 7},
    "02| Launch": {"max_days": 14},
    "03| Growth": {"max_days": 30},
    "04| Mature": {"max_days": float('inf')}
}

# Sales category configuration
SALES_CATEGORY_BINS = {
    "00| Draft": {"special": "before_first_sales"},
    "01| Dead": {"max_sales": 0},
    "02| Very Low": {"max_multiplier": 1},
    "03| Low": {"max_multiplier": 2},
    "04| Alive": {"max_multiplier": 4},
    "05| Medium": {"max_multiplier": 6},
    "06| Winning": {"max_multiplier": 10},
    "07| High Winning": {"max_multiplier": float('inf')}
}

# Sales category summarization
SALES_CATEGORY_SUMMARY = {
    "00| Draft": ["00| Draft"],
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
        "00| Draft", "01| New"  # Products that haven't been launched yet
    ],
    "sales_categories": [
        "01| Dead"   # Products with no recent sales
    ]
}

# =============================================================================
# FORECASTING CONFIGURATION
# =============================================================================




FORECAST_CONFIG = {
    "FORECAST_START_DATE": FORECAST_START_DATE,  # Automatically calculated as HISTORY_END_DATE + 1
    "FORECAST_HORIZON": 14,
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
            "OVER_FORECAST_FACTOR": 1.1,
            "NUMERIC_REGRESSORS": [],
            "CATEGORICAL_REGRESSORS": [],
            "changepoint_prior_scale": 0.08
        },
        "02| LAUNCH_03| HIGH": {
            "LOOK_BACK_HORIZON": 7,
            "OVER_FORECAST_FACTOR": 1.15,
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
            "OVER_FORECAST_FACTOR": 1.2,
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
            "OVER_FORECAST_FACTOR": 1.2,
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
            "OVER_FORECAST_FACTOR": 1.2,
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
            "OVER_FORECAST_FACTOR": 1.2,
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

