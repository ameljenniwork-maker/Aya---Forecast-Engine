# Aya Forecast Engine

A Prophet-based demand forecasting system with lifecycle-aware configurations and calendar effects.

## Quick Start

### Prerequisites
- Python 3.10+
- Java JDK 8/11 (for PySpark)
- Microsoft C++ Build Tools (Windows) or `gcc g++ make` (Linux)

### Setup
```bash
# 1. Create virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1  # Windows
# source .venv/bin/activate  # Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure Supabase credentials in configuration.py
# Update SUPABASE_URL, SUPABASE_ANON_KEY, TEST_USER_EMAIL, TEST_USER_PASSWORD
```

### Usage
```bash
# Run the forecast engine
python forecast_engine.py

# Run evaluation notebook
jupyter notebook evaluate_forecast.ipynb
```

## Architecture

- **`forecast_engine.py`**: Main pipeline orchestrator
- **`functions_library/`**: Core modules
  - `data_processing.py`: Data aggregation and feature generation
  - `forecast_generation.py`: Prophet/Croston forecasting
  - `forecast_evaluation.py`: Performance analysis and visualization
  - `supabase_connection.py`: Database connectivity
- **`evaluate_forecast.ipynb`**: Interactive analysis notebook

## Features

- **Lifecycle-aware forecasting**: Different models for product maturity stages
- **Calendar effects**: Islamic holidays, salary periods, promotions
- **Dual forecasting methods**: Prophet for complex patterns, Croston for sparse data
- **Performance evaluation**: Bias analysis, error distribution, category performance

## Inputs and Outputs

### Inputs
- **Historical sales data**: Product sales history from Supabase database "Aya Supply"
- **Product information**: SKU details, categories, and lifecycle status from "Aya Supply"
- **Calendar data**: Islamic holidays, salary periods, and promotional events from "Aya Supply"
- **Configuration parameters**: Forecasting horizons, model parameters, and category thresholds

### Outputs
- **Preprocessed features**: Parquet files containing processed data up to history end date
- **Demand forecasts**: Incremental forecast runs saved in Supabase Storage "demand-forecast" bucket


## Configuration

Key settings in `configuration.py`:
- `HISTORY_START_DATE`: Start of historical data
- `HISTORY_END_DATE`: End of historical data  
- `FORECAST_HORIZON`: Days to forecast ahead
- `AGE_SALES_CATEGORY_CONFIG`: Lifecycle-specific parameters

## Category Definitions

### Age Categories (based on days in stock)
- **00| Draft**: `day_in_stock = 0` (before first sales)
- **01| New**: `day_in_stock <= 7` (first week)
- **02| Launch**: `day_in_stock <= 14` (first two weeks)
- **03| Growth**: `day_in_stock <= 30` (first month)
- **04| Mature**: `day_in_stock > 30` (beyond first month)

### Sales Categories (based on recent 14-day sales)
- **00| Draft**: `day_in_stock <= 0` (before first sales)
- **01| Dead**: `day_in_stock > 0 AND recent_sales_units = 0` (launched but no recent sales)
- **02| Very Low**: `recent_sales_units < 14` (less than 1 unit per day)
- **03| Low**: `recent_sales_units < 28` (less than 2 units per day)
- **04| Alive**: `recent_sales_units < 56` (less than 4 units per day)
- **05| Medium**: `recent_sales_units < 84` (less than 6 units per day)
- **06| Winning**: `recent_sales_units < 140` (less than 10 units per day)
- **07| High Winning**: `recent_sales_units >= 140` (10+ units per day)

### Non-Eligible Categories (filtered out from forecasting)
- **Age**: `00| Draft`, `01| New` (products that haven't been launched yet or are very new)
- **Sales**: `01| Dead` (products with no recent sales)

## Monitoring

### Log Files
- **Location**: `logs/run_{RUN_ID}_YYYYMMDD_HHMMSS.log`
- **Monitor for errors**: Check for `ERROR` or `WARNING` messages
- **Performance tracking**: Review step timing and data processing metrics
- **Debugging**: Detailed execution flow and Spark task information

