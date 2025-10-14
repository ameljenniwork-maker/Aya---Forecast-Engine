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

## Configuration

Key settings in `configuration.py`:
- `HISTORY_START_DATE`: Start of historical data
- `HISTORY_END_DATE`: End of historical data  
- `FORECAST_HORIZON`: Days to forecast ahead
- `AGE_SALES_CATEGORY_CONFIG`: Lifecycle-specific parameters

## Monitoring

### Log Files
- **Location**: `logs/forecast_run{RUN_ID}_YYYYMMDD_HHMMSS.log`
- **Monitor for errors**: Check for `ERROR` or `WARNING` messages
- **Performance tracking**: Review step timing and data processing metrics
- **Debugging**: Detailed execution flow and Spark task information

