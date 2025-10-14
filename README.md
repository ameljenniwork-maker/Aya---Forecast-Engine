## Aya Forecast - Initial Setup

### Prerequisites
- Python 3.10+
- Git (optional)
- Java JDK 8 or 11 (required for `pyspark`)
  - Windows: set `JAVA_HOME` and add `%JAVA_HOME%\bin` to PATH
  - Linux: ensure `java -version` works
- Build tools for Prophet
  - Windows: Microsoft C++ Build Tools
  - Linux: `gcc g++ make`

### 1) Clone or open the project
```bash
# if not already in the folder
cd "Aya - Forecast"
```

### 2) Create and activate a virtual environment
```bash
# Windows (PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1

# Linux/macOS
python -m venv .venv
source .venv/bin/activate
```

### 3) Install Python dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4) Configure environment variables
Create a `.env` file in the project root with your Supabase and DB settings:
```bash
SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
SUPABASE_DB_URL=postgresql://user:password@host:5432/dbname

# Optional runtime params
FORECAST_START_DATE=2025-10-10
FORECAST_HORIZON=14
```

Notes:
- `SUPABASE_DB_URL` is used for fast bulk IO via SQLAlchemy.
- Keep the Service Role Key secure; never commit `.env` to source control.

### 5) Validate Java for Spark
```bash
java -version
```
If this fails, install JDK 8/11 and set `JAVA_HOME`.

### 6) Run notebooks (dev workflow)
Open `1_process.ipynb` and `2_forecast.ipynb` for exploration and development.

### 7) Next: Production runner (outline)
Create a small runner that reads from Supabase, executes the forecast, and writes back.
Suggested files:
- `config.py`: constants and env loading
- `data_io.py`: read/write with SQLAlchemy
- `forecast_engine.py`: orchestrates read → forecast → write with logging

Example snippets
```python
# data_io.py (example)
import os
import pandas as pd
from sqlalchemy import create_engine

def get_engine():
    return create_engine(os.environ["SUPABASE_DB_URL"])  # postgres URL

def read_training_data(forecast_start: str) -> pd.DataFrame:
    sql = """
    select sku,
           date::date as date,
           sales_units,
           on_forecast_age_sales_category,
           on_forecast_age_category,
           on_forecast_sales_category,
           marketing_cost, rank_score, conversion_rate,
           is_salary, is_ramadan, is_friday, is_promotion
    from public.daily_product_metrics
    where date <= %(forecast_start)s
    """
    return pd.read_sql(sql, get_engine(), params={"forecast_start": forecast_start})

def write_forecast(df: pd.DataFrame):
    df.to_sql("daily_product_forecasts", get_engine(), schema="public", if_exists="append", index=False)
```

### 8) Scheduling (when ready)
- Windows Task Scheduler: run `python forecast_engine.py` daily after the Supabase sync.
- Linux cron/systemd or containerized job.

### Troubleshooting
- Prophet build errors on Windows: install Microsoft C++ Build Tools, restart shell.
- Spark errors about Java: ensure `JAVA_HOME` and PATH are set to JDK.
- Postgres SSL/auth issues: verify `SUPABASE_DB_URL` format and network access.
