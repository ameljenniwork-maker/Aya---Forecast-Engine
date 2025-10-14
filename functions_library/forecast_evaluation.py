"""
Forecast Evaluation Functions

This module contains functions for evaluating forecast performance,
including bias calculation, error distribution analysis, and visualization.
"""

# Standard library imports
from typing import Optional

# Third-party imports
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# PySpark imports
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# Constants
PLOT_TEMPLATE = "plotly_white"

COLORS = {
    # Core metrics
    "sales_units": "#1f77b4",       # Blue
    "forecast": "#ff7f0e",          # Orange
    "baseline": "#2ca02c",          # Green
    "bias": "#D55E00",              # Orange-red
    "conversion_rate": "#56B4E9",   # Light blue
    "marketing_cost": "#E69F00",    # Yellow-orange
    "rank_score": "#009E73",        # Teal
    "total_views": "#F0E442",       # Yellow
    "active_skus_count": "#CC79A7", # Pink
    "business_avg_sales_units": "#000000", # Black
    "business_conversion_rate": "#000000", # Black
    
    # Status indicators
    "approved": "#2ca02c",          # Green
    "pending": "#ff7f0e",           # Orange
    "rejected": "#d62728",          # Red
    "on_track": "#2ca02c",          # Green
    "running_low": "#ff7f0e",       # Orange
    "selling_out": "#d62728",       # Red
    
    # Flags
    "is_salary": "#800080",         # Purple
    "is_ramadan": "#ff7f0e",        # Orange
    "is_friday": "#2ca02c",         # Green
    "is_promotion": "#1f77b4",      # Blue
    
    # UI elements
    "evaluation_lines": "#ff0000",  # Red
    "good_range_lines": "#0000ff",  # Blue
    "zero_line": "#ccccff",         # Light blue
    "background": "#ffffff",        # White
    "text": "#2f4f4f",              # Dark text
    "border": "#c0c0c0"             # Light border
}


def plot_attribute_distribution(df: DataFrame, group_by: list[str]) -> pd.DataFrame:
    """
    Plot attribute distribution showing product count and sales units by group.
    
    Args:
        df: Spark DataFrame containing the data
        group_by: List of column names to group by
        
    Returns:
        pandas.DataFrame: Aggregated results with counts, totals, and percentages
    """
    agg_df = (df.groupBy(group_by)
                .agg(
                    F.countDistinct("product_id").alias("product_count"),
                    F.sum("sales_units").alias("total_sales_units")
                )
                .orderBy(group_by)
                .toPandas())

    agg_df["product_count_pct"] = (agg_df["product_count"] / agg_df["product_count"].sum() * 100).round(1)
    agg_df["total_sales_units_pct"] = (agg_df["total_sales_units"] / agg_df["total_sales_units"].sum() * 100).round(1)

    headroom_factor = 1.2
    y_max = agg_df["product_count"].max() * headroom_factor
    y2_max = agg_df["total_sales_units"].max() * headroom_factor

    fig = go.Figure()

    # Primary y-axis (Product Count)
    fig.add_trace(go.Bar(
        x=agg_df[group_by[0]],
        y=agg_df["product_count"],
        name="Product Count",
        marker_color=COLORS["baseline"],
        text=agg_df["product_count_pct"].astype(str) + "%",
        textposition="outside",
        offsetgroup=1
    ))

    # Secondary y-axis (Total Sales Units)
    fig.add_trace(go.Bar(
        x=agg_df[group_by[0]],
        y=agg_df["total_sales_units"],
        name="Total Sales Units",  # keep in legend
        marker_color=COLORS["sales_units"],
        text=agg_df["total_sales_units_pct"].astype(str) + "%",
        textposition="outside",
        yaxis="y2",
        offsetgroup=2
    ))

    fig.update_layout(
        title=f"Distribution by {', '.join(group_by)}",
        xaxis=dict(title=", ".join(group_by)),
        yaxis=dict(title="Product Count", range=[0, y_max], automargin=True),
        yaxis2=dict(
            title="Total Sales Units",
            overlaying="y",
            side="right",
            range=[0, y2_max]
        ),
        height=500,
        barmode="group",
        template=PLOT_TEMPLATE,
        margin=dict(t=100),
        legend=dict(
            orientation="h",       # horizontal legend
            yanchor="bottom",
            y=1.02,               # place above plot
            xanchor="center",
            x=0.5
        )
    )
    return agg_df


def plot_business_level(
    all_data: DataFrame, 
    EVALUATION_START_DATE: str = "2025-07-01", 
    EVALUATION_END_DATE: str = "2025-07-15"
) -> pd.DataFrame:
    """
    Aggregate all_data to business level and plot Bias, Sales/Forecast,
    and distinct product counts per age_category.
    """
    # --- Business-level aggregation ---
    business_df = (
        all_data
        .groupBy("date")
        .agg(
            F.sum("sales_units").alias("sales_units"),
            F.sum("forecast").alias("forecast")
        )
        .withColumn(
            "bias",
            F.when(
                (F.col("sales_units").isNotNull()) &
                (F.col("forecast").isNotNull()) &
                (F.col("sales_units") > 0),
                (F.col("forecast") - F.col("sales_units")) / F.col("sales_units")
            ).otherwise(F.lit(None))
        )
        .orderBy("date")
    )
    pdf = business_df.toPandas()
    pdf["date"] = pd.to_datetime(pdf["date"])

    # --- Distinct products per date × age_category ---
    product_counts = (
        all_data
        .groupBy("date", "age_category")
        .agg(F.countDistinct("product_id").alias("distinct_products"))
        .toPandas()
    )
    product_counts["date"] = pd.to_datetime(product_counts["date"])

    # Pivot to have one column per age category
    product_pivot = (
        product_counts
        .pivot(index="date", columns="age_category", values="distinct_products")
        .fillna(0)
    ).reset_index()

    # --- Evaluation frame ---
    eval_start = pd.to_datetime(EVALUATION_START_DATE)
    eval_end = pd.to_datetime(EVALUATION_END_DATE)
    eval_mask = (pdf["date"] >= eval_start) & (pdf["date"] <= eval_end)
    total_sales = pdf.loc[eval_mask, "sales_units"].sum()
    total_forecast = pdf.loc[eval_mask, "forecast"].sum()
    diff_qty = total_forecast - total_sales
    diff_pct = (diff_qty / total_sales * 100) if total_sales != 0 else None

    print(f"Evaluation frame ({EVALUATION_START_DATE} - {EVALUATION_END_DATE}):")
    print(f"  Total Sales: {total_sales}")
    print(f"  Total Forecast: {total_forecast}")
    if diff_pct is not None:
        print(f"  Over/Under Forecast: {diff_qty} units ({diff_pct:.1f}%)")
    else:
        print(f"  Over/Under Forecast: {diff_qty} units (percent undefined, total sales is zero)")

    # --- Plotly setup with 3 rows ---
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.3, 0.7],  
        subplot_titles=("Business Bias (%)", "Sales / Forecast", "Distinct Products by Age Category")
    )

    # --- Bias subplot ---
    fig.add_trace(go.Bar(
        x=pdf["date"], y=pdf["bias"], name="Bias",
        marker_color=COLORS["bias"], opacity=0.7
    ), row=1, col=1)
    fig.update_yaxes(title_text="Bias (%)", row=1, col=1)

    # --- Sales / Forecast subplot ---
    fig.add_trace(go.Scatter(
        x=pdf["date"], y=pdf["sales_units"], mode="lines+markers",
        name="Sales Units", line=dict(color=COLORS["sales_units"])
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=pdf["date"], y=pdf["forecast"], mode="lines+markers",
        name="Forecast", line=dict(color=COLORS["forecast"], dash="dot")
    ), row=2, col=1)
    fig.update_yaxes(title_text="Sales Units", row=2, col=1)

    # --- Vertical lines for evaluation frame ---
    for d in [eval_start, eval_end]:
        for r in [1, 2]:
            fig.add_vline(x=d, line=dict(color=COLORS["evaluation_lines"], dash="dash"), row=r, col=1)

    fig.update_layout(title="Business-Level Sales, Forecast & Bias", height=600, width=1100, template=PLOT_TEMPLATE)

    return pdf


def calculate_bias_and_error_bins(
    df: DataFrame,
    target_column: str = "sales_units",
    predicted_column_name: str = "forecast"
) -> DataFrame:
    """
    Calculate forecast bias and signed error and assign bins for both metrics.
    Assumes data is already aggregated.

    Args:
        df: Spark DataFrame containing pre-aggregated data
        target_column: Name of the actual values column (default "sales_units")
        predicted_column_name: Name of the predicted values column (default "forecast")

    Returns:
        DataFrame: Spark DataFrame with bias calculations, error calculations, and bin assignments
    """
    
    # Use the data as-is since it's already aggregated
    agg_df = df
    
    # Bias calculation with epsilon handling for zero sales
    agg_df = agg_df.withColumn(
        "bias",
        F.when(F.col(target_column) > 0,
               (F.col(predicted_column_name) - F.col(target_column)) / F.col(target_column)
        ).when(F.col(target_column) == 0,
               (F.col(predicted_column_name) - F.col(target_column)) / 0.00001
        ).otherwise(0)
    )
    
    # Signed error calculation
    agg_df = agg_df.withColumn(
        "error",
        F.col(predicted_column_name) - F.col(target_column)
    )
    
    # Bias bins with full range names
    agg_df = agg_df.withColumn(
        "bias_bin",
        F.when(F.col("bias") < -1, "01| < -100")
         .when(F.col("bias") < -0.9, "02| -100 -90")
         .when(F.col("bias") < -0.8, "03| -90 -80")
         .when(F.col("bias") < -0.7, "04| -80 -70")
         .when(F.col("bias") < -0.6, "05| -70 -60")
         .when(F.col("bias") < -0.5, "06| -60 -50")
         .when(F.col("bias") < -0.4, "07| -50 -40")
         .when(F.col("bias") < -0.3, "08| -40 -30")
         .when(F.col("bias") < -0.2, "09| -30 -20")
         .when(F.col("bias") < -0.1, "10| -20 -10")
         .when(F.col("bias") < 0,    "11| -10 0")
         .when(F.col("bias") < 0.1,  "12| 0 +10")
         .when(F.col("bias") < 0.2,  "13| +10 +20")
         .when(F.col("bias") < 0.3,  "14| +20 +30")
         .when(F.col("bias") < 0.4,  "15| +30 +40")
         .when(F.col("bias") < 0.5,  "16| +40 +50")
         .when(F.col("bias") < 0.6,  "17| +50 +60")
         .when(F.col("bias") < 0.7,  "18| +60 +70")
         .when(F.col("bias") < 0.8,  "19| +70 +80")
         .when(F.col("bias") < 0.9,  "20| +80 +90")
         .when(F.col("bias") < 1,    "21| +90 +100")
         .otherwise("22| > +100")
    )
    
    # Error bins with 5-unit steps and full range names
    agg_df = agg_df.withColumn(
        "error_bin",
        F.when(F.col("error") < -50, "01| < -50")
         .when(F.col("error") < -45, "02| -50 -45")
         .when(F.col("error") < -40, "03| -45 -40")
         .when(F.col("error") < -35, "04| -40 -35")
         .when(F.col("error") < -30, "05| -35 -30")
         .when(F.col("error") < -25, "06| -30 -25")
         .when(F.col("error") < -20, "07| -25 -20")
         .when(F.col("error") < -15, "08| -20 -15")
         .when(F.col("error") < -10, "09| -15 -10")
         .when(F.col("error") < -5,  "10| -10 -5")
         .when(F.col("error") < 0,    "11| -5 0")
         .when(F.col("error") < 5,    "12| 0 +5")
         .when(F.col("error") < 10,   "13| +5 +10")
         .when(F.col("error") < 15,   "14| +10 +15")
         .when(F.col("error") < 20,   "15| +15 +20")
         .when(F.col("error") < 25,   "16| +20 +25")
         .when(F.col("error") < 30,   "17| +25 +30")
         .when(F.col("error") < 35,   "18| +30 +35")
         .when(F.col("error") < 40,   "19| +35 +40")
         .when(F.col("error") < 45,   "20| +40 +45")
         .when(F.col("error") < 50,   "21| +45 +50")
         .otherwise("22| > +50")
    )
    
    return agg_df


def analyze_category_performance(
    df: DataFrame,
    group_by: str = "style"
) -> pd.DataFrame:
    """
    Analyze performance for filtered data.
    
    Args:
        df: Pre-filtered Spark DataFrame with sales_units, forecast, and error columns
        group_by: Column to group by for counting distinct items (default "style")
        
    Returns:
        pandas.DataFrame: Aggregated results with product count, sales units, forecast, and error
    """
    result = (df
        .groupBy().agg(
            F.countDistinct(group_by).alias("product_count"), 
            F.sum("sales_units").alias("sales_units"), 
            F.sum("forecast").alias("forecast"), 
            F.sum("error").alias("error")
        )
    ).toPandas()
    
    return result


def plot_product_details_daily_short(
    df: DataFrame,
    style: str,
    products_df: Optional[DataFrame] = None,
    EVALUATION_START_DATE: str = "2025-07-01",
    EVALUATION_END_DATE: str = "2025-07-15"
) -> tuple[pd.DataFrame, go.Figure]:
    """
    Plot daily details for a specific style showing sales vs forecast.
    
    Args:
        df: Spark DataFrame with style, date, sales_units, and forecast columns
        style: The style to plot details for
        products_df: Optional Spark DataFrame with product information (id, product_title, image_url, etc.)
        EVALUATION_START_DATE: Start date for the evaluation period
        EVALUATION_END_DATE: End date for the evaluation period
        
    Returns:
        tuple: (pandas.DataFrame, go.Figure) - Data and plot figure
    """
    # Display product information if products_df is provided
    if products_df is not None:
        try:
            # Filter products for the selected style
            product_info = products_df.filter(F.col("product_number") == style).toPandas()
            
            if not product_info.empty:
                # Display product title
                if 'product_title' in product_info.columns:
                    title = product_info['product_title'].iloc[0]
                    if pd.notna(title) and title.strip():
                        print(f"Product Title: {title}")
                
                # Display price
                if 'price' in product_info.columns:
                    price = product_info['price'].iloc[0]
                    if pd.notna(price):
                        print(f"Price: {price}")
                
                # Display status
                if 'status' in product_info.columns:
                    status = product_info['status'].iloc[0]
                    if pd.notna(status):
                        print(f"Status: {status}")
                
                # Display image if available
                if 'image_url' in product_info.columns:
                    image_url = product_info['image_url'].iloc[0]
                    if pd.notna(image_url) and image_url.strip():
                        try:
                            from IPython.display import Image, display
                            display(Image(url=image_url, width=300, height=300))
                        except Exception as e:
                            print(f"Could not display image: {e}")
            else:
                print(f"No product information found for style: {style}")
        except Exception as e:
            print(f"Error displaying product information: {e}")
    
    # Filter for the specific style only (no date filtering)
    product_data = (df
        .where(F.col("style") == style)
        .orderBy("date")
    ).toPandas()
    
    if product_data.empty:
        print(f"No data found for style {style}")
        return None, None
    
    # Convert date to datetime
    product_data["date"] = pd.to_datetime(product_data["date"])
    
    # Create subplots with flags on top
    fig = make_subplots(
        rows=2, cols=1, 
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.2, 0.8],
        subplot_titles=("Calendar Effects (Day 0)", "Sales vs Forecast")
    )
    
    # Add calendar effects subplot (top) - only day 0 effects (actual effect dates)
    calendar_effect_columns = [col for col in product_data.columns if col.endswith('_DAY_0') or col == 'SALARY_PERIOD']
    
    # Create y-position mapping for each effect (like PRTF) - only for effects that have data
    effect_y_positions = {}
    active_effects = []
    if calendar_effect_columns:
        for effect_col in calendar_effect_columns:
            if effect_col in product_data.columns:
                # Check if this effect is ever active (value > 0)
                effect_data = product_data[product_data[effect_col] > 0].copy()
                if not effect_data.empty:
                    active_effects.append(effect_col)
        
        # Assign y-positions only to active effects
        for i, effect_col in enumerate(active_effects):
            effect_y_positions[effect_col] = i + 1
            
            # Filter to only show days where the effect is active (value > 0)
            effect_data = product_data[product_data[effect_col] > 0].copy()
            
            # Use muted colors for calendar effects
            muted_colors = ["#8B5CF6", "#10B981", "#3B82F6", "#F59E0B", "#EF4444", "#8B5A2B", "#6B21A8", "#059669"]
            color = muted_colors[i % len(muted_colors)]
            
            fig.add_trace(go.Scatter(
                x=effect_data["date"],
                y=[effect_y_positions[effect_col]] * len(effect_data),  # Use fixed y-position
                mode="markers",
                name=effect_col,
                marker=dict(size=10, color=color),
                showlegend=True
            ), row=1, col=1)
    
    # Add sales line (bottom)
    fig.add_trace(go.Scatter(
        x=product_data["date"],
        y=product_data["sales_units"],
        mode="lines+markers",
        name="Actual Sales",
        line=dict(color=COLORS["sales_units"], width=2),
        marker=dict(size=6)
    ), row=2, col=1)
    
    # Add forecast line (bottom)
    fig.add_trace(go.Scatter(
        x=product_data["date"],
        y=product_data["forecast"],
        mode="lines+markers",
        name="Forecast",
        line=dict(color=COLORS["forecast"], width=2, dash="dash"),
        marker=dict(size=6)
    ), row=2, col=1)
    
    # Add evaluation period lines using add_shape
    eval_start = pd.to_datetime(EVALUATION_START_DATE)
    eval_end = pd.to_datetime(EVALUATION_END_DATE)
    
    # Ensure dates are in the correct format for Plotly
    eval_start_str = eval_start.strftime('%Y-%m-%d')
    eval_end_str = eval_end.strftime('%Y-%m-%d')
    
    # Get y-axis range for the lines
    y_max = max(product_data["sales_units"].max(), product_data["forecast"].max()) * 1.1
    
    # Get y-axis range for calendar effects
    calendar_effect_max = 0
    if calendar_effect_columns:
        for effect_col in calendar_effect_columns:
            if effect_col in product_data.columns:
                effect_max = product_data[product_data[effect_col] > 0][effect_col].max()
                if not pd.isna(effect_max):
                    calendar_effect_max = max(calendar_effect_max, effect_max)
    
    # Add vertical lines for both subplots
    # For calendar effects subplot (row 1)
    fig.add_vline(
        x=eval_start,
        line=dict(color=COLORS["evaluation_lines"], dash="dash", width=2),
        row=1, col=1
    )
    fig.add_vline(
        x=eval_end,
        line=dict(color=COLORS["evaluation_lines"], dash="dash", width=2),
        row=1, col=1
    )
    
    # For sales/forecast subplot (row 2)
    fig.add_vline(
        x=eval_start,
        line=dict(color=COLORS["evaluation_lines"], dash="dash", width=2),
        row=2, col=1
    )
    fig.add_vline(
        x=eval_end,
        line=dict(color=COLORS["evaluation_lines"], dash="dash", width=2),
        row=2, col=1
    )
    
    
    # Update layout
    fig.update_layout(
        title=f"Daily Sales vs Forecast - Style {style}",
        template=PLOT_TEMPLATE,
        height=800,
        width=1400,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        )
    )
    
    # Update subplot axes
    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_yaxes(title_text="Calendar Effects", row=1, col=1)
    fig.update_yaxes(title_text="Units", row=2, col=1)
    
    # Set y-axis range and labels for calendar effects subplot (like PRTF)
    if calendar_effect_columns and effect_y_positions:
        tickvals = list(effect_y_positions.values())
        ticktext = list(effect_y_positions.keys())
        fig.update_yaxes(
            range=[0.5, len(calendar_effect_columns) + 0.5], 
            tickvals=tickvals, 
            ticktext=ticktext,
            row=1, col=1
        )
    
    return product_data, fig


def plot_accuracy_distribution(
    agg_df: DataFrame,
    method: str = "bias",
    target_column: str = "sales_units",
    on_forecast_sales_category: str = None,
    good_thresholds: dict = None,
    include_partial_bins: bool = False,
) -> go.Figure:
    """
    Plot forecast error or bias units distribution for a Spark DataFrame using static bins.

    Args:
        agg_df: Spark DataFrame with error_bin or bias_bin column and target_column
        method: Which method to plot ("error" or "bias") - default "bias"
        target_column: Column to weight bars by (default "sales_units")
        on_forecast_sales_category: Optional category filter
        good_thresholds: Either (under_tolerance, over_tolerance) BOTH >=0 -> interpreted as (-under, +over)
                        OR explicit (lower_bound, upper_bound) where lower_bound can be negative
        include_partial_bins: If True, mark bin good if it overlaps the good range at all (legacy).
                             If False (default), mark bin good only if the bin is fully inside the good range

    Returns:
        go.Figure: Plotly figure showing the forecast error or bias units distribution
    """

    import re
    import math

    # Validate method
    if method not in ("error", "bias"):
        raise ValueError("`method` must be either 'error' or 'bias'")

    bin_column = f"{method}_bin"  # Choose error_bin or bias_bin dynamically
    bin_label = "Forecast Error Units Bin" if method == "error" else "Forecast Bias Bin"

    # Optional category filter
    if on_forecast_sales_category:
        agg_df = agg_df.filter(F.col("on_forecast_sales_category") == on_forecast_sales_category)

    # Define bin labels for ordering (must match bin logic in your Spark function)
    BINS = [
        "01| < -50", "02| -50 -45", "03| -45 -40", "04| -40 -35", "05| -35 -30",
        "06| -30 -25", "07| -25 -20", "08| -20 -15", "09| -15 -10", "10| -10 -5",
        "11| -5 0", "12| 0 +5", "13| +5 +10", "14| +10 +15", "15| +15 +20",
        "16| +20 +25", "17| +25 +30", "18| +30 +35", "19| +35 +40", "20| +40 +45",
        "21| +45 +50", "22| > +50"
    ] if method == "error" else [
        "01| < -100", "02| -100 -90", "03| -90 -80", "04| -80 -70", "05| -70 -60",
        "06| -60 -50", "07| -50 -40", "08| -40 -30", "09| -30 -20", "10| -20 -10",
        "11| -10 0", "12| 0 +10", "13| +10 +20", "14| +20 +30", "15| +30 +40",
        "16| +40 +50", "17| +50 +60", "18| +60 +70", "19| +70 +80", "20| +80 +90",
        "21| +90 +100", "22| > +100"
    ]

    # Aggregate units per bin (Spark -> pandas)
    aggregated_df = (
        agg_df.groupBy(bin_column)
        .agg(F.sum(target_column).alias("units_in_bin"))
    ).toPandas()

    bins_df = pd.DataFrame({bin_column: BINS})
    pandas_agg_df = bins_df.merge(aggregated_df, on=bin_column, how="left").fillna(0)

    # Helper: parse a bin label into numeric (start,end) with infinities for open-ended bins
    def parse_bin_range(label):
        s = label.split("|", 1)[1].strip()
        nums = re.findall(r'[-+]?\d+', s)
        if '<' in s and len(nums) >= 1:
            return (-math.inf, int(nums[-1]))
        if '>' in s and len(nums) >= 1:
            return (int(nums[0]), math.inf)
        if len(nums) >= 2:
            return (int(nums[0]), int(nums[1]))
        if len(nums) == 1:
            n = int(nums[0])
            return (n, n)
        return (math.nan, math.nan)

    # Determine good bounds
    DEFAULT_LOWER, DEFAULT_UPPER = -5, 20
    if good_thresholds and on_forecast_sales_category in good_thresholds:
        raw_low, raw_high = good_thresholds[on_forecast_sales_category]
        if isinstance(raw_low, (int, float)) and isinstance(raw_high, (int, float)) and raw_low >= 0 and raw_high >= 0:
            good_lower, good_upper = -raw_low, raw_high
        else:
            good_lower, good_upper = float(raw_low), float(raw_high)
    else:
        good_lower, good_upper = DEFAULT_LOWER, DEFAULT_UPPER

    parsed = pandas_agg_df[bin_column].apply(parse_bin_range)
    pandas_agg_df["bin_start"] = parsed.apply(lambda t: t[0])
    pandas_agg_df["bin_end"]   = parsed.apply(lambda t: t[1])

    total_units = pandas_agg_df["units_in_bin"].sum() or 1.0

    # Mark bins as good/bad
    if include_partial_bins:
        pandas_agg_df["is_good"] = (
            (pandas_agg_df["bin_start"] <= good_upper) & 
            (pandas_agg_df["bin_end"] >= good_lower)
        )
    else:
        pandas_agg_df["is_good"] = (
            (pandas_agg_df["bin_start"] >= good_lower) & 
            (pandas_agg_df["bin_end"] <= good_upper)
        )

    # Calculate percentages
    pandas_agg_df["percentage_of_target"] = (pandas_agg_df["units_in_bin"] / total_units * 100).round(1)

    # Calculate good percentage
    good_units = pandas_agg_df[pandas_agg_df["is_good"]]["units_in_bin"].sum()
    good_percent = (good_units / total_units * 100).round(1) if total_units > 0 else 0

    # Create numeric position for coloring (like PRTF)
    zero_idx = None
    for i, bin_name in enumerate(BINS):
        if "0" in bin_name and ("-5 0" in bin_name or "0 +5" in bin_name or "-10 0" in bin_name or "0 +10" in bin_name):
            zero_idx = i
            break
    
    if zero_idx is None:
        zero_idx = len(BINS) // 2  # fallback to middle

    # Create bin_numeric_pos for coloring (distance from zero)
    pandas_agg_df["bin_numeric_pos"] = pandas_agg_df.index - zero_idx

    # Plot using px.bar like PRTF
    color_scale = ["red", "yellow", "green", "yellow", "red"]
    fig = px.bar(
        pandas_agg_df,
        x=bin_column,
        y="units_in_bin",
        text="percentage_of_target",
        labels={"units_in_bin": "Units", bin_column: bin_label},
        color="bin_numeric_pos",
        color_continuous_scale=color_scale,
        template=PLOT_TEMPLATE
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")

    # Vertical line at zero
    fig.add_shape(
        type="line",
        x0=zero_idx + 0.5, x1=zero_idx + 0.5,
        y0=0, y1=pandas_agg_df["units_in_bin"].max() * 1.1,
        line=dict(color="#ccccff", dash="dash")
    )

    # Good range lines
    good_bins_idx = pandas_agg_df.index[pandas_agg_df["is_good"]]
    if not good_bins_idx.empty:
        min_idx = min(good_bins_idx) - 0.5
        max_idx = max(good_bins_idx) + 0.5
        y_max = pandas_agg_df["units_in_bin"].max() * 1.1
        fig.add_shape(type="line", x0=min_idx, x1=min_idx, y0=0, y1=y_max, line=dict(color=COLORS["good_range_lines"], dash="dot"))
        fig.add_shape(type="line", x0=max_idx, x1=max_idx, y0=0, y1=y_max, line=dict(color=COLORS["good_range_lines"], dash="dot"))
        fig.add_annotation(
            x=(min_idx + max_idx) / 2,
            y=y_max * 0.95,
            text=f"{good_percent}% in good range ({good_lower} to {good_upper})",
            showarrow=False,
            font=dict(color=COLORS["good_range_lines"], size=12, family="Arial"),
            bgcolor="white",
            bordercolor=COLORS["good_range_lines"],
            borderwidth=1
        )

    # Update layout with dynamic title
    fig.update_layout(
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False),
        title=(
            f"Forecast Units {'Error' if method == 'error' else 'Bias'} Distribution"
            f" - {on_forecast_sales_category or 'All Categories'}"
        ),
        title_x=0.5,
        height=450,
        width=900
    )

    return fig
