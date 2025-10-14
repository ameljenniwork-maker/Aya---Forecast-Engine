#!/usr/bin/env python3
"""
Data validation module for the forecast engine
Validates configuration parameters and data integrity
"""

# Standard library imports
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple

# Local imports
import configuration as CONFIG


def validate_configuration(logger: logging.Logger) -> None:
    """
    Validate all configuration parameters before starting the pipeline
    
    Args:
        logger: Logger instance
    """
    logger.info("  [VALIDATE] Checking configuration parameters...")
    
    # Validate date parameters
    validate_date_parameters(logger)
    
    # Validate configuration sections
    validate_config_sections(logger)
    
    # Validate forecast configuration
    validate_forecast_config(logger)
    
    # Validate non-eligible categories
    validate_non_eligible_categories(logger)
    
    # Validate age-sales categories
    validate_age_sales_categories(logger)
    
    # Validate log level
    validate_log_level(logger)
    
    logger.info("  [SUCCESS] All configuration parameters validated successfully")


def validate_date_parameters(logger: logging.Logger) -> None:
    """Validate date parameters using only configuration values"""
    logger.info("  [DATES] Configured dates:")
    logger.info(f"    [CONFIG] History start date: {CONFIG.HISTORY_START_DATE}")
    logger.info(f"    [CONFIG] History end date: {CONFIG.HISTORY_END_DATE}")
    logger.info(f"    [CONFIG] Forecast start date: {CONFIG.FORECAST_CONFIG['FORECAST_START_DATE']}")
    
    logger.info("  [DATES] Validating date parameters...")
    
    # Parse dates from configuration only
    try:
        start_date = datetime.strptime(CONFIG.HISTORY_START_DATE, "%Y-%m-%d")
        end_date = datetime.strptime(CONFIG.HISTORY_END_DATE, "%Y-%m-%d")
        forecast_start = datetime.strptime(CONFIG.FORECAST_CONFIG['FORECAST_START_DATE'], "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"    [ERROR] Invalid date format: {e}")
        raise
    
    # Validate date range
    if start_date >= end_date:
        logger.error("    [ERROR] Start date must be before end date")
        raise ValueError("Start date must be before end date")
    
    # Validate forecast start date
    expected_forecast_start = end_date + timedelta(days=1)
    if forecast_start != expected_forecast_start:
        logger.warning(f"    [WARNING] Forecast start date {CONFIG.FORECAST_CONFIG['FORECAST_START_DATE']} doesn't match expected {expected_forecast_start.strftime('%Y-%m-%d')}")
    
    logger.info(f"    [OK] History date range: {CONFIG.HISTORY_START_DATE} to {CONFIG.HISTORY_END_DATE}")
    logger.info(f"    [OK] Forecast start date: {CONFIG.FORECAST_CONFIG['FORECAST_START_DATE']} (history end + 1 day)")


def validate_config_sections(logger: logging.Logger) -> None:
    """Validate that all required configuration sections exist"""
    required_sections = [
        'HISTORY_START_DATE',
        'HISTORY_END_DATE', 
        'FORECAST_CONFIG',
        'NON_ELIGIBLE_CATEGORIES'
    ]
    
    for section in required_sections:
        if not hasattr(CONFIG, section):
            logger.error(f"    [ERROR] Missing configuration section: {section}")
            raise ValueError(f"Missing configuration section: {section}")


def validate_forecast_config(logger: logging.Logger) -> None:
    """Validate forecast configuration parameters"""
    logger.info("  [FORECAST] Validating forecast configuration...")
    
    required_params = ['FORECAST_HORIZON', 'MIN_OBS_FOR_PROPHET']
    
    for param in required_params:
        if param in CONFIG.FORECAST_CONFIG:
            logger.info(f"    [OK] Found forecast parameter: {param} = {CONFIG.FORECAST_CONFIG[param]}")
        else:
            logger.error(f"    [ERROR] Missing forecast parameter: {param}")
            raise ValueError(f"Missing forecast parameter: {param}")


def validate_non_eligible_categories(logger: logging.Logger) -> None:
    """Validate non-eligible categories configuration"""
    logger.info("  [FILTERS] Validating non-eligible categories...")
    
    if hasattr(CONFIG, 'NON_ELIGIBLE_CATEGORIES'):
        categories = CONFIG.NON_ELIGIBLE_CATEGORIES
        logger.info(f"    [OK] Non-eligible age categories: {categories.get('age_categories', [])}")
        logger.info(f"    [OK] Non-eligible sales categories: {categories.get('sales_categories', [])}")
    else:
        logger.warning("    [WARNING] No non-eligible categories configured")


def validate_age_sales_categories(logger: logging.Logger) -> None:
    """Validate age-sales category configuration"""
    logger.info("  [CATEGORIES] Validating age-sales category configuration...")
    
    if 'AGE_SALES_CATEGORY_CONFIG' in CONFIG.FORECAST_CONFIG:
        categories = CONFIG.FORECAST_CONFIG['AGE_SALES_CATEGORY_CONFIG']
        logger.info(f"    [OK] Found {len(categories)} age-sales category configurations")
    else:
        logger.error("    [ERROR] Missing AGE_SALES_CATEGORY_CONFIG")
        raise ValueError("Missing AGE_SALES_CATEGORY_CONFIG")


def validate_log_level(logger: logging.Logger) -> None:
    """Validate log level parameter from configuration"""
    # Get log level from configuration (default to INFO if not set)
    log_level = getattr(CONFIG, 'LOG_LEVEL', 'INFO')
    
    valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR']
    if log_level not in valid_log_levels:
        logger.error(f"    [ERROR] Invalid log level: {log_level}")
        raise ValueError(f"Invalid log level: {log_level}")