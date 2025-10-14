#!/usr/bin/env python3
"""
Centralized logging configuration for the forecast engine
"""

# Standard library imports
import logging
import sys
import os
from datetime import datetime

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

def setup_logger(name: str = "aya_forecast", level: str = LOG_LEVEL) -> logging.Logger:
    """Setup centralized logger for the pipeline"""
    
    # Suppress unwanted loggers completely
    logging.getLogger("cmdstanpy").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
    logging.getLogger("org.apache.hadoop").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.sql").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.SparkContext").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.scheduler").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.executor").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.storage").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.util").setLevel(logging.ERROR)
    
    # Suppress root logger for these packages
    logging.getLogger().setLevel(logging.WARNING)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level))
    
    # Create file handler
    log_filename = f"logs/forecast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(getattr(logging, level))
    
    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT)
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def get_logger(name: str) -> logging.Logger:
    """Get logger instance for a module"""
    return logging.getLogger(name)