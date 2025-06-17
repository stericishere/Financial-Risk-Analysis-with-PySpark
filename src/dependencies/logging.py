"""
Logging utilities for PySpark applications.

This module provides standardized logging configuration and utilities
for PySpark ETL jobs and analysis workflows.
"""

import logging
import sys
from typing import Optional, Dict, Any


def setup_logging(
    log_level: str = "INFO",
    log_format: Optional[str] = None,
    include_timestamp: bool = True,
) -> logging.Logger:
    """
    Set up standardized logging for PySpark applications.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_format: Custom log format string.
        include_timestamp: Whether to include timestamp in log format.

    Returns:
        Configured logger instance.
    """
    if log_format is None:
        if include_timestamp:
            log_format = (
                "%(asctime)s - %(name)s - %(levelname)s - "
                "%(funcName)s:%(lineno)d - %(message)s"
            )
        else:
            log_format = (
                "%(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
            )

    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        stream=sys.stdout,
        force=True,
    )

    # Suppress verbose Spark logging
    logging.getLogger("py4j").setLevel(logging.WARN)
    logging.getLogger("pyspark").setLevel(logging.WARN)

    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured with level: {log_level}")

    return logger


def setup_spark_logging(spark_session, log_level: str = "WARN") -> None:
    """
    Configure Spark-specific logging settings.

    Args:
        spark_session: Active Spark session.
        log_level: Spark log level (ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN).
    """
    spark_context = spark_session.sparkContext
    spark_context.setLogLevel(log_level)


def log_dataframe_info(
    df,
    name: str = "DataFrame",
    logger: Optional[logging.Logger] = None,
    show_schema: bool = True,
    show_count: bool = True,
    show_sample: bool = False,
    sample_rows: int = 5,
) -> None:
    """
    Log comprehensive information about a DataFrame.

    Args:
        df: PySpark DataFrame to analyze.
        name: Name to use in log messages.
        logger: Logger instance to use.
        show_schema: Whether to log the DataFrame schema.
        show_count: Whether to log the row count.
        show_sample: Whether to log sample rows.
        sample_rows: Number of sample rows to show.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info(f"=== {name} Information ===")

    if show_schema:
        logger.info(f"{name} Schema:")
        df.printSchema()

    if show_count:
        count = df.count()
        logger.info(f"{name} Row Count: {count:,}")

    if show_sample and sample_rows > 0:
        logger.info(f"{name} Sample ({sample_rows} rows):")
        df.show(sample_rows, truncate=False)

    logger.info(f"=== End {name} Information ===")


def log_processing_step(
    step_name: str,
    logger: Optional[logging.Logger] = None,
    log_level: str = "INFO",
) -> None:
    """
    Log a processing step with consistent formatting.

    Args:
        step_name: Name of the processing step.
        logger: Logger instance to use.
        log_level: Log level for the message.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    separator = "=" * 50
    message = f"\n{separator}\n{step_name}\n{separator}"

    log_func = getattr(logger, log_level.lower())
    log_func(message)


def log_config(config: Dict[str, Any], logger: Optional[logging.Logger] = None) -> None:
    """
    Log configuration parameters in a readable format.

    Args:
        config: Configuration dictionary to log.
        logger: Logger instance to use.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("=== Configuration ===")
    for section, values in config.items():
        logger.info(f"{section}:")
        if isinstance(values, dict):
            for key, value in values.items():
                logger.info(f"  {key}: {value}")
        else:
            logger.info(f"  {values}")
    logger.info("=== End Configuration ===")


class LoggingMixin:
    """
    Mixin class to add logging capabilities to other classes.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def log_step(self, step_name: str, log_level: str = "INFO") -> None:
        """Log a processing step."""
        log_processing_step(step_name, self.logger, log_level)

    def log_dataframe(
        self,
        df,
        name: str = "DataFrame",
        show_schema: bool = True,
        show_count: bool = True,
        show_sample: bool = False,
        sample_rows: int = 5,
    ) -> None:
        """Log DataFrame information."""
        log_dataframe_info(
            df, name, self.logger, show_schema, show_count, show_sample, sample_rows
        )
