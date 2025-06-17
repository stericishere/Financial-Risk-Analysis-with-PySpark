# Dependencies Module

This module contains the foundational infrastructure components that support the Financial Risk Analysis ETL pipeline. These dependencies provide essential services like Spark session management, logging, and configuration handling that are used across all parts of the system.

## Module Components

### `spark.py` - Spark Session Management
**Purpose**: Centralized Spark session creation and configuration management

**Key Functions**:
- `start_spark()`: Creates and configures PySpark session with optimal settings
- `stop_spark()`: Properly shuts down Spark session and releases resources

**Features**:
- **Environment Detection**: Automatically adjusts configuration for local vs cluster deployment
- **Configuration Loading**: Loads Spark settings from configuration files
- **Resource Management**: Optimizes memory and CPU allocation
- **Debugging Support**: Enables detailed logging when needed

**Usage in Financial ETL**:
```python
spark, logger, config = start_spark(app_name="FinancialRiskETL")
# Process financial data with optimized Spark session
stop_spark(spark)
```

### `logging.py` - Logging Infrastructure  
**Purpose**: Structured logging system for monitoring and debugging the ETL pipeline

**Key Classes & Functions**:
- `LoggingMixin`: Base class that adds logging capabilities to any class
- `setup_logging()`: Configures logging levels and formats
- `log_dataframe_info()`: Specialized logging for DataFrame operations
- `log_processing_step()`: Tracks ETL pipeline progress

**Features**:
- **Structured Logging**: JSON-formatted logs for easy parsing
- **DataFrame Insights**: Logs schema, row counts, and sample data
- **Performance Tracking**: Measures execution time for operations
- **Error Context**: Captures detailed error information with stack traces

**Usage in Financial ETL**:
```python
log_processing_step("Risk Score Calculation")
log_dataframe_info("Clean Data", cleaned_df)
logger.info("Processed 10,000 loan applications")
```

## Integration with ETL Pipeline

These dependencies form the foundation of the financial risk analysis system:

1. **Spark Session**: All DataFrame operations rely on the optimized Spark configuration
2. **Logging**: Every major step in the ETL process is logged for monitoring
3. **Error Handling**: Comprehensive error tracking helps identify issues in production

## Configuration Management

The dependencies module handles:
- **Spark Configuration**: Memory settings, parallelism, and optimization flags
- **Logging Configuration**: Log levels, output formats, and destination settings
- **Environment Variables**: Development vs production environment detection

## Production Considerations

- **Resource Management**: Proper cleanup of Spark resources
- **Log Aggregation**: Structured logs ready for tools like ELK stack
- **Error Recovery**: Graceful handling of infrastructure failures
- **Monitoring**: Built-in metrics for system health monitoring 