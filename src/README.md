# Source Code Directory

This directory contains the core modules and utilities that power the Financial Risk Analysis ETL pipeline. All source code is organized into logical modules that handle different aspects of the data processing workflow.

## Directory Structure

```
src/
├── dependencies/          # Core infrastructure and shared services
├── utils/                 # Business logic and data transformation utilities
└── __init__.py           # Package initialization
```

## Module Overview

### dependencies/
Contains foundational services and infrastructure components:
- **Spark Session Management**: Handles PySpark initialization and configuration
- **Logging Infrastructure**: Provides structured logging for monitoring and debugging
- **Configuration Loading**: Manages environment-specific settings

### utils/
Contains business-specific logic and data transformation functions:
- **Financial Risk Analysis**: Core algorithms for risk scoring and categorization
- **Approval Decision Engine**: Confidence-based loan approval system
- **Feature Engineering**: Financial domain-specific feature creation
- **Data Quality**: Validation, cleaning, and outlier detection

## Key Design Principles

1. **Modularity**: Each module has a single responsibility
2. **Testability**: All functions are pure and easily testable
3. **Reusability**: Components can be used across different ETL jobs
4. **Type Safety**: Full type hints for better IDE support and error detection
5. **Documentation**: Comprehensive docstrings for all public functions

## Usage in ETL Pipeline

These modules are imported and used by the main ETL job (`jobs/financial_risk_etl.py`) to:

1. **Initialize Spark Session** using `dependencies.spark`
2. **Set up logging** using `dependencies.logging`
3. **Transform raw data** using `utils.financial_risk_utils`
4. **Make approval decisions** using the confidence-based approval system
5. **Validate data quality** throughout the process

The modular design ensures that each component can be developed, tested, and maintained independently while working together to create a robust financial risk analysis system. 