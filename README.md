# Financial Risk Analysis with PySpark

A comprehensive PySpark project for analyzing financial risk data, combining modern development practices with robust ETL patterns. This project demonstrates best practices for PySpark development, testing, and deployment.

## 🎯 Project Overview

This project analyzes financial risk using the [Kaggle Financial Risk Dataset](https://www.kaggle.com/datasets/preethamgouda/financial-risk/data), implementing:

- **Modular ETL Architecture**: Following patterns from [AlexIoannides/pyspark-example-project](https://github.com/AlexIoannides/pyspark-example-project)
- **Modern Development Tooling**: Inspired by [nsphung/pyspark-template](https://github.com/nsphung/pyspark-template)
- **Financial Risk Modeling**: Custom risk scoring and categorization algorithms
- **Comprehensive Testing**: Unit tests for all transformation functions
- **Production-Ready Code**: Type hints, logging, error handling, and documentation

## 🏗️ Architecture

This project follows a modular structure with comprehensive documentation for each component:

```
financial-risk-analysis/
├── src/                     # Core source code (📖 src/README.md)
│   ├── dependencies/        # Infrastructure components (📖 src/dependencies/README.md)
│   │   ├── spark.py        # Spark session management
│   │   └── logging.py      # Logging utilities
│   └── utils/              # Business logic utilities (📖 src/utils/README.md)
│       └── financial_risk_utils.py  # Risk analysis algorithms
├── jobs/                   # ETL pipeline implementation (📖 jobs/README.md)
│   └── financial_risk_etl.py # Main financial risk ETL job
├── configs/                # Configuration management (📖 configs/README.md)
│   └── etl_config.json    # ETL pipeline configuration
├── tests/                  # Quality assurance & validation (📖 tests/README.md)
│   ├── conftest.py        # PyTest fixtures and test infrastructure
│   └── test_financial_risk_utils.py  # Comprehensive business logic tests
├── data/                   # Input data directory
├── output/                 # Processed results and reports
└── notebooks/             # Jupyter notebooks for analysis
```

### 📖 Detailed Documentation

Each directory contains detailed README files explaining:
- **[src/README.md](src/README.md)**: Core modules and design principles
- **[src/dependencies/README.md](src/dependencies/README.md)**: Infrastructure components (Spark, logging)
- **[src/utils/README.md](src/utils/README.md)**: Financial analysis algorithms and approval system
- **[jobs/README.md](jobs/README.md)**: ETL pipeline implementation and workflow
- **[configs/README.md](configs/README.md)**: Configuration management and business rules
- **[tests/README.md](tests/README.md)**: Testing framework and quality assurance

## 🚀 Features

### ETL Pipeline
- **Extract**: Flexible data loading (CSV, Parquet, JSON)
- **Transform**: Risk scoring, feature engineering, data cleaning
- **Load**: Partitioned output with summary reports

### Risk Analysis & Approval System 🎯
- **Risk Scoring**: Composite risk score using weighted financial features
- **Risk Categorization**: Low, Medium, High risk classifications
- **Confidence-Based Approval System**: Binary loan approval decisions (APPROVE/REJECT/MANUAL_REVIEW)
- **Intelligent Confidence Scoring**: Confidence levels based on distance from decision boundaries
- **Feature Engineering**: Age groups, income brackets, loan-to-income ratios
- **Outlier Detection**: IQR and Z-score based outlier identification

### Business Decision Engine
- **Automated Approvals**: High-confidence, low-risk applications auto-approved
- **Automated Rejections**: High-confidence, high-risk applications auto-rejected
- **Manual Review Queue**: Borderline cases and low-confidence decisions flagged for human review
- **Configurable Thresholds**: Business rules easily adjusted via configuration

### Data Quality
- **Data Validation**: Comprehensive data quality checks
- **Data Cleaning**: Removal of invalid records and handling missing values
- **Quality Metrics**: Detailed reporting on data quality issues

## 📋 Prerequisites

- Python 3.8+
- Java 8+ (for Spark)
- Poetry (for dependency management)

## 🛠️ Installation & Quick Start

### Step-by-Step Setup

1. **Clone the repository**:
```bash
git clone <repository-url>
cd financial-risk-analysis
```

2. **Install Poetry** (if not already installed):
```bash
curl -sSL https://install.python-poetry.org | python3 -
export PATH="$HOME/.local/bin:$PATH"
```

3. **Install all dependencies**:
```bash
make install-dev
# or manually:
poetry install --with dev
```

4. **Verify installation** (should pass 14/14 tests):
```bash
make test
```

5. **Run the financial risk analysis**:
```bash
make run-etl
```

### 🎯 First Run - See Approval Decisions in Action

**Option 1: Quick Test Script** (Recommended for first-time users)
```bash
# Test the approval system with 5 sample financial profiles
python test_approval_system.py
```

**Expected Output:**
```
🎯 Testing Financial Risk Approval System
Profile 1:
  👤 Age: 35, Income: $85,000, Credit: 780, DTI: 20.0%
  📈 Risk Score: 0.185 (Low Risk)
  ✅ Decision: APPROVE (HIGH Confidence)
  💡 Recommendation: Auto-approve: Low risk with high confidence

Profile 2:
  👤 Age: 22, Income: $25,000, Credit: 480, DTI: 75.0%
  📈 Risk Score: 0.892 (High Risk)
  ✅ Decision: REJECT (HIGH Confidence)
  💡 Recommendation: Auto-reject: High risk with high confidence
```

**Option 2: Full ETL Pipeline**
```bash
# Run the complete ETL pipeline with included sample data
python jobs/financial_risk_etl.py
```

### 🧪 Verify Financial Features Work

Test each component individually:

```bash
# Test risk scoring (should pass)
poetry run pytest tests/test_financial_risk_utils.py::TestRiskScoreCalculation::test_calculate_risk_score_returns_valid_scores -v

# Test approval decisions (should pass)  
poetry run pytest tests/test_financial_risk_utils.py::TestApprovalDecisions::test_calculate_approval_decision_basic -v

# Test feature engineering (should pass)
poetry run pytest tests/test_financial_risk_utils.py::TestFeatureEngineering -v
```

### Optional Setup

```bash
# Set up pre-commit hooks for development
poetry run pre-commit install

# Start Jupyter notebook for exploration
make jupyter
```

## 📊 Data Setup & Testing

### Quick Start with Sample Data

The project includes sample financial data for immediate testing:

```bash
# The project includes a sample dataset at data/Financial_Risk_Dataset.csv
# You can start testing immediately without downloading external data
ls data/Financial_Risk_Dataset.csv  # Should show the sample file
```

### Full Dataset (Optional)

For comprehensive analysis, you can download the complete [Financial Risk Dataset](https://www.kaggle.com/datasets/preethamgouda/financial-risk/data) and replace the sample data.

### Verify Your Setup

Test that everything works correctly:

```bash
# 1. Run the test suite (should pass 14/14 tests)
make test

# 2. Run the ETL pipeline with sample data
make run-etl

# 3. Check the outputs
ls output/  # Should show processed results
```

## 🏃‍♂️ Usage

### Running the ETL Job

**Local execution** (recommended for development):
```bash
make run-etl
# or
poetry run python jobs/financial_risk_etl.py
```

**Spark submit** (for production):
```bash
make build-package  # Build dependencies package
make run-etl-spark  # Run with spark-submit
```

### Development Workflow

**Run tests**:
```bash
make test
# or with coverage
make test-coverage
```

**Code quality checks**:
```bash
make quality-check  # Runs formatting, linting, and type checking
```

**Format code**:
```bash
make format
```

**Start Jupyter notebook**:
```bash
make jupyter
```

## 🧪 Testing & Validation

### Quick Test - Verify Everything Works

After cloning, run this simple test to ensure the project works:

```bash
# 1. Install dependencies
make install-dev

# 2. Run full test suite (should pass 14/14 tests)
make test

# 3. Test the approval system with sample data
python jobs/financial_risk_etl.py
```

**Expected Output**: You should see approval decisions like:
```
|Age|Income|CreditScore|approval_decision|confidence_level|recommendation                    |
+---+------+-----------+-----------------+----------------+----------------------------------+
|35 |75000 |720        |APPROVE          |LOW             |Manual review recommended...      |
|28 |30000 |500        |REJECT           |HIGH            |Auto-reject: High risk...         |
|30 |60000 |700        |MANUAL_REVIEW    |LOW             |Manual review required...         |
```

### Comprehensive Testing

The project includes extensive tests for financial analysis functions:

```bash
# Run all tests with detailed output
poetry run pytest tests/ -v

# Test specific functionality
poetry run pytest tests/test_financial_risk_utils.py::TestApprovalDecisions -v

# Run with coverage report
poetry run pytest --cov=src tests/ --cov-report=html
```

### Test the Financial Features

Test individual financial analysis components:

```bash
# Test risk scoring algorithm
poetry run pytest tests/test_financial_risk_utils.py::TestRiskScoreCalculation -v

# Test approval decision engine  
poetry run pytest tests/test_financial_risk_utils.py::TestApprovalDecisions -v

# Test feature engineering (age groups, income brackets)
poetry run pytest tests/test_financial_risk_utils.py::TestFeatureEngineering -v
```

### Test Structure
- **Unit Tests**: Test individual financial functions (risk scoring, categorization)
- **Integration Tests**: Test complete ETL workflow with sample data
- **Approval System Tests**: Validate confidence-based decision making
- **Data Quality Tests**: Verify data cleaning and validation
- **Fixtures**: Realistic financial profiles for testing

### Sample Test Data

The tests use realistic financial profiles:

```python
# Low-risk profile (should auto-approve)
Age: 35, Income: $75K, Credit: 750, DTI: 25%

# High-risk profile (should auto-reject)  
Age: 22, Income: $25K, Credit: 500, DTI: 65%

# Borderline profile (should manual review)
Age: 30, Income: $50K, Credit: 650, DTI: 45%
```

## 📈 Output

The ETL job produces several outputs:

1. **Main Analysis Results**: `reports/financial_risk_analysis/`
   - Partitioned by risk category
   - Contains all processed records with risk scores and categories

2. **Summary Reports**: 
   - `reports/financial_risk_analysis_summary_risk_category/`
   - `reports/financial_risk_analysis_summary_age_group/`
   - `reports/financial_risk_analysis_summary_income_bracket/`

3. **Data Quality Reports**: Console output with quality metrics

## ⚙️ Configuration

Edit `configs/etl_config.json` to customize:

- **Input/Output paths and formats**
- **Risk thresholds and feature weights**
- **Spark configuration parameters**
- **Processing options**

## 🔧 Development

### Adding New Features

1. **Business Logic**: Add functions to `src/utils/financial_risk_utils.py`
2. **Tests**: Add corresponding tests to `tests/test_financial_risk_utils.py`
3. **Integration**: Update the `transform_data` function in `jobs/financial_risk_etl.py`

### Code Quality

This project enforces code quality through:

- **Black**: Code formatting
- **Flake8**: Linting and style checking
- **MyPy**: Static type checking
- **Pre-commit hooks**: Automatic quality checks

### Project Structure Philosophy

Following the principles from the reference projects:

1. **Testable Transformations**: Business logic isolated in pure functions
2. **Configuration Management**: External JSON configuration
3. **Modular Dependencies**: Reusable utilities in `dependencies/`
4. **Modern Tooling**: Poetry, type hints, comprehensive testing

## 🚀 Production Deployment

### Spark Submit Example

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 10 \
    --executor-cores 4 \
    --executor-memory 8g \
    --driver-memory 4g \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/financial_risk_etl.py
```

### Environment Variables

Set these for production:
```bash
export SPARK_HOME=/path/to/spark
export JAVA_HOME=/path/to/java
```

## 📚 Key Concepts Demonstrated

### PySpark Best Practices
- **DataFrame API**: Efficient transformations using PySpark SQL functions
- **Performance Optimization**: Partitioning, caching, and broadcast joins
- **Memory Management**: Proper resource configuration and cleanup

### Software Engineering
- **Separation of Concerns**: ETL steps isolated for testability
- **Type Safety**: Comprehensive type hints throughout
- **Error Handling**: Graceful error handling and logging
- **Documentation**: Comprehensive docstrings and README

### Financial Risk Analysis
- **Risk Modeling**: Multi-factor risk scoring algorithms
- **Feature Engineering**: Domain-specific financial transformations
- **Data Quality**: Robust data validation and cleaning
- **Business Intelligence**: Meaningful aggregations and reporting

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and add tests
4. Run quality checks: `make quality-check`
5. Commit changes: `git commit -m "Description"`
6. Push and create a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- [AlexIoannides/pyspark-example-project](https://github.com/AlexIoannides/pyspark-example-project) for ETL architecture patterns
- [nsphung/pyspark-template](https://github.com/nsphung/pyspark-template) for modern development tooling
- [Kaggle Financial Risk Dataset](https://www.kaggle.com/datasets/preethamgouda/financial-risk/data) for the sample data

## 📞 Support

For questions or issues:
1. Check the documentation above
2. Review existing issues in the repository
3. Create a new issue with detailed description

---

## 🛠️ Troubleshooting

### Common Issues & Solutions

**Poetry not found error:**
```bash
# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -
export PATH="$HOME/.local/bin:$PATH"
```

**Java/Spark errors:**
```bash
# Ensure Java 8+ is installed
java -version

# On macOS, install via Homebrew:
brew install openjdk@11
```

**Test failures:**
```bash
# Clean and reinstall dependencies
poetry cache clear pypi --all
poetry install --with dev

# Re-run tests
make test
```

**PySpark DataFrame display issues:**
```bash
# If you see truncated output, this is normal for PySpark
# Use the test script for cleaner output:
python test_approval_system.py
```

### Getting Help

- **Check the logs**: Look for detailed error messages in the console output
- **Run individual tests**: Isolate issues by testing specific components
- **Review configuration**: Ensure `configs/etl_config.json` settings are valid
- **Verify data**: Check that sample data exists in `data/Financial_Risk_Dataset.csv`

**Happy Sparking! 🔥** 