# Jobs Directory - ETL Pipeline Implementation

This directory contains the main ETL (Extract, Transform, Load) jobs that orchestrate the financial risk analysis workflow. These jobs combine the core modules from `src/` to create end-to-end data processing pipelines.

## Directory Structure

```
jobs/
├── financial_risk_etl.py    # Main financial risk analysis ETL job
└── README.md               # This documentation
```

## Main ETL Job

### `financial_risk_etl.py` - Financial Risk Analysis Pipeline

**Purpose**: Complete ETL pipeline for processing loan applications and generating risk-based approval decisions

**Architecture**: Follows the modular ETL pattern with separated Extract, Transform, and Load phases for better maintainability and testing.

## Pipeline Workflow

### 1. Extract Phase
**Function**: `extract_data(spark, config)`

**Responsibilities**:
- Load financial data from CSV/Parquet/JSON sources
- Handle different data formats and schemas
- Perform initial data validation
- Log data ingestion metrics

**Data Sources**:
- Financial risk datasets (loan applications)
- Customer demographics
- Credit history information
- Employment and income data

### 2. Transform Phase  
**Function**: `transform_data(df, config)`

**This is the core business logic phase with 6 sequential steps**:

#### Step 1: Data Cleaning
- Remove invalid records (null values, impossible ages/incomes)
- Apply business rules validation
- Handle missing values with smart defaults
- Log data quality metrics

#### Step 2: Feature Engineering
- Calculate loan-to-income ratios
- Create age groups (18-24, 25-34, 35-44, 45-54, 55-64, 65+)
- Create income brackets (Low, Lower-Middle, Middle, Upper-Middle, High)
- Generate derived financial indicators

#### Step 3: Risk Score Calculation
- Apply weighted scoring algorithm:
  - Credit Score (30%)
  - DTI Ratio (25%) 
  - Interest Rate (20%)
  - Loan Amount (15%)
  - Income (10%)
- Normalize scores to 0-1 range
- Generate composite risk score

#### Step 4: Risk Categorization
- Convert risk scores to categories:
  - **Low Risk**: Score ≤ 0.3
  - **Medium Risk**: 0.3 < Score ≤ 0.6  
  - **High Risk**: Score > 0.6
- Assign human-readable risk labels

#### Step 5: Outlier Detection
- Identify statistical outliers using IQR method for loan amounts and income
- Apply Z-score method for credit scores
- Flag unusual patterns for review

#### Step 6: Approval Decision Engine 🎯
- **Binary Decision Logic**:
  - **APPROVE**: Risk ≤ 0.3 with high confidence
  - **REJECT**: Risk ≥ 0.7 with high confidence
  - **MANUAL_REVIEW**: Moderate risk or low confidence
- **Confidence Scoring**: Based on distance from decision boundaries
- **Smart Recommendations**: Context-aware approval guidance

### 3. Load Phase
**Function**: `load_data(df, config)`

**Responsibilities**:
- Save processed data to output location
- Partition data by risk category for optimal querying
- Generate summary reports and statistics
- Create data quality reports

**Output Formats**:
- Partitioned Parquet files for production use
- CSV files for analysis and reporting
- JSON summary reports for dashboards

## Business Intelligence & Reporting

The ETL job generates comprehensive reporting:

### Approval Statistics
- **Total Applications Processed**: Volume metrics
- **Approval Rate**: Percentage of approved applications  
- **Manual Review Rate**: Applications requiring human review
- **Auto-Decision Rate**: Fully automated decisions
- **High Confidence Rate**: Decisions with high certainty

### Risk Distribution Analysis
- Applications by risk category (Low/Medium/High)
- Risk score distributions and percentiles
- Demographic risk patterns
- Financial indicator correlations

### Sample Output Reports
```
=== APPROVAL DECISION SUMMARY ===
Total Applications: 12,000
Approval Rate: 45.2%
Manual Review Rate: 32.1%
Auto-Decision Rate: 67.9%
High Confidence Rate: 58.3%

Decision Breakdown:
  APPROVE: 5,424 (45.2%)
  REJECT: 2,712 (22.6%)
  MANUAL_REVIEW: 3,864 (32.2%)
```

## Configuration Management

**File**: `configs/etl_config.json`

**Key Configuration Areas**:
- **Input/Output Paths**: Data source and destination locations
- **Risk Thresholds**: Customizable risk category boundaries
- **Approval Configuration**: Decision thresholds and confidence levels
- **Feature Columns**: Which features to include in risk scoring
- **Spark Settings**: Performance and resource optimization

## Error Handling & Monitoring

**Comprehensive Error Management**:
- Graceful handling of data quality issues
- Detailed error logging with context
- Automatic retry mechanisms for transient failures
- Data validation checkpoints throughout pipeline

**Monitoring & Observability**:
- Processing step timing and performance metrics
- Data volume and quality tracking
- Decision accuracy monitoring
- Resource utilization logging

## Usage Examples

### Running the ETL Job
```bash
# Basic execution with default configuration
python jobs/financial_risk_etl.py

# With custom input file
python jobs/financial_risk_etl.py --input /path/to/data.csv

# With custom configuration
python jobs/financial_risk_etl.py --config /path/to/custom_config.json
```

### Integration with Scheduler
```bash
# Cron job for daily processing
0 2 * * * cd /app && make run-etl

# Airflow DAG integration
from jobs.financial_risk_etl import main as run_etl
run_etl()
```

## Production Deployment

**Scalability Features**:
- Handles datasets from thousands to millions of records
- Configurable Spark parallelism and memory settings
- Partitioned output for efficient querying
- Incremental processing capabilities

**Enterprise Integration**:
- Compatible with data lakes (S3, HDFS, Azure Data Lake)
- Integrates with data warehouses (Snowflake, Redshift, BigQuery)  
- Supports streaming processing for real-time decisions
- API-ready for microservices architecture 