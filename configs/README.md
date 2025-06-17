# Configuration Directory

This directory contains configuration files that control the behavior of the Financial Risk Analysis ETL pipeline. Configuration-driven design enables easy customization without code changes, supporting different environments and business requirements.

## Directory Structure

```
configs/
├── etl_config.json    # Main ETL pipeline configuration
└── README.md          # This documentation
```

## Configuration Philosophy

**Design Principles**:
- **Environment Agnostic**: Same codebase works across dev/staging/production
- **Business Configurable**: Risk thresholds and approval criteria can be adjusted
- **Performance Tunable**: Spark and processing settings can be optimized
- **Validation Enabled**: Configuration validation prevents runtime errors

## Main Configuration File

### `etl_config.json` - ETL Pipeline Configuration

**Purpose**: Central configuration for all aspects of the financial risk analysis pipeline

## Configuration Sections

### 1. Data Sources (`data`)
**Purpose**: Configure input and output data locations

```json
{
  "data": {
    "input_path": "data/Financial_Risk_Dataset.csv",
    "output_path": "output/processed_data",
    "backup_path": "output/backups"
  }
}
```

**Key Settings**:
- **input_path**: Location of raw financial data files
- **output_path**: Where processed results are saved
- **backup_path**: Backup location for data recovery

**Environment Support**:
- **Local Development**: File system paths
- **Production**: S3/HDFS/Azure Data Lake paths
- **Testing**: Temporary directories

### 2. Processing Configuration (`processing`)

#### Risk Assessment Thresholds
**Purpose**: Define business rules for risk categorization

```json
{
  "risk_thresholds": {
    "low_risk": 0.3,      // Scores ≤ 0.3 = Low Risk
    "medium_risk": 0.6,   // Scores 0.3-0.6 = Medium Risk  
    "high_risk": 0.8      // Scores > 0.6 = High Risk
  }
}
```

**Business Impact**:
- **Conservative Settings**: Lower thresholds = More manual reviews
- **Aggressive Settings**: Higher thresholds = More auto-approvals
- **Regulatory Compliance**: Thresholds can reflect regulatory requirements

#### Approval Decision Engine 🎯
**Purpose**: Configure the confidence-based approval system

```json
{
  "approval_config": {
    "approve_threshold": 0.3,     // Auto-approve if risk ≤ 0.3
    "reject_threshold": 0.7,      // Auto-reject if risk ≥ 0.7
    "min_confidence": 0.8         // Require 80%+ confidence for auto-decisions
  }
}
```

**Decision Matrix**:
- **Risk ≤ 0.3 + High Confidence**: Auto-approve
- **Risk ≥ 0.7 + High Confidence**: Auto-reject  
- **0.3 < Risk < 0.7**: Manual review required
- **Any Risk + Low Confidence**: Manual review recommended

#### Feature Engineering Settings
**Purpose**: Control which features are used in risk calculations

```json
{
  "feature_columns": [
    "CreditScore",      // 30% weight - Most important
    "DTIRatio",         // 25% weight - Debt sustainability
    "InterestRate",     // 20% weight - Market risk assessment
    "LoanAmount",       // 15% weight - Absolute exposure
    "Income"            // 10% weight - Repayment capacity
  ]
}
```

**Feature Weights**:
The risk scoring algorithm uses industry-standard weightings that can be adjusted based on:
- **Regulatory Requirements**: Different jurisdictions may require different weightings
- **Business Strategy**: Conservative vs aggressive lending approaches
- **Market Conditions**: Economic factors may influence feature importance

### 3. Spark Configuration (`spark`)
**Purpose**: Optimize PySpark performance for different environments

```json
{
  "spark": {
    "app_name": "FinancialRiskETL",
    "master": "local[*]",
    "config": {
      "spark.sql.adaptive.enabled": "true",
      "spark.sql.adaptive.coalescePartitions.enabled": "true",
      "spark.sql.adaptive.skewJoin.enabled": "true"
    }
  }
}
```

**Performance Optimizations**:
- **Adaptive Query Execution**: Automatically optimizes joins and aggregations
- **Dynamic Partition Coalescing**: Reduces small file problems  
- **Skew Join Handling**: Improves performance with uneven data distribution

**Environment-Specific Settings**:

**Local Development**:
```json
{
  "master": "local[*]",
  "config": {
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g"
  }
}
```

**Production Cluster**:
```json
{
  "master": "yarn",
  "config": {
    "spark.driver.memory": "8g",
    "spark.executor.memory": "16g",
    "spark.executor.instances": "10",
    "spark.dynamicAllocation.enabled": "true"
  }
}
```

## Environment-Specific Configuration

### Development Environment
**Characteristics**:
- Small data samples for fast iteration
- Verbose logging for debugging
- Local file system paths

```json
{
  "data": {
    "input_path": "data/sample_data.csv",
    "output_path": "output/dev"
  },
  "processing": {
    "approval_config": {
      "min_confidence": 0.6  // Lower confidence for testing
    }
  }
}
```

### Production Environment
**Characteristics**:
- Full datasets from data lake
- Optimized performance settings
- Strict confidence requirements

```json
{
  "data": {
    "input_path": "s3://financial-data/risk-assessments/",
    "output_path": "s3://processed-data/risk-analysis/"
  },
  "processing": {
    "approval_config": {
      "min_confidence": 0.85  // High confidence for production
    }
  }
}
```

## Configuration Management Best Practices

### 1. Version Control
- Configuration files are versioned alongside code
- Changes are tracked and can be rolled back
- Different branches can have different configurations

### 2. Environment Separation
```bash
# Development
export ETL_CONFIG_PATH="configs/dev_config.json"

# Production  
export ETL_CONFIG_PATH="configs/prod_config.json"
```

### 3. Security Considerations
- Sensitive values (API keys, database passwords) use environment variables
- Configuration files contain no secrets
- Secrets are injected at runtime

### 4. Validation
```python
# Configuration is validated at startup
def validate_config(config):
    assert 0 <= config['risk_thresholds']['low_risk'] <= 1
    assert config['approval_config']['min_confidence'] >= 0.5
    # ... additional validations
```

## Business Configuration Examples

### Conservative Lending Strategy
```json
{
  "risk_thresholds": {
    "low_risk": 0.2,      // Stricter low-risk definition
    "medium_risk": 0.4    // Smaller medium-risk zone
  },
  "approval_config": {
    "approve_threshold": 0.2,   // Only approve very low risk
    "min_confidence": 0.9       // Require very high confidence
  }
}
```

### Aggressive Lending Strategy
```json
{
  "risk_thresholds": {
    "low_risk": 0.4,      // More lenient low-risk definition
    "medium_risk": 0.7    // Larger approval zone
  },
  "approval_config": {
    "approve_threshold": 0.4,   // Approve moderate risk
    "min_confidence": 0.7       // Accept lower confidence
  }
}
```

## Configuration Impact on Business Metrics

**Conservative Configuration**:
- ✅ Lower default rates
- ✅ Reduced risk exposure
- ❌ Lower approval rates
- ❌ More manual review required

**Aggressive Configuration**:
- ✅ Higher approval rates  
- ✅ More automated decisions
- ❌ Higher risk exposure
- ❌ Potential for increased defaults

## Integration with ETL Pipeline

The configuration system integrates seamlessly with the ETL pipeline:

```python
# Load configuration
config = load_config("configs/etl_config.json")

# Use in risk assessment
risk_thresholds = config["processing"]["risk_thresholds"]
categorized_df = categorize_risk(df, risk_thresholds)

# Use in approval decisions
approval_config = config["processing"]["approval_config"]
approved_df = calculate_approval_decision(df, approval_config)
```

This configuration-driven approach enables rapid business adaptation without code changes, supporting agile business requirements and regulatory compliance. 