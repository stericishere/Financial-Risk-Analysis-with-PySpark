# Utils Module - Financial Risk Analysis

This module contains the core business logic and financial domain-specific utilities for the loan risk analysis system. It implements sophisticated algorithms for risk assessment, feature engineering, and automated decision-making in the financial lending domain.

## Module Overview

### `financial_risk_utils.py` - Core Financial Analysis Engine

This is the heart of the financial risk analysis system, containing all business logic for processing loan applications and making risk-based decisions.

## Core Functionality Areas

### 1. Risk Scoring & Categorization
**Purpose**: Quantitative risk assessment using financial indicators

**Key Functions**:
- `calculate_risk_score()`: Multi-factor risk scoring algorithm
- `categorize_risk()`: Converts scores to Low/Medium/High categories

**Algorithm Features**:
- **Weighted Feature Scoring**: CreditScore (30%), DTI Ratio (25%), Interest Rate (20%), Loan Amount (15%), Income (10%)
- **Normalization**: All features scaled to 0-1 range for fair comparison
- **Domain Knowledge**: Weights based on financial industry best practices

**Example Risk Calculation**:
```python
# High credit score (750) + Low DTI (0.2) = Low risk score (~0.25)
# Low credit score (500) + High DTI (0.6) = High risk score (~0.85)
risk_scored_df = calculate_risk_score(df, feature_columns)
```

### 2. Confidence-Based Approval System 🎯
**Purpose**: Binary loan approval decisions with confidence thresholds

**Key Functions**:
- `calculate_approval_decision()`: Main approval engine with confidence scoring
- `calculate_approval_statistics()`: Comprehensive approval metrics
- `generate_approval_summary()`: Grouped approval analysis

**Decision Logic**:
- **APPROVE**: Risk score ≤ 0.3 (Low risk threshold)
- **REJECT**: Risk score ≥ 0.7 (High risk threshold)  
- **MANUAL_REVIEW**: 0.3 < Risk score < 0.7 (Moderate risk zone)

**Confidence Scoring**:
- **HIGH Confidence**: Score far from decision boundaries (≥80% confidence)
- **MEDIUM Confidence**: Moderately certain decisions (60-80%)
- **LOW Confidence**: Close to boundaries, recommend manual review

**Example Usage**:
```python
approval_config = {
    "approve_threshold": 0.3,
    "reject_threshold": 0.7, 
    "min_confidence": 0.8
}
approved_df = calculate_approval_decision(df, approval_config)
```

### 3. Feature Engineering
**Purpose**: Create meaningful financial indicators from raw data

**Key Functions**:
- `create_age_groups()`: Demographic segmentation (18-24, 25-34, 35-44, etc.)
- `create_income_brackets()`: Economic classification (Low, Lower-Middle, Middle, Upper-Middle, High)
- `calculate_loan_to_income_ratio()`: Key affordability metric

**Financial Domain Features**:
- **Age Groups**: Life stage analysis for risk patterns
- **Income Brackets**: Socioeconomic risk factors
- **Loan-to-Income Ratio**: Debt burden assessment
- **Credit Utilization**: Available vs used credit analysis

### 4. Data Quality & Validation
**Purpose**: Ensure data integrity for reliable risk assessment

**Key Functions**:
- `clean_data()`: Remove invalid records and handle missing values
- `validate_data_quality()`: Comprehensive data quality reporting
- `identify_outliers()`: Statistical outlier detection (IQR and Z-score methods)

**Data Quality Checks**:
- **Range Validation**: Age (18-100), Credit Score (300-850), Interest Rate (0-50%)
- **Business Rules**: Positive income/loan amounts, valid employment history
- **Missing Value Handling**: Smart defaults for optional fields
- **Outlier Detection**: Identify unusual patterns that might indicate errors

### 5. Statistical Analysis
**Purpose**: Generate insights and summary statistics

**Key Functions**:
- `calculate_summary_statistics()`: Grouped statistical analysis
- Advanced aggregations by risk category, age group, income bracket

## Integration with ETL Pipeline

The utils module is used throughout the ETL process:

```python
# 1. Data Cleaning
cleaned_df = clean_data(raw_df)

# 2. Feature Engineering  
enriched_df = calculate_loan_to_income_ratio(cleaned_df)
enriched_df = create_age_groups(enriched_df)
enriched_df = create_income_brackets(enriched_df)

# 3. Risk Assessment
risk_df = calculate_risk_score(enriched_df, feature_columns)
categorized_df = categorize_risk(risk_df, risk_thresholds)

# 4. Approval Decisions
final_df = calculate_approval_decision(categorized_df, approval_config)

# 5. Quality Control
outlier_df = identify_outliers(final_df, "LoanAmount", method="iqr")
quality_metrics = validate_data_quality(final_df)
```

## Business Value

This module delivers direct business value through:

1. **Automated Risk Assessment**: Consistent, objective loan evaluation
2. **Confidence-Based Decisions**: Reduces manual review burden while maintaining safety
3. **Data-Driven Insights**: Statistical analysis for portfolio management
4. **Quality Assurance**: Automated data validation prevents bad decisions
5. **Scalability**: Handle thousands of applications with consistent logic

## Financial Domain Expertise

The algorithms incorporate financial industry knowledge:
- **Credit Score Weighting**: Most important factor (30% weight)
- **Debt-to-Income Ratios**: Critical affordability measure (25% weight)
- **Interest Rate Risk**: Higher rates indicate higher perceived risk
- **Loan Size Consideration**: Larger loans carry more absolute risk
- **Income Stability**: Higher income provides repayment capacity 