# Tests Directory - Quality Assurance & Validation

This directory contains comprehensive test suites that ensure the reliability, accuracy, and robustness of the Financial Risk Analysis system. The tests validate both individual functions and integrated workflows using real-world scenarios.

## Testing Framework

**Technology Stack**:
- **pytest**: Primary testing framework with powerful fixtures and plugins
- **pytest-spark**: Specialized PySpark testing capabilities
- **Coverage Analysis**: Code coverage tracking for comprehensive test validation

## Directory Structure

```
tests/
├── conftest.py                    # Pytest configuration and shared fixtures
├── test_financial_risk_utils.py   # Core business logic tests
├── __init__.py                    # Package initialization
└── README.md                     # This documentation
```

## Test Configuration

### `conftest.py` - Test Infrastructure
**Purpose**: Centralized test configuration and reusable fixtures

**Key Components**:
- **Spark Session Fixture**: Isolated Spark environment for testing
- **Sample Financial Data**: Realistic test datasets for validation
- **Configuration Fixtures**: Test-specific settings and parameters

**Features**:
- **Isolated Testing**: Each test gets a clean Spark session
- **Realistic Data**: Sample financial records with known expected outcomes
- **Performance Optimized**: Minimal resource usage for fast test execution

## Core Business Logic Tests

### `test_financial_risk_utils.py` - Comprehensive Function Testing

**Test Categories**:

#### 1. Risk Score Calculation Tests
**Class**: `TestRiskScoreCalculation`

**Test Coverage**:
- ✅ **Valid Score Generation**: Ensures scores are in 0-1 range
- ✅ **Feature Weight Validation**: Verifies correct algorithmic weights
- ✅ **Edge Case Handling**: Tests with minimal/maximal feature values
- ✅ **Empty Feature Handling**: Graceful degradation with missing features

**Example Test Scenarios**:
```python
# High-risk applicant: Low credit score + High DTI
test_data = [(500, 0.8, 12.0)]  # Should produce high risk score

# Low-risk applicant: High credit score + Low DTI  
test_data = [(800, 0.2, 3.0)]   # Should produce low risk score
```

#### 2. Risk Categorization Tests
**Class**: `TestRiskCategorization`

**Test Coverage**:
- ✅ **Threshold Boundary Testing**: Validates category assignment at boundaries
- ✅ **Configuration Flexibility**: Tests with different threshold settings
- ✅ **Category Label Accuracy**: Ensures correct Low/Medium/High assignments

#### 3. Feature Engineering Tests
**Class**: `TestFeatureEngineering`

**Test Coverage**:
- ✅ **Age Group Assignment**: Validates demographic segmentation
- ✅ **Income Bracket Classification**: Tests economic categorization
- ✅ **Loan-to-Income Calculations**: Verifies financial ratio accuracy

**Financial Domain Validation**:
```python
# Age group testing
assert age_35_record.age_group == "35-44"
assert age_22_record.age_group == "18-24"

# Income bracket validation
assert income_45k_record.income_bracket == "Lower-Middle ($30K-$60K)"
assert income_120k_record.income_bracket == "Upper-Middle ($100K-$150K)"
```

#### 4. Data Cleaning Tests
**Class**: `TestDataCleaning`

**Test Coverage**:
- ✅ **Invalid Record Removal**: Filters out impossible values
- ✅ **Business Rule Validation**: Enforces financial domain constraints
- ✅ **Missing Value Handling**: Smart default assignment

**Quality Assurance Scenarios**:
```python
# Invalid records that should be filtered
invalid_cases = [
    (None, 50000, 200000, 650),     # Null age
    (25, -50000, 200000, 650),      # Negative income
    (25, 50000, 0, 650),            # Zero loan amount
    (15, 50000, 200000, 650),       # Under-age applicant
]
```

#### 5. Outlier Detection Tests
**Class**: `TestOutlierDetection`

**Test Coverage**:
- ✅ **IQR Method Validation**: Statistical outlier identification
- ✅ **Z-Score Method Testing**: Alternative outlier detection
- ✅ **Boundary Case Testing**: Edge cases near outlier thresholds

#### 6. Approval Decision Tests 🎯
**Class**: `TestApprovalDecisions`

**Test Coverage**:
- ✅ **Binary Decision Logic**: APPROVE/REJECT/MANUAL_REVIEW validation
- ✅ **Confidence Scoring**: Accuracy of confidence calculations
- ✅ **Integrated Pipeline**: End-to-end approval workflow testing
- ✅ **Statistics Generation**: Approval metrics calculation validation

**Decision Validation Scenarios**:
```python
# Clear approval case
test_data = [(0.1,)]  # Low risk → Should APPROVE

# Clear rejection case  
test_data = [(0.8,)]  # High risk → Should REJECT

# Manual review case
test_data = [(0.5,)]  # Medium risk → Should require MANUAL_REVIEW
```

#### 7. Integration & End-to-End Tests
**Class**: `TestIntegrationScenarios`

**Test Coverage**:
- ✅ **Complete Pipeline Testing**: Full ETL workflow validation
- ✅ **Data Flow Integrity**: Ensures data consistency through all steps
- ✅ **Performance Validation**: Confirms acceptable processing times
- ✅ **Error Handling**: Tests graceful failure scenarios

## Test Data & Scenarios

### Realistic Financial Profiles
The tests use realistic financial scenarios that mirror real-world loan applications:

**Low-Risk Profile**:
- Age: 35, Income: $75,000, Credit Score: 750, DTI: 0.25
- Expected: Low risk score, auto-approval with high confidence

**High-Risk Profile**:
- Age: 22, Income: $25,000, Credit Score: 500, DTI: 0.65
- Expected: High risk score, auto-rejection with high confidence

**Borderline Profile**:
- Age: 30, Income: $50,000, Credit Score: 650, DTI: 0.45
- Expected: Medium risk score, manual review required

### Statistical Validation
Tests verify statistical accuracy:
- **Risk Score Distribution**: Ensures realistic score distributions
- **Feature Correlations**: Validates expected relationships between variables
- **Outlier Rates**: Confirms reasonable outlier detection percentages

## Quality Metrics

### Test Coverage
- **Function Coverage**: 100% of public functions tested
- **Branch Coverage**: All conditional logic paths validated
- **Edge Case Coverage**: Boundary conditions and error scenarios tested

### Performance Benchmarks
- **Individual Function Tests**: < 1 second per test
- **Integration Tests**: < 10 seconds for full pipeline
- **Memory Usage**: Minimal memory footprint for fast CI/CD

### Validation Accuracy
- **Financial Accuracy**: All calculations verified against known formulas
- **Business Rule Compliance**: Domain constraints properly enforced
- **Data Quality Validation**: Comprehensive error detection

## Running Tests

### Basic Test Execution
```bash
# Run all tests
make test

# Run specific test class
poetry run pytest tests/test_financial_risk_utils.py::TestApprovalDecisions -v

# Run with coverage
poetry run pytest tests/ --cov=src --cov-report=html
```

### Continuous Integration
```bash
# CI pipeline commands
make lint          # Code quality checks
make test          # Full test suite
make type-check    # Type validation
```

## Test-Driven Development

The testing approach follows TDD principles:

1. **Red Phase**: Write failing tests for new features
2. **Green Phase**: Implement minimal code to pass tests
3. **Refactor Phase**: Improve code while maintaining test coverage

### Example TDD Workflow for Approval System
```python
# 1. Write failing test
def test_approval_decision_basic():
    # Test that doesn't pass yet
    assert calculate_approval_decision(df, config) is not None

# 2. Implement basic functionality
def calculate_approval_decision(df, config):
    # Minimal implementation
    return df.withColumn("approval_decision", lit("PENDING"))

# 3. Refactor with full business logic
def calculate_approval_decision(df, config):
    # Complete implementation with confidence scoring
    # ... (full implementation)
```

## Business Value of Testing

**Risk Mitigation**:
- Prevents financial calculation errors that could cost thousands
- Ensures regulatory compliance through validated business rules
- Protects against data quality issues that could affect decisions

**Confidence in Deployment**:
- Comprehensive validation enables safe production deployments
- Regression testing catches breaking changes early
- Performance testing ensures scalability

**Documentation Through Tests**:
- Tests serve as executable documentation of expected behavior
- Example scenarios demonstrate correct usage patterns
- Edge case tests document system limitations and assumptions 