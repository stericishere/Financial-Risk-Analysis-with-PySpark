"""
Unit tests for financial risk utility functions.

These tests verify the business logic functions in isolation,
following the pattern of testing transformation functions independently.
"""

import sys
import os

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.financial_risk_utils import (  # noqa: E402
    calculate_risk_score,
    categorize_risk,
    create_age_groups,
    create_income_brackets,
    calculate_loan_to_income_ratio,
    clean_data,
    identify_outliers,
    calculate_summary_statistics,
    validate_data_quality,
    calculate_approval_decision,
    calculate_approval_statistics,
    generate_approval_summary,
)


class TestRiskScoreCalculation:
    """Test risk score calculation functionality."""

    def test_calculate_risk_score_returns_valid_scores(self, sample_financial_data):
        """Test that risk scores are calculated and within valid range [0,1]."""
        feature_columns = [
            "CreditScore",
            "DTIRatio",
            "InterestRate",
            "LoanAmount",
            "Income",
        ]

        result_df = calculate_risk_score(sample_financial_data, feature_columns)

        # Check that risk_score column was added
        assert "risk_score" in result_df.columns

        # Collect risk scores and verify they're in valid range
        risk_scores = [
            row.risk_score for row in result_df.select("risk_score").collect()
        ]

        for score in risk_scores:
            assert (
                0.0 <= score <= 1.0
            ), f"Risk score {score} is outside valid range [0,1]"

    def test_calculate_risk_score_handles_empty_features(self, sample_financial_data):
        """Test risk score calculation with empty feature list."""
        result_df = calculate_risk_score(sample_financial_data, [])

        # Should add risk_score column with default values
        assert "risk_score" in result_df.columns

        # All scores should be around 0.5 (base risk)
        risk_scores = [
            row.risk_score for row in result_df.select("risk_score").collect()
        ]
        for score in risk_scores:
            assert abs(score - 0.5) < 0.1


class TestRiskCategorization:
    """Test risk categorization functionality."""

    def test_categorize_risk_assigns_correct_categories(self, spark_session):
        """Test that risk categories are assigned correctly based on thresholds."""
        # Create test data with known risk scores
        test_data = [
            (0.1,),  # Should be Low
            (0.3,),  # Should be Low (boundary)
            (0.5,),  # Should be Medium
            (0.6,),  # Should be Medium (boundary)
            (0.8,),  # Should be High
        ]

        df = spark_session.createDataFrame(test_data, ["risk_score"])

        thresholds = {"low_risk": 0.3, "medium_risk": 0.6, "high_risk": 0.8}

        result_df = categorize_risk(df, thresholds)

        # Collect results
        results = result_df.select("risk_score", "risk_category").collect()

        assert results[0].risk_category == "Low"  # 0.1
        assert results[1].risk_category == "Low"  # 0.3
        assert results[2].risk_category == "Medium"  # 0.5
        assert results[3].risk_category == "Medium"  # 0.6
        assert results[4].risk_category == "High"  # 0.8


class TestFeatureEngineering:
    """Test feature engineering functions."""

    def test_create_age_groups_assigns_correct_categories(self, spark_session):
        """Test age group categorization."""
        test_data = [(20,), (30,), (40,), (50,), (60,), (70,)]
        df = spark_session.createDataFrame(test_data, ["Age"])

        result_df = create_age_groups(df)

        results = result_df.select("Age", "age_group").collect()

        assert results[0].age_group == "18-24"  # 20
        assert results[1].age_group == "25-34"  # 30
        assert results[2].age_group == "35-44"  # 40
        assert results[3].age_group == "45-54"  # 50
        assert results[4].age_group == "55-64"  # 60
        assert results[5].age_group == "65+"  # 70

    def test_create_income_brackets_assigns_correct_categories(self, spark_session):
        """Test income bracket categorization."""
        test_data = [(25000.0,), (50000.0,), (80000.0,), (120000.0,), (200000.0,)]
        df = spark_session.createDataFrame(test_data, ["Income"])

        result_df = create_income_brackets(df)

        results = result_df.select("Income", "income_bracket").collect()

        assert results[0].income_bracket == "Low (<$30K)"
        assert results[1].income_bracket == "Lower-Middle ($30K-$60K)"
        assert results[2].income_bracket == "Middle ($60K-$100K)"
        assert results[3].income_bracket == "Upper-Middle ($100K-$150K)"
        assert results[4].income_bracket == "High ($150K+)"

    def test_calculate_loan_to_income_ratio(self, spark_session):
        """Test loan-to-income ratio calculation."""
        test_data = [
            (150000.0, 50000.0),  # LoanAmount, Income -> ratio = 3.0
            (100000.0, 50000.0),  # ratio = 2.0
            (75000.0, 75000.0),  # ratio = 1.0
        ]
        df = spark_session.createDataFrame(test_data, ["LoanAmount", "Income"])

        result_df = calculate_loan_to_income_ratio(df)

        results = result_df.select("loan_to_income_ratio").collect()

        assert abs(results[0].loan_to_income_ratio - 3.0) < 0.001
        assert abs(results[1].loan_to_income_ratio - 2.0) < 0.001
        assert abs(results[2].loan_to_income_ratio - 1.0) < 0.001


class TestDataCleaning:
    """Test data cleaning functionality."""

    def test_clean_data_removes_invalid_records(self, spark_session):
        """Test that data cleaning removes invalid records."""
        # Include invalid data
        test_data = [
            (25, 50000.0, 200000.0, 650, 24, 3, 6.5, 30, 0.35, 0.0),  # Valid
            (
                None,
                50000.0,
                200000.0,
                650,
                24,
                3,
                6.5,
                30,
                0.35,
                0.0,
            ),  # Invalid: null age
            (
                25,
                -50000.0,
                200000.0,
                650,
                24,
                3,
                6.5,
                30,
                0.35,
                0.0,
            ),  # Invalid: negative income
            (25, 50000.0, 0.0, 650, 24, 3, 6.5, 30, 0.35, 0.0),  # Invalid: zero loan
            (
                25,
                50000.0,
                200000.0,
                200,
                24,
                3,
                6.5,
                30,
                0.35,
                0.0,
            ),  # Invalid: credit score too low
            (
                15,
                50000.0,
                200000.0,
                650,
                24,
                3,
                6.5,
                30,
                0.35,
                0.0,
            ),  # Invalid: age too low
        ]

        from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

        schema = StructType(
            [
                StructField("Age", IntegerType(), True),
                StructField("Income", DoubleType(), True),
                StructField("LoanAmount", DoubleType(), True),
                StructField("CreditScore", IntegerType(), True),
                StructField("MonthsEmployed", IntegerType(), True),
                StructField("NumCreditLines", IntegerType(), True),
                StructField("InterestRate", DoubleType(), True),
                StructField("LoanTerm", IntegerType(), True),
                StructField("DTIRatio", DoubleType(), True),
                StructField("Default", DoubleType(), True),
            ]
        )

        df = spark_session.createDataFrame(test_data, schema)

        cleaned_df = clean_data(df)

        # Should only have 1 valid record
        assert cleaned_df.count() == 1

        # The remaining record should be the valid one
        remaining_record = cleaned_df.collect()[0]
        assert remaining_record.Age == 25
        assert remaining_record.Income == 50000.0


class TestOutlierDetection:
    """Test outlier detection functionality."""

    def test_identify_outliers_iqr_method(self, spark_session):
        """Test IQR-based outlier detection."""
        # Create data with obvious outliers
        test_data = [
            (50000,),  # Normal
            (55000,),  # Normal
            (60000,),  # Normal
            (52000,),  # Normal
            (200000,),  # Outlier (high)
            (10000,),  # Outlier (low)
        ]

        df = spark_session.createDataFrame(test_data, ["Income"])

        result_df = identify_outliers(df, "Income", method="iqr")

        # Check that outlier column was added
        assert "Income_outlier" in result_df.columns

        # Collect results and check outlier detection
        results = result_df.select("Income", "Income_outlier").collect()

        # The extreme values should be marked as outliers
        income_outlier_map = {row.Income: row.Income_outlier for row in results}

        # 200000 and 10000 should be outliers
        assert income_outlier_map[200000] is True
        assert income_outlier_map[10000] is True

        # Normal values should not be outliers
        assert income_outlier_map[50000] is False
        assert income_outlier_map[55000] is False


class TestSummaryStatistics:
    """Test summary statistics calculation."""

    def test_calculate_summary_statistics(self, sample_financial_data):
        """Test summary statistics calculation by group."""
        # Add risk categories first
        df_with_categories = create_age_groups(sample_financial_data)

        summary_df = calculate_summary_statistics(df_with_categories, ["age_group"])

        # Check that summary contains expected columns
        expected_columns = ["age_group", "total_count", "default_count", "default_rate"]
        for col_name in expected_columns:
            assert col_name in summary_df.columns

        # Check that we have summaries for different age groups
        assert summary_df.count() > 0

        # Verify that default_rate is calculated correctly
        results = summary_df.collect()
        for row in results:
            if row.total_count > 0:
                expected_rate = row.default_count / row.total_count
                assert abs(row.default_rate - expected_rate) < 0.0001


class TestDataQuality:
    """Test data quality validation."""

    def test_validate_data_quality(self, sample_financial_data):
        """Test data quality metrics calculation."""
        quality_metrics = validate_data_quality(sample_financial_data)

        # Check that all expected metrics are present
        assert "total_rows" in quality_metrics
        assert "null_counts" in quality_metrics
        assert "duplicate_count" in quality_metrics

        # Check total rows
        assert quality_metrics["total_rows"] == sample_financial_data.count()

        # Check null counts structure
        assert isinstance(quality_metrics["null_counts"], dict)

        # Each column should have null count information
        for column in sample_financial_data.columns:
            assert column in quality_metrics["null_counts"]
            assert "count" in quality_metrics["null_counts"][column]
            assert "percentage" in quality_metrics["null_counts"][column]


class TestIntegrationScenarios:
    """Test integrated transformation scenarios."""

    def test_complete_transformation_pipeline(
        self, sample_financial_data, sample_config
    ):
        """Test the complete transformation pipeline with real data."""
        # This simulates the transform_data function from the ETL job
        processing_config = sample_config["processing"]

        # Step 1: Clean data
        cleaned_df = clean_data(sample_financial_data)
        assert cleaned_df.count() > 0

        # Step 2: Feature engineering
        enriched_df = calculate_loan_to_income_ratio(cleaned_df)
        enriched_df = create_age_groups(enriched_df)
        enriched_df = create_income_brackets(enriched_df)

        # Check new columns were added
        assert "loan_to_income_ratio" in enriched_df.columns
        assert "age_group" in enriched_df.columns
        assert "income_bracket" in enriched_df.columns

        # Step 3: Risk scoring
        feature_columns = processing_config["feature_columns"]
        risk_scored_df = calculate_risk_score(enriched_df, feature_columns)
        assert "risk_score" in risk_scored_df.columns

        # Step 4: Risk categorization
        risk_thresholds = processing_config["risk_thresholds"]
        final_df = categorize_risk(risk_scored_df, risk_thresholds)
        assert "risk_category" in final_df.columns

        # Verify final data integrity
        final_count = final_df.count()
        assert final_count > 0

        # Check that all risk categories are valid
        risk_categories = [
            row.risk_category for row in final_df.select("risk_category").collect()
        ]
        valid_categories = {"Low", "Medium", "High"}
        for category in risk_categories:
            assert category in valid_categories


class TestApprovalDecisions:
    """Test approval decision functionality."""

    def test_calculate_approval_decision_basic(self, spark_session):
        """Test basic approval decision calculation."""
        test_data = [
            (0.1,),  # Should be APPROVE
            (0.5,),  # Should be MANUAL_REVIEW
            (0.8,),  # Should be REJECT
        ]
        
        df = spark_session.createDataFrame(test_data, ["risk_score"])
        
        approval_config = {
            "approve_threshold": 0.3,
            "reject_threshold": 0.7,
            "min_confidence": 0.8
        }
        
        result_df = calculate_approval_decision(df, approval_config)
        
        # Check that new columns were added
        expected_columns = ["approval_decision", "confidence_score", "confidence_level", "recommendation"]
        for col_name in expected_columns:
            assert col_name in result_df.columns
        
        # Collect results
        results = result_df.select("risk_score", "approval_decision").collect()
        
        assert results[0].approval_decision == "APPROVE"      # 0.1
        assert results[1].approval_decision == "MANUAL_REVIEW"  # 0.5
        assert results[2].approval_decision == "REJECT"      # 0.8

    def test_calculate_approval_statistics(self, spark_session):
        """Test approval statistics calculation."""
        test_data = [
            (0.1, "APPROVE", "HIGH"),
            (0.2, "APPROVE", "HIGH"), 
            (0.5, "MANUAL_REVIEW", "LOW"),
            (0.8, "REJECT", "HIGH"),
            (0.9, "REJECT", "HIGH"),
        ]
        
        df = spark_session.createDataFrame(
            test_data, 
            ["risk_score", "approval_decision", "confidence_level"]
        )
        
        stats = calculate_approval_statistics(df)
        
        # Check structure
        assert "total_applications" in stats
        assert "decision_breakdown" in stats
        assert "confidence_breakdown" in stats
        assert "approval_rate" in stats
        
        # Check values
        assert stats["total_applications"] == 5
        assert stats["approval_rate"] == 40.0  # 2/5 * 100
        assert stats["decision_breakdown"]["APPROVE"] == 2
        assert stats["decision_breakdown"]["REJECT"] == 2
        assert stats["decision_breakdown"]["MANUAL_REVIEW"] == 1

    def test_approval_decision_with_risk_pipeline(self, sample_financial_data):
        """Test approval decisions integrated with full risk pipeline."""
        # Calculate risk scores first
        feature_columns = ["CreditScore", "DTIRatio", "InterestRate", "LoanAmount", "Income"]
        risk_scored_df = calculate_risk_score(sample_financial_data, feature_columns)
        
        # Apply approval decisions
        approval_config = {
            "approve_threshold": 0.3,
            "reject_threshold": 0.7,
            "min_confidence": 0.8
        }
        
        final_df = calculate_approval_decision(risk_scored_df, approval_config)
        
        # Verify pipeline worked
        assert "risk_score" in final_df.columns
        assert "approval_decision" in final_df.columns
        assert "confidence_level" in final_df.columns
        
        # Check that all decisions are valid
        decisions = [row.approval_decision for row in final_df.select("approval_decision").collect()]
        valid_decisions = {"APPROVE", "REJECT", "MANUAL_REVIEW"}
        for decision in decisions:
            assert decision in valid_decisions
        
        # Check that confidence levels are valid
        confidence_levels = [row.confidence_level for row in final_df.select("confidence_level").collect()]
        valid_levels = {"HIGH", "MEDIUM", "LOW"}
        for level in confidence_levels:
            assert level in valid_levels
