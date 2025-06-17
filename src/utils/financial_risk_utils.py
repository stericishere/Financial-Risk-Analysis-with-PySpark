"""
Financial risk analysis utility functions.

This module contains business logic and utility functions for processing
financial risk data, including risk scoring, categorization, and feature engineering.
"""

from typing import Dict, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit,
    round as spark_round,
    mean,
    stddev,
    min as spark_min,
    max as spark_max,
    count,
    sum as spark_sum,
    abs as spark_abs,
)
from pyspark.sql.types import DoubleType


def calculate_risk_score(df: DataFrame, feature_columns: List[str]) -> DataFrame:
    """
    Calculate a composite risk score based on financial features.

    Args:
        df: Input DataFrame with financial data.
        feature_columns: List of columns to include in risk calculation.

    Returns:
        DataFrame with added risk_score column.
    """
    # Normalize features to 0-1 scale for consistent weighting
    normalized_df = df

    # Define weights for different risk factors
    weights = {
        "CreditScore": -0.3,  # Higher credit score = lower risk
        "DTIRatio": 0.25,  # Higher DTI = higher risk
        "InterestRate": 0.2,  # Higher rate = higher risk
        "LoanAmount": 0.15,  # Higher amount = higher risk
        "Income": -0.1,  # Higher income = lower risk
    }

    # Calculate weighted risk score
    risk_expr = lit(0.0)
    for feature in feature_columns:
        if feature in weights:
            # Normalize the feature (simple min-max scaling)
            feature_stats = df.agg(
                spark_min(col(feature)).alias("min_val"),
                spark_max(col(feature)).alias("max_val"),
            ).collect()[0]

            min_val = feature_stats["min_val"]
            max_val = feature_stats["max_val"]

            if max_val != min_val:
                normalized_feature = (col(feature) - lit(min_val)) / lit(
                    max_val - min_val
                )
                risk_expr = risk_expr + (normalized_feature * lit(weights[feature]))

    # Add base risk and normalize to 0-1 scale
    risk_score = (risk_expr + lit(0.5)).cast(DoubleType())

    return normalized_df.withColumn(
        "risk_score",
        when(risk_score < 0, lit(0.0))
        .when(risk_score > 1, lit(1.0))
        .otherwise(risk_score),
    )


def categorize_risk(df: DataFrame, thresholds: Dict[str, float]) -> DataFrame:
    """
    Categorize risk scores into low, medium, and high risk categories.

    Args:
        df: DataFrame with risk_score column.
        thresholds: Dictionary with risk threshold values.

    Returns:
        DataFrame with added risk_category column.
    """
    return df.withColumn(
        "risk_category",
        when(col("risk_score") <= thresholds["low_risk"], lit("Low"))
        .when(col("risk_score") <= thresholds["medium_risk"], lit("Medium"))
        .otherwise(lit("High")),
    )


def create_age_groups(df: DataFrame) -> DataFrame:
    """
    Create age group categories for analysis.

    Args:
        df: DataFrame with Age column.

    Returns:
        DataFrame with added age_group column.
    """
    return df.withColumn(
        "age_group",
        when(col("Age") < 25, lit("18-24"))
        .when(col("Age") < 35, lit("25-34"))
        .when(col("Age") < 45, lit("35-44"))
        .when(col("Age") < 55, lit("45-54"))
        .when(col("Age") < 65, lit("55-64"))
        .otherwise(lit("65+")),
    )


def create_income_brackets(df: DataFrame) -> DataFrame:
    """
    Create income bracket categories for analysis.

    Args:
        df: DataFrame with Income column.

    Returns:
        DataFrame with added income_bracket column.
    """
    return df.withColumn(
        "income_bracket",
        when(col("Income") < 30000, lit("Low (<$30K)"))
        .when(col("Income") < 60000, lit("Lower-Middle ($30K-$60K)"))
        .when(col("Income") < 100000, lit("Middle ($60K-$100K)"))
        .when(col("Income") < 150000, lit("Upper-Middle ($100K-$150K)"))
        .otherwise(lit("High ($150K+)")),
    )


def calculate_loan_to_income_ratio(df: DataFrame) -> DataFrame:
    """
    Calculate loan-to-income ratio as an additional risk factor.

    Args:
        df: DataFrame with LoanAmount and Income columns.

    Returns:
        DataFrame with added loan_to_income_ratio column.
    """
    return df.withColumn(
        "loan_to_income_ratio", spark_round(col("LoanAmount") / col("Income"), 3)
    )


def identify_outliers(df: DataFrame, column: str, method: str = "iqr") -> DataFrame:
    """
    Identify outliers in a specific column using IQR or Z-score method.

    Args:
        df: Input DataFrame.
        column: Column name to check for outliers.
        method: Method to use ("iqr" or "zscore").

    Returns:
        DataFrame with added outlier flag column.
    """
    if method == "iqr":
        # Calculate quartiles
        quantiles = df.approxQuantile(column, [0.25, 0.75], 0.05)
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        return df.withColumn(
            f"{column}_outlier",
            when(
                (col(column) < lit(lower_bound)) | (col(column) > lit(upper_bound)),
                lit(True),
            ).otherwise(lit(False)),
        )

    elif method == "zscore":
        # Import col function to ensure we have the right reference
        from pyspark.sql.functions import col as col_function
        
        # Calculate mean and standard deviation
        stats = df.agg(
            mean(col_function(column)).alias("mean"), stddev(col_function(column)).alias("stddev")
        ).collect()[0]

        col_mean = stats["mean"]
        col_stddev = stats["stddev"]

        return df.withColumn(
            f"{column}_outlier",
            when(
                spark_abs(col_function(column) - lit(col_mean)) / lit(col_stddev) > lit(3),
                lit(True),
            ).otherwise(lit(False)),
        )

    else:
        raise ValueError("Method must be either 'iqr' or 'zscore'")


def calculate_summary_statistics(
    df: DataFrame, group_by_columns: List[str]
) -> DataFrame:
    """
    Calculate comprehensive summary statistics grouped by specified columns.

    Args:
        df: Input DataFrame.
        group_by_columns: Columns to group by.

    Returns:
        DataFrame with summary statistics.
    """
    numeric_columns = [
        "Age",
        "Income",
        "LoanAmount",
        "CreditScore",
        "MonthsEmployed",
        "NumCreditLines",
        "InterestRate",
        "LoanTerm",
        "DTIRatio",
        "risk_score",
    ]

    # Filter to only existing numeric columns
    existing_numeric_cols = [column_name for column_name in numeric_columns if column_name in df.columns]

    summary_df = df.groupBy(group_by_columns).agg(
        count(lit(1)).alias("total_count"),
        spark_sum(col("Default")).alias("default_count"),
        *[mean(col(c)).alias(f"{c}_mean") for c in existing_numeric_cols],
        *[stddev(col(c)).alias(f"{c}_stddev") for c in existing_numeric_cols],
        *[spark_min(col(c)).alias(f"{c}_min") for c in existing_numeric_cols],
        *[spark_max(col(c)).alias(f"{c}_max") for c in existing_numeric_cols],
    )

    # Calculate default rate
    summary_df = summary_df.withColumn(
        "default_rate", spark_round(col("default_count") / col("total_count"), 4)
    )

    return summary_df


def clean_data(df: DataFrame) -> DataFrame:
    """
    Clean the financial risk dataset by handling missing values and data quality issues.

    Args:
        df: Raw input DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    # Remove rows where critical columns are null
    critical_columns = ["Age", "Income", "LoanAmount", "CreditScore", "Default"]
    cleaned_df = df

    for col_name in critical_columns:
        if col_name in df.columns:
            cleaned_df = cleaned_df.filter(col(col_name).isNotNull())

    # Handle specific data quality issues
    cleaned_df = cleaned_df.filter(
        (col("Age") >= 18)
        & (col("Age") <= 100)
        & (col("Income") > 0)
        & (col("LoanAmount") > 0)
        & (col("CreditScore") >= 300)
        & (col("CreditScore") <= 850)
        & (col("InterestRate") >= 0)
        & (col("InterestRate") <= 50)
    )

    # Fill missing values for optional columns
    if "MonthsEmployed" in df.columns:
        cleaned_df = cleaned_df.fillna({"MonthsEmployed": 0})

    if "NumCreditLines" in df.columns:
        cleaned_df = cleaned_df.fillna({"NumCreditLines": 1})

    return cleaned_df


def validate_data_quality(df: DataFrame) -> Dict[str, Any]:
    """
    Validate data quality and return quality metrics.

    Args:
        df: DataFrame to validate.

    Returns:
        Dictionary containing data quality metrics.
    """
    total_rows = df.count()

    quality_metrics = {
        "total_rows": total_rows,
        "null_counts": {},
        "duplicate_count": df.count() - df.dropDuplicates().count(),
    }

    # Check null counts for each column
    for column_name in df.columns:
        null_count = df.filter(col(column_name).isNull()).count()
        quality_metrics["null_counts"][column_name] = {
            "count": null_count,
            "percentage": round((null_count / total_rows) * 100, 2)
            if total_rows > 0
            else 0,
        }

    return quality_metrics


def calculate_approval_decision(
    df: DataFrame, 
    approval_config: Dict[str, Any]
) -> DataFrame:
    """
    Make binary approval decisions based on risk scores with confidence thresholds.
    
    Args:
        df: DataFrame with risk_score column.
        approval_config: Configuration containing thresholds and confidence settings.
        
    Returns:
        DataFrame with approval_decision, confidence_level, and recommendation columns.
    """
    # Extract configuration
    approve_threshold = approval_config.get("approve_threshold", 0.3)  # Below this = approve
    reject_threshold = approval_config.get("reject_threshold", 0.7)    # Above this = reject
    min_confidence = approval_config.get("min_confidence", 0.8)        # Confidence threshold
    
    # Calculate distance from thresholds for confidence scoring
    approve_distance = col("risk_score") / lit(approve_threshold)
    reject_distance = (lit(1.0) - col("risk_score")) / lit(1.0 - reject_threshold)
    
    # Calculate confidence (higher when far from boundary)
    confidence = when(
        col("risk_score") <= approve_threshold,
        lit(1.0) - approve_distance  # More confident when risk is much lower than threshold
    ).when(
        col("risk_score") >= reject_threshold,
        lit(1.0) - reject_distance   # More confident when risk is much higher than threshold
    ).otherwise(
        lit(0.5)  # Low confidence in the middle zone
    )
    
    # Make approval decisions
    approval_decision = when(
        col("risk_score") <= approve_threshold, lit("APPROVE")
    ).when(
        col("risk_score") >= reject_threshold, lit("REJECT")
    ).otherwise(lit("MANUAL_REVIEW"))  # Middle zone requires manual review
    
    # Add confidence level categorization
    confidence_level = when(
        confidence >= lit(min_confidence), lit("HIGH")
    ).when(
        confidence >= lit(0.6), lit("MEDIUM")
    ).otherwise(lit("LOW"))
    
    # Generate recommendation with reasoning
    recommendation = when(
        (col("approval_decision") == "APPROVE") & (col("confidence_level") == "HIGH"),
        lit("Auto-approve: Low risk with high confidence")
    ).when(
        (col("approval_decision") == "REJECT") & (col("confidence_level") == "HIGH"),
        lit("Auto-reject: High risk with high confidence")
    ).when(
        col("approval_decision") == "MANUAL_REVIEW",
        lit("Manual review required: Moderate risk zone")
    ).when(
        col("confidence_level") == "LOW",
        lit("Manual review recommended: Low confidence in decision")
    ).otherwise(lit("Review decision parameters"))
    
    return df.withColumn("approval_decision", approval_decision)\
             .withColumn("confidence_score", spark_round(confidence, 3))\
             .withColumn("confidence_level", confidence_level)\
             .withColumn("recommendation", recommendation)


def calculate_approval_statistics(df: DataFrame) -> Dict[str, Any]:
    """
    Calculate comprehensive approval statistics and metrics.
    
    Args:
        df: DataFrame with approval decisions.
        
    Returns:
        Dictionary containing approval statistics.
    """
    total_applications = df.count()
    
    # Count decisions
    decision_counts = df.groupBy("approval_decision").count().collect()
    decision_stats = {row["approval_decision"]: row["count"] for row in decision_counts}
    
    # Count confidence levels
    confidence_counts = df.groupBy("confidence_level").count().collect()
    confidence_stats = {row["confidence_level"]: row["count"] for row in confidence_counts}
    
    # Calculate approval rate
    approved = decision_stats.get("APPROVE", 0)
    approval_rate = (approved / total_applications) * 100 if total_applications > 0 else 0
    
    # Calculate manual review rate
    manual_review = decision_stats.get("MANUAL_REVIEW", 0)
    manual_review_rate = (manual_review / total_applications) * 100 if total_applications > 0 else 0
    
    # High confidence rate
    high_confidence = confidence_stats.get("HIGH", 0)
    high_confidence_rate = (high_confidence / total_applications) * 100 if total_applications > 0 else 0
    
    return {
        "total_applications": total_applications,
        "decision_breakdown": decision_stats,
        "confidence_breakdown": confidence_stats,
        "approval_rate": round(float(approval_rate), 2),
        "manual_review_rate": round(float(manual_review_rate), 2),
        "high_confidence_rate": round(float(high_confidence_rate), 2),
        "auto_decision_rate": round(float(100 - manual_review_rate), 2)
    }


def generate_approval_summary(df: DataFrame, group_by_columns: List[str]) -> DataFrame:
    """
    Generate approval summary statistics grouped by specified columns.
    
    Args:
        df: DataFrame with approval decisions.
        group_by_columns: Columns to group by for summary.
        
    Returns:
        DataFrame with approval summary statistics.
    """
    summary_df = df.groupBy(group_by_columns).agg(
        count(lit(1)).alias("total_count"),
        sum(when(col("approval_decision") == "APPROVE", 1).otherwise(0)).alias("approved_count"),
        sum(when(col("approval_decision") == "REJECT", 1).otherwise(0)).alias("rejected_count"),
        sum(when(col("approval_decision") == "MANUAL_REVIEW", 1).otherwise(0)).alias("manual_review_count"),
        sum(when(col("confidence_level") == "HIGH", 1).otherwise(0)).alias("high_confidence_count"),
        mean(col("risk_score")).alias("avg_risk_score"),
        mean(col("confidence_score")).alias("avg_confidence_score")
    )
    
    # Calculate rates
    summary_df = summary_df.withColumn(
        "approval_rate", 
        spark_round((col("approved_count") / col("total_count")) * 100, 2)
    ).withColumn(
        "rejection_rate",
        spark_round((col("rejected_count") / col("total_count")) * 100, 2)
    ).withColumn(
        "manual_review_rate",
        spark_round((col("manual_review_count") / col("total_count")) * 100, 2)
    ).withColumn(
        "high_confidence_rate",
        spark_round((col("high_confidence_count") / col("total_count")) * 100, 2)
    )
    
    return summary_df
