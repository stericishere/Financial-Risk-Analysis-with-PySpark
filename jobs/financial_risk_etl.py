"""
Financial Risk Analysis ETL Job

This module implements a complete ETL pipeline for analyzing financial risk data.
It follows the modular pattern of isolating Extract, Transform, and Load steps
for better testability and maintainability.
"""

import sys
import os
from typing import Dict, Any

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

# PySpark imports
from pyspark.sql import DataFrame  # noqa: E402

# Local module imports
from dependencies.spark import start_spark, stop_spark  # noqa: E402
from dependencies.logging import log_dataframe_info, log_processing_step  # noqa: E402
from utils.financial_risk_utils import (  # noqa: E402
    clean_data,
    calculate_risk_score,
    categorize_risk,
    create_age_groups,
    create_income_brackets,
    calculate_loan_to_income_ratio,
    calculate_summary_statistics,
    validate_data_quality,
    identify_outliers,
    calculate_approval_decision,
    calculate_approval_statistics,
    generate_approval_summary,
)


def extract_data(spark, config: Dict[str, Any]) -> DataFrame:
    """
    Extract data from the configured source.

    Args:
        spark: Active Spark session.
        config: Configuration dictionary.

    Returns:
        Raw DataFrame from the data source.
    """
    input_config = config["input"]

    if input_config["format"].lower() == "csv":
        df = (
            spark.read.format("csv")
            .options(**input_config["options"])
            .load(input_config["data_path"])
        )
    elif input_config["format"].lower() == "parquet":
        df = spark.read.parquet(input_config["data_path"])
    elif input_config["format"].lower() == "json":
        df = spark.read.json(input_config["data_path"])
    else:
        raise ValueError(f"Unsupported format: {input_config['format']}")

    return df


def transform_data(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """
    Transform the raw financial risk data.

    This function contains all the business logic for processing financial risk data,
    making it easily testable in isolation.

    Args:
        df: Raw input DataFrame.
        config: Configuration dictionary containing processing parameters.

    Returns:
        Transformed DataFrame ready for analysis and loading.
    """
    processing_config = config["processing"]

    # Step 1: Clean the data
    log_processing_step("Step 1: Data Cleaning")
    cleaned_df = clean_data(df)

    # Step 2: Feature Engineering
    log_processing_step("Step 2: Feature Engineering")

    # Add derived features
    enriched_df = calculate_loan_to_income_ratio(cleaned_df)
    enriched_df = create_age_groups(enriched_df)
    enriched_df = create_income_brackets(enriched_df)

    # Step 3: Risk Scoring
    log_processing_step("Step 3: Risk Score Calculation")
    feature_columns = processing_config["feature_columns"]
    risk_scored_df = calculate_risk_score(enriched_df, feature_columns)

    # Step 4: Risk Categorization
    log_processing_step("Step 4: Risk Categorization")
    risk_thresholds = processing_config["risk_thresholds"]
    categorized_df = categorize_risk(risk_scored_df, risk_thresholds)

    # Step 5: Outlier Detection
    log_processing_step("Step 5: Outlier Detection")
    outlier_df = identify_outliers(categorized_df, "LoanAmount", method="iqr")
    outlier_df = identify_outliers(outlier_df, "Income", method="iqr")
    outlier_df = identify_outliers(outlier_df, "CreditScore", method="zscore")

    # Step 6: Approval Decision Making
    log_processing_step("Step 6: Approval Decision Making")
    approval_config = processing_config["approval_config"]
    final_df = calculate_approval_decision(outlier_df, approval_config)

    return final_df


def load_data(df: DataFrame, config: Dict[str, Any]) -> None:
    """
    Load the transformed data to the configured destination.

    Args:
        df: Transformed DataFrame to save.
        config: Configuration dictionary containing output settings.
    """
    output_config = config["output"]

    # Write main analysis results
    log_processing_step("Loading Main Analysis Results")
    writer = df.write.mode(output_config["mode"])

    if "partitionBy" in output_config and output_config["partitionBy"]:
        writer = writer.partitionBy(output_config["partitionBy"])

    if output_config["format"].lower() == "parquet":
        writer.parquet(output_config["path"])
    elif output_config["format"].lower() == "csv":
        writer.option("header", "true").csv(output_config["path"])
    elif output_config["format"].lower() == "json":
        writer.json(output_config["path"])
    else:
        raise ValueError(f"Unsupported format: {output_config['format']}")


def generate_summary_reports(df: DataFrame, config: Dict[str, Any]) -> None:
    """
    Generate and save summary reports for analysis.

    Args:
        df: Transformed DataFrame.
        config: Configuration dictionary.
    """
    processing_config = config["processing"]
    output_path = config["output"]["path"]

    log_processing_step("Generating Summary Reports")

    # Generate summaries by different aggregation levels
    for agg_level in processing_config["aggregation_levels"]:
        log_processing_step(f"Creating {agg_level} Summary")

        if agg_level == "risk_category":
            summary_df = calculate_summary_statistics(df, ["risk_category"])
        elif agg_level == "age_group":
            summary_df = calculate_summary_statistics(df, ["age_group"])
        elif agg_level == "income_bracket":
            summary_df = calculate_summary_statistics(df, ["income_bracket"])
        else:
            continue

        # Save summary report
        summary_path = f"{output_path}_summary_{agg_level}"
        (
            summary_df.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(summary_path)
        )


def run_data_quality_checks(df: DataFrame) -> Dict[str, Any]:
    """
    Run comprehensive data quality checks and return metrics.

    Args:
        df: DataFrame to validate.

    Returns:
        Dictionary containing data quality metrics.
    """
    log_processing_step("Running Data Quality Checks")

    quality_metrics = validate_data_quality(df)

    # Log quality metrics
    print("\n=== Data Quality Report ===")
    print(f"Total Rows: {quality_metrics['total_rows']:,}")
    print(f"Duplicate Rows: {quality_metrics['duplicate_count']:,}")

    print("\nNull Value Analysis:")
    for column, metrics in quality_metrics["null_counts"].items():
        if metrics["count"] > 0:
            print(f"  {column}: {metrics['count']:,} ({metrics['percentage']:.2f}%)")

    print("=== End Data Quality Report ===\n")

    return quality_metrics


def main() -> None:
    """
    Main ETL job execution function.
    """
    # Initialize Spark session and logging
    spark, logger, config = start_spark(
        app_name="FinancialRiskETL", files=["configs/etl_config.json"]
    )

    if config is None:
        logger.error("No configuration found. Exiting.")
        stop_spark(spark)
        sys.exit(1)

    try:
        logger.info("Starting Financial Risk Analysis ETL Job")

        # ETL Pipeline Execution

        # EXTRACT
        log_processing_step("EXTRACT: Loading Raw Data")
        raw_data = extract_data(spark, config)
        log_dataframe_info(raw_data, "Raw Data", logger, show_sample=True)

        # Data Quality Check on Raw Data
        run_data_quality_checks(raw_data)

        # TRANSFORM
        log_processing_step("TRANSFORM: Processing Financial Risk Data")
        transformed_data = transform_data(raw_data, config)
        log_dataframe_info(
            transformed_data, "Transformed Data", logger, show_sample=True
        )

        # Data Quality Check on Transformed Data
        run_data_quality_checks(transformed_data)

        # LOAD
        log_processing_step("LOAD: Saving Results")
        load_data(transformed_data, config)

        # Generate additional summary reports
        generate_summary_reports(transformed_data, config)

        # Final Statistics
        log_processing_step("Final Statistics")
        total_rows = transformed_data.count()
        risk_distribution = (
            transformed_data.groupBy("risk_category")
            .count()
            .orderBy("count", ascending=False)
        )

        # Calculate and display approval statistics
        approval_stats = calculate_approval_statistics(transformed_data)
        
        logger.info("Processing completed successfully!")
        logger.info(f"Total records processed: {total_rows:,}")
        logger.info("Risk Category Distribution:")
        risk_distribution.show()
        
        # Display approval statistics
        logger.info("\n=== APPROVAL DECISION SUMMARY ===")
        logger.info(f"Total Applications: {approval_stats['total_applications']:,}")
        logger.info(f"Approval Rate: {approval_stats['approval_rate']:.1f}%")
        logger.info(f"Manual Review Rate: {approval_stats['manual_review_rate']:.1f}%")
        logger.info(f"Auto-Decision Rate: {approval_stats['auto_decision_rate']:.1f}%")
        logger.info(f"High Confidence Rate: {approval_stats['high_confidence_rate']:.1f}%")
        
        logger.info("\nDecision Breakdown:")
        for decision, count in approval_stats['decision_breakdown'].items():
            percentage = (count / approval_stats['total_applications']) * 100
            logger.info(f"  {decision}: {count:,} ({percentage:.1f}%)")
            
        logger.info("\nConfidence Level Breakdown:")
        for level, count in approval_stats['confidence_breakdown'].items():
            percentage = (count / approval_stats['total_applications']) * 100
            logger.info(f"  {level}: {count:,} ({percentage:.1f}%)")
        logger.info("=== END APPROVAL SUMMARY ===\n")

        # Show sample of approval decisions
        approval_sample = (
            transformed_data.select(
                "Age",
                "Income",
                "LoanAmount",
                "CreditScore",
                "risk_score",
                "approval_decision",
                "confidence_level",
                "recommendation"
            )
            .limit(10)
        )

        logger.info("Sample Approval Decisions:")
        approval_sample.show(truncate=False)
        
        # Show high-confidence auto-decisions
        auto_decisions = (
            transformed_data.filter(
                (transformed_data.confidence_level == "HIGH") & 
                (transformed_data.approval_decision != "MANUAL_REVIEW")
            )
            .select(
                "Age",
                "Income", 
                "CreditScore",
                "risk_score",
                "approval_decision",
                "confidence_score"
            )
            .limit(5)
        )
        
        logger.info("Sample High-Confidence Auto-Decisions:")
        auto_decisions.show()

    except Exception as e:
        logger.error(f"ETL job failed with error: {str(e)}")
        raise

    finally:
        # Clean up
        stop_spark(spark)
        logger.info("Spark session closed. ETL job completed.")


if __name__ == "__main__":
    main()
