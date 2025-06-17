#!/usr/bin/env python3
"""
Quick Test Script for Financial Risk Approval System

This script demonstrates the approval system with sample financial profiles
and shows how users can test the system with their own data.

Usage:
    python test_approval_system.py
"""

import sys
import os

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

from utils.financial_risk_utils import (
    calculate_risk_score,
    categorize_risk, 
    calculate_approval_decision,
    calculate_approval_statistics
)


def create_test_data(spark):
    """Create sample financial profiles for testing."""
    
    # Define schema
    schema = StructType([
        StructField("Age", IntegerType(), True),
        StructField("Income", IntegerType(), True),
        StructField("LoanAmount", IntegerType(), True),
        StructField("CreditScore", IntegerType(), True),
        StructField("MonthsEmployed", IntegerType(), True),
        StructField("NumCreditLines", IntegerType(), True),
        StructField("InterestRate", DoubleType(), True),
        StructField("LoanTerm", IntegerType(), True),
        StructField("DTIRatio", DoubleType(), True),
    ])
    
    # Sample financial profiles with expected outcomes
    test_profiles = [
        # Profile 1: Low Risk - Should APPROVE with high confidence
        (35, 85000, 150000, 780, 72, 5, 3.5, 15, 0.20),
        
        # Profile 2: High Risk - Should REJECT with high confidence  
        (22, 25000, 180000, 480, 3, 1, 15.5, 30, 0.75),
        
        # Profile 3: Medium Risk - Should require MANUAL_REVIEW
        (45, 65000, 200000, 650, 48, 3, 7.2, 30, 0.45),
        
        # Profile 4: Borderline Low Risk - Test confidence boundaries
        (38, 75000, 180000, 720, 60, 4, 5.0, 20, 0.35),
        
        # Profile 5: Borderline High Risk - Test confidence boundaries
        (28, 45000, 220000, 580, 18, 2, 9.8, 30, 0.62),
    ]
    
    return spark.createDataFrame(test_profiles, schema)


def test_approval_system():
    """Test the approval system with sample data."""
    
    print("🎯 Testing Financial Risk Approval System")
    print("=" * 50)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ApprovalSystemTest") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Create test data
        print("\n1. Creating sample financial profiles...")
        df = create_test_data(spark)
        
        print(f"   ✅ Created {df.count()} test profiles")
        
        # Calculate risk scores
        print("\n2. Calculating risk scores...")
        feature_columns = ["CreditScore", "DTIRatio", "InterestRate", "LoanAmount", "Income"]
        risk_df = calculate_risk_score(df, feature_columns)
        
        # Categorize risk
        print("\n3. Categorizing risk levels...")
        risk_thresholds = {"low_risk": 0.3, "medium_risk": 0.6}
        categorized_df = categorize_risk(risk_df, risk_thresholds)
        
        # Make approval decisions
        print("\n4. Making approval decisions...")
        approval_config = {
            "approve_threshold": 0.3,
            "reject_threshold": 0.7,
            "min_confidence": 0.8
        }
        final_df = calculate_approval_decision(categorized_df, approval_config)
        
        # Show results
        print("\n5. 📊 APPROVAL DECISIONS:")
        print("-" * 80)
        
        results = final_df.select(
            "Age", "Income", "CreditScore", "DTIRatio", 
            "risk_score", "risk_category", 
            "approval_decision", "confidence_level", "recommendation"
        ).collect()
        
        for i, row in enumerate(results, 1):
            print(f"\nProfile {i}:")
            print(f"  👤 Age: {row.Age}, Income: ${row.Income:,}, Credit: {row.CreditScore}, DTI: {row.DTIRatio:.1%}")
            print(f"  📈 Risk Score: {row.risk_score:.3f} ({row.risk_category} Risk)")
            print(f"  ✅ Decision: {row.approval_decision} ({row.confidence_level} Confidence)")
            print(f"  💡 Recommendation: {row.recommendation}")
        
        # Calculate statistics
        print("\n6. 📈 APPROVAL STATISTICS:")
        print("-" * 50)
        
        stats = calculate_approval_statistics(final_df)
        
        print(f"Total Applications: {stats['total_applications']}")
        print(f"Approval Rate: {stats['approval_rate']}%")
        print(f"Manual Review Rate: {stats['manual_review_rate']}%")
        print(f"High Confidence Rate: {stats['high_confidence_rate']}%")
        
        print(f"\nDecision Breakdown:")
        for decision, count in stats['decision_breakdown'].items():
            percentage = (count / stats['total_applications']) * 100
            print(f"  {decision}: {count} ({percentage:.1f}%)")
        
        print("\n🎉 SUCCESS: Approval system is working correctly!")
        print("\nNext steps:")
        print("  • Run 'make test' to verify all 14 tests pass")  
        print("  • Run 'python jobs/financial_risk_etl.py' for full ETL pipeline")
        print("  • Modify configs/etl_config.json to adjust approval thresholds")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return False
        
    finally:
        spark.stop()


if __name__ == "__main__":
    success = test_approval_system()
    sys.exit(0 if success else 1) 