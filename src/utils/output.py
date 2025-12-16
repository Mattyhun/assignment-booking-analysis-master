"""Output utilities for writing reports"""
import csv
import json
from pathlib import Path
from typing import List

from src.models.report import ReportRow

import logging

logger = logging.getLogger(__name__)


def write_report_csv(report_rows: List[ReportRow], output_path: str) -> None:
    """
    Write report to CSV file (local or HDFS)
    
    Args:
        report_rows: List of report rows
        output_path: Output file path (local or HDFS URI)
    """
    is_hdfs = output_path.startswith('hdfs://') or output_path.startswith('hdfs:/')
    
    if is_hdfs:
        _write_csv_to_hdfs(report_rows, output_path)
    else:
        _write_csv_to_local(report_rows, output_path)


def _write_csv_to_local(report_rows: List[ReportRow], output_path: str) -> None:
    """Write CSV report to local filesystem"""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header with enhanced fields
        writer.writerow([
            'Country', 
            'Day of Week', 
            'Season', 
            'Passenger Count',
            'Adults Count',
            'Children Count',
            'Average Age'
        ])
        
        # Write rows
        for row in report_rows:
            writer.writerow([
                row.country,
                row.day_of_week,
                row.season,
                row.passenger_count,
                row.adults_count if row.adults_count is not None else '',
                row.children_count if row.children_count is not None else '',
                row.average_age if row.average_age is not None else ''
            ])
    
    logger.debug(f"Report written to {output_path} with {len(report_rows)} rows")


def _write_csv_to_hdfs(report_rows: List[ReportRow], hdfs_path: str) -> None:
    """
    Write CSV report to HDFS using Spark.
    
    For large datasets, this uses Spark's distributed writing capabilities.
    
    Args:
        report_rows: List of report rows
        hdfs_path: HDFS URI for output file
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    
    logger.debug(f"Writing report to HDFS: {hdfs_path}")
    
    # Get or create Spark session
    spark = SparkSession.getActiveSession()
    if not spark:
        from src.utils.spark_utils import create_spark_session
        spark = create_spark_session("HDFS Output Writer")
        created_session = True
    else:
        created_session = False
    
    try:
        # Convert report rows to Spark DataFrame
        schema = StructType([
            StructField("country", StringType(), False),
            StructField("day_of_week", StringType(), False),
            StructField("season", StringType(), False),
            StructField("passenger_count", IntegerType(), False),
            StructField("adults_count", IntegerType(), True),
            StructField("children_count", IntegerType(), True),
            StructField("average_age", DoubleType(), True),
        ])
        
        # Prepare data
        data = []
        for row in report_rows:
            data.append((
                row.country,
                row.day_of_week,
                row.season,
                row.passenger_count,
                row.adults_count,
                row.children_count,
                row.average_age
            ))
        
        # Create DataFrame and write to HDFS
        df = spark.createDataFrame(data, schema=schema)
        
        # Write as CSV to HDFS
        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(hdfs_path)
        
        logger.debug(f"Report written to HDFS {hdfs_path} with {len(report_rows)} rows")
        
    finally:
        if created_session:
            spark.stop()


def write_report_json(report_rows: List[ReportRow], output_path: str) -> None:
    """
    Write report to JSON file (local or HDFS)
    
    Args:
        report_rows: List of report rows
        output_path: Output file path (local or HDFS URI)
    """
    is_hdfs = output_path.startswith('hdfs://') or output_path.startswith('hdfs:/')
    
    if is_hdfs:
        _write_json_to_hdfs(report_rows, output_path)
    else:
        _write_json_to_local(report_rows, output_path)


def _write_json_to_local(report_rows: List[ReportRow], output_path: str) -> None:
    """Write JSON report to local filesystem"""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    data = [row.model_dump() for row in report_rows]
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)
    
    logger.debug(f"Report written to {output_path} with {len(report_rows)} rows")


def _write_json_to_hdfs(report_rows: List[ReportRow], hdfs_path: str) -> None:
    """
    Write JSON report to HDFS using Spark.
    
    Args:
        report_rows: List of report rows
        hdfs_path: HDFS URI for output file
    """
    from pyspark.sql import SparkSession
    
    logger.debug(f"Writing JSON report to HDFS: {hdfs_path}")
    
    # Get or create Spark session
    spark = SparkSession.getActiveSession()
    if not spark:
        from src.utils.spark_utils import create_spark_session
        spark = create_spark_session("HDFS Output Writer")
        created_session = True
    else:
        created_session = False
    
    try:
        # Convert to JSON and write
        data = [row.model_dump() for row in report_rows]
        json_str = json.dumps(data, indent=2, default=str)
        
        # Write JSON to HDFS using Spark
        # For simplicity, we'll write as text file
        # In production, you might use JSON format with proper partitioning
        spark.sparkContext.parallelize([json_str]).saveAsTextFile(hdfs_path)
        
        logger.debug(f"Report written to HDFS {hdfs_path} with {len(report_rows)} rows")
        
    finally:
        if created_session:
            spark.stop()


