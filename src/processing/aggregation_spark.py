"""Spark DataFrame-based aggregation functions"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, when,
    collect_set, first, concat, lit
)
from pyspark.sql.types import IntegerType
from src.models.report import ReportRow
from typing import List

import logging

logger = logging.getLogger(__name__)


def aggregate_passengers_by_country_day_season_spark(
    df: DataFrame
) -> DataFrame:
    """
    Aggregate passengers by country, day of week, and season using Spark.
    
    Each passenger is counted once per flight leg (unique passenger-flight combination).
    
    Args:
        df: Enriched Spark DataFrame with booking data
        
    Returns:
        Aggregated DataFrame with columns:
        - country
        - day_of_week
        - season
        - passenger_count
        - adults_count
        - children_count
        - average_age
    """
    logger.debug("Aggregating passengers by country, day of week, and season...")
    
    # Create unique passenger-flight key for deduplication
    # We'll use passenger_uci + origin + destination + departure_date
    df_with_key = df.withColumn(
        "passenger_flight_key",
        concat(
            col("passenger_uci").cast("string"),
            lit("_"),
            col("origin_airport"),
            lit("_"),
            col("destination_airport"),
            lit("_"),
            col("departure_date").cast("string")
        )
    )
    
    # Aggregate by country, day_of_week, season
    aggregated = df_with_key.groupBy(
        "country",
        "day_of_week",
        "season"
    ).agg(
        # Count unique passenger-flight combinations
        countDistinct("passenger_flight_key").alias("passenger_count"),
        
        # Count adults (passenger_type == 'ADT')
        countDistinct(
            when(col("passenger_type") == "ADT", col("passenger_flight_key"))
        ).alias("adults_count"),
        
        # Count children (passenger_type == 'CHD')
        countDistinct(
            when(col("passenger_type") == "CHD", col("passenger_flight_key"))
        ).alias("children_count"),
        
        # Average age (only for passengers with age data)
        avg(
            when(col("passenger_age").isNotNull(), col("passenger_age"))
        ).alias("average_age")
    )
    
    # Sort by passenger_count descending, then season and day_of_week
    aggregated = aggregated.orderBy(
        col("passenger_count").desc(),
        col("season"),
        col("day_of_week")
    )
    
    logger.debug(f"Aggregation complete: {aggregated.count()} report rows")
    
    return aggregated


def convert_to_report_rows(df: DataFrame) -> List[ReportRow]:
    """
    Convert Spark DataFrame to list of ReportRow objects.
    
    Args:
        df: Aggregated Spark DataFrame
        
    Returns:
        List of ReportRow objects
    """
    from src.models.report import ReportRow
    
    # Collect results (this is the only place we collect to driver)
    rows = df.collect()
    
    report_rows = []
    for row in rows:
        report_rows.append(
            ReportRow(
                country=row.country,
                day_of_week=row.day_of_week,
                season=row.season,
                passenger_count=row.passenger_count,
                adults_count=int(row.adults_count) if row.adults_count else None,
                children_count=int(row.children_count) if row.children_count else None,
                average_age=round(float(row.average_age), 2) if row.average_age else None
            )
        )
    
    return report_rows

