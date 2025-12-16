"""Spark DataFrame-based enrichment functions"""
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, udf, broadcast, when, month, date_format, to_timestamp, lit
)
from pyspark.sql.types import StringType
from typing import Dict
import pytz

import logging

logger = logging.getLogger(__name__)


def create_season_udf():
    """Create UDF for calculating season from date"""
    def calculate_season(date):
        if date is None:
            return None
        month_num = date.month
        if month_num in [12, 1, 2]:
            return "Winter"
        elif month_num in [3, 4, 5]:
            return "Spring"
        elif month_num in [6, 7, 8]:
            return "Summer"
        else:  # 9, 10, 11
            return "Fall"
    
    return udf(calculate_season, StringType())


def create_day_of_week_udf(timezone_map: Dict[str, str]):
    """Create UDF for calculating day of week in destination timezone"""
    def get_day_of_week(departure_date, destination_airport):
        if departure_date is None or destination_airport is None:
            return None
        
        # Get timezone for destination airport
        tz_str = timezone_map.get(destination_airport)
        if not tz_str:
            # Fallback to UTC
            tz = pytz.UTC
        else:
            try:
                tz = pytz.timezone(tz_str)
            except:
                tz = pytz.UTC
        
        # Convert to destination timezone
        if departure_date.tzinfo is None:
            # Assume UTC if no timezone info
            dt_utc = pytz.UTC.localize(departure_date)
        else:
            dt_utc = departure_date
        
        dt_local = dt_utc.astimezone(tz)
        
        # Get day of week
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        return days[dt_local.weekday()]
    
    return udf(get_day_of_week, StringType())


def enrich_flights_with_country_and_time(
    df: DataFrame,
    airport_lookup,
    spark
) -> DataFrame:
    """
    Enrich flight DataFrame with country, day of week, and season.
    
    Uses broadcast join for airport lookup (small dataset).
    
    Args:
        df: Spark DataFrame with flight data
        airport_lookup: AirportLookup service instance
        spark: SparkSession for creating broadcast variables
        
    Returns:
        Enriched DataFrame with country, day_of_week, and season columns
    """
    logger.debug("Enriching flights with country, day of week, and season...")
    
    # Create airport lookup DataFrame (small, will be broadcast)
    airport_data = []
    for iata, airport in airport_lookup._cache.items():
        airport_data.append({
            'iata': iata,
            'country': airport.country,
            'timezone': airport.tz_database
        })
    
    if not airport_data:
        logger.warning("No airport data available for enrichment")
        return df.withColumn("country", lit(None).cast(StringType())) \
                .withColumn("day_of_week", lit(None).cast(StringType())) \
                .withColumn("season", lit(None).cast(StringType()))
    
    airport_df = spark.createDataFrame(airport_data)
    
    # Broadcast join for airport lookup (small dataset)
    enriched_df = df.join(
        broadcast(airport_df),
        df.destination_airport == airport_df.iata,
        "left"
    ).select(
        df["*"],
        col("country").alias("destination_country"),
        col("timezone").alias("destination_timezone")
    )
    
    # Create timezone map for UDF
    timezone_map = {
        iata: airport.tz_database
        for iata, airport in airport_lookup._cache.items()
    }
    
    # Add season column
    season_udf = create_season_udf()
    enriched_df = enriched_df.withColumn(
        "season",
        season_udf(col("departure_date"))
    )
    
    # Add day of week column (using destination timezone)
    day_of_week_udf = create_day_of_week_udf(timezone_map)
    enriched_df = enriched_df.withColumn(
        "day_of_week",
        day_of_week_udf(col("departure_date"), col("destination_airport"))
    )
    
    # Rename country column
    enriched_df = enriched_df.withColumnRenamed("destination_country", "country")
    
    # Filter out rows where enrichment failed
    enriched_df = enriched_df.filter(
        col("country").isNotNull() &
        col("day_of_week").isNotNull() &
        col("season").isNotNull()
    )
    
    logger.debug(f"Enrichment complete: {df.count()} -> {enriched_df.count()} rows")
    
    return enriched_df

