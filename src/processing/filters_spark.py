"""Spark DataFrame-based filtering functions for distributed processing"""
from datetime import datetime
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, when, array_contains, lit, count
import logging

logger = logging.getLogger(__name__)


def filter_by_date_range_spark(
    df: DataFrame,
    start_date: datetime,
    end_date: datetime
) -> DataFrame:
    """
    Filter DataFrame by departure date range (distributed Spark operation)
    
    Args:
        df: Spark DataFrame with booking data
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        
    Returns:
        Filtered DataFrame
    """
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    filtered = df.filter(
        (to_date(col("departure_date")) >= lit(start_date_str)) &
        (to_date(col("departure_date")) <= lit(end_date_str))
    )
    
    # Cache for performance (will be used multiple times)
    df_count = df.cache().count()
    filtered_count = filtered.count()
    logger.debug(f"Date range filter: {df_count} -> {filtered_count} rows")
    return filtered


def filter_klm_flights_spark(df: DataFrame) -> DataFrame:
    """
    Filter DataFrame to only include KL flights (distributed Spark operation)
    
    Args:
        df: Spark DataFrame with booking data
        
    Returns:
        Filtered DataFrame with only KL flights
    """
    filtered = df.filter(col("operating_airline") == "KL")
    
    filtered_count = filtered.count()
    logger.debug(f"KL filter: {df.count()} -> {filtered_count} rows")
    return filtered


def filter_netherlands_origin_spark(
    df: DataFrame,
    netherlands_airports: List[str]
) -> DataFrame:
    """
    Filter DataFrame to only include flights from Netherlands (distributed Spark operation)
    
    Args:
        df: Spark DataFrame with booking data
        netherlands_airports: List of Netherlands airport IATA codes
        
    Returns:
        Filtered DataFrame with only Netherlands origin flights
    """
    filtered = df.filter(col("origin_airport").isin(netherlands_airports))
    
    filtered_count = filtered.count()
    logger.debug(f"Netherlands origin filter: {df.count()} -> {filtered_count} rows")
    return filtered


def filter_confirmed_status_spark(df: DataFrame) -> DataFrame:
    """
    Filter DataFrame to only include CONFIRMED bookings (distributed Spark operation)
    
    Note: This filters by current status. For "latest status" logic, we need
    window functions to track status changes over time.
    
    Args:
        df: Spark DataFrame with booking data
        
    Returns:
        Filtered DataFrame with only CONFIRMED bookings
    """
    filtered = df.filter(col("booking_status") == "CONFIRMED")
    
    filtered_count = filtered.count()
    logger.debug(f"Confirmed status filter: {df.count()} -> {filtered_count} rows")
    return filtered


def get_latest_booking_status_spark(df: DataFrame) -> DataFrame:
    """
    Determine latest booking status for each passenger-flight combination.
    
    Uses Spark window functions to find the most recent booking event for each
    unique passenger-flight combination.
    
    Args:
        df: Spark DataFrame with booking data
        
    Returns:
        DataFrame with is_confirmed column indicating if latest status is CONFIRMED
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    # Create window partitioned by passenger-flight, ordered by timestamp descending
    # Match the Python logic EXACTLY: group by (passenger_uci, origin, destination, departure_date.isoformat())
    # Python uses isoformat() which creates a string, so we need to match that exactly
    # For exact matching, we convert to string to match Python's string-based comparison
    
    from pyspark.sql.functions import date_format
    
    # Convert departure_date to string format matching Python's isoformat()
    # Python: datetime.isoformat() -> "2019-06-15T10:00:00+00:00"
    # Spark: Use similar string format for exact matching
    window_spec = Window.partitionBy(
        "passenger_uci",
        "origin_airport",
        "destination_airport",
        date_format(col("departure_date"), "yyyy-MM-dd'T'HH:mm:ss")  # Match Python's isoformat() date part
    ).orderBy(
        col("booking_timestamp").desc(),
        col("booking_status")  # Secondary sort for tie-breaking (deterministic)
    )
    
    # Add row number to identify latest event
    df_with_rank = df.withColumn(
        "row_num",
        row_number().over(window_spec)
    )
    
    # Keep only the latest event for each passenger-flight
    latest_df = df_with_rank.filter(col("row_num") == 1).drop("row_num")
    
    # Add flag for confirmed status
    latest_df = latest_df.withColumn(
        "is_confirmed",
        col("booking_status") == "CONFIRMED"
    )
    
    logger.debug(f"Latest status calculation: {df.count()} -> {latest_df.count()} unique passenger-flight combinations")
    
    return latest_df


def apply_all_filters_spark(
    df: DataFrame,
    start_date: datetime,
    end_date: datetime,
    netherlands_airports: List[str]
) -> DataFrame:
    """
    Apply all business rule filters using Spark operations (distributed)
    
    Args:
        df: Spark DataFrame with booking data
        start_date: Start date for filtering
        end_date: End date for filtering
        netherlands_airports: List of Netherlands airport codes
        
    Returns:
        Filtered DataFrame
    """
    logger.debug("Applying all business rule filters using Spark...")
    
    # First, determine latest booking status for each passenger-flight
    df_with_latest_status = get_latest_booking_status_spark(df)
    
    # Filter by latest status = CONFIRMED
    filtered = df_with_latest_status.filter(col("is_confirmed") == True)
    
    # Apply other filters
    filtered = filter_by_date_range_spark(filtered, start_date, end_date)
    filtered = filter_klm_flights_spark(filtered)
    filtered = filter_netherlands_origin_spark(filtered, netherlands_airports)
    
    df_count = df.count()
    filtered_count = filtered.count()
    logger.info(f"Applied all filters: {df_count} -> {filtered_count} rows")
    
    return filtered

