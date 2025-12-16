"""Real-time streaming analyzer for booking data"""
from datetime import datetime, timedelta
from typing import Optional
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, desc
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from src.models.config import Config
from src.services.airport_lookup import AirportLookup

logger = logging.getLogger(__name__)


def run_streaming_analysis(
    spark: SparkSession,
    config: Config,
    airport_lookup: AirportLookup,
    checkpoint_location: str = "checkpoints/streaming"
) -> None:
    """
    Run real-time streaming analysis on booking data.
    
    Processes streaming data and shows top destination country for the current day.
    
    Args:
        spark: SparkSession instance
        config: Application configuration
        airport_lookup: Airport lookup service
        checkpoint_location: Location for Spark streaming checkpoints
    """
    logger.info("Starting real-time streaming analysis...")
    
    # Define schema for booking JSON
    booking_schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("event", StructType([
            StructField("DataElement", StructType([
                StructField("travelrecord", StructType([
                    StructField("passengersList", StringType(), True),
                    StructField("productsList", StringType(), True)
                ]), True)
            ]), True)
        ]), True)
    ])
    
    # Read streaming data
    if config.is_hdfs_input:
        stream_df = spark.readStream \
            .format("json") \
            .schema(booking_schema) \
            .option("maxFilesPerTrigger", 10) \
            .load(config.bookings_input)
    else:
        # For local testing, simulate streaming from directory
        stream_df = spark.readStream \
            .format("json") \
            .schema(booking_schema) \
            .option("maxFilesPerTrigger", 1) \
            .load(config.bookings_input)
    
    # Process streaming data
    processed_stream = process_streaming_bookings(
        stream_df, 
        airport_lookup, 
        config.netherlands_airport_codes
    )
    
    # Aggregate by day and country
    daily_aggregation = processed_stream \
        .withWatermark("departure_timestamp", "1 day") \
        .groupBy(
            window(col("departure_timestamp"), "1 day").alias("day_window"),
            col("destination_country")
        ) \
        .agg(count("*").alias("passenger_count")) \
        .select(
            col("day_window.start").alias("date"),
            col("destination_country").alias("country"),
            col("passenger_count")
        ) \
        .orderBy(desc("passenger_count"))
    
    # Output to console (for demonstration)
    query = daily_aggregation.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    logger.info("Streaming query started. Showing top countries for each day...")
    logger.info("Press Ctrl+C to stop")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping streaming query...")
        query.stop()
        logger.info("Streaming stopped")


def process_streaming_bookings(stream_df, airport_lookup: AirportLookup, netherlands_airports: list):
    """
    Process streaming booking data to extract relevant information.
    
    This is a simplified version for streaming - in production, you'd use
    more sophisticated parsing with UDFs or structured operations.
    """
    from pyspark.sql.functions import udf, from_json, col, lit
    from pyspark.sql.types import StringType
    
    # For streaming, we'll use a simplified approach
    # In production, you'd parse the JSON more carefully
    
    # Extract destination country from flight data
    # This is a simplified version - full implementation would parse JSON properly
    def extract_destination_country(products_json):
        """Extract destination country from products list"""
        try:
            import json
            products = json.loads(products_json) if isinstance(products_json, str) else products_json
            if products and len(products) > 0:
                flight = products[0].get('flight', {})
                destination = flight.get('destinationAirport', '')
                # Lookup country - simplified, would need UDF with broadcast lookup
                return destination
        except:
            pass
        return None
    
    # For now, return simplified stream
    # In production, implement proper JSON parsing with UDFs
    return stream_df.select(
        col("timestamp").alias("departure_timestamp"),
        lit("Unknown").alias("destination_country")  # Placeholder
    )


def run_simple_streaming_demo(spark: SparkSession, config: Config) -> None:
    """
    Simplified streaming demo that processes data in batches.
    
    This version processes data in time windows and shows top country for the day.
    More suitable for demonstration purposes.
    """
    logger.info("Running simplified streaming analysis (batch mode)...")
    
    # Use date from config (or today if not specified)
    if config.start_date:
        current_date = config.start_date.date()
    else:
        current_date = datetime.now().date()
    
    logger.info(f"Processing bookings for date: {current_date}")
    
    # This would be replaced with actual streaming in production
    # For now, we'll process the data file and show top countries
    from src.services.data_reader import DataReader
    from src.processing.pipeline import (
        load_and_parse_booking_data,
        apply_business_rule_filters,
        aggregate_passenger_statistics
    )
    from src.services.airport_lookup import AirportLookup
    
    # Initialize services
    airport_lookup = AirportLookup(config.airports_input)
    data_reader = DataReader(spark)
    
    # Load today's data (filtered by date)
    bookings = load_and_parse_booking_data(data_reader, config.bookings_input)
    
    # Filter for today's date
    today_start = datetime.combine(current_date, datetime.min.time())
    today_end = datetime.combine(current_date, datetime.max.time())
    
    filtered_bookings = apply_business_rule_filters(
        bookings=bookings,
        start_date=today_start,
        end_date=today_end,
        netherlands_airports=config.netherlands_airport_codes
    )
    
    if not filtered_bookings:
        logger.info("No bookings found for the specified date")
        return
    
    # Aggregate by country (simplified - no day/season grouping for streaming)
    from collections import defaultdict
    
    country_counts = defaultdict(int)
    for booking in filtered_bookings:
        from src.processing.filters import get_kl_flights_from_netherlands
        from src.processing.enrichment import enrich_flight_with_country_and_time
        
        kl_flights = get_kl_flights_from_netherlands(
            booking,
            config.netherlands_airport_codes,
            today_start,
            today_end
        )
        
        for product in kl_flights:
            country, _, _ = enrich_flight_with_country_and_time(product, airport_lookup)
            if country:
                # Count unique passengers per country
                for passenger in booking.travel_record.passengers_list:
                    country_counts[country] += 1
    
    # Show top countries
    sorted_countries = sorted(country_counts.items(), key=lambda x: x[1], reverse=True)
    
    print(f"\n{'='*60}")
    logger.info(f"Top Destination Countries for {current_date}")
    print(f"{'='*60}")
    # Print report lines without logger prefix for cleaner output
    for i, (country, count) in enumerate(sorted_countries[:10], 1):
        print(f"{i:2d}. {country:30s} - {count:6d} passengers")
    print(f"{'='*60}\n")

