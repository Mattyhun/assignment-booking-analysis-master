"""Optimized Spark DataFrame-based processing pipeline"""
from typing import List
from pyspark.sql import SparkSession

from src.models.config import Config
from src.models.report import ReportRow
from src.services.airport_lookup import AirportLookup
from src.services.data_reader_spark import SparkDataReader
from src.processing.filters_spark import apply_all_filters_spark
from src.processing.enrichment_spark import enrich_flights_with_country_and_time
from src.processing.aggregation_spark import (
    aggregate_passengers_by_country_day_season_spark,
    convert_to_report_rows
)
from src.utils.spark_utils import create_spark_session, stop_spark_session

import logging

logger = logging.getLogger(__name__)


def run_booking_analysis_pipeline_spark(config: Config) -> List[ReportRow]:
    """
    Execute optimized booking analysis pipeline using Spark DataFrames.
    
    This version uses Spark DataFrames throughout for distributed processing:
    1. Read data as Spark DataFrame (distributed)
    2. Filter using Spark operations (distributed)
    3. Enrich using Spark operations (distributed)
    4. Aggregate using Spark operations (distributed)
    5. Only collect final results to driver
    
    Args:
        config: Application configuration
        
    Returns:
        List of report rows sorted by passenger count descending
    """
    logger.info("Starting Optimized KLM Booking Analysis Pipeline (Spark DataFrames)")
    logger.debug(f"Date range: {config.start_date} to {config.end_date}")
    logger.debug(f"Input: {config.bookings_input}")
    
    spark = create_spark_session()
    
    try:
        # Initialize services
        airport_lookup = AirportLookup(config.airports_input)
        data_reader = SparkDataReader(spark)
        
        # Step 1: Read data as Spark DataFrame (distributed)
        logger.info("Step 1: Reading booking data as Spark DataFrame...")
        df = data_reader.read_bookings_dataframe(config.bookings_input)
        
        if df.count() == 0:
            logger.warning("No valid bookings found!")
            return []
        
        logger.info(f"Loaded {df.count()} passenger-flight combinations")
        
        # Step 2: Apply filters using Spark (distributed)
        logger.info("Step 2: Applying business rule filters using Spark...")
        filtered_df = apply_all_filters_spark(
            df=df,
            start_date=config.start_date,
            end_date=config.end_date,
            netherlands_airports=config.netherlands_airport_codes
        )
        
        if filtered_df.count() == 0:
            logger.warning("No bookings match the filter criteria!")
            return []
        
        logger.info(f"Filtered to {filtered_df.count()} passenger-flight combinations")
        
        # Step 3: Enrich with country, day of week, season (distributed)
        logger.info("Step 3: Enriching flights with country, day of week, and season...")
        enriched_df = enrich_flights_with_country_and_time(
            df=filtered_df,
            airport_lookup=airport_lookup,
            spark=spark
        )
        
        if enriched_df.count() == 0:
            logger.warning("No flights could be enriched!")
            return []
        
        logger.info(f"Enriched {enriched_df.count()} passenger-flight combinations")
        
        # Step 4: Aggregate using Spark (distributed)
        logger.info("Step 4: Aggregating passengers by country, day, and season...")
        aggregated_df = aggregate_passengers_by_country_day_season_spark(enriched_df)
        
        # Step 5: Convert to ReportRow objects (only collect final results)
        logger.info("Step 5: Converting to report rows...")
        report_rows = convert_to_report_rows(aggregated_df)
        
        logger.info(f"Pipeline completed successfully. Generated {len(report_rows)} report rows")
        return report_rows
        
    finally:
        stop_spark_session(spark)

