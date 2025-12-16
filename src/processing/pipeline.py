"""Main processing pipeline for booking analysis"""
from typing import List

from pyspark.sql import SparkSession

from src.models.config import Config
from src.models.report import ReportRow
from src.services.airport_lookup import AirportLookup
from src.services.data_reader import DataReader
from src.processing.filters import (
    filter_by_date_range,
    filter_klm_flights,
    filter_netherlands_origin,
    filter_confirmed_status
)
from src.processing.aggregation import aggregate_passengers_by_country_day_season
from src.utils.spark_utils import create_spark_session

import logging

logger = logging.getLogger(__name__)


def run_booking_analysis_pipeline(config: Config) -> List[ReportRow]:
    """
    Execute the complete booking analysis pipeline.
    
    This is the main orchestrator that coordinates all processing steps:
    1. Initialize services (Spark, AirportLookup, DataReader)
    2. Load and parse booking data
    3. Apply business rule filters
    4. Aggregate passengers by country, day, and season
    5. Return sorted report rows
    
    Args:
        config: Application configuration
        
    Returns:
        List of report rows sorted by season, day of week, and passenger count
    """
    logger.info("Starting KLM Booking Analysis Pipeline")
    logger.debug(f"Date range: {config.start_date} to {config.end_date}")
    logger.debug(f"Input: {config.bookings_input}")
    
    spark = initialize_spark_session()
    
    try:
        services = initialize_services(config, spark)
        
        bookings = load_and_parse_booking_data(services['data_reader'], config.bookings_input)
        
        if not bookings:
            logger.warning("No valid bookings found!")
            return []
        
        filtered_bookings = apply_business_rule_filters(
            bookings=bookings,
            start_date=config.start_date,
            end_date=config.end_date,
            netherlands_airports=config.netherlands_airport_codes
        )
        
        if not filtered_bookings:
            logger.warning("No bookings match the filter criteria!")
            return []
        
        report_rows = aggregate_passenger_statistics(
            bookings=filtered_bookings,
            airport_lookup=services['airport_lookup'],
            netherlands_airports=config.netherlands_airport_codes,
            start_date=config.start_date,
            end_date=config.end_date
        )
        
        logger.info(f"Pipeline completed successfully. Generated {len(report_rows)} report rows")
        return report_rows
        
    finally:
        cleanup_spark_session(spark)


def initialize_spark_session() -> SparkSession:
    """
    Initialize and configure Spark session for data processing.
    
    Returns:
        Configured SparkSession instance
    """
    logger.debug("Initializing Spark session...")
    spark = create_spark_session()
    logger.debug("Spark session initialized successfully")
    return spark


def initialize_services(config: Config, spark: SparkSession) -> dict:
    """
    Initialize all required services for data processing.
    
    Args:
        config: Application configuration
        spark: SparkSession instance
        
    Returns:
        Dictionary containing initialized services:
        - 'airport_lookup': AirportLookup service
        - 'data_reader': DataReader service
    """
    logger.debug("Initializing services...")
    
    airport_lookup = AirportLookup(config.airports_input)
    data_reader = DataReader(spark)
    
    logger.debug("Services initialized successfully")
    
    return {
        'airport_lookup': airport_lookup,
        'data_reader': data_reader
    }


def load_and_parse_booking_data(data_reader: DataReader, bookings_input: str) -> List:
    """
    Load booking data from files and parse into BookingEvent objects.
    
    Args:
        data_reader: DataReader service instance
        bookings_input: Path to bookings directory or file
        
    Returns:
        List of valid BookingEvent objects
    """
    logger.debug("Loading and parsing booking data...")
    
    bookings = list(data_reader.read_bookings(bookings_input))
    valid_bookings = [b for b in bookings if b is not None]
    
    logger.info(f"Loaded {len(valid_bookings)} valid bookings")
    logger.debug(f"Total records processed: {len(bookings)}")
    
    return valid_bookings


def apply_business_rule_filters(
    bookings: List,
    start_date,
    end_date,
    netherlands_airports: List[str]
) -> List:
    """
    Apply all business rule filters to booking data.
    
    Filters applied in order:
    1. Date range filter (only flights within date range)
    2. KL airline filter (only KLM operating flights)
    3. Netherlands origin filter (only flights departing from Netherlands)
    4. Confirmed status filter (only confirmed bookings)
    
    Args:
        bookings: List of BookingEvent objects
        start_date: Start date for filtering
        end_date: End date for filtering
        netherlands_airports: List of Netherlands airport IATA codes
        
    Returns:
        Filtered list of BookingEvent objects
    """
    logger.debug("Applying business rule filters...")
    
    filtered = filter_by_date_range(bookings, start_date, end_date)
    filtered = filter_klm_flights(filtered)
    filtered = filter_netherlands_origin(filtered, netherlands_airports)
    filtered = filter_confirmed_status(filtered)
    
    logger.info(f"Filtered to {len(filtered)} bookings matching criteria")
    
    return filtered


def aggregate_passenger_statistics(
    bookings: List,
    airport_lookup: AirportLookup,
    netherlands_airports: List[str],
    start_date,
    end_date
) -> List[ReportRow]:
    """
    Aggregate passenger counts by country, day of week, and season.
    
    Each passenger is counted once per flight leg (unique passenger-flight combination).
    
    Args:
        bookings: List of filtered BookingEvent objects
        airport_lookup: AirportLookup service for country/timezone lookups
        netherlands_airports: List of Netherlands airport codes
        start_date: Start date for date filtering
        end_date: End date for date filtering
        
    Returns:
        List of ReportRow objects sorted by season, day of week, and passenger count
    """
    logger.debug("Aggregating passenger statistics...")
    
    report_rows = aggregate_passengers_by_country_day_season(
        bookings=bookings,
        airport_lookup=airport_lookup,
        netherlands_airports=netherlands_airports,
        start_date=start_date,
        end_date=end_date
    )
    
    logger.debug(f"Generated {len(report_rows)} report rows")
    
    return report_rows


def cleanup_spark_session(spark: SparkSession) -> None:
    """
    Clean up Spark session resources.
    
    Args:
        spark: SparkSession instance to stop
    """
    logger.debug("Cleaning up Spark session...")
    spark.stop()
    logger.debug("Spark session stopped")

