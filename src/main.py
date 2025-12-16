"""Main entry point for KLM Booking Analysis"""
# Windows compatibility fix - must be imported before any PySpark imports
import src.utils.pyspark_windows_fix  # noqa: F401

import logging
import sys
from typing import List

import click

from src.models.config import Config
from src.models.report import ReportRow
from src.processing.pipeline import run_booking_analysis_pipeline
from src.processing.pipeline_spark import run_booking_analysis_pipeline_spark
from src.streaming.realtime_analyzer import run_simple_streaming_demo
from src.utils.output import write_report_csv, write_report_json
from src.utils.spark_utils import create_spark_session, stop_spark_session

logger = logging.getLogger(__name__)


def configure_logging(debug: bool = False):
    """Configure logging level based on debug flag"""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True  # Override any existing configuration
    )
    # Suppress Spark's verbose logging unless in debug mode
    if not debug:
        logging.getLogger('pyspark').setLevel(logging.WARN)
        logging.getLogger('py4j').setLevel(logging.WARN)


@click.command()
@click.option('--bookings-input', required=True,
              help='Path to bookings directory (local or HDFS URI like hdfs://path/to/bookings)')
@click.option('--airports-input', required=True,
              help='Path to airports.dat file')
@click.option('--output', required=True,
              help='Output file path (CSV or JSON based on extension)')
@click.option('--start-date', required=False,
              help='Start date (YYYY-MM-DD format). Required for batch mode, optional for streaming.')
@click.option('--end-date', required=False,
              help='End date (YYYY-MM-DD format). Required for batch mode, optional for streaming.')
@click.option('--netherlands-airports',
              help='Comma-separated list of Netherlands airport codes (default: AMS,EIN,RTM,GRQ,MST)')
@click.option('--streaming', is_flag=True,
              help='Run in streaming mode for real-time analysis (shows top country for the day)')
@click.option('-d', '--debug', is_flag=True,
              help='Enable debug logging for detailed output')
@click.option('--use-spark-dataframes', is_flag=True,
              help='Use optimized Spark DataFrame processing (recommended for large datasets)')
def main(bookings_input: str, airports_input: str, output: str,
         start_date: str, end_date: str, netherlands_airports: str, streaming: bool, debug: bool, use_spark_dataframes: bool):
    """
    KLM Booking Analysis - Generate report of popular destinations by country, day of week, and season.
    
    This tool analyzes booking data to identify the most popular destination countries
    for KLM flights departing from the Netherlands, grouped by season and day of week.
    """
    configure_logging(debug=debug)
    """
    KLM Booking Analysis - Generate report of popular destinations by country, day of week, and season.
    
    This tool analyzes booking data to identify the most popular destination countries
    for KLM flights departing from the Netherlands, grouped by season and day of week.
    """
    try:
        # Validate required parameters based on mode
        if not streaming and (not start_date or not end_date):
            logger.error("--start-date and --end-date are required for batch mode")
            sys.exit(1)
        
        # For streaming, use today's date if not provided
        if streaming:
            from datetime import datetime
            if not start_date:
                start_date = datetime.now().strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
        
        config = create_application_config(
            bookings_input=bookings_input,
            airports_input=airports_input,
            output_path=output,
            start_date=start_date,
            end_date=end_date,
            netherlands_airports=netherlands_airports
        )
        
        if streaming:
            run_streaming_mode(config)
        else:
            run_batch_mode(config, output, use_spark_dataframes)
        
    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)
        sys.exit(1)


def create_application_config(
    bookings_input: str,
    airports_input: str,
    output_path: str,
    start_date: str,
    end_date: str,
    netherlands_airports: str
) -> Config:
    """
    Create and validate application configuration from command-line arguments.
    
    Args:
        bookings_input: Path to bookings directory or file
        airports_input: Path to airports.dat file
        output_path: Output file path
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        netherlands_airports: Comma-separated airport codes (optional)
        
    Returns:
        Validated Config object
    """
    nl_airports = None
    if netherlands_airports:
        nl_airports = [code.strip().upper() for code in netherlands_airports.split(',')]
    
    return Config(
        bookings_input=bookings_input,
        airports_input=airports_input,
        output_path=output_path,
        start_date=start_date,
        end_date=end_date,
        netherlands_airports=nl_airports
    )


def save_report_to_file(report_rows: List[ReportRow], output_path: str) -> None:
    """
    Save analysis report to file in appropriate format.
    
    Args:
        report_rows: List of report rows to save
        output_path: Output file path (format determined by extension)
    """
    if output_path.endswith('.json'):
        write_report_json(report_rows, output_path)
    else:
        write_report_csv(report_rows, output_path)
    
    logger.debug(f"Report saved to {output_path}")


def run_batch_mode(config: Config, output_path: str, use_spark_dataframes: bool = False) -> None:
    """
    Run analysis in batch mode (default).
    
    Args:
        config: Application configuration
        output_path: Output file path
        use_spark_dataframes: If True, use optimized Spark DataFrame processing
    """
    if use_spark_dataframes:
        logger.info("Using optimized Spark DataFrame processing")
        report_rows = run_booking_analysis_pipeline_spark(config)
    else:
        logger.info("Using standard processing (Python lists)")
        report_rows = run_booking_analysis_pipeline(config)
    
    save_report_to_file(report_rows, output_path)
    
    display_analysis_summary(report_rows)


def run_streaming_mode(config: Config) -> None:
    """
    Run analysis in streaming mode for real-time data.
    
    Shows top destination country for the current day.
    
    Args:
        config: Application configuration
    """
    logger.debug("Starting streaming mode...")
    
    spark = create_spark_session("KLM Booking Analysis - Streaming")
    
    try:
        run_simple_streaming_demo(spark, config)
    finally:
        stop_spark_session(spark)


def display_analysis_summary(report_rows: List[ReportRow]) -> None:
    """
    Display summary of analysis results to console.
    
    Args:
        report_rows: List of report rows from analysis
    """
    logger.info("Analysis complete!")
    logger.info(f"Total report rows: {len(report_rows)}")
    
    if report_rows:
        print(f"\n{'='*60}")
        logger.info("Top 10 destinations:")
        print(f"{'='*60}")
        # Print report lines without logger prefix for cleaner output
        for i, row in enumerate(report_rows[:10], 1):
            # Format with clear separation and tabulation
            country_info = f"{i:2d}. {row.country:30s} - {row.season} {row.day_of_week:9s}"
            passenger_info = f"{row.passenger_count:6d} passengers"
            
            # Adults/children breakdown (always show both for consistent formatting)
            breakdown = ""
            if row.adults_count is not None:
                adults = row.adults_count
                children = row.children_count if row.children_count else 0
                breakdown = f"{adults:3d} adults, {children:2d} children"
            
            # Average age
            age_info = ""
            if row.average_age is not None:
                age_info = f"Avg age: {row.average_age:5.1f}"
            
            # Combine with clear separators and proper alignment
            summary = f"{country_info}  |  {passenger_info:18s}  |  {breakdown:20s}  |  {age_info}"
            print(summary)
        print(f"{'='*60}\n")


if __name__ == '__main__':
    main()

