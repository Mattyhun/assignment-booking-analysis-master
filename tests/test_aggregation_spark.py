"""Unit tests for Spark DataFrame aggregation functions"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

from src.processing.aggregation_spark import (
    aggregate_passengers_by_country_day_season_spark,
    convert_to_report_rows
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing"""
    from src.utils.spark_utils import create_spark_session
    spark = create_spark_session(app_name="test")
    yield spark
    spark.stop()


@pytest.fixture
def enriched_booking_df(spark):
    """Create enriched booking DataFrame with country, day_of_week, season"""
    schema = StructType([
        StructField("passenger_uci", StringType(), True),
        StructField("passenger_type", StringType(), True),
        StructField("passenger_age", IntegerType(), True),
        StructField("origin_airport", StringType(), True),
        StructField("destination_airport", StringType(), True),
        StructField("departure_date", TimestampType(), True),
        StructField("country", StringType(), True),
        StructField("day_of_week", StringType(), True),
        StructField("season", StringType(), True),
    ])
    
    from datetime import datetime
    
    data = [
        # United States, Spring, Monday - 2 passengers
        ("PASS1", "ADT", 30, "AMS", "JFK", datetime(2019, 4, 15, 10, 0), "United States", "Monday", "Spring"),
        ("PASS2", "ADT", 35, "AMS", "JFK", datetime(2019, 4, 15, 10, 0), "United States", "Monday", "Spring"),
        # United States, Spring, Sunday - 1 passenger
        ("PASS3", "CHD", 10, "AMS", "JFK", datetime(2019, 4, 14, 10, 0), "United States", "Sunday", "Spring"),
        # United Kingdom, Spring, Monday - 1 passenger
        ("PASS4", "ADT", 40, "AMS", "LHR", datetime(2019, 4, 15, 10, 0), "United Kingdom", "Monday", "Spring"),
    ]
    
    return spark.createDataFrame(data, schema)


class TestAggregationSpark:
    """Test aggregation functions on Spark DataFrame"""
    
    def test_aggregation_groups_correctly(self, spark, enriched_booking_df):
        """Test that aggregation groups by country, day, season correctly"""
        aggregated = aggregate_passengers_by_country_day_season_spark(enriched_booking_df)
        
        results = aggregated.collect()
        
        # Should have 3 groups:
        # 1. United States, Monday, Spring - 2 passengers
        # 2. United States, Sunday, Spring - 1 passenger
        # 3. United Kingdom, Monday, Spring - 1 passenger
        assert len(results) == 3
        
        # Check passenger counts
        us_monday = [r for r in results if r.country == "United States" and r.day_of_week == "Monday"][0]
        us_sunday = [r for r in results if r.country == "United States" and r.day_of_week == "Sunday"][0]
        uk_monday = [r for r in results if r.country == "United Kingdom" and r.day_of_week == "Monday"][0]
        
        assert us_monday.passenger_count == 2
        assert us_sunday.passenger_count == 1
        assert uk_monday.passenger_count == 1
    
    def test_aggregation_counts_adults_and_children(self, spark, enriched_booking_df):
        """Test that aggregation correctly counts adults vs children"""
        aggregated = aggregate_passengers_by_country_day_season_spark(enriched_booking_df)
        
        results = aggregated.collect()
        
        # United States, Monday, Spring: 2 adults, 0 children
        us_monday = [r for r in results if r.country == "United States" and r.day_of_week == "Monday"][0]
        assert us_monday.adults_count == 2
        assert us_monday.children_count == 0
        
        # United States, Sunday, Spring: 0 adults, 1 child
        us_sunday = [r for r in results if r.country == "United States" and r.day_of_week == "Sunday"][0]
        assert us_sunday.adults_count == 0
        assert us_sunday.children_count == 1
    
    def test_aggregation_calculates_average_age(self, spark, enriched_booking_df):
        """Test that aggregation correctly calculates average age"""
        aggregated = aggregate_passengers_by_country_day_season_spark(enriched_booking_df)
        
        results = aggregated.collect()
        
        # United States, Monday, Spring: (30 + 35) / 2 = 32.5
        us_monday = [r for r in results if r.country == "United States" and r.day_of_week == "Monday"][0]
        assert us_monday.average_age == pytest.approx(32.5, abs=0.1)
    
    def test_aggregation_sorts_by_passenger_count(self, spark, enriched_booking_df):
        """Test that results are sorted by passenger count descending"""
        aggregated = aggregate_passengers_by_country_day_season_spark(enriched_booking_df)
        
        results = aggregated.collect()
        
        # Should be sorted: 2, 1, 1
        passenger_counts = [r.passenger_count for r in results]
        assert passenger_counts == sorted(passenger_counts, reverse=True)
    
    def test_convert_to_report_rows(self, spark, enriched_booking_df):
        """Test conversion from Spark DataFrame to ReportRow objects"""
        aggregated = aggregate_passengers_by_country_day_season_spark(enriched_booking_df)
        report_rows = convert_to_report_rows(aggregated)
        
        assert len(report_rows) == 3
        assert all(hasattr(row, 'country') for row in report_rows)
        assert all(hasattr(row, 'passenger_count') for row in report_rows)
        assert all(hasattr(row, 'adults_count') for row in report_rows)
        assert all(hasattr(row, 'children_count') for row in report_rows)

