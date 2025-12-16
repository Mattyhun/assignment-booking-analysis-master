"""Unit tests for Spark DataFrame filtering functions"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

from src.processing.filters_spark import (
    filter_by_date_range_spark,
    filter_klm_flights_spark,
    filter_netherlands_origin_spark,
    get_latest_booking_status_spark,
    apply_all_filters_spark
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing"""
    from src.utils.spark_utils import create_spark_session
    spark = create_spark_session(app_name="test")
    yield spark
    spark.stop()


@pytest.fixture
def sample_booking_df(spark):
    """Create sample booking DataFrame"""
    schema = StructType([
        StructField("passenger_uci", StringType(), True),
        StructField("passenger_type", StringType(), True),
        StructField("passenger_age", IntegerType(), True),
        StructField("booking_status", StringType(), True),
        StructField("origin_airport", StringType(), True),
        StructField("destination_airport", StringType(), True),
        StructField("departure_date", TimestampType(), True),
        StructField("arrival_date", TimestampType(), True),
        StructField("operating_airline", StringType(), True),
        StructField("booking_timestamp", TimestampType(), True),
    ])
    
    data = [
        ("PASS1", "ADT", 30, "CONFIRMED", "AMS", "JFK", datetime(2019, 6, 15, 10, 0), datetime(2019, 6, 15, 20, 0), "KL", datetime(2019, 3, 1, 10, 0)),
        ("PASS2", "ADT", 25, "CONFIRMED", "AMS", "LHR", datetime(2019, 6, 20, 10, 0), datetime(2019, 6, 20, 12, 0), "KL", datetime(2019, 3, 2, 10, 0)),
        ("PASS3", "CHD", 10, "CANCELLED", "AMS", "CDG", datetime(2019, 7, 1, 10, 0), datetime(2019, 7, 1, 12, 0), "AF", datetime(2019, 3, 3, 10, 0)),  # July - outside range
        ("PASS4", "ADT", 40, "CONFIRMED", "JFK", "LAX", datetime(2019, 5, 15, 10, 0), datetime(2019, 5, 15, 20, 0), "KL", datetime(2019, 3, 4, 10, 0)),  # May - outside range
    ]
    
    return spark.createDataFrame(data, schema)


class TestDateRangeFilterSpark:
    """Test date range filtering on Spark DataFrame"""
    
    def test_filter_includes_flights_in_range(self, spark, sample_booking_df):
        """Test that flights within date range are included"""
        start = datetime(2019, 6, 1)
        end = datetime(2019, 6, 30)
        
        filtered = filter_by_date_range_spark(sample_booking_df, start, end)
        count = filtered.count()
        
        assert count == 2  # Only PASS1 and PASS2 are in June
    
    def test_filter_excludes_flights_outside_range(self, spark, sample_booking_df):
        """Test that flights outside date range are excluded"""
        start = datetime(2019, 4, 1)
        end = datetime(2019, 4, 30)
        
        filtered = filter_by_date_range_spark(sample_booking_df, start, end)
        count = filtered.count()
        
        assert count == 0


class TestKLMFlightsFilterSpark:
    """Test KL airline filtering on Spark DataFrame"""
    
    def test_filter_keeps_kl_flights(self, spark, sample_booking_df):
        """Test that KL flights are kept"""
        filtered = filter_klm_flights_spark(sample_booking_df)
        count = filtered.count()
        
        assert count == 3  # PASS1, PASS2, PASS4 are KL
    
    def test_filter_removes_non_kl_flights(self, spark, sample_booking_df):
        """Test that non-KL flights are removed"""
        filtered = filter_klm_flights_spark(sample_booking_df)
        airlines = [row.operating_airline for row in filtered.collect()]
        
        assert all(airline == "KL" for airline in airlines)


class TestNetherlandsOriginFilterSpark:
    """Test Netherlands origin filtering on Spark DataFrame"""
    
    def test_filter_keeps_netherlands_origins(self, spark, sample_booking_df):
        """Test that Netherlands origin flights are kept"""
        netherlands_airports = ["AMS", "EIN", "RTM"]
        filtered = filter_netherlands_origin_spark(sample_booking_df, netherlands_airports)
        count = filtered.count()
        
        assert count == 3  # PASS1, PASS2, PASS3 are from Netherlands
    
    def test_filter_removes_non_netherlands_origins(self, spark, sample_booking_df):
        """Test that non-Netherlands origin flights are removed"""
        netherlands_airports = ["AMS", "EIN", "RTM"]
        filtered = filter_netherlands_origin_spark(sample_booking_df, netherlands_airports)
        origins = [row.origin_airport for row in filtered.collect()]
        
        assert all(origin in netherlands_airports for origin in origins)


class TestLatestBookingStatusSpark:
    """Test latest booking status calculation using window functions"""
    
    def test_latest_status_identifies_most_recent(self, spark):
        """Test that window function correctly identifies latest status"""
        # Create DataFrame with multiple events for same passenger-flight
        schema = StructType([
            StructField("passenger_uci", StringType(), True),
            StructField("origin_airport", StringType(), True),
            StructField("destination_airport", StringType(), True),
            StructField("departure_date", TimestampType(), True),
            StructField("booking_status", StringType(), True),
            StructField("booking_timestamp", TimestampType(), True),
        ])
        
        data = [
            # Passenger 1: First event (older, CONFIRMED)
            ("PASS1", "AMS", "JFK", datetime(2019, 6, 15, 10, 0), "CONFIRMED", datetime(2019, 3, 1, 10, 0)),
            # Passenger 1: Second event (newer, CANCELLED) - should be latest
            ("PASS1", "AMS", "JFK", datetime(2019, 6, 15, 10, 0), "CANCELLED", datetime(2019, 3, 2, 10, 0)),
            # Passenger 2: First event (older, CANCELLED)
            ("PASS2", "AMS", "LHR", datetime(2019, 6, 20, 10, 0), "CANCELLED", datetime(2019, 3, 1, 10, 0)),
            # Passenger 2: Second event (newer, CONFIRMED) - should be latest
            ("PASS2", "AMS", "LHR", datetime(2019, 6, 20, 10, 0), "CONFIRMED", datetime(2019, 3, 2, 10, 0)),
        ]
        
        df = spark.createDataFrame(data, schema)
        latest_df = get_latest_booking_status_spark(df)
        
        # Collect results
        results = latest_df.collect()
        
        # Should have 2 rows (one per unique passenger-flight)
        assert len(results) == 2
        
        # Check that latest status is correct
        pass1_result = [r for r in results if r.passenger_uci == "PASS1"][0]
        pass2_result = [r for r in results if r.passenger_uci == "PASS2"][0]
        
        assert pass1_result.booking_status == "CANCELLED"  # Latest
        assert pass1_result.is_confirmed == False
        
        assert pass2_result.booking_status == "CONFIRMED"  # Latest
        assert pass2_result.is_confirmed == True


class TestApplyAllFiltersSpark:
    """Test combined filter application"""
    
    def test_all_filters_applied(self, spark, sample_booking_df):
        """Test that all filters are applied correctly"""
        start = datetime(2019, 6, 1)
        end = datetime(2019, 6, 30)
        netherlands_airports = ["AMS", "EIN", "RTM"]
        
        filtered = apply_all_filters_spark(
            sample_booking_df,
            start,
            end,
            netherlands_airports
        )
        
        count = filtered.count()
        
        # After all filters:
        # - Date range: PASS1, PASS2 (June only)
        # - KL airline: PASS1, PASS2 (both KL)
        # - Netherlands origin: PASS1, PASS2 (both from AMS)
        # - Latest status CONFIRMED: PASS1, PASS2 (both CONFIRMED)
        assert count == 2

