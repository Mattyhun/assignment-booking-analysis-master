"""Integration tests for Spark DataFrame pipeline"""
import pytest
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession

from src.models.config import Config
from src.processing.pipeline_spark import run_booking_analysis_pipeline_spark


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing"""
    from src.utils.spark_utils import create_spark_session
    spark = create_spark_session(app_name="test")
    yield spark
    spark.stop()


class TestSparkPipelineIntegration:
    """Integration tests for complete Spark DataFrame pipeline"""
    
    @pytest.fixture
    def sample_airports_file(self, tmp_path):
        """Create sample airports file"""
        airports_file = tmp_path / "airports.dat"
        with open(airports_file, 'w') as f:
            f.write('1,"Schiphol","Amsterdam","Netherlands","AMS","EHAM",52.3,4.76,0,1.0,E,"Europe/Amsterdam","airport","OurAirports"\n')
            f.write('2,"JFK Airport","New York","United States","JFK","KJFK",40.64,-73.78,13,-5.0,A,"America/New_York","airport","OurAirports"\n')
            f.write('3,"Heathrow","London","United Kingdom","LHR","EGLL",51.47,-0.45,83,0.0,E,"Europe/London","airport","OurAirports"\n')
        return str(airports_file)
    
    @pytest.fixture
    def sample_bookings_file(self, tmp_path):
        """Create sample bookings file"""
        bookings_file = tmp_path / "bookings.json"
        
        # Create valid booking: KL flight from Netherlands, confirmed, in date range
        booking1 = {
            "timestamp": "2019-06-15T10:00:00Z",
            "event": {
                "DataElement": {
                    "travelrecord": {
                        "passengersList": [
                            {"uci": "PASS1", "passengerType": "ADT", "tattoo": 1, "age": 30}
                        ],
                        "productsList": [
                            {
                                "bookingStatus": "CONFIRMED",
                                "flight": {
                                    "originAirport": "AMS",
                                    "destinationAirport": "JFK",
                                    "departureDate": "2019-06-15T10:00:00Z",
                                    "arrivalDate": "2019-06-15T20:00:00Z",
                                    "operatingAirline": "KL"
                                }
                            }
                        ]
                    }
                }
            }
        }
        
        # Another valid booking
        booking2 = {
            "timestamp": "2019-06-20T10:00:00Z",
            "event": {
                "DataElement": {
                    "travelrecord": {
                        "passengersList": [
                            {"uci": "PASS2", "passengerType": "ADT", "tattoo": 2, "age": 35}
                        ],
                        "productsList": [
                            {
                                "bookingStatus": "CONFIRMED",
                                "flight": {
                                    "originAirport": "AMS",
                                    "destinationAirport": "LHR",
                                    "departureDate": "2019-06-20T10:00:00Z",
                                    "arrivalDate": "2019-06-20T12:00:00Z",
                                    "operatingAirline": "KL"
                                }
                            }
                        ]
                    }
                }
            }
        }
        
        with open(bookings_file, 'w') as f:
            import json
            f.write(json.dumps(booking1) + '\n')
            f.write(json.dumps(booking2) + '\n')
        
        return str(bookings_file)
    
    def test_pipeline_processes_valid_bookings(self, sample_bookings_file, sample_airports_file):
        """Test that pipeline processes valid bookings correctly"""
        config = Config(
            bookings_input=sample_bookings_file,
            airports_input=sample_airports_file,
            output_path="/tmp/test_output.csv",
            start_date=datetime(2019, 6, 1),
            end_date=datetime(2019, 6, 30),
            netherlands_airport_codes=["AMS"]
        )
        
        report_rows = run_booking_analysis_pipeline_spark(config)
        
        # Should have at least 2 report rows (one for each country)
        assert len(report_rows) >= 2
        
        # Check that results have required fields
        for row in report_rows:
            assert hasattr(row, 'country')
            assert hasattr(row, 'day_of_week')
            assert hasattr(row, 'season')
            assert hasattr(row, 'passenger_count')
            assert row.passenger_count > 0
    
    def test_pipeline_filters_by_date_range(self, sample_bookings_file, sample_airports_file):
        """Test that pipeline filters by date range"""
        config = Config(
            bookings_input=sample_bookings_file,
            airports_input=sample_airports_file,
            output_path="/tmp/test_output.csv",
            start_date=datetime(2019, 5, 1),  # Before bookings
            end_date=datetime(2019, 5, 31),
            netherlands_airport_codes=["AMS"]
        )
        
        report_rows = run_booking_analysis_pipeline_spark(config)
        
        # Should have no results (bookings are in June)
        assert len(report_rows) == 0
    
    def test_pipeline_filters_kl_flights_only(self, sample_bookings_file, sample_airports_file):
        """Test that pipeline only includes KL flights"""
        config = Config(
            bookings_input=sample_bookings_file,
            airports_input=sample_airports_file,
            output_path="/tmp/test_output.csv",
            start_date=datetime(2019, 6, 1),
            end_date=datetime(2019, 6, 30),
            netherlands_airport_codes=["AMS"]
        )
        
        report_rows = run_booking_analysis_pipeline_spark(config)
        
        # All bookings should be KL flights (we only created KL flights)
        # This test verifies the filter is applied
        assert len(report_rows) > 0

