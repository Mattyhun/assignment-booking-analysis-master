"""Integration tests covering end-to-end functional requirements"""
import pytest
from datetime import datetime
from pathlib import Path
from src.models.booking import BookingEvent, Passenger, Flight, Product, TravelRecord
from src.models.config import Config
from src.processing.pipeline import run_booking_analysis_pipeline
from src.services.airport_lookup import AirportLookup


class TestEndToEndRequirements:
    """Integration tests for complete functional requirements"""
    
    @pytest.fixture
    def sample_airports_file(self, tmp_path):
        """Create sample airports file"""
        airports_file = tmp_path / "airports.dat"
        with open(airports_file, 'w') as f:
            f.write('1,"Schiphol","Amsterdam","Netherlands","AMS","EHAM",52.3,4.76,0,1.0,E,"Europe/Amsterdam","airport","OurAirports"\n')
            f.write('2,"JFK Airport","New York","United States","JFK","KJFK",40.64,-73.78,13,-5.0,A,"America/New_York","airport","OurAirports"\n')
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
        
        # Create invalid booking: Non-KL flight (should be filtered out)
        booking2 = {
            "timestamp": "2019-06-15T10:00:00Z",
            "event": {
                "DataElement": {
                    "travelrecord": {
                        "passengersList": [
                            {"uci": "PASS2", "passengerType": "ADT", "tattoo": 1}
                        ],
                        "productsList": [
                            {
                                "bookingStatus": "CONFIRMED",
                                "flight": {
                                    "originAirport": "AMS",
                                    "destinationAirport": "CDG",
                                    "departureDate": "2019-06-15T10:00:00Z",
                                    "arrivalDate": "2019-06-15T12:00:00Z",
                                    "operatingAirline": "AF"  # Air France, not KL
                                }
                            }
                        ]
                    }
                }
            }
        }
        
        # Create invalid booking: Outside date range (should be filtered out)
        booking3 = {
            "timestamp": "2020-01-15T10:00:00Z",
            "event": {
                "DataElement": {
                    "travelrecord": {
                        "passengersList": [
                            {"uci": "PASS3", "passengerType": "ADT", "tattoo": 1}
                        ],
                        "productsList": [
                            {
                                "bookingStatus": "CONFIRMED",
                                "flight": {
                                    "originAirport": "AMS",
                                    "destinationAirport": "JFK",
                                    "departureDate": "2020-01-15T10:00:00Z",  # Outside 2019 range
                                    "arrivalDate": "2020-01-15T20:00:00Z",
                                    "operatingAirline": "KL"
                                }
                            }
                        ]
                    }
                }
            }
        }
        
        import json
        with open(bookings_file, 'w') as f:
            f.write(json.dumps(booking1) + '\n')
            f.write(json.dumps(booking2) + '\n')
            f.write(json.dumps(booking3) + '\n')
        
        return str(bookings_file)
    
    def test_pipeline_filters_correctly(self, sample_bookings_file, sample_airports_file):
        """Test that pipeline correctly filters according to all requirements"""
        config = Config(
            bookings_input=sample_bookings_file,
            airports_input=sample_airports_file,
            output_path="/tmp/test_output.csv",
            start_date=datetime(2019, 6, 1),
            end_date=datetime(2019, 6, 30)
        )
        
        report_rows = run_booking_analysis_pipeline(config)
        
        # Should only include KL flight from Netherlands in date range
        # Should exclude: Non-KL flight, flights outside date range
        assert len(report_rows) > 0
        
        # Check that all rows are for United States (JFK destination)
        for row in report_rows:
            assert row.country == "United States"
            assert row.season == "Summer"  # June is summer
            # Day of week depends on timezone calculation
    
    def test_latest_status_requirement(self, sample_airports_file, tmp_path):
        """Test that only passengers with latest status = Confirmed are counted"""
        bookings_file = tmp_path / "bookings.json"
        
        import json
        
        # Booking 1: Confirmed (earlier)
        booking1 = {
            "timestamp": "2019-01-01T10:00:00Z",
            "event": {
                "DataElement": {
                    "travelrecord": {
                        "passengersList": [{"uci": "PASS1", "passengerType": "ADT", "tattoo": 1}],
                        "productsList": [{
                            "bookingStatus": "CONFIRMED",
                            "flight": {
                                "originAirport": "AMS",
                                "destinationAirport": "JFK",
                                "departureDate": "2019-06-15T10:00:00Z",
                                "arrivalDate": "2019-06-15T20:00:00Z",
                                "operatingAirline": "KL"
                            }
                        }]
                    }
                }
            }
        }
        
        # Booking 2: Cancelled (later) - same passenger-flight
        booking2 = {
            "timestamp": "2019-01-02T10:00:00Z",  # Later timestamp
            "event": {
                "DataElement": {
                    "travelrecord": {
                        "passengersList": [{"uci": "PASS1", "passengerType": "ADT", "tattoo": 1}],
                        "productsList": [{
                            "bookingStatus": "CANCELLED",  # Latest status is cancelled
                            "flight": {
                                "originAirport": "AMS",
                                "destinationAirport": "JFK",
                                "departureDate": "2019-06-15T10:00:00Z",
                                "arrivalDate": "2019-06-15T20:00:00Z",
                                "operatingAirline": "KL"
                            }
                        }]
                    }
                }
            }
        }
        
        with open(bookings_file, 'w') as f:
            f.write(json.dumps(booking1) + '\n')
            f.write(json.dumps(booking2) + '\n')
        
        config = Config(
            bookings_input=str(bookings_file),
            airports_input=sample_airports_file,
            output_path="/tmp/test_output.csv",
            start_date=datetime(2019, 6, 1),
            end_date=datetime(2019, 6, 30)
        )
        
        report_rows = run_booking_analysis_pipeline(config)
        
        # Should NOT count passenger because latest status is CANCELLED
        # The report should be empty or not include this passenger
        total_passengers = sum(row.passenger_count for row in report_rows)
        # Passenger should not be counted because latest status is CANCELLED
        assert total_passengers == 0 or all(row.passenger_count == 0 for row in report_rows)

