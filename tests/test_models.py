"""Unit tests for data models - covering validation requirements"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from src.models.booking import BookingEvent, Flight, Product, TravelRecord
from src.models.config import Config
from src.models.report import ReportRow


class TestBookingModels:
    """Test booking data model validation"""
    
    def test_passenger_model_validation(self):
        """Test that Passenger model validates required fields"""
        # Valid passenger using model_validate with aliases
        from src.models.booking import Passenger
        passenger = Passenger.model_validate({"uci": "TEST123", "passengerType": "ADT", "tattoo": 1})
        assert passenger.uci == "TEST123"
        assert passenger.passenger_type == "ADT"
        
        # Missing required field should raise error
        with pytest.raises(ValidationError):
            Passenger.model_validate({"uci": "TEST123"})  # Missing passengerType and tattoo
    
    def test_flight_model_validation(self):
        """Test that Flight model validates required fields"""
        flight = Flight.model_validate({
            "originAirport": "AMS",
            "destinationAirport": "JFK",
            "departureDate": "2019-06-15T10:00:00Z",
            "arrivalDate": "2019-06-15T20:00:00Z",
            "operatingAirline": "KL",
            "bookingStatus": "CONFIRMED"
        })
        assert flight.origin_airport == "AMS"
        assert flight.destination_airport == "JFK"
        assert flight.operating_airline == "KL"
    
    def test_flight_datetime_parsing(self):
        """Test that Flight model parses ISO datetime strings"""
        flight_data = {
            "originAirport": "AMS",
            "destinationAirport": "JFK",
            "departureDate": "2019-06-15T10:00:00Z",
            "arrivalDate": "2019-06-15T20:00:00Z",
            "operatingAirline": "KL",
            "bookingStatus": "CONFIRMED"
        }
        flight = Flight.model_validate(flight_data)
        assert isinstance(flight.departure_date, datetime)
        assert isinstance(flight.arrival_date, datetime)
    
    def test_booking_event_from_raw_json(self):
        """Test that BookingEvent can be created from raw JSON"""
        raw_json = {
            "timestamp": "2019-06-15T10:00:00Z",
            "event": {
                "DataElement": {
                    "travelrecord": {
                        "passengersList": [
                            {"uci": "TEST1", "passengerType": "ADT", "tattoo": 1}
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
        
        booking = BookingEvent.from_raw_json(raw_json)
        assert booking is not None
        assert len(booking.travel_record.passengers_list) == 1
        assert len(booking.travel_record.products_list) == 1
        assert booking.travel_record.products_list[0].flight.operating_airline == "KL"
    
    def test_booking_event_invalid_json_returns_none(self):
        """Test that invalid JSON returns None gracefully"""
        invalid_json = {
            "timestamp": "2019-06-15T10:00:00Z",
            "event": {}  # Missing travelrecord
        }
        
        booking = BookingEvent.from_raw_json(invalid_json)
        assert booking is None


class TestConfigModel:
    """Test configuration model validation"""
    
    def test_config_validates_date_range(self):
        """Test that Config validates end_date is after start_date"""
        with pytest.raises(ValidationError):
            Config(
                bookings_input="data/bookings",
                airports_input="data/airports.dat",
                output_path="output.csv",
                start_date=datetime(2019, 12, 31),
                end_date=datetime(2019, 1, 1)  # Before start_date
            )
    
    def test_config_parses_date_strings(self):
        """Test that Config parses date strings correctly"""
        config = Config(
            bookings_input="data/bookings",
            airports_input="data/airports.dat",
            output_path="output.csv",
            start_date="2019-01-01",
            end_date="2019-12-31"
        )
        assert isinstance(config.start_date, datetime)
        assert isinstance(config.end_date, datetime)
        assert config.start_date.date() == datetime(2019, 1, 1).date()
        assert config.end_date.date() == datetime(2019, 12, 31).date()
    
    def test_config_default_netherlands_airports(self):
        """Test that Config has default Netherlands airports"""
        config = Config(
            bookings_input="data/bookings",
            airports_input="data/airports.dat",
            output_path="output.csv",
            start_date="2019-01-01",
            end_date="2019-12-31"
        )
        assert len(config.netherlands_airport_codes) > 0
        assert "AMS" in config.netherlands_airport_codes


class TestReportModel:
    """Test report row model"""
    
    def test_report_row_creation(self):
        """Test that ReportRow can be created with required fields"""
        row = ReportRow(
            country="United States",
            day_of_week="Monday",
            season="Spring",
            passenger_count=100
        )
        assert row.country == "United States"
        assert row.passenger_count == 100
        assert row.adults_count is None  # Optional field
    
    def test_report_row_with_optional_fields(self):
        """Test that ReportRow accepts optional bonus fields"""
        row = ReportRow(
            country="United States",
            day_of_week="Monday",
            season="Spring",
            passenger_count=100,
            adults_count=90,
            children_count=10,
            average_age=35.5
        )
        assert row.adults_count == 90
        assert row.children_count == 10
        assert row.average_age == 35.5
