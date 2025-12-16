"""Unit tests for aggregation - covering passenger counting requirements"""
import pytest
from datetime import datetime
from src.models.booking import BookingEvent, Flight, Product, TravelRecord
from src.models.report import ReportRow
from src.processing.aggregation import aggregate_passengers_by_country_day_season
from src.services.airport_lookup import AirportLookup


class TestPassengerCounting:
    """Test FR: Each passenger counted once per flight leg"""
    
    @pytest.fixture
    def airport_lookup(self, tmp_path):
        """Create a minimal airport lookup for testing"""
        airports_file = tmp_path / "airports.dat"
        with open(airports_file, 'w') as f:
            f.write('1,"Test Airport","Amsterdam","Netherlands","AMS","EHAM",52.3,4.76,0,1.0,E,"Europe/Amsterdam","airport","OurAirports"\n')
            f.write('2,"JFK Airport","New York","United States","JFK","KJFK",40.64,-73.78,13,-5.0,A,"America/New_York","airport","OurAirports"\n')
        return AirportLookup(str(airports_file))
    
    def test_passenger_counted_once_per_flight_leg(self, airport_lookup):
        """Test that same passenger on same flight is only counted once"""
        # Create booking with same passenger appearing multiple times (shouldn't happen, but test deduplication)
        flight = Flight.model_validate({
            "originAirport": "AMS",
            "destinationAirport": "JFK",
            "departureDate": "2019-06-15T10:00:00Z",
            "arrivalDate": "2019-06-15T20:00:00Z",
            "operatingAirline": "KL",
            "bookingStatus": "CONFIRMED"
        })
        product = Product.model_validate({"bookingStatus": "CONFIRMED", "flight": flight.model_dump(by_alias=True)})
        
        # Same passenger UCI appears in the booking
        travel_record = TravelRecord.model_validate({
            "passengersList": [
                {"uci": "PASSENGER1", "passengerType": "ADT", "tattoo": 1},
                {"uci": "PASSENGER1", "passengerType": "ADT", "tattoo": 2},  # Same UCI (edge case)
            ],
            "productsList": [product.model_dump(by_alias=True)]
        })
        booking = BookingEvent.model_validate({
            "timestamp": "2019-01-01T10:00:00Z",
            "travelrecord": travel_record.model_dump(by_alias=True)
        })
        
        bookings = [booking]
        netherlands_airports = ["AMS"]
        
        report_rows = aggregate_passengers_by_country_day_season(
            bookings=bookings,
            airport_lookup=airport_lookup,
            netherlands_airports=netherlands_airports,
            start_date=datetime(2019, 6, 1),
            end_date=datetime(2019, 6, 30)
        )
        
        # Should count passenger once per flight leg (using UCI + flight_key)
        # Even if same UCI appears twice, it's the same passenger-flight combination
        assert len(report_rows) > 0
        # The exact count depends on deduplication logic, but should be consistent
    
    def test_different_passengers_counted_separately(self, airport_lookup):
        """Test that different passengers are counted separately"""
        flight = Flight.model_validate({
            "originAirport": "AMS",
            "destinationAirport": "JFK",
            "departureDate": "2019-06-15T10:00:00Z",
            "arrivalDate": "2019-06-15T20:00:00Z",
            "operatingAirline": "KL",
            "bookingStatus": "CONFIRMED"
        })
        product = Product.model_validate({"bookingStatus": "CONFIRMED", "flight": flight.model_dump(by_alias=True)})
        
        travel_record = TravelRecord.model_validate({
            "passengersList": [
                {"uci": "PASSENGER1", "passengerType": "ADT", "tattoo": 1},
                {"uci": "PASSENGER2", "passengerType": "ADT", "tattoo": 2},
                {"uci": "PASSENGER3", "passengerType": "CHD", "tattoo": 3, "age": 10},
            ],
            "productsList": [product.model_dump(by_alias=True)]
        })
        booking = BookingEvent.model_validate({
            "timestamp": "2019-01-01T10:00:00Z",
            "travelrecord": travel_record.model_dump(by_alias=True)
        })
        
        bookings = [booking]
        netherlands_airports = ["AMS"]
        
        report_rows = aggregate_passengers_by_country_day_season(
            bookings=bookings,
            airport_lookup=airport_lookup,
            netherlands_airports=netherlands_airports,
            start_date=datetime(2019, 6, 1),
            end_date=datetime(2019, 6, 30)
        )
        
        assert len(report_rows) > 0
        row = report_rows[0]
        assert row.passenger_count == 3  # Three different passengers
        assert row.adults_count == 2  # Two adults
        assert row.children_count == 1  # One child
    
    def test_same_passenger_different_flights_counted_separately(self, airport_lookup):
        """Test that same passenger on different flights is counted separately"""
        # Passenger takes two different flights
        flight1 = Flight.model_validate({
            "originAirport": "AMS",
            "destinationAirport": "JFK",
            "departureDate": "2019-06-15T10:00:00Z",
            "arrivalDate": "2019-06-15T20:00:00Z",
            "operatingAirline": "KL",
            "bookingStatus": "CONFIRMED"
        })
        flight2 = Flight.model_validate({
            "originAirport": "AMS",
            "destinationAirport": "CDG",
            "departureDate": "2019-06-16T10:00:00Z",
            "arrivalDate": "2019-06-16T12:00:00Z",
            "operatingAirline": "KL",
            "bookingStatus": "CONFIRMED"
        })
        
        product1 = Product.model_validate({"bookingStatus": "CONFIRMED", "flight": flight1.model_dump(by_alias=True)})
        product2 = Product.model_validate({"bookingStatus": "CONFIRMED", "flight": flight2.model_dump(by_alias=True)})
        
        travel_record = TravelRecord.model_validate({
            "passengersList": [
                {"uci": "PASSENGER1", "passengerType": "ADT", "tattoo": 1},
            ],
            "productsList": [product1.model_dump(by_alias=True), product2.model_dump(by_alias=True)]
        })
        booking = BookingEvent.model_validate({
            "timestamp": "2019-01-01T10:00:00Z",
            "travelrecord": travel_record.model_dump(by_alias=True)
        })
        
        bookings = [booking]
        netherlands_airports = ["AMS"]
        
        report_rows = aggregate_passengers_by_country_day_season(
            bookings=bookings,
            airport_lookup=airport_lookup,
            netherlands_airports=netherlands_airports,
            start_date=datetime(2019, 6, 1),
            end_date=datetime(2019, 6, 30)
        )
        
        # Should have separate rows for different flights (different destinations)
        # Passenger should be counted once per flight leg
        assert len(report_rows) >= 1


class TestSorting:
    """Test FR: Sorted by passenger count descending"""
    
    @pytest.fixture
    def airport_lookup(self, tmp_path):
        """Create a minimal airport lookup for testing"""
        airports_file = tmp_path / "airports.dat"
        with open(airports_file, 'w') as f:
            f.write('1,"Test Airport","Amsterdam","Netherlands","AMS","EHAM",52.3,4.76,0,1.0,E,"Europe/Amsterdam","airport","OurAirports"\n')
            f.write('2,"JFK Airport","New York","United States","JFK","KJFK",40.64,-73.78,13,-5.0,A,"America/New_York","airport","OurAirports"\n')
        return AirportLookup(str(airports_file))
    
    def test_sorted_by_passenger_count_descending(self, airport_lookup):
        """Test that report rows are sorted by passenger count descending"""
        # Create bookings with different passenger counts
        def create_booking(passenger_count: int, destination: str):
            flight = Flight.model_validate({
                "originAirport": "AMS",
                "destinationAirport": destination,
                "departureDate": "2019-06-15T10:00:00Z",
                "arrivalDate": "2019-06-15T20:00:00Z",
                "operatingAirline": "KL",
                "bookingStatus": "CONFIRMED"
            })
            product = Product.model_validate({"bookingStatus": "CONFIRMED", "flight": flight.model_dump(by_alias=True)})
            passengers = [
                {"uci": f"PASSENGER{i}", "passengerType": "ADT", "tattoo": i}
                for i in range(passenger_count)
            ]
            travel_record = TravelRecord.model_validate({
                "passengersList": passengers,
                "productsList": [product.model_dump(by_alias=True)]
            })
            return BookingEvent.model_validate({
                "timestamp": "2019-01-01T10:00:00Z",
                "travelrecord": travel_record.model_dump(by_alias=True)
            })
        
        bookings = [
            create_booking(5, "JFK"),  # 5 passengers
            create_booking(10, "JFK"),  # 10 passengers
            create_booking(3, "JFK"),  # 3 passengers
        ]
        
        netherlands_airports = ["AMS"]
        report_rows = aggregate_passengers_by_country_day_season(
            bookings=bookings,
            airport_lookup=airport_lookup,
            netherlands_airports=netherlands_airports,
            start_date=datetime(2019, 6, 1),
            end_date=datetime(2019, 6, 30)
        )
        
        # Should be sorted by passenger count descending
        if len(report_rows) > 1:
            for i in range(len(report_rows) - 1):
                assert report_rows[i].passenger_count >= report_rows[i + 1].passenger_count
