"""
Manual validation tests - Compare test results with actual business logic execution.

These tests run the actual business logic and compare results with expected behavior,
making it easy to verify that tests are testing the right things.
"""
import pytest
from datetime import datetime
from pathlib import Path
import json

from src.models.booking import BookingEvent
from src.processing.filters import (
    filter_by_date_range,
    filter_klm_flights,
    filter_netherlands_origin,
    get_latest_booking_status_map
)
from src.processing.enrichment import calculate_season, get_day_of_week
from src.services.airport_lookup import AirportLookup


class TestManualValidation:
    """Tests that demonstrate business logic with real examples"""
    
    @pytest.fixture
    def sample_bookings(self):
        """Load real booking data for validation"""
        bookings_file = Path("data/bookings/booking.json")
        if not bookings_file.exists():
            pytest.skip(f"Sample data not found: {bookings_file}")
        
        bookings = []
        with open(bookings_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    booking = BookingEvent.from_raw_json(data)
                    if booking:
                        bookings.append(booking)
                except Exception:
                    continue
        
        return bookings
    
    @pytest.fixture
    def airport_lookup(self):
        """Load airport data"""
        airports_file = Path("data/airports/airports.dat")
        if not airports_file.exists():
            pytest.skip(f"Airport data not found: {airports_file}")
        return AirportLookup(str(airports_file))
    
    def test_date_filtering_with_real_data(self, sample_bookings):
        """Validate date filtering works correctly with real data"""
        # Filter for 2019 data
        start_date = datetime(2019, 1, 1)
        end_date = datetime(2019, 12, 31)
        
        filtered = filter_by_date_range(sample_bookings, start_date, end_date)
        
        # Note: filter_by_date_range keeps bookings if ANY flight is in range
        # So a booking might have flights outside range, but is kept if it has at least one in range
        # Verify that each filtered booking has at least one flight in range
        for booking in filtered:
            has_flight_in_range = any(
                start_date.date() <= product.flight.departure_date.date() <= end_date.date()
                for product in booking.travel_record.products_list
            )
            assert has_flight_in_range, \
                f"Booking should have at least one flight in date range"
        
        # Verify we filtered something (if there's data outside range)
        print(f"\nDate filtering: {len(sample_bookings)} -> {len(filtered)} bookings")
        assert len(filtered) <= len(sample_bookings)
    
    def test_kl_filtering_with_real_data(self, sample_bookings):
        """Validate KL filtering works correctly with real data"""
        filtered = filter_klm_flights(sample_bookings)
        
        # Note: filter_klm_flights keeps bookings if ANY flight is KL
        # So a booking might have non-KL flights, but is kept if it has at least one KL flight
        # Verify that each filtered booking has at least one KL flight
        for booking in filtered:
            has_kl_flight = any(
                product.flight.operating_airline == "KL"
                for product in booking.travel_record.products_list
            )
            assert has_kl_flight, \
                f"Booking should have at least one KL flight"
        
        # Count airlines in original data
        all_airlines = set()
        for booking in sample_bookings:
            for product in booking.travel_record.products_list:
                all_airlines.add(product.flight.operating_airline)
        
        print(f"\nKL filtering: {len(sample_bookings)} -> {len(filtered)} bookings")
        print(f"Airlines in data: {sorted(all_airlines)}")
        assert len(filtered) <= len(sample_bookings)
    
    def test_netherlands_origin_with_real_data(self, sample_bookings):
        """Validate Netherlands origin filtering with real data"""
        netherlands_airports = ["AMS", "EIN", "RTM", "GRQ", "MST"]
        filtered = filter_netherlands_origin(sample_bookings, netherlands_airports)
        
        # Note: filter_netherlands_origin keeps bookings if ANY flight originates from Netherlands
        # So a booking might have non-Netherlands flights, but is kept if it has at least one from Netherlands
        # Verify that each filtered booking has at least one flight from Netherlands
        for booking in filtered:
            has_nl_origin = any(
                product.flight.origin_airport in netherlands_airports
                for product in booking.travel_record.products_list
            )
            assert has_nl_origin, \
                f"Booking should have at least one flight from Netherlands"
        
        # Count origins in original data
        all_origins = set()
        for booking in sample_bookings:
            for product in booking.travel_record.products_list:
                all_origins.add(product.flight.origin_airport)
        
        print(f"\nNetherlands filtering: {len(sample_bookings)} -> {len(filtered)} bookings")
        print(f"Origins in data: {sorted(all_origins)}")
        assert len(filtered) <= len(sample_bookings)
    
    def test_latest_status_tracking_with_real_data(self, sample_bookings):
        """Validate latest status tracking with real data"""
        status_map = get_latest_booking_status_map(sample_bookings)
        
        # Verify map structure
        assert len(status_map) > 0, "Status map should not be empty"
        
        # Count confirmed vs cancelled
        confirmed = sum(1 for is_confirmed, _ in status_map.values() if is_confirmed)
        cancelled = len(status_map) - confirmed
        
        print(f"\nLatest status tracking:")
        print(f"  Total passenger-flight combinations: {len(status_map)}")
        print(f"  CONFIRMED: {confirmed}")
        print(f"  CANCELLED/Other: {cancelled}")
        
        # Verify structure
        for key, (is_confirmed, timestamp) in status_map.items():
            assert isinstance(key, tuple), "Key should be tuple"
            assert len(key) == 4, "Key should have 4 elements (uci, origin, dest, date)"
            assert isinstance(is_confirmed, bool), "is_confirmed should be bool"
            assert isinstance(timestamp, datetime), "timestamp should be datetime"
    
    def test_season_calculation_examples(self):
        """Validate season calculation with known examples"""
        test_cases = [
            (datetime(2019, 1, 15), "Winter"),
            (datetime(2019, 2, 15), "Winter"),
            (datetime(2019, 3, 15), "Spring"),
            (datetime(2019, 4, 15), "Spring"),
            (datetime(2019, 5, 15), "Spring"),
            (datetime(2019, 6, 15), "Summer"),
            (datetime(2019, 7, 15), "Summer"),
            (datetime(2019, 8, 15), "Summer"),
            (datetime(2019, 9, 15), "Fall"),
            (datetime(2019, 10, 15), "Fall"),
            (datetime(2019, 11, 15), "Fall"),
            (datetime(2019, 12, 15), "Winter"),
        ]
        
        for date, expected_season in test_cases:
            actual = calculate_season(date)
            assert actual == expected_season, \
                f"Date {date.date()} should be {expected_season}, got {actual}"
        
        print(f"\nSeason calculation: All {len(test_cases)} test cases passed")
    
    def test_day_of_week_timezone_examples(self, airport_lookup):
        """Validate day of week calculation uses timezone correctly"""
        # Test that same UTC time gives different days in different timezones
        utc_time = datetime(2019, 6, 15, 22, 0, 0)  # 10 PM UTC
        
        # Get timezones for known airports
        ams_tz = airport_lookup.get_timezone("AMS")
        jfk_tz = airport_lookup.get_timezone("JFK")
        
        if ams_tz and jfk_tz:
            day_ams = get_day_of_week(utc_time, ams_tz)
            day_jfk = get_day_of_week(utc_time, jfk_tz)
            
            print(f"\nDay of week calculation:")
            print(f"  UTC time: {utc_time}")
            print(f"  Amsterdam ({ams_tz}): {day_ams}")
            print(f"  New York ({jfk_tz}): {day_jfk}")
            
            # They might be different due to timezone offset
            assert day_ams in ["Saturday", "Sunday", "Monday"]
            assert day_jfk in ["Saturday", "Sunday"]
    
    def test_end_to_end_with_real_data(self, sample_bookings, airport_lookup):
        """Validate complete pipeline with real data"""
        from src.processing.aggregation import aggregate_passengers_by_country_day_season
        
        # Apply all filters
        start_date = datetime(2019, 1, 1)
        end_date = datetime(2019, 12, 31)
        netherlands_airports = ["AMS", "EIN", "RTM", "GRQ", "MST"]
        
        filtered_by_date = filter_by_date_range(sample_bookings, start_date, end_date)
        filtered_by_kl = filter_klm_flights(filtered_by_date)
        filtered_by_origin = filter_netherlands_origin(filtered_by_kl, netherlands_airports)
        
        # Aggregate
        report_rows = aggregate_passengers_by_country_day_season(
            bookings=filtered_by_origin,
            airport_lookup=airport_lookup,
            netherlands_airports=netherlands_airports,
            start_date=start_date,
            end_date=end_date
        )
        
        # Validate results
        assert len(report_rows) > 0, "Should have at least one report row"
        
        # Check sorting
        for i in range(len(report_rows) - 1):
            assert report_rows[i].passenger_count >= report_rows[i + 1].passenger_count, \
                "Report should be sorted by passenger count descending"
        
        # Check all required fields present
        for row in report_rows:
            assert row.country, "Country should not be empty"
            assert row.day_of_week, "Day of week should not be empty"
            assert row.season, "Season should not be empty"
            assert row.passenger_count > 0, "Passenger count should be positive"
        
        print(f"\nEnd-to-end validation:")
        print(f"  Input bookings: {len(sample_bookings)}")
        print(f"  After filtering: {len(filtered_by_origin)}")
        print(f"  Report rows: {len(report_rows)}")
        print(f"  Top 3 destinations:")
        for i, row in enumerate(report_rows[:3], 1):
            print(f"    {i}. {row.country} - {row.season} {row.day_of_week}: {row.passenger_count} passengers")

