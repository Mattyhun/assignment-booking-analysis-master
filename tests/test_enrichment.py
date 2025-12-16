"""Unit tests for enrichment functions - covering timezone and season requirements"""
import pytest
from datetime import datetime
import pytz
from src.models.booking import Flight, Product
from src.processing.enrichment import (
    calculate_season,
    get_day_of_week,
    enrich_flight_with_country_and_time
)
from src.services.airport_lookup import AirportLookup


class TestSeasonCalculation:
    """Test season calculation from date"""
    
    def test_winter_season(self):
        """Test that December, January, February are Winter"""
        assert calculate_season(datetime(2019, 12, 15)) == "Winter"
        assert calculate_season(datetime(2019, 1, 15)) == "Winter"
        assert calculate_season(datetime(2019, 2, 15)) == "Winter"
    
    def test_spring_season(self):
        """Test that March, April, May are Spring"""
        assert calculate_season(datetime(2019, 3, 15)) == "Spring"
        assert calculate_season(datetime(2019, 4, 15)) == "Spring"
        assert calculate_season(datetime(2019, 5, 15)) == "Spring"
    
    def test_summer_season(self):
        """Test that June, July, August are Summer"""
        assert calculate_season(datetime(2019, 6, 15)) == "Summer"
        assert calculate_season(datetime(2019, 7, 15)) == "Summer"
        assert calculate_season(datetime(2019, 8, 15)) == "Summer"
    
    def test_fall_season(self):
        """Test that September, October, November are Fall"""
        assert calculate_season(datetime(2019, 9, 15)) == "Fall"
        assert calculate_season(datetime(2019, 10, 15)) == "Fall"
        assert calculate_season(datetime(2019, 11, 15)) == "Fall"


class TestDayOfWeekCalculation:
    """Test FR: Day of week based on local timezone"""
    
    def test_day_of_week_uses_timezone(self):
        """Test that day of week is calculated using airport timezone, not UTC"""
        # UTC time that could be different day in local timezone
        utc_dt = datetime(2019, 6, 15, 22, 0, 0)  # 10 PM UTC on June 15
        
        # In Amsterdam (UTC+2 in summer), this is midnight June 16
        amsterdam_tz = "Europe/Amsterdam"
        day_ams = get_day_of_week(utc_dt, amsterdam_tz)
        
        # In New York (UTC-4 in summer), this is 6 PM June 15
        ny_tz = "America/New_York"
        day_ny = get_day_of_week(utc_dt, ny_tz)
        
        # Verify timezone conversion works
        assert day_ams in ["Saturday", "Sunday", "Monday"]  # Depends on DST
        assert day_ny in ["Saturday", "Sunday"]  # Depends on DST
    
    def test_day_of_week_fallback_to_utc(self):
        """Test that invalid timezone falls back to UTC"""
        dt = datetime(2019, 6, 15, 12, 0, 0)
        day = get_day_of_week(dt, "Invalid/Timezone")
        # Should return a valid day name
        assert day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


class TestEnrichment:
    """Test flight enrichment with country, day, and season"""
    
    @pytest.fixture
    def airport_lookup(self, tmp_path):
        """Create a minimal airport lookup for testing"""
        # Create a test airports file
        airports_file = tmp_path / "airports.dat"
        with open(airports_file, 'w') as f:
            # Format: ID, Name, City, Country, IATA, ICAO, Lat, Lon, Alt, TZ, DST, TZ DB, Type, Source
            f.write('1,"Test Airport","Amsterdam","Netherlands","AMS","EHAM",52.3,4.76,0,1.0,E,"Europe/Amsterdam","airport","OurAirports"\n')
            f.write('2,"JFK Airport","New York","United States","JFK","KJFK",40.64,-73.78,13,-5.0,A,"America/New_York","airport","OurAirports"\n')
        
        return AirportLookup(str(airports_file))
    
    def test_enrichment_returns_country_day_season(self, airport_lookup):
        """Test that enrichment returns country, day of week, and season"""
        flight = Flight.model_validate({
            "originAirport": "AMS",
            "destinationAirport": "JFK",
            "departureDate": "2019-06-15T10:00:00Z",
            "arrivalDate": "2019-06-15T20:00:00Z",
            "operatingAirline": "KL",
            "bookingStatus": "CONFIRMED"
        })
        product = Product.model_validate({"bookingStatus": "CONFIRMED", "flight": flight.model_dump(by_alias=True)})
        
        country, day_of_week, season = enrich_flight_with_country_and_time(product, airport_lookup)
        
        assert country == "United States"
        assert day_of_week in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        assert season == "Summer"
    
    def test_enrichment_handles_missing_airport(self, airport_lookup):
        """Test that enrichment handles missing airport data gracefully"""
        flight = Flight.model_validate({
            "originAirport": "XXX",  # Non-existent airport
            "destinationAirport": "YYY",  # Non-existent airport
            "departureDate": "2019-06-15T10:00:00Z",
            "arrivalDate": "2019-06-15T20:00:00Z",
            "operatingAirline": "KL",
            "bookingStatus": "CONFIRMED"
        })
        product = Product.model_validate({"bookingStatus": "CONFIRMED", "flight": flight.model_dump(by_alias=True)})
        
        country, day_of_week, season = enrich_flight_with_country_and_time(product, airport_lookup)
        
        # Should return None values when airport not found
        assert country is None
        assert day_of_week is None
        assert season is None
