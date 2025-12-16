"""Unit tests for airport lookup service"""
import pytest
from pathlib import Path
from src.services.airport_lookup import AirportLookup


class TestAirportLookup:
    """Test airport lookup functionality"""
    
    @pytest.fixture
    def sample_airports_file(self, tmp_path):
        """Create a sample airports.dat file for testing"""
        airports_file = tmp_path / "airports.dat"
        with open(airports_file, 'w') as f:
            # Format: ID, Name, City, Country, IATA, ICAO, Lat, Lon, Alt, TZ, DST, TZ DB, Type, Source
            f.write('1,"Schiphol","Amsterdam","Netherlands","AMS","EHAM",52.3,4.76,0,1.0,E,"Europe/Amsterdam","airport","OurAirports"\n')
            f.write('2,"JFK Airport","New York","United States","JFK","KJFK",40.64,-73.78,13,-5.0,A,"America/New_York","airport","OurAirports"\n')
            f.write('3,"Heathrow","London","United Kingdom","LHR","EGLL",51.47,-0.46,83,0.0,E,"Europe/London","airport","OurAirports"\n')
            f.write('4,"Unknown Airport","City","Country","\\N","\\N",0.0,0.0,0,0.0,N,"UTC","airport","OurAirports"\n')
        return str(airports_file)
    
    def test_loads_airports_from_file(self, sample_airports_file):
        """Test that airports are loaded from file"""
        lookup = AirportLookup(sample_airports_file)
        assert lookup.exists("AMS")
        assert lookup.exists("JFK")
        assert lookup.exists("LHR")
    
    def test_get_country(self, sample_airports_file):
        """Test getting country for IATA code"""
        lookup = AirportLookup(sample_airports_file)
        assert lookup.get_country("AMS") == "Netherlands"
        assert lookup.get_country("JFK") == "United States"
        assert lookup.get_country("LHR") == "United Kingdom"
        assert lookup.get_country("XXX") is None  # Non-existent
    
    def test_get_timezone(self, sample_airports_file):
        """Test getting timezone for IATA code"""
        lookup = AirportLookup(sample_airports_file)
        assert lookup.get_timezone("AMS") == "Europe/Amsterdam"
        assert lookup.get_timezone("JFK") == "America/New_York"
        assert lookup.get_timezone("LHR") == "Europe/London"
    
    def test_handles_null_values(self, sample_airports_file):
        """Test that NULL values (\\N) are handled gracefully"""
        lookup = AirportLookup(sample_airports_file)
        # Should not raise error when loading airports with NULL values
        assert lookup is not None
    
    def test_get_all_netherlands_airports(self, sample_airports_file):
        """Test getting all Netherlands airports"""
        lookup = AirportLookup(sample_airports_file)
        nl_airports = lookup.get_all_netherlands_airports()
        assert "AMS" in nl_airports
        assert "JFK" not in nl_airports

