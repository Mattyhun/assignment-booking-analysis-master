"""Airport lookup service for mapping IATA codes to countries and timezones"""
import csv
from pathlib import Path
from typing import Optional, Dict
import logging

from src.models.airport import Airport

logger = logging.getLogger(__name__)


class AirportLookup:
    """Service for looking up airport information by IATA code"""
    
    def __init__(self, airports_file: str):
        """
        Initialize airport lookup from airports.dat file
        
        Args:
            airports_file: Path to airports.dat file
        """
        self._cache: Dict[str, Airport] = {}
        self._load_airports(airports_file)
        logger.debug(f"Loaded {len(self._cache)} airports into lookup cache")
    
    def _load_airports(self, airports_file: str) -> None:
        """Load airports from CSV file"""
        try:
            with open(airports_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    try:
                        if len(row) < 14:
                            continue
                        
                        # Handle \N (NULL) values
                        def safe_float(value, default=0.0):
                            if not value or value == '\\N':
                                return default
                            try:
                                return float(value)
                            except (ValueError, TypeError):
                                return default
                        
                        def safe_int(value, default=0):
                            if not value or value == '\\N':
                                return default
                            try:
                                return int(value)
                            except (ValueError, TypeError):
                                return default
                        
                        airport = Airport(
                            airport_id=safe_int(row[0], 0),
                            name=row[1] if row[1] != '\\N' else '',
                            city=row[2] if row[2] != '\\N' else '',
                            country=row[3] if row[3] != '\\N' else '',
                            iata=row[4] if row[4] != '\\N' else None,
                            icao=row[5] if row[5] != '\\N' else None,
                            latitude=safe_float(row[6], 0.0),
                            longitude=safe_float(row[7], 0.0),
                            altitude=safe_int(row[8], 0),
                            timezone_offset=safe_float(row[9], 0.0),
                            dst=row[10] if row[10] != '\\N' else 'N',
                            tz_database=row[11] if row[11] != '\\N' else 'UTC',
                            airport_type=row[12] if row[12] != '\\N' else 'unknown',
                            source=row[13] if len(row) > 13 and row[13] != '\\N' else 'Unknown'
                        )
                        
                        # Index by IATA code if available
                        if airport.iata:
                            self._cache[airport.iata] = airport
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Skipping invalid airport row: {row[0] if row else 'unknown'}, error: {e}")
                        continue
        except FileNotFoundError:
            logger.error(f"Airports file not found: {airports_file}")
            raise
        except Exception as e:
            logger.error(f"Error loading airports file: {e}")
            raise
    
    def get_country(self, iata: str) -> Optional[str]:
        """
        Get country for an IATA code
        
        Args:
            iata: IATA airport code
            
        Returns:
            Country name or None if not found
        """
        airport = self._cache.get(iata)
        return airport.country if airport else None
    
    def get_timezone(self, iata: str) -> Optional[str]:
        """
        Get timezone (Olson format) for an IATA code
        
        Args:
            iata: IATA airport code
            
        Returns:
            Timezone string (e.g., 'Europe/Amsterdam') or None if not found
        """
        airport = self._cache.get(iata)
        return airport.tz_database if airport else None
    
    def get_timezone_offset(self, iata: str) -> Optional[float]:
        """
        Get timezone offset in hours from UTC
        
        Args:
            iata: IATA airport code
            
        Returns:
            Hours offset from UTC or None if not found
        """
        airport = self._cache.get(iata)
        return airport.timezone_offset if airport else None
    
    def exists(self, iata: str) -> bool:
        """Check if airport exists in lookup"""
        return iata in self._cache
    
    def get_all_netherlands_airports(self) -> list[str]:
        """Get all Netherlands airport IATA codes"""
        return [
            iata for iata, airport in self._cache.items()
            if airport.country == 'Netherlands'
        ]

