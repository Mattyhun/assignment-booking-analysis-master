"""Enrichment functions for adding derived data"""
from datetime import datetime
from typing import List, Tuple
import pytz

from src.models.booking import Product
from src.services.airport_lookup import AirportLookup

import logging

logger = logging.getLogger(__name__)


def calculate_season(date: datetime) -> str:
    """
    Calculate season from date
    
    Args:
        date: Datetime object
        
    Returns:
        Season name: Spring, Summer, Fall, or Winter
    """
    month = date.month
    
    if month in [12, 1, 2]:
        return "Winter"
    elif month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    else:  # 9, 10, 11
        return "Fall"


def get_day_of_week(dt: datetime, timezone_str: str) -> str:
    """
    Get day of week based on local timezone
    
    Args:
        dt: UTC datetime
        timezone_str: Timezone string (Olson format, e.g., 'Europe/Amsterdam')
        
    Returns:
        Day of week name (Monday, Tuesday, etc.)
    """
    try:
        tz = pytz.timezone(timezone_str)
        local_dt = dt.astimezone(tz)
        return local_dt.strftime("%A")
    except Exception as e:
        logger.warning(f"Error converting timezone {timezone_str} for datetime {dt}: {e}")
        # Fallback to UTC day of week
        return dt.strftime("%A")


def enrich_flight_with_country_and_time(
    product: Product,
    airport_lookup: AirportLookup
) -> Tuple[str, str, str]:
    """
    Enrich flight with destination country, day of week, and season
    
    Args:
        product: Product (flight segment)
        airport_lookup: Airport lookup service
        
    Returns:
        Tuple of (country, day_of_week, season) or (None, None, None) if not found
    """
    flight = product.flight
    destination = flight.destination_airport
    
    # Get country
    country = airport_lookup.get_country(destination)
    if not country:
        logger.warning(f"Country not found for airport {destination}")
        return None, None, None
    
    # Get timezone for origin airport (for day of week calculation)
    origin_timezone = airport_lookup.get_timezone(flight.origin_airport)
    if not origin_timezone:
        # Fallback to destination timezone
        origin_timezone = airport_lookup.get_timezone(destination)
    
    if not origin_timezone:
        logger.warning(f"Timezone not found for airport {flight.origin_airport}")
        # Use UTC as fallback
        origin_timezone = 'UTC'
    
    # Calculate day of week based on origin airport timezone
    day_of_week = get_day_of_week(flight.departure_date, origin_timezone)
    
    # Calculate season
    season = calculate_season(flight.departure_date)
    
    return country, day_of_week, season


