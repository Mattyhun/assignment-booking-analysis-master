"""Aggregation functions for report generation"""
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Tuple, Optional

from src.models.booking import BookingEvent
from src.models.report import ReportRow
from src.services.airport_lookup import AirportLookup
from src.processing.filters import get_kl_flights_from_netherlands, get_latest_booking_status_map
from src.processing.enrichment import enrich_flight_with_country_and_time

import logging

logger = logging.getLogger(__name__)


def aggregate_passengers_by_country_day_season(
    bookings: List[BookingEvent],
    airport_lookup: AirportLookup,
    netherlands_airports: List[str],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> List[ReportRow]:
    """
    Aggregate passengers by country, day of week, and season
    
    Each passenger is counted once per flight leg (unique passenger-flight combination)
    
    Args:
        bookings: List of filtered booking events
        airport_lookup: Airport lookup service
        netherlands_airports: List of Netherlands airport codes
        start_date: Optional start date for filtering flights
        end_date: Optional end date for filtering flights
        
    Returns:
        List of ReportRow objects sorted by passenger count descending
    """
    # Get latest booking status map to ensure we only count passengers with latest status = Confirmed
    latest_status_map = get_latest_booking_status_map(bookings)
    
    # Dictionary: (country, day_of_week, season) -> dict with passenger data
    # Structure: {
    #   'passengers': set of (passenger_uci, flight_key),
    #   'adults': set of (passenger_uci, flight_key) where passenger_type == 'ADT',
    #   'children': set of (passenger_uci, flight_key) where passenger_type == 'CHD',
    #   'ages': list of ages for passengers with age data
    # }
    aggregation: Dict[Tuple[str, str, str], dict] = defaultdict(
        lambda: {
            'passengers': set(),
            'adults': set(),
            'children': set(),
            'ages': []
        }
    )
    
    for booking in bookings:
        # Get KL flights from Netherlands (with date filtering)
        kl_flights = get_kl_flights_from_netherlands(
            booking, 
            netherlands_airports,
            start_date,
            end_date
        )
        
        for product in kl_flights:
            # Enrich with country, day of week, season
            country, day_of_week, season = enrich_flight_with_country_and_time(
                product, airport_lookup
            )
            
            if not country or not day_of_week or not season:
                continue
            
            # Create unique flight key (origin-destination-date)
            flight_key = (
                product.flight.origin_airport,
                product.flight.destination_airport,
                product.flight.departure_date.isoformat()
            )
            
            # Count each passenger once per flight leg
            # Only count if latest booking status is Confirmed (requirement)
            for passenger in booking.travel_record.passengers_list:
                # Create key to check latest status
                status_check_key = (
                    passenger.uci,
                    product.flight.origin_airport,
                    product.flight.destination_airport,
                    product.flight.departure_date.isoformat()
                )
                
                # Only count if latest status is Confirmed
                if status_check_key not in latest_status_map:
                    continue
                
                is_confirmed, _ = latest_status_map[status_check_key]
                if not is_confirmed:
                    continue
                
                passenger_key = (passenger.uci, flight_key)
                key = (country, day_of_week, season)
                
                # Add to passenger set
                aggregation[key]['passengers'].add(passenger_key)
                
                # Track adults vs children
                if passenger.passenger_type.upper() == 'ADT':
                    aggregation[key]['adults'].add(passenger_key)
                elif passenger.passenger_type.upper() == 'CHD':
                    aggregation[key]['children'].add(passenger_key)
                
                # Track age for average calculation
                if passenger.age is not None:
                    aggregation[key]['ages'].append(passenger.age)
    
    # Convert to ReportRow objects with enhanced statistics
    report_rows = []
    for (country, day_of_week, season), data in aggregation.items():
        passenger_count = len(data['passengers'])
        adults_count = len(data['adults'])
        children_count = len(data['children'])
        
        # Calculate average age
        average_age = None
        if data['ages']:
            average_age = sum(data['ages']) / len(data['ages'])
            average_age = round(average_age, 2)  # Round to 2 decimal places
        
        report_rows.append(
            ReportRow(
                country=country,
                day_of_week=day_of_week,
                season=season,
                passenger_count=passenger_count,
                adults_count=adults_count if adults_count > 0 else None,
                children_count=children_count if children_count > 0 else None,
                average_age=average_age
            )
        )
    
    # Sort by passenger_count descending, then by season and day_of_week for grouping
    # This ensures highest passenger counts appear first, while maintaining season/day grouping
    report_rows.sort(
        key=lambda x: (-x.passenger_count, x.season, x.day_of_week)
    )
    
    logger.debug(f"Generated {len(report_rows)} report rows")
    return report_rows

