"""Filtering functions for booking data"""
from datetime import datetime
from typing import List, Optional

from src.models.booking import BookingEvent, Product
from src.models.config import Config

import logging

logger = logging.getLogger(__name__)


def filter_by_date_range(
    bookings: List[BookingEvent],
    start_date: datetime,
    end_date: datetime
) -> List[BookingEvent]:
    """
    Filter bookings by departure date range
    
    Only includes bookings that have at least one flight departing within the date range.
    
    Args:
        bookings: List of booking events
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        
    Returns:
        Filtered list of bookings
    """
    filtered = []
    for booking in bookings:
        # Check if any flight departs within date range
        # Note: We keep the entire booking, but will only count flights in date range during aggregation
        for product in booking.travel_record.products_list:
            flight = product.flight
            # Compare dates (ignore time for date comparison)
            flight_date = flight.departure_date.date()
            start_date_only = start_date.date()
            end_date_only = end_date.date()
            
            if start_date_only <= flight_date <= end_date_only:
                filtered.append(booking)
                break
    
    logger.debug(f"Filtered {len(filtered)} bookings with flights in date range {start_date.date()} to {end_date.date()}")
    return filtered


def filter_klm_flights(bookings: List[BookingEvent]) -> List[BookingEvent]:
    """
    Filter bookings to only include KLM (KL) operating flights
    
    Args:
        bookings: List of booking events
        
    Returns:
        Filtered list containing only KL flights
    """
    filtered = []
    for booking in bookings:
        # Check if any product has KL operating airline
        has_kl_flight = any(
            product.flight.operating_airline == 'KL'
            for product in booking.travel_record.products_list
        )
        if has_kl_flight:
            filtered.append(booking)
    
    logger.debug(f"Filtered {len(filtered)} bookings with KL flights")
    return filtered


def filter_netherlands_origin(
    bookings: List[BookingEvent],
    netherlands_airports: List[str]
) -> List[BookingEvent]:
    """
    Filter bookings to only include flights departing from Netherlands
    
    Args:
        bookings: List of booking events
        netherlands_airports: List of Netherlands airport IATA codes
        
    Returns:
        Filtered list containing only Netherlands origin flights
    """
    netherlands_set = set(netherlands_airports)
    filtered = []
    
    for booking in bookings:
        # Check if any flight departs from Netherlands
        has_netherlands_origin = any(
            product.flight.origin_airport in netherlands_set
            for product in booking.travel_record.products_list
        )
        if has_netherlands_origin:
            filtered.append(booking)
    
    logger.debug(f"Filtered {len(filtered)} bookings departing from Netherlands")
    return filtered


def get_latest_booking_status_map(bookings: List[BookingEvent]) -> dict:
    """
    Create a map of (passenger_uci, flight_key) -> (is_confirmed, latest_timestamp)
    
    For each passenger-flight combination, finds the latest booking event and records
    whether its status is Confirmed.
    
    Args:
        bookings: List of booking events
        
    Returns:
        Dictionary mapping (passenger_uci, flight_key) -> (is_confirmed: bool, timestamp: datetime)
    """
    # Key: (passenger_uci, origin, destination, departure_date_iso)
    # Value: (is_confirmed: bool, timestamp: datetime)
    latest_status_map: dict = {}
    
    for booking in bookings:
        timestamp = booking.timestamp
        
        # Process each passenger-flight combination in this booking
        for passenger in booking.travel_record.passengers_list:
            for product in booking.travel_record.products_list:
                flight = product.flight
                
                # Create unique key for passenger-flight combination
                flight_key = (
                    passenger.uci,
                    flight.origin_airport,
                    flight.destination_airport,
                    flight.departure_date.isoformat()
                )
                
                is_confirmed = product.booking_status.upper() == 'CONFIRMED'
                
                # Track latest booking event for this passenger-flight
                if flight_key not in latest_status_map:
                    latest_status_map[flight_key] = (is_confirmed, timestamp)
                else:
                    # Update if this is a more recent event
                    _, existing_timestamp = latest_status_map[flight_key]
                    if timestamp > existing_timestamp:
                        latest_status_map[flight_key] = (is_confirmed, timestamp)
    
    return latest_status_map


def filter_confirmed_status(bookings: List[BookingEvent]) -> List[BookingEvent]:
    """
    Filter bookings to only include those that have at least one passenger-flight
    with latest status = Confirmed.
    
    Note: The actual latest status check per passenger-flight is done during aggregation
    using the status map. This filter is a pre-filter to reduce data volume.
    
    Args:
        bookings: List of booking events
        
    Returns:
        Filtered list containing bookings that may have confirmed passenger-flights
    """
    # Get latest status map
    status_map = get_latest_booking_status_map(bookings)
    
    # Filter to only include bookings that have at least one passenger-flight with latest status = Confirmed
    filtered = []
    booking_ids_with_confirmed = set()
    
    for booking in bookings:
        has_confirmed = False
        for passenger in booking.travel_record.passengers_list:
            for product in booking.travel_record.products_list:
                flight = product.flight
                flight_key = (
                    passenger.uci,
                    flight.origin_airport,
                    flight.destination_airport,
                    flight.departure_date.isoformat()
                )
                
                if flight_key in status_map:
                    is_confirmed, _ = status_map[flight_key]
                    if is_confirmed:
                        has_confirmed = True
                        break
            if has_confirmed:
                break
        
        if has_confirmed and id(booking) not in booking_ids_with_confirmed:
            filtered.append(booking)
            booking_ids_with_confirmed.add(id(booking))
    
    logger.debug(f"Filtered {len(filtered)} bookings with at least one confirmed passenger-flight")
    return filtered


def get_kl_flights_from_netherlands(
    booking: BookingEvent,
    netherlands_airports: List[str],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> List[Product]:
    """
    Extract KL flights departing from Netherlands from a booking
    
    Args:
        booking: Booking event
        netherlands_airports: List of Netherlands airport IATA codes
        start_date: Optional start date for filtering (inclusive)
        end_date: Optional end date for filtering (inclusive)
        
    Returns:
        List of product segments (flights) matching criteria
    """
    netherlands_set = set(netherlands_airports)
    
    matching_products = []
    for product in booking.travel_record.products_list:
        flight = product.flight
        
        # Check all criteria
        if flight.operating_airline != 'KL':
            continue
        if flight.origin_airport not in netherlands_set:
            continue
        if product.booking_status.upper() != 'CONFIRMED':
            continue
        
        # Optional date filtering
        if start_date and end_date:
            flight_date = flight.departure_date.date()
            start_date_only = start_date.date()
            end_date_only = end_date.date()
            if not (start_date_only <= flight_date <= end_date_only):
                continue
        
        matching_products.append(product)
    
    return matching_products

