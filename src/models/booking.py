"""Data models for booking records"""
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator


class Passenger(BaseModel):
    """Passenger information"""
    uci: str = Field(..., description="Unique passenger identifier")
    age: Optional[int] = Field(None, description="Passenger age")
    passenger_type: str = Field(..., alias="passengerType", description="ADT or CHD")
    tattoo: int = Field(..., description="Passenger tattoo number")


class Flight(BaseModel):
    """Flight segment information"""
    origin_airport: str = Field(..., alias="originAirport", description="IATA code of origin airport")
    destination_airport: str = Field(..., alias="destinationAirport", description="IATA code of destination airport")
    departure_date: datetime = Field(..., alias="departureDate", description="UTC departure datetime")
    arrival_date: datetime = Field(..., alias="arrivalDate", description="UTC arrival datetime")
    operating_airline: str = Field(..., alias="operatingAirline", description="Operating airline code (KL for KLM)")
    booking_status: str = Field(..., alias="bookingStatus", description="Booking status")
    
    @field_validator('departure_date', 'arrival_date', mode='before')
    @classmethod
    def parse_datetime(cls, v):
        """Parse ISO format datetime strings"""
        if isinstance(v, str):
            # Handle ISO format with Z suffix
            if v.endswith('Z'):
                v = v[:-1] + '+00:00'
            try:
                return datetime.fromisoformat(v)
            except ValueError:
                # Try parsing without timezone
                return datetime.fromisoformat(v.replace('Z', ''))
        return v


class Product(BaseModel):
    """Product segment (flight)"""
    booking_status: str = Field(..., alias="bookingStatus")
    flight: Flight


class TravelRecord(BaseModel):
    """Travel record containing passengers and products"""
    passengers_list: List[Passenger] = Field(..., alias="passengersList")
    products_list: List[Product] = Field(..., alias="productsList")


class BookingEvent(BaseModel):
    """Booking event wrapper"""
    timestamp: datetime
    travel_record: TravelRecord = Field(..., alias="travelrecord")
    
    @field_validator('timestamp', mode='before')
    @classmethod
    def parse_timestamp(cls, v):
        """Parse ISO format timestamp"""
        if isinstance(v, str):
            if v.endswith('Z'):
                v = v[:-1] + '+00:00'
            return datetime.fromisoformat(v)
        return v
    
    @classmethod
    def from_raw_json(cls, data: dict) -> Optional['BookingEvent']:
        """Parse booking from raw JSON structure"""
        try:
            event_data = data.get('event', {}).get('DataElement', {})
            travel_record = event_data.get('travelrecord', {})
            
            if not travel_record:
                return None
            
            # Process productsList to add bookingStatus to flight objects
            products_list = travel_record.get('productsList', [])
            processed_products = []
            for product in products_list:
                if 'flight' in product and 'bookingStatus' in product:
                    # Copy bookingStatus into flight object for Flight model
                    flight_data = product['flight'].copy()
                    flight_data['bookingStatus'] = product['bookingStatus']
                    product_copy = product.copy()
                    product_copy['flight'] = flight_data
                    processed_products.append(product_copy)
                else:
                    processed_products.append(product)
            
            return cls(
                timestamp=data.get('timestamp'),
                travelrecord={
                    'passengersList': travel_record.get('passengersList', []),
                    'productsList': processed_products
                }
            )
        except Exception as e:
            # Log the exception for debugging
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Error parsing booking: {e}", exc_info=True)
            return None

