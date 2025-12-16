"""Airport data model"""
from typing import Optional
from pydantic import BaseModel, Field


class Airport(BaseModel):
    """Airport information"""
    airport_id: int
    name: str
    city: str
    country: str
    iata: Optional[str] = Field(None, description="3-letter IATA code")
    icao: Optional[str] = Field(None, description="4-letter ICAO code")
    latitude: float
    longitude: float
    altitude: int
    timezone_offset: float = Field(..., description="Hours offset from UTC")
    dst: str = Field(..., description="Daylight savings time code")
    tz_database: str = Field(..., description="Timezone in Olson format")
    airport_type: str
    source: str

