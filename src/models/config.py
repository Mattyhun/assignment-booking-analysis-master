"""Configuration models"""
from datetime import datetime
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class Config(BaseModel):
    """Application configuration"""
    bookings_input: str = Field(..., description="Path to bookings directory (local or HDFS)")
    airports_input: str = Field(..., description="Path to airports.dat file")
    output_path: str = Field(..., description="Output path for report")
    start_date: datetime = Field(..., description="Start date for filtering bookings")
    end_date: datetime = Field(..., description="End date for filtering bookings")
    netherlands_airports: Optional[list[str]] = Field(
        default=None,
        description="List of Netherlands airport IATA codes (default: AMS, EIN, RTM)"
    )
    
    @field_validator('start_date', 'end_date', mode='before')
    @classmethod
    def parse_date(cls, v):
        """Parse date string to datetime"""
        if isinstance(v, str):
            # Try ISO format first
            try:
                return datetime.fromisoformat(v)
            except ValueError:
                # Try YYYY-MM-DD format
                return datetime.strptime(v, '%Y-%m-%d')
        return v
    
    @field_validator('end_date')
    @classmethod
    def validate_date_range(cls, v, info):
        """Ensure end_date is after start_date"""
        if 'start_date' in info.data and v < info.data['start_date']:
            raise ValueError("end_date must be after start_date")
        return v
    
    def is_hdfs_path(self, path: str) -> bool:
        """Check if path is HDFS URI"""
        return path.startswith('hdfs://') or path.startswith('hdfs:/')
    
    @property
    def is_hdfs_input(self) -> bool:
        """Check if bookings input is HDFS"""
        return self.is_hdfs_path(self.bookings_input)
    
    @property
    def netherlands_airport_codes(self) -> list[str]:
        """Get Netherlands airport codes"""
        if self.netherlands_airports:
            return self.netherlands_airports
        # Default Netherlands airports
        return ['AMS', 'EIN', 'RTM', 'GRQ', 'MST']


