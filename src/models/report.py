"""Report output models"""
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class ReportRow(BaseModel):
    """Single row in the output report"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "country": "United States",
                "day_of_week": "Monday",
                "season": "Spring",
                "passenger_count": 1250,
                "adults_count": 1100,
                "children_count": 150,
                "average_age": 42.5
            }
        }
    )
    
    country: str
    day_of_week: str
    season: str
    passenger_count: int
    adults_count: Optional[int] = Field(None, description="Number of adult passengers")
    children_count: Optional[int] = Field(None, description="Number of child passengers")
    average_age: Optional[float] = Field(None, description="Average age of passengers")


