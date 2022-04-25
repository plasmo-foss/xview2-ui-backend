from pydantic import BaseModel

class Coordinate(BaseModel):
    start_lon: float
    start_lat: float
    end_lon: float
    end_lat: float