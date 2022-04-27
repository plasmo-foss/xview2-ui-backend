from pydantic import BaseModel, Field

class Coordinate(BaseModel):
    start_lon: float = Field(None, example=30.500974655593204)
    start_lat: float = Field(None, example=50.45644226518019)
    end_lon: float = Field(None, example=30.50661292823786)
    end_lat: float = Field(None, example=50.453302476353784)