from pydantic import BaseModel, Field

class Coordinate(BaseModel):
    start_lon: float = Field(None, example=30.500974655593204)
    start_lat: float = Field(None, example=50.45644226518019)
    end_lon: float = Field(None, example=30.50661292823786)
    end_lat: float = Field(None, example=50.453302476353784)


class JobId(BaseModel):
    job_id: str = Field(None, example="73a42ed6-901b-4d08-9776-f548620e94ea")