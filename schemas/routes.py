from pydantic import BaseModel, Field
from schemas.coordinate import Coordinate
from typing import Optional


class SearchOsmPolygons(BaseModel):
    coordinate: Coordinate = Field(
        None,
        example=Coordinate(
            start_lon=30.500974655593204,
            start_lat=50.45644226518019,
            end_lon=30.50661292823786,
            end_lat=50.453302476353784,
        ),
    )
    job_id: Optional[str] = Field(None, example="73a42ed6-901b-4d08-9776-f548620e94ea")


class FetchPlanetImagery(BaseModel):
    current_date: Optional[str] = Field(None, example="2022-04-04T11:57:06Z")
    job_id: Optional[str] = Field(None, example="73a42ed6-901b-4d08-9776-f548620e94ea")
