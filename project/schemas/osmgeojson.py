from pydantic import BaseModel, Field

class OsmGeoJson(BaseModel):
    uid: str = Field(None, example="73a42ed6-901b-4d08-9776-f548620e94ea")
    geojson: dict