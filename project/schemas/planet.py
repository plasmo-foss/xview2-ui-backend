from typing import List, Dict
from pydantic import BaseModel, Field


# Todo: rename me...and my file
class Planet(BaseModel):
    uid: str = Field(None, example="73a42ed6-901b-4d08-9776-f548620e94ea")
    images: List[Dict] = Field(
        None,
        example=[
            {
                "timestamp": "2022-04-07T21:05:38Z",
                "item_type": "SkySatCollect",
                "item_id": "20220407_120032_ssc6_u0001",
                "provider": "Planet",
                "return_type": "tileset",
                "url": "https://tile{1-3}...."
            },
            {
                "timestamp": "2022-04-04T11:57:06Z",
                "item_type": "DF_Feature",
                "item_id": "20220404_083931_ssc4_u0001",
                "provider": "MAXAR",
                "return_type": "raster",
                "url": "Leaflet/Deck.gl compatible url"
            },
        ],
    )
