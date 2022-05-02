from typing import List, Dict
from pydantic import BaseModel, Field


class Planet(BaseModel):
    uid: str = Field(None, example="73a42ed6-901b-4d08-9776-f548620e94ea")
    images: List[Dict] = Field(
        None,
        example=[
            {
                "timestamp": "2022-04-07T21:05:38Z",
                "item_type": "SkySatCollect",
                "item_id": "20220407_120032_ssc6_u0001",
            },
            {
                "timestamp": "2022-04-04T11:57:06Z",
                "item_type": "SkySatCollect",
                "item_id": "20220404_083931_ssc4_u0001",
            },
        ],
    )

