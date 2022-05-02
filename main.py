import json
import os
import uuid
from decimal import Decimal

import boto3
from dotenv import load_dotenv
from fastapi import FastAPI

from schemas import Coordinate, OsmGeoJson, Planet
from utils import (
    create_bounding_box_poly,
    get_planet_imagery,
    order_coordinate,
    osm_geom_to_poly_geojson,
)
import requests
import planet

from typing import Optional, Dict, List

app = FastAPI(
    title="xView Vulcan Backend",
    description="The Python backend supporting the xView Vulcan BDA frontend",
    version="0.0.1",
    license_info={
        "name": "MIT",
        "url": "https://github.com/plasmo-foss/xview2-ui-backend/blob/main/LICENSE",
    },
)

client = None
ddb = None


@app.on_event("startup")
async def startup_event():
    global ddb

    conf = load_dotenv()
    client = boto3.client(
        "dynamodb",
        region_name=os.getenv("DB_REGION_NAME"),
        aws_access_key_id=os.getenv("DB_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("DB_SECRET_ACCESS_KEY"),
    )
    ddb = boto3.resource(
        "dynamodb",
        region_name=os.getenv("DB_REGION_NAME"),
        aws_access_key_id=os.getenv("DB_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("DB_SECRET_ACCESS_KEY"),
    )
    ddb_exceptions = client.exceptions


@app.post("/send-coordinates")
async def send_coordinates(coordinate: Coordinate) -> str:
    # Generate a UID
    uid = uuid.uuid4()

    # Fix the ordering of the coordinate
    coordinate = order_coordinate(coordinate)

    # Convert floats to Decimals
    item = json.loads(coordinate.json(), parse_float=Decimal)

    # Insert into DynamoDB
    ddb.Table("xview2-ui-job").put_item(Item={"uid": str(uid), **item})
    ddb.Table("xview2-ui-status").put_item(Item={"uid": str(uid), "status": "waiting_imagery"})

    return uid


@app.get("/fetch-coordinates", response_model=Coordinate)
async def fetch_coordinates(job_id: str) -> Coordinate:
    resp = ddb.Table("xview2-ui-job").get_item(Key={"uid": job_id})

    if "Item" in resp:
        ret = resp["Item"]
        return Coordinate(
            start_lon=ret["start_lon"],
            start_lat=ret["start_lat"],
            end_lon=ret["end_lon"],
            end_lat=ret["end_lat"],
        )
    else:
        return None


@app.get("/job-status")
async def job_status(job_id: str) -> Dict:
    resp = ddb.Table("xview2-ui-status").get_item(Key={"uid": job_id})

    if "Item" in resp:
        return resp["Item"]
    else:
        return None


@app.post("/search-osm-polygons", response_model=OsmGeoJson)
async def search_osm_polygons(
    coordinate: Coordinate, job_id: Optional[str] = None
) -> Dict:
    """
    Returns GeoJSON for all building polygons for a given bounding box from OSM.

        Parameters:
            coordinate (Coordinate): A set of bounding box coordinates
            job_id (str, optional): The Job ID string to persist the GeoJSON to

        Returns:
            osm_geojson (dict): The FeatureCollection representing all building polygons for the bounding box
    """
    # Fix the ordering of the coordinate
    coordinate = order_coordinate(coordinate)
    # Needs to be south west north east -> end_lat start_lon start_lat end_lon

    BASE_URL = "https://www.overpass-api.de/api/interpreter"
    bounding_box = f"{coordinate.end_lat},{coordinate.start_lon},{coordinate.start_lat},{coordinate.end_lon}"
    params = f"data=[out:json];(way[building]({bounding_box});relation[building]({bounding_box}););out%20geom;"

    r = requests.get(BASE_URL, params=params)

    data = r.json()
    if len(data["elements"]) == 0:
        return None

    osm_geojson = json.loads(osm_geom_to_poly_geojson(data))

    if job_id:
        # Convert floats to Decimals
        item = json.loads(json.dumps(osm_geojson), parse_float=Decimal)

        ddb.Table("xview2-ui-osm-polys").put_item(
            Item={"uid": str(job_id), "geojson": item}
        )

    return OsmGeoJson(uid=job_id, geojson=osm_geojson)


@app.get("/fetch-osm-polygons", response_model=OsmGeoJson)
async def fetch_osm_polygons(job_id: str) -> Dict:
    """
    Returns GeoJSON for a Job ID that exists in DynamoDB.

        Parameters:
            job_id (str): Job ID for a task

        Returns:
            osm_geojson (dict): The FeatureCollection representing all building polygons for the bounding box
    """
    resp = ddb.Table("xview2-ui-osm-polys").get_item(Key={"uid": job_id})

    if "Item" in resp:
        return resp["Item"]
    else:
        return None


@app.post("/fetch-planet-imagery", response_model=Planet)
async def fetch_planet_imagery(job_id: str, current_date: str) -> List[Dict]:
    # Get the coordinates for the job from DynamoDB
    coords = await fetch_coordinates(job_id)

    # Convery the coordinates to a Shapely polygon
    bounding_box = create_bounding_box_poly(coords)

    client = planet.api.ClientV1(os.getenv("PLANET_API_KEY"))
    imagery_list = get_planet_imagery(client, bounding_box, current_date)

    ret = []
    for image in imagery_list:
        ret.append(
            {
                "timestamp": image["timestamp"],
                "item_type": "SkySatCollect",
                "item_id": image["image_id"]
            }
        )

    # Persist the response to DynamoDB
    ddb.Table("xview2-ui-planet-api").put_item(
        Item={"uid": str(job_id), "planet_response": ret}
    )
    
    # Update job status
    ddb.Table("xview2-ui-status").put_item(
        Item={"uid": str(job_id), "status": "waiting_assessment"}
    )

    return Planet(uid=job_id, images=ret)


@app.post("/launch-assessment")
async def launch_assessment(job_id: str):
    # TODO run assessment
    return

    # Update job status
    ddb.Table("xview2-ui-status").put_item(
        Item={"uid": str(job_id), "status": "running_assessment"}
    )

# TODO
# 1. User presses submit on the UI -> coordinates sent to backend ✅
# 1A. Fetch OSM polygons for given coordinates ✅
# 2. Fetch variety of imagery from Maxar/Planet APIs ✅
# 3. Send imagery to UI. ✅
# 4. User selects pre and post image and submits.
# 5. Launch the AI inference.
# 6. Once 1A and 5 are done, clip the AI polygons with the OSM polygons
# 7. Return the GeoJSON to the UI for rendering
