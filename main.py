import json
import os
import uuid
from decimal import Decimal

import boto3
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from schemas import Coordinate
from utils import order_coordinate, osm_geom_to_poly_geojson
import requests

from typing import Optional

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
async def send_coordinates(coordinate: Coordinate):
    # Generate a UID
    uid = uuid.uuid4()

    # Fix the ordering of the coordinate
    coordinate = order_coordinate(coordinate)

    # Convert floats to Decimals
    item = json.loads(coordinate.json(), parse_float=Decimal)

    # Insert into DynamoDB
    ddb.Table("xview2-ui-job").put_item(Item={"uid": str(uid), **item})

    return uid


@app.get("/job-status")
async def job_status(job_id: str):
    resp = ddb.Table("xview2-ui-inference").get_item(Key={"uid": job_id})

    if "Item" in resp:
        return resp["Item"]
    else:
        return None


@app.post("/search-osm-polygons")
async def search_osm_polygons(coordinate: Coordinate, job_id: Optional[str] = None):
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
    params = f"data=[out:json];way[building=yes]({bounding_box});convert%20item%20::=::,::geom=geom(),_osm_type=type();out%20geom;"

    r = requests.get(BASE_URL, params=params)

    data = r.json()
    if len(data["elements"]) == 0:
        return None

    osm_geojson = osm_geom_to_poly_geojson(data)

    if job_id:
        item = json.loads(json.dumps(osm_geojson), parse_float=Decimal)
        ddb.Table("xview2-ui-osm-polys").put_item(
            Item={"uid": str(job_id), "geojson": item}
        )

    return osm_geojson


@app.get("/fetch-osm-polygons")
async def fetch_osm_polygons(job_id: str):
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
