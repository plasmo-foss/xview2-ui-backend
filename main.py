import json
import os
import uuid
from decimal import Decimal

import boto3
from dotenv import load_dotenv
from fastapi import FastAPI

from schemas import Coordinate
from utils import order_coordinate, osm_geom_to_poly_geojson
import requests


app = FastAPI()

client = None
ddb = None

@app.on_event("startup")
async def startup_event():
    global ddb

    conf = load_dotenv()
    client = boto3.client('dynamodb',
                         region_name=os.getenv('DB_REGION_NAME'),
                         aws_access_key_id=os.getenv('DB_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('DB_SECRET_ACCESS_KEY')
    )
    ddb = boto3.resource('dynamodb',
                         region_name=os.getenv('DB_REGION_NAME'),
                         aws_access_key_id=os.getenv('DB_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('DB_SECRET_ACCESS_KEY')
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
    ddb.Table('xview2-ui-job').put_item(
        Item={
            'uid': str(uid),
            **item
        }
    )

    return uid

@app.get("/job-status")
async def job_status(job_id: str):
    resp = ddb.Table('xview2-ui-inference').get_item(
        Key={
            'uid': job_id
        }
    )

    if "Item" in resp:
        return resp["Item"]
    else:
        return None

@app.post("/osm-polygons")
async def osm_polygons(coordinate: Coordinate):
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

    return osm_geom_to_poly_geojson(data)


