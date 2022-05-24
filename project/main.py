import json
import os
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List

import boto3
import planet
from celery import group, chain, chord
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from schemas import (
    Coordinate,
    FetchPlanetImagery,
    LaunchAssessment,
    OsmGeoJson,
    Planet,
    SearchOsmPolygons,
)
from utils import (
    Converter,
    create_bounding_box_poly,
    download_planet_imagery,
    get_planet_imagery,
    order_coordinate,
)
from worker import get_osm_polys, run_xv


def verify_key(access_key: str = Header("null")) -> bool:
    if not access_key in access_keys:
        raise HTTPException(status_code=401, detail="Please provide a valid access_key")
    return True


app = FastAPI(
    title="xView Vulcan Backend",
    description="The Python backend supporting the xView Vulcan BDA frontend",
    version="0.0.1",
    license_info={
        "name": "MIT",
        "url": "https://github.com/plasmo-foss/xview2-ui-backend/blob/main/LICENSE",
    },
    dependencies=[Depends(verify_key)],
)

client = None
ddb = None
access_keys = {}


@app.on_event("startup")
async def startup_event():
    global ddb
    global access_keys

    conf = load_dotenv(override=True)

    client = boto3.client(
        "dynamodb",
        region_name=os.getenv("DB_REGION_NAME"),
        aws_access_key_id=os.getenv("DB_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("DB_SECRET_ACCESS_KEY"),
        endpoint_url=os.getenv("DB_ENDPOINT_URL"),
    )
    ddb = boto3.resource(
        "dynamodb",
        region_name=os.getenv("DB_REGION_NAME"),
        aws_access_key_id=os.getenv("DB_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("DB_SECRET_ACCESS_KEY"),
        endpoint_url=os.getenv("DB_ENDPOINT_URL"),
    )
    ddb_exceptions = client.exceptions

    access_keys = set(
        [key.strip() for key in open(".env.access_keys", "r").readlines()]
    )


@app.post("/send-coordinates")
def send_coordinates(coordinate: Coordinate) -> str:

    # Generate a UID
    uid = uuid.uuid4()

    # Fix the ordering of the coordinate
    coordinate = order_coordinate(coordinate)

    # Convert floats to Decimals
    item = json.loads(coordinate.json(), parse_float=Decimal)

    # Insert into DynamoDB
    ddb.Table("xview2-ui-coordinates").put_item(Item={"uid": str(uid), **item})
    ddb.Table("xview2-ui-status").put_item(
        Item={"uid": str(uid), "status": "waiting_imagery"}
    )

    return uid


@app.get("/fetch-coordinates", response_model=Coordinate)
def fetch_coordinates(job_id: str) -> Coordinate:

    resp = ddb.Table("xview2-ui-coordinates").get_item(Key={"uid": job_id})

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
def job_status(job_id: str) -> Dict:

    resp = ddb.Table("xview2-ui-status").get_item(Key={"uid": job_id})

    if "Item" in resp:
        return resp["Item"]
    else:
        return None


@app.get("/fetch-osm-polygons", response_model=OsmGeoJson)
def fetch_osm_polygons(job_id: str) -> Dict:
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
def fetch_planet_imagery(body: FetchPlanetImagery) -> List[Dict]:
    # Get the coordinates for the job from DynamoDB
    coords = fetch_coordinates(body.job_id)

    # Convert the coordinates to a Shapely polygon
    bounding_box = create_bounding_box_poly(coords)

    client = planet.api.ClientV1(os.getenv("PLANET_API_KEY"))
    if body.current_date is None:
        body.current_date = datetime.now().isoformat()
    imagery_list = get_planet_imagery(client, bounding_box, body.current_date)

    ret = []
    for image in imagery_list:
        ret.append(
            {
                "timestamp": image["timestamp"],
                "item_type": "SkySatCollect",
                "item_id": image["image_id"],
                # "asset": image["asset"]
            }
        )

    item = json.loads(json.dumps(ret), parse_float=Decimal)
    # Persist the response to DynamoDB
    ddb.Table("xview2-ui-planet-api").put_item(
        Item={"uid": str(body.job_id), "planet_response": item}
    )

    # Update job status
    ddb.Table("xview2-ui-status").put_item(
        Item={"uid": str(body.job_id), "status": "waiting_assessment"}
    )

    return Planet(uid=body.job_id, images=ret)


@app.post("/launch-assessment")
def launch_assessment(body: LaunchAssessment):

    # Persist the response to DynamoDB
    ddb.Table("xview2-ui-selected-imagery").put_item(
        Item={
            "uid": body.job_id,
            "pre_image_id": body.pre_image_id,
            "post_image_id": body.post_image_id,
        }
    )

    # Download the images for given job
    coords = fetch_coordinates(body.job_id)

    for pre_post in ["pre", "post"]:
        converter = Converter(
            Path(os.getenv("PLANET_IMAGERY_TEMP_DIR")),
            Path(os.getenv("PLANET_IMAGERY_OUTPUT_DIR")),
            coords,
            18,
            body.job_id,
        )
        if pre_post == "pre":
            image = body.pre_image_id
        else:
            image = body.post_image_id

        url = f"https://tiles0.planet.com/data/v1/SkySatCollect/{image}/{{z}}/{{x}}/{{y}}.png?api_key={os.getenv('PLANET_API_KEY')}"

        # Todo: celery-ize this...well maybe later...don't think we can pass the converter object through serialization
        ret_counter = download_planet_imagery(
            converter=converter, url=url, prepost=pre_post
        )

    # TODO run assessment
    args = []
    args += ["--pre_dictionary", converter.output_dir / converter.job_id / "pre"]
    args += ["--post_directory", converter.output_dir / converter.job_id / "post"]
    args += ["--output_directory", converter.output_dir / converter.job_id / "output"]
    run_xv.delay(args)

    # Update job status
    ddb.Table("xview2-ui-status").put_item(
        Item={"uid": str(body.job_id), "status": "running_assessment"}
    )

    return ret_counter


# No longer works but this is how we should call our chain/chord
# @app.get("/test-celery")
# def test_celery():
#     # use pipes to avoid bug chain/chord bug https://github.com/celery/celery/issues/6197
#     t = group(dummya.s(5), dummyb.s(10)) | dummyc.s(20)
#     ret = t.apply_async()
#     return JSONResponse({"task_id": ret.id})


@app.post("/celery_osm")
def get_osm(body: Coordinate):
    # Order coordinates (south, north, east, east) for osmnx
    bbox = (body.start_lat, body.end_lat, body.end_lon, body.start_lon)

    t = get_osm_polys.delay(ddb, bbox)
    return t.get()  # Todo: remove this...testing only


# TODO
# 1. User presses submit on the UI -> coordinates sent to backend ✅
# 1A. Fetch OSM polygons for given coordinates ✅
# 2. Fetch variety of imagery from Maxar/Planet APIs ✅
# 3. Send imagery to UI. ✅
# 4. User selects pre and post image and submits. ✅
# 5. Launch the AI inference.
# 6. Once 1A and 5 are done, clip the AI polygons with the OSM polygons
# 7. Return the GeoJSON to the UI for rendering

# Nice to haves
# 1. A simple pane that displays all jobs the user has kicked off and their statuses
# 2. Opacity slider for the displayed GeoJSON.
# 3. A method in which a count of damaged polygons can be displayed to the user.
# 4. The ability to search for a location using Military Grid Reference System.
# 5. Caching user requests.
# 6. Better localization models / improved ability to create regular polygons (not blobby).