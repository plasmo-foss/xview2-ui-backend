import json
import os
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List

import geopandas as gpd
from celery import chain, chord, group
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
    awsddb_client,
    create_bounding_box_poly,
    create_postgres_tables,
    download_planet_imagery,
    get_pdb_coordinate,
    get_pdb_status,
    get_planet_imagery,
    insert_pdb_coordinates,
    insert_pdb_planet_result,
    insert_pdb_selected_imagery,
    insert_pdb_status,
    order_coordinate,
    rdspostgis_client,
    rdspostgis_sa_client,
    update_pdb_status,
)
from worker import get_osm_polys, run_xv, store_results


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
cursor = None

conf = load_dotenv(override=True)

access_keys = {}


@app.on_event("startup")
async def startup_event():
    global ddb
    global conn
    global access_keys

    # Set up DynamoDB
    ddb = awsddb_client()

    # Create connection to AWS RDS Postgres
    conn = rdspostgis_client()
    create_postgres_tables(conn)

    # Load valid access keys into memory
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

    insert_pdb_coordinates(conn, uid, item)
    insert_pdb_status(conn, uid, "waiting_imagery")

    return uid


@app.get("/fetch-coordinates", response_model=Coordinate)
def fetch_coordinates(job_id: str) -> Coordinate:

    resp = get_pdb_coordinate(conn, job_id)
    return resp


@app.get("/job-status")
def job_status(job_id: str) -> Dict:

    resp = get_pdb_status(conn, job_id)

    if resp is None:
        return None
    else:
        return {"uid": job_id, "status": resp}


@app.get("/fetch-osm-polygons", response_model=OsmGeoJson)
def fetch_osm_polygons(job_id: str) -> Dict:
    """
    Returns GeoJSON for a Job ID that exists in DynamoDB.

        Parameters:
            job_id (str): Job ID for a task

        Returns:
            osm_geojson (dict): The FeatureCollection representing all building polygons for the bounding box
    """
    engine = rdspostgis_sa_client()
    sql = f"SELECT geometry FROM xviewui_osm_polys WHERE uid='{job_id}'"
    gdf = gpd.GeoDataFrame.from_postgis(sql, engine, geom_col="geometry")

    geojson = json.loads(gdf.to_json())

    if len(geojson["features"]) == 0:
        return None
    else:
        return {"uid": job_id, "geojson": geojson}


@app.post("/fetch-planet-imagery", response_model=Planet)
def fetch_planet_imagery(body: FetchPlanetImagery) -> List[Dict]:
    # Get the coordinates for the job from DynamoDB
    coords = fetch_coordinates(body.job_id)

    # Convert the coordinates to a Shapely polygon
    bounding_box = create_bounding_box_poly(coords)

    if body.current_date is None:
        body.current_date = datetime.now().isoformat()
    imagery_list = get_planet_imagery(
        os.getenv("PLANET_API_KEY"), bounding_box, body.current_date
    )

    ret = []
    for image in imagery_list:
        ret.append(
            {
                "timestamp": image["timestamp"],
                "item_type": "SkySatCollect",
                "item_id": image["image_id"],
            }
        )

    # Insert Planet API results to Postgres as blob
    insert_pdb_planet_result(conn, body.job_id, json.dumps(ret))

    # Update status of job
    update_pdb_status(conn, body.job_id, "waiting_assessment")

    return Planet(uid=body.job_id, images=ret)


@app.post("/launch-assessment")
def launch_assessment(body: LaunchAssessment):

    # Insert selected imagery IDs to Postgres
    insert_pdb_selected_imagery(
        conn, body.job_id, body.pre_image_id, body.post_image_id
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

    # Prepare our args for fetching OSM data
    bbox = (coords.start_lat, coords.end_lat, coords.end_lon, coords.start_lon)
    osm_out_path = (
        converter.output_dir
        / converter.job_id
        / "in_polys"
        / f"{converter.job_id}_osm_poly.geojson"
    ).resolve()
    osm_out_path.parent.mkdir(parents=True, exist_ok=True)

    # Prepare our args for xv2 run
    args = []
    args += ["--pre_directory", str(converter.output_dir / converter.job_id / "pre")]
    args += ["--post_directory", str(converter.output_dir / converter.job_id / "post")]
    args += [
        "--output_directory",
        str(converter.output_dir / converter.job_id / "output"),
    ]
    # Todo: check that we got polygons before we write the file, and make sure we have the file before we pass it as an arg
    args += ["--bldg_polys", str(osm_out_path)]

    # Run our celery tasks
    infer = (
        get_osm_polys.s(converter.job_id, str(osm_out_path), bbox)
        | run_xv.si(args)
        | store_results.si(
            str(
                converter.output_dir
                / converter.job_id
                / "output"
                / "vector"
                / "damage.geojson"
            ),
            converter.job_id,
        )
    )
    result = infer.apply_async()

    # Update job status
    ddb.Table("xview2-ui-status").put_item(
        Item={"uid": str(body.job_id), "status": "running_assessment"}
    )

    return ret_counter


@app.get("/fetch-assessment")
def fetch_assessment(job_id: str):

    # Required for serialization of DDB object
    def dumps(item: dict) -> str:
        return json.dumps(item, default=default_type_error_handler)

    def default_type_error_handler(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError

    resp = ddb.Table("xview2-ui-results").get_item(Key={"uid": job_id})

    # The stored response is in a local, projected CRS. We reproject to EPSG 4326 for Deck.gl to render.
    if "Item" in resp:
        crs = resp["Item"]["geojson"]["crs"]["properties"]["name"].split("::")[1]
        gdf = gpd.read_file(dumps(resp["Item"]["geojson"]), driver="GeoJSON")
        gdf = gdf.set_crs(crs, allow_override=True)
        return json.loads(gdf.to_crs(4326).to_json())
    else:
        return None


# No longer works but this is how we should call our chain/chord
# @app.get("/test-celery")
# def test_celery():
#     # use pipes to avoid bug chain/chord bug https://github.com/celery/celery/issues/6197
#     t = group(dummya.s(5), dummyb.s(10)) | dummyc.s(20)
#     ret = t.apply_async()
#     return JSONResponse({"task_id": ret.id})


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
