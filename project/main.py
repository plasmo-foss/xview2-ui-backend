import json
import os
import uuid
from datetime import datetime
from decimal import Decimal 
from typing import Dict, List
import dateutil
from dateutil.relativedelta import relativedelta

import geopandas as gpd
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
from imagery import Imagery
from utils import (
    create_bounding_box_poly,
    create_postgres_tables,
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
from worker import get_osm_polys, run_xv, store_results, task_error_callback


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
cursor = None

conf = load_dotenv(override=True)

access_keys = {}


@app.on_event("startup")
async def startup_event():
    global conn
    global access_keys

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
    # Todo: Move this work to 'utils' and point backend runner to the utils implementation
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
    # Get the coordinates for the job from DynamoDB and create shapely polygon
    coords = fetch_coordinates(body.job_id)
    bounding_box = create_bounding_box_poly(coords)

    if body.current_date is None:
        body.current_date = datetime.now().isoformat()

    end_date = dateutil.parser.isoparse(body.current_date)
    start_date = end_date - relativedelta(years=1)

    converter = Imagery.get_provider(os.getenv("IMG_PROVIDER"), os.getenv("PLANET_API_KEY")
    )

    imagery_list = converter.get_imagery_list_helper(bounding_box, start_date, end_date)

    insert_pdb_planet_result(conn, body.job_id, json.dumps(imagery_list))

    # Update status of job
    update_pdb_status(conn, body.job_id, "waiting_assessment")

    return Planet(uid=body.job_id, images=imagery_list)


@app.post("/launch-assessment")
def launch_assessment(body: LaunchAssessment):

    # Insert selected imagery IDs to Postgres
    insert_pdb_selected_imagery(
        conn, body.job_id, body.pre_image_id, body.post_image_id
    )

    # Download the images for given job
    coords = fetch_coordinates(body.job_id)
    bounding_box = create_bounding_box_poly(coords)
    bbox = (coords.start_lat, coords.end_lat, coords.end_lon, coords.start_lon)

    # Run our celery tasks
    infer = run_xv.si(
        body.job_id,
        body.pre_image_id,
        body.post_image_id,
        get_osm=True,
        poly_dict=dict(coords),
    )


    # Update job status
    update_pdb_status(conn, body.job_id, "running_assessment")

    result = infer.apply_async(link_error=task_error_callback.s(body.job_id))

    return None


@app.get("/fetch-assessment")
def fetch_assessment(job_id: str):

    def default_type_error_handler(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError

    engine = rdspostgis_sa_client()
    sql = f"SELECT dmg, geometry FROM xviewui_results WHERE uid='{job_id}'"
    gdf = gpd.GeoDataFrame.from_postgis(sql, engine, geom_col="geometry")

    # The stored response is in a local, projected CRS. We reproject to EPSG 4326 for Deck.gl to render.
    if gdf is not None:
        return json.loads(gdf.to_json())
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
