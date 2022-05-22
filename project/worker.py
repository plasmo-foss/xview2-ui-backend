import os
import time

from celery import Celery
from schemas.osmgeojson import OsmGeoJson
from schemas.routes import SearchOsmPolygons
from utils import order_coordinate, osm_geom_to_poly_geojson
import requests
import json
from decimal import Decimal


celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")


@celery.task(name="create_task")
def create_task(task_type):
    time.sleep(int(task_type) * 10)
    return True


@celery.task(name="passthrough")
def passthrough(*args, **kwargs):
    return "OK"


@celery.task(name="dummya")
def dummya(var):
    print(f"Dummy A: {var}")


@celery.task(name="dummyb")
def dummyb(var):
    print(f"Dummy B: {var}")


@celery.task(name="dummyc")
def dummyc(var):
    print(f"Dummy C: {var}")


@celery.task(name="search_osm_polygon")
def celery_search_osm_polygon(body: SearchOsmPolygons, ddb):
    # Fix the ordering of the coordinate
    coordinate = order_coordinate(body.coordinate)
    # Needs to be south west north east -> end_lat start_lon start_lat end_lon

    BASE_URL = "https://www.overpass-api.de/api/interpreter"
    bounding_box = f"{coordinate.end_lat},{coordinate.start_lon},{coordinate.start_lat},{coordinate.end_lon}"
    params = f"data=[out:json];(way[building]({bounding_box});relation[building]({bounding_box}););out%20geom;"

    r = requests.get(BASE_URL, params=params)

    data = r.json()
    if len(data["elements"]) == 0:
        return None

    osm_geojson = json.loads(osm_geom_to_poly_geojson(data))

    if body.job_id:
        # Convert floats to Decimals
        item = json.loads(json.dumps(osm_geojson), parse_float=Decimal)

        ddb.Table("xview2-ui-osm-polys").put_item(
            Item={"uid": str(body.job_id), "geojson": item}
        )

    return OsmGeoJson(uid=body.job_id, geojson=osm_geojson)