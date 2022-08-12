import json
import os
import subprocess
import sys
from decimal import Decimal

import geopandas as gpd
import osmnx as ox
from celery import Celery
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon

from schemas.osmgeojson import OsmGeoJson
from schemas.routes import SearchOsmPolygons
from utils import (awsddb_client, insert_pdb_status, order_coordinate,
                   osm_geom_to_poly_geojson, rdspostgis_client,
                   rdspostgis_sa_client, update_pdb_status)

STATE_START = "start"
STATE_END = "end"
STATE_ERROR = "error"
STATE_UNDEFINED = "undefined"
STATE_DELIMITER = ":" 


celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get(
    "CELERY_RESULT_BACKEND", "redis://localhost:6379"
)

#ddb = awsddb_client()
#conn = rdspostgis_client()


def parse_status(state):
    pieces = state.parse(STATE_DELIMITER)
    if len(pieces) == 1:
        return pieces[0], STATE_UNDEFINED
    return pieces[0], pieces[1]


def make_status(task_name, state):
    return f"{task_name}{STATE_DELIMITER}{state}"


def publish_task_status(job_id, task_name, state):
    conn = rdspostgis_client()
    task_status = make_status(task_name, state)
    print(f"Publishing task status {task_status} for job_id={job_id}")
    update_pdb_status(conn, job_id, task_status)


@celery.task()
def task_error_callback(request, exc, traceback, job_id):
    print(f"Task Error: {request.task} job_id={job_id} exc={exc}")
    publish_task_status(job_id, request.task, STATE_ERROR)


@celery.task(bind=True)
def get_osm_polys(self,
    job_id: str, out_file: str, bbox: tuple, osm_tags: dict = {"building": True}
) -> dict:
    publish_task_status(job_id, self.request.task, STATE_START)
    
    gdf = ox.geometries_from_bbox(bbox[0], bbox[1], bbox[2], bbox[3], tags=osm_tags)

    cols = ["geometry", "osmid"]
    gdf = gdf.reset_index()
    gdf = gdf.loc[gdf.element_type != "node", cols]
    gdf["uid"] = job_id

    gdf["geometry"] = [MultiPolygon([feature]) if isinstance(feature, Polygon) else feature for feature in gdf["geometry"]]

    gdf.to_file(out_file)

    engine = rdspostgis_sa_client()
    gdf.to_postgis("xviewui_osm_polys", engine, if_exists="append")

    item = json.loads(gdf.reset_index().to_json(), parse_float=Decimal)
    # Todo: add CRS info to geojson

    publish_task_status(job_id, self.request.task, STATE_END)
    return item


@celery.task()
def get_imagery():
    pass


@celery.task(bind=True)
def run_xv(self, job_id: str, args: list) -> None:
    publish_task_status(job_id, self.request.task, STATE_START)

    subprocess.run(
        [
            "conda",
            "run",
            "-n",
            "xview2",
            "python",
            "/home/ubuntu/xView2_FDNY/handler.py",
        ]
        + args
    )

    publish_task_status(job_id, self.request.task, STATE_END)


@celery.task(bind=True)
def store_results(self, in_file: str, job_id: str):
    publish_task_status(job_id, self.request.task, STATE_START)
    
    gdf = gpd.read_file(in_file)
    gdf['uid'] = job_id
    gdf = gdf.to_crs(4326)

    gdf["geometry"] = [MultiPolygon([feature]) if isinstance(feature, Polygon) else feature for feature in gdf["geometry"]]

    # Push results to Postgres
    engine = rdspostgis_sa_client()
    gdf.to_postgis("xviewui_results", engine, if_exists="append")

    # Update job status
    conn = rdspostgis_client()
    update_pdb_status(conn, job_id, "done")
    #publish_task_status(job_id, self.request.task, STATE_END)

    return
