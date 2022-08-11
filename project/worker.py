import json
import os
import subprocess
import sys
from decimal import Decimal

import geopandas as gpd
import osmnx as ox
from celery import Celery

from schemas.osmgeojson import OsmGeoJson
from schemas.routes import SearchOsmPolygons
from utils import (
    awsddb_client,
    insert_pdb_status,
    order_coordinate,
    osm_geom_to_poly_geojson,
    rdspostgis_client,
    rdspostgis_sa_client,
)

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get(
    "CELERY_RESULT_BACKEND", "redis://localhost:6379"
)

ddb = awsddb_client()
conn = rdspostgis_client()


@celery.task()
def get_osm_polys(
    job_id: str, out_file: str, bbox: tuple, osm_tags: dict = {"building": True}
) -> dict:

    gdf = ox.geometries_from_bbox(bbox[0], bbox[1], bbox[2], bbox[3], tags=osm_tags)

    cols = ["geometry", "osmid"]
    gdf = gdf.reset_index()
    gdf = gdf.loc[gdf.element_type != "node", cols]
    gdf["uid"] = job_id

    gdf.to_file(out_file)

    engine = rdspostgis_sa_client()
    gdf.to_postgis("xviewui_osm_polys", engine, if_exists="append")

    item = json.loads(gdf.reset_index().to_json(), parse_float=Decimal)
    # Todo: add CRS info to geojson

    return item


@celery.task()
def get_imagery():
    pass


@celery.task()
def run_xv(args: list) -> None:
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


@celery.task()
def store_results(in_file: str, job_id: str):
    gdf = gpd.read_file(in_file)
    gdf['uid'] = job_id
    gdf = gdf.to_crs(4326)

    # Push results to Postgres
    engine = rdspostgis_sa_client()
    gdf.to_postgis("xviewui_results", engine, if_exists="append")

    # Update job status
    insert_pdb_status(conn, job_id, "done")

    return
