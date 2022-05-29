import os
import subprocess
import sys

from celery import Celery
from schemas.osmgeojson import OsmGeoJson
from schemas.routes import SearchOsmPolygons
from utils import order_coordinate, osm_geom_to_poly_geojson, awsddb_client
from decimal import Decimal
import json
import osmnx as ox
import geopandas as gpd


celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get(
    "CELERY_RESULT_BACKEND", "redis://localhost:6379"
)

ddb = awsddb_client()


@celery.task()
def get_osm_polys(job_id: str, out_file: str, bbox: tuple, osm_tags: dict = {"building": True}) -> dict:

    gdf = ox.geometries_from_bbox(bbox[0], bbox[1], bbox[2], bbox[3], tags=osm_tags)

    cols = ["geometry", "osmid"]
    gdf = gdf.reset_index()
    gdf = gdf.loc[gdf.element_type != "node", cols]

    item = json.loads(gdf.reset_index().to_json(), parse_float=Decimal)
    # Todo: add CRS info to geojson

    ddb.Table("xview2-ui-osm-polys").put_item(
        Item={"uid": job_id, "geojson": item}
    )

    gdf.to_file(out_file)

    return item


@celery.task()
def get_imagery():
    pass


@celery.task()
def run_xv(args: list) -> None:
    subprocess.run(['conda', 'run', '-n', 'xview2', 'python', '/home/ubuntu/xView2_FDNY/handler.py'] + args)


@celery.task()
def store_results(in_file: str, job_id: str):
    gdf = gpd.read_file(in_file)
    item = json.loads(gdf.reset_index().to_json(), parse_float=Decimal)
    
    # df.to_json does not output the crs currently. Existing bug filed (and PR). Stop gap until that is implemented.
    # https://github.com/geopandas/geopandas/issues/1774
    authority, code = gdf.crs.to_authority()
    ogc_crs = f"urn:ogc:def:crs:{authority}::{code}"
    item["crs"] = {"type": "name", "properties": {"name": ogc_crs}}
    
    ddb.Table("xview2-ui-results").put_item(
        Item={"uid": job_id, "geojson": item}
    )

    return
