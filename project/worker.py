import os
import subprocess

from celery import Celery
from schemas.osmgeojson import OsmGeoJson
from schemas.routes import SearchOsmPolygons
from utils import order_coordinate, osm_geom_to_poly_geojson, awsddb_client
from decimal import Decimal
import json
import osmnx as ox


celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get(
    "CELERY_RESULT_BACKEND", "redis://localhost:6379"
)

ddb = awsddb_client()


@celery.task()
def get_osm_polys(job_id: str, out_file: str, bbox: tuple, osm_tags: dict = {"building": True}) -> dict:

    gdf = ox.geometries_from_bbox(bbox[0], bbox[1], bbox[2], bbox[3], tags=osm_tags)

    cols = ["geometry", "name"]
    gdf = gdf.reset_index()
    gdf = gdf.loc[gdf.element_type != "node", cols]

    item = json.loads(gdf.reset_index().to_json(), parse_float=Decimal)

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
    subprocess.run(['conda', 'run', '-n', 'xv2', 'python', '/Users/lb/Documents/Code/xView2_FDNY/handler.py'] + args)
