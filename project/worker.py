import os
import subprocess

from celery import Celery
from schemas.osmgeojson import OsmGeoJson
from schemas.routes import SearchOsmPolygons
from utils import order_coordinate, osm_geom_to_poly_geojson
import json
import osmnx as ox


celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get(
    "CELERY_RESULT_BACKEND", "redis://localhost:6379"
)


@celery.task()
def get_osm_polys(ddb, bbox: tuple, osm_tags: dict = {"building": True}) -> dict:

    gdf = ox.geometries_from_bbox(bbox[0], bbox[1], bbox[2], bbox[3], tags=osm_tags)

    cols = ["geometry", "name"]
    gdf = gdf.reset_index()
    gdf = gdf.loc[gdf.element_type != "node", cols]

    item = json.loads(gdf.reset_index().to_json())

    ddb.Table("xview2-ui-osm-polys").put_item(
        Item={"geojson": item}  # Todo: add job id to output
    )

    return item


@celery.task()
def get_imagery():
    pass


@celery.task()
def run_xv(cmd: list) -> None:
    subprocess.run(cmd)
