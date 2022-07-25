import os
import subprocess

from celery import Celery
from utils import create_bounding_box_poly
from db import get_coordinates, awsddb_client
from decimal import Decimal
from imagery import Imagery
from pathlib import Path
import json
import osmnx as ox
import geopandas as gpd
import inf_launcher

# Todo: Add flower task monitoring


celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get(
    "CELERY_RESULT_BACKEND", "redis://localhost:6379"
)

ddb = awsddb_client()


@celery.task()
def instance_launch():
    inf_launcher.inf_launch()


def instance_setup():
    pass


@celery.task()
def get_osm_polys(
    job_id: str, out_file: str, bbox: tuple, osm_tags: dict = {"building": True}
) -> dict:

    gdf = ox.geometries_from_bbox(bbox[0], bbox[1], bbox[2], bbox[3], tags=osm_tags)

    cols = ["geometry", "osmid"]
    gdf = gdf.reset_index()
    # BUG: This breaks if there are no polygons
    gdf = gdf.loc[gdf.element_type != "node", cols]

    item = json.loads(gdf.reset_index().to_json(), parse_float=Decimal)
    # Todo: add CRS info to geojson

    ddb.Table("xview2-ui-osm-polys").put_item(Item={"uid": job_id, "geojson": item})

    gdf.to_file(out_file)

    return item


@celery.task()
def get_imagery(job_id, pre_post, image_id, bbox, temp_path, out_path):
    coords = get_coordinates(job_id)
    bounding_box = create_bounding_box_poly(coords)

    converter = Imagery.get_provider("Planet", os.getenv("PLANET_API_KEY"))
    converter.download_imagery_helper(
        job_id, pre_post, image_id, bounding_box, Path(temp_path), Path(out_path),
    )


@celery.task()
def run_xv(args: list) -> None:
    subprocess.run(
        [
            "conda",
            "run",
            "-n",
            "xv2",
            "python",
            "/home/ubuntu/xView2_FDNY/handler.py",
        ]
        + args
    )


@celery.task()
def store_results(in_file: str, job_id: str):
    gdf = gpd.read_file(in_file)
    item = json.loads(gdf.reset_index().to_json(), parse_float=Decimal)

    # df.to_json does not output the crs currently. Existing bug filed (and PR). Stop gap until that is implemented.
    # https://github.com/geopandas/geopandas/issues/1774
    authority, code = gdf.crs.to_authority()
    ogc_crs = f"urn:ogc:def:crs:{authority}::{code}"
    item["crs"] = {"type": "name", "properties": {"name": ogc_crs}}

    ddb.Table("xview2-ui-results").put_item(Item={"uid": job_id, "geojson": item})

    # Update job status
    ddb.Table("xview2-ui-status").put_item(Item={"uid": job_id, "status": "done"})

    return
