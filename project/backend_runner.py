import argparse
from schemas.coordinate import Coordinate
import json
from imagery import Imagery
from pathlib import Path

# from worker import get_osm_polys
import osmnx as ox
import geopandas as gpd
from decimal import Decimal


def init():
    parser = argparse.ArgumentParser(
        description="Create arguments for imagery handling."
    )
    subparsers = parser.add_subparsers()

    # args for downloading imagery
    im_args = subparsers.add_parser("imagery", help="Download imagery")
    im_args.add_argument(
        "--provider", required=True, help="Imagery provider",
    )
    im_args.add_argument(
        "--api_key", required=True, help="API key for imagery provider"
    )
    im_args.add_argument(
        "--coordinates",
        required=True,
        type=json.loads,
        help="Dictionary from Coordinate object",
    )
    im_args.add_argument("--job_id", required=True, help="Job ID")
    im_args.add_argument("--image_id", required=True, help="ID of image to retrieve")
    im_args.add_argument("--out_path", required=True, help="Path to save image(s)")
    im_args.add_argument(
        "--pre_post",
        required=True,
        help="String indicating whether this is 'pre' or 'post' imagery",
    )
    im_args.set_defaults(func=fetch_imagery)

    # args for fetching OSM polys
    osm_args = subparsers.add_parser(
        "fetch_polys", help="Fetch OSM building footprints"
    )
    osm_args.add_argument("--job_id", required=True, help="Job ID")
    osm_args.add_argument(
        "--coordinates",
        required=True,
        type=json.loads,
        help="Dictionary from Coordinate object",
    )
    osm_args.set_defaults(func=fetch_polys)

    # # args for persisting results from geojson
    # persist_args = subparsers.add_parser("persist_results", help="Persist results from GeoJSON file")
    # persist_args.add_argument("--job_id", required=True, help="Job ID")
    # persist_args.add_argument("--geojson", required=True, help="GeoJSON file to persist results")
    # persist_args.set_defaults(func=persist_results)

    args = parser.parse_args()
    return args.func(args)


def fetch_imagery(args):
    from utils import create_bounding_box_poly

    coords = Coordinate(
        start_lon=args.coordinates["start_lon"],
        start_lat=args.coordinates["start_lat"],
        end_lon=args.coordinates["end_lon"],
        end_lat=args.coordinates["end_lat"],
    )
    poly = create_bounding_box_poly(coords)

    out_path = Path(args.out_path)

    provider = Imagery.get_provider(args.provider, args.api_key)
    provider.download_imagery_helper(
        args.job_id, args.pre_post, args.image_id, poly, out_path
    )

    return


def fetch_polys(args):
    # Todo: test me!...move logic to utils
    gdf = ox.geometries_from_bbox(
        args.coordinates.get("start_lat"),
        args.coordinates.get("end_lat"),
        args.coordinates.get("end_lon"),
        args.coordinates.get("start_lon"),
        tags={"building": True},
    )

    cols = ["geometry", "osmid"]
    gdf = gdf.reset_index()

    # BUG: This breaks if there are no polygons
    gdf = gdf.loc[gdf.element_type != "node", cols]

    item = json.loads(gdf.reset_index().to_json(), parse_float=Decimal)
    # Todo: add CRS info to geojson

    # Todo: persist

    gdf.to_file("/output/polys.geojson")

    return item


if __name__ == "__main__":
    init()
