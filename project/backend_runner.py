import argparse
from schemas.coordinate import Coordinate
import json
from imagery import Imagery
from pathlib import Path
from worker import get_osm_polys


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
        "--temp_path", required=True, help="Path for storage of temporary files"
    )
    im_args.add_argument(
        "--pre_post",
        required=True,
        help="String indicating whether this is 'pre' or 'post' imagery",
    )
    im_args.set_defaults(func=fetch_imagery)

    # args for fetching OSM polys
    osm_args = subparsers.add_parser("fetch_polys", help="Fetch OSM building footprints")
    osm_args.add_argument("--job_id", required=True, help="Job ID")
    osm_args.add_argument(
        "--coordinates",
        required=True,
        type=json.loads,
        help="Dictionary from Coordinate object",
    )
    osm_args.set_defaults(func=fetch_polys)

    # Todo;
    # args for wrapping up inference (push json to DB)

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

    temp_path = Path(args.temp_path)
    out_path = Path(args.out_path)

    provider = Imagery.get_provider(args.provider, args.api_key)
    print(
        provider.download_imagery_helper(
            args.job_id, args.pre_post, args.image_id, poly, temp_path, out_path
        ).resolve()
    )

    return


def fetch_polys(args):
    # Todo: test me!
    get_osm_polys(
        args.job_id,
        "~/input/polys",
        (
            args.coordinates.get("start_lat"),
            args.coordinates.get("end_lat"),
            args.coordinates.get("end_lon"),
            args.coordinates.get("start_lon")
        ),
    )

    return


if __name__ == "__main__":
    init()