import argparse
from schemas.coordinate import Coordinate
import json
from imagery import Imagery
from pathlib import Path


def init():
    parser = argparse.ArgumentParser(
        description="Create arguments for imagery handling."
    )

    parser.add_argument("--task", required=True, help="Task to perform.")

    im_args = parser.add_argument_group(
        "Imagery", "Arguments for imagery download task"
    )
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

    finish_args = parser.add_argument_group(
        "Finish", "Arguments for finishing up inference run"
    )

    args = parser.parse_args()

    return args


def backend_helper(args):
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


if __name__ == "__main__":
    args = init()
    if args.task.lower() == "imagery":
        backend_helper(args)
