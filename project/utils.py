import glob
import json
import os
import shutil
import urllib.request

import boto3
import dateutil.parser
import geopandas as gpd
import planet.api as api
import requests
import sqlalchemy
from sqlalchemy.sql import text
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from osgeo import gdal
from requests.auth import HTTPBasicAuth
from shapely.geometry import MultiPolygon, Polygon, mapping

from schemas import Coordinate
from tileserverutils import bbox_to_xyz, x_to_lon_edges, y_to_lat_edges
import psycopg2
from schemas import Coordinate

class Converter:
    def __init__(self, temp_dir, output_dir, bounding_box, zoom, job_id):
        self.temp_dir = temp_dir
        self.output_dir = output_dir
        self.bounding_box = bounding_box
        self.zoom = zoom
        self.job_id = job_id

    def tile_edges(self, x, y, z):
        lat1, lat2 = y_to_lat_edges(y, z)
        lon1, lon2 = x_to_lon_edges(x, z)
        return [lon1, lat1, lon2, lat2]

    def fetch_tile(self, x, y, z, tile_source):
        url = (
            tile_source.replace("{x}", str(x))
            .replace("{y}", str(y))
            .replace("{z}", str(z))
        )

        if not tile_source.startswith("http"):
            return url.replace("file:///", "")

        path = f"{self.temp_dir}/{x}_{y}_{z}.png"
        urllib.request.urlretrieve(url, path)
        return path

    def merge_tiles(self, input_pattern, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        vrt_path = self.temp_dir / "tiles.vrt"
        gdal.BuildVRT(vrt_path.as_posix(), glob.glob(input_pattern))
        gdal.Translate(output_path.as_posix(), vrt_path.as_posix())

    def georeference_raster_tile(self, x, y, z, path):
        bounds = self.tile_edges(x, y, z)
        gdal.Translate(
            (self.temp_dir / f"{x}_{y}_{z}.tif").as_posix(),
            path,
            outputSRS="EPSG:4326",
            outputBounds=bounds,
        )

    def convert(self, tile_source: str, prepost: str) -> int:
        """
        Take in the URL for a tile server and save the raster to disk

        Parameters:
            tile_source (str): the URL to the tile server
            prepost: (str) whether or not the tile server URL is of pre or post-disaster imagery

        Returns:
            ret_counter (int): how many tiles failed to download
        """
        box = order_coordinate(self.bounding_box)
        lon_min = box.start_lon
        lat_min = box.end_lat
        lon_max = box.end_lon
        lat_max = box.start_lat

        # Script start:
        self.temp_dir = self.temp_dir / f"{self.job_id}_{prepost}"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        x_min, x_max, y_min, y_max = bbox_to_xyz(
            lon_min, lon_max, lat_min, lat_max, self.zoom
        )
        print(
            f"Fetching & georeferencing {(x_max - x_min + 1) * (y_max - y_min + 1)} tiles for {tile_source}"
        )

        ret_counter = 0
        for x in range(x_min, x_max + 1):
            for y in range(y_min, y_max + 1):
                try:
                    png_path = self.fetch_tile(x, y, self.zoom, tile_source)
                    self.georeference_raster_tile(x, y, self.zoom, png_path)
                except OSError:
                    print(f"Error, failed to get {x},{y}")
                    ret_counter += 1
                    pass

        print("Resolving and georeferencing of raster tiles complete")

        print("Merging tiles")
        self.merge_tiles(
            (self.temp_dir / "*.tif").as_posix(),
            self.output_dir
            / self.job_id
            / prepost
            / f"{self.job_id}_{prepost}_merged.tif",
        )
        print("Merge complete")

        shutil.rmtree(self.temp_dir)

        return ret_counter


def order_coordinate(coordinate: Coordinate) -> Coordinate:
    south = min(coordinate.start_lat, coordinate.end_lat)
    north = max(coordinate.start_lat, coordinate.end_lat)
    west = min(coordinate.start_lon, coordinate.end_lon)
    east = max(coordinate.start_lon, coordinate.end_lon)

    return Coordinate(start_lon=west, start_lat=north, end_lon=east, end_lat=south)


def osm_geom_to_poly_geojson(osm_data: dict):
    buildings = []

    # Get the list of elements in the OSM query
    elements = osm_data["elements"]
    for element in elements:
        # If it is a way, then it's as simply polygon
        if element["type"] == "way":
            poly = Polygon([(x["lon"], x["lat"]) for x in element["geometry"]])
            buildings.append(poly)

        # If it is a relation, then it has an outer and inners
        elif element["type"] == "relation":
            outers = []
            inners = []
            for member in element["members"]:
                if member["role"] == "outer":
                    outers.append(member["geometry"])
                elif member["role"] == "inner":
                    inners.append(member["geometry"])

            outers_lonlat = []
            inners_lonlat = []

            for outer in outers:
                if len(outer) <= 2:
                    continue
                outers_lonlat.append(Polygon([(x["lon"], x["lat"]) for x in outer]))

            for inner in inners:
                if len(inner) <= 2:
                    continue
                inners_lonlat.append(Polygon([(x["lon"], x["lat"]) for x in inner]))

            # Create a MultiPoly from the outer, then remove the inners
            merged_outer = MultiPolygon(outers_lonlat)
            for inner in inners_lonlat:
                merged_outer = merged_outer - inner

            buildings.append(merged_outer)

    gdf = gpd.GeoDataFrame({"geometry": buildings})
    return gdf.to_json()


def create_bounding_box_poly(coordinate: Coordinate) -> Polygon:
    """
    Creates a rectangular bounding box Polygon given an input Coordinate

        Parameters:
            coordinate (Coordinate): the input bounding box from the user UI

        Returns:
            poly (Polygon): a rectangular Shapely polygon defining the same input bounding box
    """
    poly = Polygon(
        [
            (coordinate.end_lon, coordinate.end_lat),
            (coordinate.start_lon, coordinate.end_lat),
            (coordinate.start_lon, coordinate.start_lat),
            (coordinate.end_lon, coordinate.start_lat),
        ]
    )
    return poly


def get_planet_imagery(api_key: str, geom: Polygon, current_date: str) -> dict:
    end_date = dateutil.parser.isoparse(current_date)
    start_date = end_date - relativedelta(years=1)

    query = api.filters.and_filter(
        api.filters.geom_filter(mapping(geom)),
        api.filters.date_range("acquired", gte=start_date, lte=end_date),
        api.filters.range_filter("cloud_cover", lte=0.2),
        api.filters.permission_filter("assets.ortho_pansharpened:download"),
        api.filters.string_filter("quality_category", "standard"),
    )

    request = api.filters.build_search_request(query, ["SkySatCollect"])
    search_result = requests.post(
        "https://api.planet.com/data/v1/quick-search",
        auth=HTTPBasicAuth(api_key, ""),
        json=request,
    )

    search_result_json = json.loads(search_result.text)
    items = search_result_json["features"]

    # items_iter returns an iterator over API response pages
    return [
        {"image_id": i["id"], "timestamp": i["properties"]["published"]} for i in items
    ]


def download_planet_imagery(converter: Converter, url: str, prepost: str):
    return converter.convert(url, prepost)


conf = load_dotenv(override=True)


def awsddb_client():

    return boto3.resource(
        "dynamodb",
        region_name=os.getenv("DB_REGION_NAME"),
        aws_access_key_id=os.getenv("DB_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("DB_SECRET_ACCESS_KEY"),
        endpoint_url=os.getenv("DB_ENDPOINT_URL"),
    )


def rdspostgis_client():
    host = os.getenv("PSDB_HOST")
    port = os.getenv("PSDB_PORT")
    user = os.getenv("PSDB_USER")
    password = os.getenv("PSDB_PASSWORD")
    dbname = os.getenv("PSDB_DBNAME")
    conn = psycopg2.connect(
        f"host={host} port={port} user={user} password={password} dbname={dbname}"
    )
    conn.autocommit = True
    return conn


def rdspostgis_sa_client():
    host = os.getenv("PSDB_HOST")
    port = os.getenv("PSDB_PORT")
    user = os.getenv("PSDB_USER")
    password = os.getenv("PSDB_PASSWORD")
    dbname = os.getenv("PSDB_DBNAME")
    engine = sqlalchemy.create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    )
    return engine


def check_postgres_table_exists(conn, table_name):
    cur = conn.cursor()
    cur.execute(
        f"SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='{table_name}')"
    )
    return cur.fetchone()[0]


def create_postgres_tables(conn):
    if not check_postgres_table_exists(conn, "xviewui_coordinates"):
        print("Creating xviewui_coordinates table")
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE xviewui_coordinates (
                uid uuid UNIQUE NOT NULL,
                end_lat float8 NOT NULL,
                end_lon float8 NOT NULL,
                start_lat float8 NOT NULL,
                start_lon float8 NOT NULL
            );"""
            )

    if not check_postgres_table_exists(conn, "xviewui_osm_polys"):
        print("Creating xviewui_osm_polys table")
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE xviewui_osm_polys (
                    uid uuid NOT NULL,
                    osmid TEXT,
                    geometry geometry(POLYGON,4326) NOT NULL
                );"""
            )

    if not check_postgres_table_exists(conn, "xviewui_planet_api"):
        print("Creating xviewui_planet_api table")
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE xviewui_planet_api (
                    uid uuid UNIQUE NOT NULL,
                    planet_response json NOT NULL
                )
                """
            )

    if not check_postgres_table_exists(conn, "xviewui_results"):
        print("Creating xviewui_results table")
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE xviewui_results (
                    uid uuid NOT NULL,
                    osmid TEXT,
                    dmg float4 NOT NULL,
                    area float8 NOT NULL,
                    geometry geometry(POLYGON,4326) NOT NULL
                )"""
            )

    if not check_postgres_table_exists(conn, "xviewui_selected_imagery"):
        print("Creating xviewui_selected_imagery table")
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE xviewui_selected_imagery (
                    uid uuid UNIQUE NOT NULL,
                    pre_image_id text NOT NULL,
                    post_image_id text NOT NULL
                )"""
            )

    if not check_postgres_table_exists(conn, "xviewui_status"):
        print("Creating xviewui_status table")
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE xviewui_status (
                    uid uuid UNIQUE NOT NULL,
                    status text NOT NULL
                )"""
            )


def insert_pdb_coordinates(conn, uid, item):
    with conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO xviewui_coordinates (uid, end_lat, end_lon, start_lat, start_lon)
            VALUES
            (
                '{uid}',
                {item['end_lat']},
                {item['end_lon']},
                {item['start_lat']},
                {item['start_lon']}
            );"""
        )


def insert_pdb_status(conn, uid, status):
    with conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO xviewui_status (uid, status)
            VALUES
            (
                '{uid}',
                '{status}'
            );
            """
        )


def update_pdb_status(conn, uid, status):
    with conn.cursor() as cur:
        cur.execute(
            f"""UPDATE xviewui_status
            SET status = '{status}'
            WHERE uid = '{uid}';
            """
        )

def get_pdb_coordinate(conn, uid):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT * FROM xviewui_coordinates
            WHERE uid = '{uid}';
        """)
        record = cur.fetchone()
    
    if record is None:
        return None
    
    _, end_lat, end_lon, start_lat, start_lon = record
    return Coordinate(
        end_lat=end_lat,
        end_lon=end_lon,
        start_lat=start_lat,
        start_lon=start_lon
    )

def get_pdb_status(conn, uid):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT * FROM xviewui_status
            WHERE uid = '{uid}';
        """)
        record = cur.fetchone()

    if record is None:
        return None
    else:
        return record[1]

def insert_pdb_planet_result(conn, uid, planet_response):
    with conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO xviewui_planet_api (uid, planet_response)
            VALUES
            (
                '{uid}',
                '{planet_response}'
            );
            """
        )


def insert_pdb_selected_imagery(conn, uid, pre_image_id, post_image_id):
    with conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO xviewui_selected_imagery (uid, pre_image_id, post_image_id)
            VALUES
            (
                '{uid}',
                '{pre_image_id}',
                '{post_image_id}'
            );
            """
        )