import glob
import json
import os
from pathlib import Path
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

import psycopg2
from schemas import Coordinate

# from math import atan, cos, degrees, floor, log, pi, radians, sinh, tan


# def sec(x):
#     return 1 / cos(x)


# def latlon_to_xyz(lat, lon, z):
#     tile_count = pow(2, z)
#     x = (lon + 180) / 360
#     y = (1 - log(tan(radians(lat)) + sec(radians(lat))) / pi) / 2
#     return (tile_count * x, tile_count * y)


# def bbox_to_xyz(lon_min, lon_max, lat_min, lat_max, z):
#     x_min, y_max = latlon_to_xyz(lat_min, lon_min, z)
#     x_max, y_min = latlon_to_xyz(lat_max, lon_max, z)
#     return (floor(x_min), floor(x_max), floor(y_min), floor(y_max))


# def mercatorToLat(mercatorY):
#     return degrees(atan(sinh(mercatorY)))


# def y_to_lat_edges(y, z):
#     tile_count = pow(2, z)
#     unit = 1 / tile_count
#     relative_y1 = y * unit
#     relative_y2 = relative_y1 + unit
#     lat1 = mercatorToLat(pi * (1 - 2 * relative_y1))
#     lat2 = mercatorToLat(pi * (1 - 2 * relative_y2))
#     return (lat1, lat2)


# def x_to_lon_edges(x, z):
#     tile_count = pow(2, z)
#     unit = 360 / tile_count
#     lon1 = -180 + x * unit
#     lon2 = lon1 + unit
#     return (lon1, lon2)


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


conf = load_dotenv(override=True)


# Todo: remove all ddb references
# def awsddb_client():

#     return boto3.resource(
#         "dynamodb",
#         region_name=os.getenv("DB_REGION_NAME"),
#         aws_access_key_id=os.getenv("DB_ACCESS_KEY_ID"),
#         aws_secret_access_key=os.getenv("DB_SECRET_ACCESS_KEY"),
#         endpoint_url=os.getenv("DB_ENDPOINT_URL"),
#     )


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
                    geometry geometry(MultiPolygon,4326) NOT NULL
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
                    geometry geometry(MultiPolygon,4326) NOT NULL
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
        cur.execute(
            f"""
            SELECT * FROM xviewui_coordinates
            WHERE uid = '{uid}';
        """
        )
        record = cur.fetchone()

    if record is None:
        return None

    _, end_lat, end_lon, start_lat, start_lon = record
    return Coordinate(
        end_lat=end_lat, end_lon=end_lon, start_lat=start_lat, start_lon=start_lon
    )


def get_pdb_status(conn, uid):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT * FROM xviewui_status
            WHERE uid = '{uid}';
        """
        )
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
