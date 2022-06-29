import glob
import json
import os
import shutil
import urllib.request
import xmltodict

import boto3
import geopandas as gpd
import planet.api as api
import requests
from pathlib import Path
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from osgeo import gdal
from decimal import Decimal
from requests.auth import HTTPBasicAuth
from shapely.geometry import MultiPolygon, Polygon, mapping
from schemas import Coordinate
from tileserverutils import bbox_to_xyz, x_to_lon_edges, y_to_lat_edges


class Imagery(ABC):
    """Base class for creating imagery providers. Providers are required to provide a get_imagery_list and download_imagery method."""

    def __init__(self, api_key: str):
        self.api_key = api_key

    def download_imagery_helper(
        self,
        job_id: str,
        pre_post: str,
        image_id: str,
        geometry: Polygon,
        tmp_path: Path,
        out_path: Path,
    ) -> str:

        tmp_path = tmp_path / job_id / pre_post
        out_path = out_path

        tmp_path.mkdir(parents=True, exist_ok=True)
        out_path.mkdir(parents=True, exist_ok=True)

        result = self.download_imagery(
            job_id, pre_post, image_id, geometry, tmp_path, out_path
        )

        shutil.rmtree(tmp_path)

        return str(result)

    def tile_edges(self, x, y, z):
        lat1, lat2 = y_to_lat_edges(y, z)
        lon1, lon2 = x_to_lon_edges(x, z)
        return [lon1, lat1, lon2, lat2]

    def fetch_tile(self, x, y, z, tile_source, tmp_path):
        url = (
            tile_source.replace("{x}", str(x))
            .replace("{y}", str(y))
            .replace("{z}", str(z))
        )

        if not tile_source.startswith("http"):
            return url.replace("file:///", "")

        path = f"{tmp_path}/{x}_{y}_{z}.png"
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

    @abstractmethod
    def get_imagery_list(
        self, geometry: Polygon, start_date: str, end_date: str
    ) -> list:
        """Searches imagery provider and returns JSON of imagery available for search criteria

        Args:
            geometry (tuple): geometry of AOI
            start_date (str): beginning date to search for imagery
            end_date (str): end date to search for imagery
        """
        pass

    @abstractmethod
    def download_imagery(
        self,
        job_id: str,
        pre_post: str,
        image_id: str,
        geometry: Polygon,
        tmp_path: Path,
        out_path: Path,
    ) -> str:
        """Downloads selected imagery from provider

        Args:
            job_id (str): _description_
            pre_post (str): _description_
            image_id (str): _description_
        """
        pass


# Todo: Not working pending reply from MAXAR support on fetching images
# class MAXARIM(Imagery):
#     def get_imagery_list(self):
#         def _construct_cql(cql_list):

#             t = []

#             for query in cql_list:
#                 if query["type"] == "inequality":
#                     t.append(f"({query['key']}{query['value']})")
#                 elif query["type"] == "compound":
#                     t.append(f"({query['key']}({query['value']}))")
#                 else:
#                     t.append(f"({query['key']}={query['value']})")

#             return "AND".join(t)

#         CONNECTID = "d56487b0-b430-4342-9244-d24c2e2d289b"
#         crs = "EPSG:4326"
#         bounding_box = "34.068123,-96.509471,34.098737,-96.472678"
#         CQL_QUERY = [
#             {"key": "cloudCover", "value": "<0.10", "type": "inequality"},
#             {"key": "formattedDate", "value": ">'2021-05-01'", "type": "inequality"},
#             {"key": "BBOX", "value": f"geometry,{bounding_box}", "type": "compound"},
#         ]
#         query = _construct_cql(CQL_QUERY)

#         params = {
#             "SERVICE": "WFS",
#             "REQUEST": "GetFeature",
#             "typeName": "DigitalGlobe:FinishedFeature",
#             "VERSION": "1.1.0",
#             "connectId": CONNECTID,
#             "srsName": crs,
#             "CQL_Filter": query,
#         }

#         BASE_URL = f"https://evwhs.digitalglobe.com/catalogservice/wfsaccess"

#         resp = requests.get(BASE_URL, params=params)
#         result = xmltodict.parse(resp.text)

#         return [
#             {"image_id": i["@gml:id"], "timestamp": i["DigitalGlobe:acquisitionDate"]}
#             for i in result["wfs:FeatureCollection"]["gml:featureMembers"][
#                 "DigitalGlobe:FinishedFeature"
#             ]
#         ]

#     def download_imagery(
#         self,
#         prepost: str,
#         image_id: str,
#         temp_dir: str,
#         out_dir: str,
#         geometry: Polygon,
#         job_id: str,
#     ) -> int:
#         """
#         Take in the URL for a tile server and save the raster to disk

#         Parameters:
#             tile_source (str): the URL to the tile server
#             prepost: (str) whether or not the tile server URL is of pre or post-disaster imagery

#         Returns:
#             ret_counter (int): how many tiles failed to download
#         """

#         # url = f"https://tiles0.planet.com/data/v1/SkySatCollect/{image}/{{z}}/{{x}}/{{y}}.png?api_key={os.getenv('PLANET_API_KEY')}"
#         url = f"https://evwhs.digitalglobe.com/earthservice/wmtsaccess?CONNECTID={os.getenv('MAXAR_API_KEY')}&SERVICE=WMTS&VERSION=1.0.0&REQUEST=GetTile&TILEMATRIXSET=EPSG:4326&LAYER=DigitalGlobe:ImageryTileService&FORMAT=image/png&TILEMATRIX=EPSG:4326:{{z}}&TILEROW={{x}}&TILECOL={{y}}&FEATUREPROFILE=Global_Currency_Profile&&featureId={image_id}"

#         lon_min = self.bounding_box.start_lon
#         lat_min = self.bounding_box.end_lat
#         lon_max = self.bounding_box.end_lon
#         lat_max = self.bounding_box.start_lat

#         # Script start:
#         self.temp_dir.mkdir(parents=True, exist_ok=True)
#         self.output_dir.mkdir(parents=True, exist_ok=True)

#         x_min, x_max, y_min, y_max = bbox_to_xyz(
#             lon_min, lon_max, lat_min, lat_max, self.zoom
#         )
#         print(
#             f"Fetching & georeferencing {(x_max - x_min + 1) * (y_max - y_min + 1)} tiles for {url}"
#         )

#         ret_counter = 0
#         for x in range(x_min, x_max + 1):
#             for y in range(y_min, y_max + 1):
#                 try:
#                     png_path = self.fetch_tile(x, y, self.zoom, url)
#                     self.georeference_raster_tile(x, y, self.zoom, png_path)
#                 except OSError:
#                     print(f"Error, failed to get {x},{y}")
#                     ret_counter += 1
#                     pass

#         print("Resolving and georeferencing of raster tiles complete")

#         # Todo: Should we just allow xV2 to do this?
#         print("Merging tiles")
#         self.merge_tiles(
#             (self.temp_dir / "*.tif").as_posix(),
#             self.output_dir
#             / self.job_id
#             / prepost
#             / f"{self.job_id}_{prepost}_merged.tif",
#         )
#         print("Merge complete")

#         shutil.rmtree(self.temp_dir)

#         return ret_counter


class PlanetIM(Imagery):
    def get_imagery_list(
        self, geometry: Polygon, start_date: str, end_date: str
    ) -> list:

        query = api.filters.and_filter(
            api.filters.geom_filter(mapping(geometry)),
            api.filters.date_range("acquired", gte=start_date, lte=end_date),
            api.filters.range_filter("cloud_cover", lte=0.2),
            api.filters.permission_filter("assets.ortho_pansharpened:download"),
            api.filters.string_filter("quality_category", "standard"),
        )

        request = api.filters.build_search_request(query, ["SkySatCollect"])
        search_result = requests.post(
            "https://api.planet.com/data/v1/quick-search",
            auth=HTTPBasicAuth(self.api_key, ""),
            json=request,
        )

        search_result_json = json.loads(search_result.text, parse_float=Decimal)
        items = search_result_json["features"]

        # Todo: check that items contains records

        # items_iter returns an iterator over API response pages
        return [
            {
                "item_id": i["id"],
                "timestamp": i["properties"]["published"],
                "item_type": "SkySatCollect",
            }
            for i in items
        ]

    def download_imagery(
        self,
        job_id: str,
        pre_post: str,
        image_id: str,
        geometry: Polygon,
        tmp_path: Path,
        out_path: Path,
    ) -> str:
        """
        Take in the URL for a tile server and save the raster to disk

        Parameters:
            tile_source (str): the URL to the tile server
            prepost: (str) whether or not the tile server URL is of pre or post-disaster imagery

        Returns:
            ret_counter (int): how many tiles failed to download
        """

        zoom = 18

        url = f"https://tiles0.planet.com/data/v1/SkySatCollect/{image_id}/{zoom}/{{x}}/{{y}}.png?api_key={self.api_key}"

        bounds = geometry.bounds

        lon_min = bounds[0]
        lat_min = bounds[1]
        lon_max = bounds[2]
        lat_max = bounds[3]

        x_min, x_max, y_min, y_max = bbox_to_xyz(
            lon_min, lon_max, lat_min, lat_max, zoom
        )

        print(
            f"Fetching & georeferencing {(x_max - x_min + 1) * (y_max - y_min + 1)} tiles for {url}"
        )

        ret_counter = 0
        for x in range(x_min, x_max + 1):
            for y in range(y_min, y_max + 1):
                try:
                    png_path = self.fetch_tile(x, y, zoom, url, tmp_path)
                    self.georeference_raster_tile(x, y, zoom, png_path)
                except OSError:
                    print(f"Error, failed to get {x},{y}")
                    ret_counter += 1
                    pass

        print("Resolving and georeferencing of raster tiles complete")

        # Todo: Should we just allow xV2 to do this?
        print("Merging tiles")
        self.merge_tiles((tmp_path / "*.tif").as_posix(), out_path)
        print("Merge complete")

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


load_dotenv(override=True)


def awsddb_client():

    return boto3.resource(
        "dynamodb",
        region_name=os.getenv("DB_REGION_NAME"),
        aws_access_key_id=os.getenv("DB_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("DB_SECRET_ACCESS_KEY"),
        endpoint_url=os.getenv("DB_ENDPOINT_URL"),
    )

