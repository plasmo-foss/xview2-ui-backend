import glob
import json
import shutil
import os
import urllib.request
import xmltodict
import mercantile

# Todo: find replacement for cv2...it seems to break every environment! When replacement is found remove line in Dockerfile
import cv2
import rasterio
import numpy as np
import shapely
import rasterio.merge
from queue import Queue
import threading

import planet.api as api
import requests
from pathlib import Path
from abc import ABC, abstractmethod
from osgeo import gdal
from decimal import Decimal
from rasterio.warp import calculate_default_transform
from rasterio.crs import CRS
from requests.auth import HTTPBasicAuth
from shapely.geometry import Polygon, mapping
from schemas import Coordinate

# from utils import bbox_to_xyz, x_to_lon_edges, y_to_lat_edges

TILESET = "tileset"
RASTER = "raster"


class TileDataset:
    def __init__(self, url, output_dir, bounding_box, zoom, job_id):
        self.subdomains = ["tiles0", "tiles1", "tiles2", "tiles3"]
        self.url = url
        self.output_dir = output_dir
        self.bounding_box = bounding_box
        self.zoom = zoom
        self.job_id = job_id

    def _get_image_from_tile(self, tile):
        """
        Args:
            tile: a mercantile Tile object
        Returns
            a np.ndarray of size 256x256x3 with uint8 datatype containing the imagery
                for the input tile
        """
        url = self.url.format(
            subdomain=np.random.choice(self.subdomains), x=tile.x, y=tile.y, z=tile.z
        )
        with requests.get(url) as r:
            arr = np.asarray(bytearray(r.content), dtype=np.uint8)
        img = cv2.imdecode(arr, -1)
        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

        return img

    def _get_tile_as_virtual_raster(self, tile):
        """
        Args:
            tile: a mercantile Tile object
        Returns
            a rasterio.io.MemoryFile with the imagery for the input tile
        """
        img = self._get_image_from_tile(tile)
        geom = shapely.geometry.shape(mercantile.feature(tile)["geometry"])
        minx, miny, maxx, maxy = geom.bounds
        dst_transform = rasterio.transform.from_bounds(minx, miny, maxx, maxy, 256, 256)
        dst_profile = {
            "driver": "GTiff",
            "width": 256,
            "height": 256,
            "transform": dst_transform,
            "nodata": 0,
            "crs": "epsg:4326",
            "count": 3,
            "dtype": "uint8",
        }
        test_f = rasterio.io.MemoryFile()
        with test_f.open(**dst_profile) as test_d:
            test_d.write(img[:, :, 0], 1)
            test_d.write(img[:, :, 1], 2)
            test_d.write(img[:, :, 2], 3)
        test_f.seek(0)

        return test_f

    def _dequeue_get_tile_as_virtual_raster(self, q, virtual_files, virtual_datasets):
        while not q.empty():
            tile = q.get()
            f = self._get_tile_as_virtual_raster(tile)
            virtual_files.append(f)
            virtual_datasets.append(f.open())
            q.task_done()

    def get_data_from_extent(self, geom, zoom_level=16):
        """Gets georeferenced imagery from the input geom at a given zoom level.
        Specifically, this will iterate over all the quadkeys in the input geom at the
        given zoom level and save the imagery as a virtual raster. The virtual raster
        lets us either quickly read the data or quickly write it to file as a GeoTIFF.
        Args:
            geom: A geojson object in EPSG:4326 (i.e. with lat/lon coordinates)
        Returns:
            a rasterio.io.MemoryFile with the corresponding data
        """
        shape = shapely.geometry.shape(geom)
        minx, miny, maxx, maxy = shape.bounds

        virtual_files = []
        virtual_datasets = []

        output_width_degrees = maxx - minx
        output_height_degrees = maxy - miny
        if output_width_degrees > 1 or output_height_degrees > 1:
            raise ValueError(
                "Trying to export file with height or width larger than a degree which"
                + " will result in a huge output tile. The input geom should be split"
                + " up into smaller chunks."
            )

        # TODO: This should _probably_ be done multithreaded
        # for tile in mercantile.tiles(minx, miny, maxx, maxy, zoom_level):
        #     f = self._get_tile_as_virtual_raster(tile)
        #     virtual_files.append(f)
        #     virtual_datasets.append(f.open())

        tile_queue = Queue()
        num_threads = 4
        num_tiles = 0
        for tile in mercantile.tiles(minx, miny, maxx, maxy, zoom_level):
            tile_queue.put(tile)
            num_tiles += 1

        print(f"Fetching {num_tiles} tiles...")

        for i in range(num_threads):
            thread = threading.Thread(
                target=self._dequeue_get_tile_as_virtual_raster,
                args=(tile_queue, virtual_files, virtual_datasets),
            )
            thread.start()

        tile_queue.join()

        # get the largest x and y resolutions over all patches to use as the merged tile
        # resolution if we don't explicitly set this, then it is likely that some of
        # the patches not have the correct resolution (they will be off by tiny
        # fractions of a degree) and there will be single nodata lines between rows of
        # tiles
        x_res = 0
        y_res = 0
        for ds in virtual_datasets:
            x_res = max(x_res, ds.res[0])
            y_res = max(y_res, ds.res[1])

        out_image, out_transform = rasterio.merge.merge(
            virtual_datasets, res=(x_res, y_res), bounds=(minx, miny, maxx, maxy),
        )

        for ds in virtual_datasets:
            ds.close()
        for f in virtual_files:
            f.close()

        dst_crs = "epsg:4326"
        dst_profile = {
            "driver": "GTiff",
            "width": out_image.shape[2],
            "height": out_image.shape[1],
            "transform": out_transform,
            "crs": dst_crs,
            "count": 3,
            "dtype": "uint8",
        }
        test_f = rasterio.io.MemoryFile()
        with test_f.open(**dst_profile) as test_d:
            test_d.write(out_image)
        test_f.seek(0)

        return test_f

    def save_memory_file_to_disk(self, memory_file, prepost):
        with memory_file.open() as src:
            profile = src.profile.copy()
            profile["compress"] = ("lzw",)
            profile["predictor"] = 2
            output_path = (
                self.output_dir
                / self.job_id
                / prepost
                / f"{self.job_id}_{prepost}_merged.tif"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with rasterio.open(output_path, "w", **profile) as dst:
                dst.write(src.read())


class Imagery(ABC):
    """Base class for creating imagery providers. Providers are required to provide a get_imagery_list and download_imagery method. See abstract methods for requirements"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.provider = None
        self.item_type = None
        self.return_type = None

    @classmethod
    def get_provider(cls, provider, api_key=None):
        if provider == "Planet":
            if not api_key:
                api_key = os.getenv("PLANET_API_KEY")
            return PlanetIM(api_key)

        elif provider == "MAXAR":
            if not api_key:
                api_key = os.getenv("MAXAR_API_KEY")
            return MAXARIM(api_key)

        else:
            raise ValueError("Unsupported imagery provider provided")

    def get_imagery_list_helper(
        self, geometry: Polygon, start_date: str, end_date: str
    ) -> list:
        """searches imagery provider and returns list of suitable imagery ids

        Args:
            geometry (Polygon): geometry of AOI
            start_date (str): earliest date of imagery
            end_date (str): latest date of imagery

        Returns:
            list: list of dictionaries containing timestamp, item_id, item_type, provider, and url
        """

        timestamps, images, urls = self.get_imagery_list(geometry, start_date, end_date)

        return [
            {
                "timestamp": i[0],
                "item_type": self.item_type,
                "item_id": i[1],
                "provider": self.provider,
                "return_type": self.return_type,
                "url": i[2],
            }
            for i in zip(timestamps, images, urls)
        ]

    def download_imagery_helper(
        self,
        job_id: str,
        pre_post: str,
        image_id: str,
        geometry: Polygon,
        out_path: Path,
    ) -> Path:
        """helper for downloading imagery

        Args:
            job_id (str): id of job
            pre_post (str): whether we are downloading pre or post imagery
            image_id (str): id of image to download from provider
            geometry (Polygon): geometry of AOI
            tmp_path (Path): temporary path to use for downloading. Note: this gets deleted
            out_path (Path): output directory for saved image

        Returns:
            Path: path to saved image
        """

        out_path.mkdir(parents=True, exist_ok=True)

        result = self.download_imagery(job_id, pre_post, image_id, geometry, out_path)

        return result

    @abstractmethod
    def get_imagery_list(
        self, geometry: Polygon, start_date: str, end_date: str
    ) -> list:
        """Searches imagery provider and returns JSON of imagery available for search criteria

        Args:
            geometry (tuple): geometry of AOI
            start_date (str): beginning date to search for imagery
            end_date (str): end date to search for imagery

        Returns:
            tuple: tuple of three lists of timestamps, image_ids, and urls
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
    ) -> Path:
        """Downloads selected imagery from provider

        Args:
            job_id (str): id of job
            pre_post (str): whether this is pre or post imagery
            image_id (str): image_id of feature to retrieve from provider
            geometry (Polygon): polygon of AOI
            tmp_path (Path): temp path to use for downloading functions
            out_path (Path): out path to same final image

        Returns:
            Path: file name of saved file
        """
        # output file should follow the template below (use the appropriate file extension)
        # out_file = out_dir / f"{job_id}_{prepost}.tif"
        pass


class Local(Imagery):
    # Maybe use GeoTrellis: https://geotrellis.io/
    pass


class MAXARIM(Imagery):
    def __init__(self, api_key: str) -> None:
        super().__init__(api_key)
        self.provider = "MAXAR"
        self.item_type = "DG_Feature"
        self.return_type = RASTER


    def calculate_dims(self, coords: tuple, res: float = 0.5) -> tuple:
        """Calculates height and width of raster given bounds and resolution

        Args:
            coords (tuple): bounds of input geometry
            res (float): resolution of resulting raster

        Returns:
            tuple: height/width of raster
        """
        dims = calculate_default_transform(
            CRS({"init": "EPSG:4326"}),
            CRS({"init": "EPSG:3587"}),
            10000,
            10000,
            left=coords[0],
            bottom=coords[1],
            right=coords[2],
            top=coords[3],
            resolution=res,
        )
        return (dims[1], dims[2])


    def get_imagery_list(
        self, geometry: Polygon, start_date: str, end_date: str
    ) -> tuple:
        def _construct_cql(cql_list):

            t = []

            for query in cql_list:
                if query["type"] == "inequality":
                    t.append(f"({query['key']}{query['value']})")
                elif query["type"] == "compound":
                    t.append(f"({query['key']}({query['value']}))")
                else:
                    t.append(f"({query['key']}={query['value']})")

            return "AND".join(t)

        bounds = geometry.bounds

        crs = "EPSG:4326"
        # WFS requires bbox minimum Y, minimum X, maximum Y, and maximum X
        bounding_box = f"{bounds[1]},{bounds[0]},{bounds[3]},{bounds[2]}"
        CQL_QUERY = [
            {"key": "cloudCover", "value": "<0.10", "type": "inequality"},
            {"key": "formattedDate", "value": f">'{start_date}'", "type": "inequality"},
            {"key": "BBOX", "value": f"geometry,{bounding_box}", "type": "compound"},
        ]
        query = _construct_cql(CQL_QUERY)

        params = {
            "SERVICE": "WFS",
            "REQUEST": "GetFeature",
            "typeName": "DigitalGlobe:FinishedFeature",
            "VERSION": "1.1.0",
            "connectId": self.api_key,
            "srsName": crs,
            "CQL_Filter": query,
        }

        BASE_URL = f"https://evwhs.digitalglobe.com/catalogservice/wfsaccess"

        resp = requests.get(BASE_URL, params=params)
        result = xmltodict.parse(resp.text)

        timestamps = [
            i["DigitalGlobe:acquisitionDate"]
            for i in result["wfs:FeatureCollection"]["gml:featureMembers"][
                "DigitalGlobe:FinishedFeature"
            ]
        ]
        images = [
            i["@gml:id"]
            for i in result["wfs:FeatureCollection"]["gml:featureMembers"][
                "DigitalGlobe:FinishedFeature"
            ]
        ]
        urls = [
            i["DigitalGlobe:url"]
            for i in result["wfs:FeatureCollection"]["gml:featureMembers"][
                "DigitalGlobe:FinishedFeature"
            ]
        ]

        return timestamps, images, urls

    def download_imagery(
        self,
        job_id: str,
        prepost: str,
        image_id: str,
        geometry: Polygon,
        out_dir: str,
    ) -> bool:
        """
        Take in the URL for a tile server and save the raster to disk

        Parameters:
            tile_source (str): the URL to the tile server
            prepost: (str) whether or not the tile server URL is of pre or post-disaster imagery

        Returns:
            ret_counter (int): how many tiles failed to download
        """

        # WMS requires bbox in minimum X, minimum Y, maximum X, and maximum Y
        bounds = geometry.bounds
        bounding_box = f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}"

        height, width = self.calculate_dims(bounds)

        url = f"https://evwhs.digitalglobe.com/mapservice/wmsaccess?SERVICE=WMS&REQUEST=GetMap&VERSION=1.1.1&LAYERS=DigitalGlobe:Imagery&FORMAT=image/geotiff&HEIGHT={height}&WIDTH={width}&CONNECTID={self.api_key}&FEATUREPROFILE=Default_Profile&COVERAGE_CQL_FILTER=featureId='{image_id}'&CRS=EPSG:4326&BBOX={bounding_box}"
        out_file = out_dir / f"{job_id}_{prepost}.tif"

        urllib.request.urlretrieve(url, out_file)

        return out_file


class PlanetIM(Imagery):
    def __init__(self, api_key: str) -> None:
        super().__init__(api_key)
        self.provider = "Planet"
        self.item_type = "SkySatCollect"
        self.return_type = TILESET

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

        timestamps = [i["properties"]["published"] for i in items]
        images = [i["id"] for i in items]
        urls = [
            i["_links"]["assets"] for i in items
        ]  # Todo: this is not the url to the resource...just to the endpoint to get the url(s)

        return timestamps, images, urls

    def download_imagery(
        self,
        job_id: str,
        pre_post: str,
        image_id: str,
        geometry: Polygon,
        out_dir: Path,
    ) -> str:

        subdomains = ["tiles0", "tiles1", "tiles2", "tiles3"]
        url = f"https://tiles0.planet.com/data/v1/SkySatCollect/{image_id}/{{z}}/{{x}}/{{y}}.png?api_key={self.api_key}"

        ds = TileDataset(url, out_dir, geometry, 18, job_id)

        import time

        stime = time.time()
        memory_file = ds.get_data_from_extent(geometry, zoom_level=18)
        ds.save_memory_file_to_disk(memory_file, pre_post)
        print(f"Fetched imagery in {time.time() - stime} seconds.")

        return  # out_file

