import glob
import os
import shutil
import urllib.request
from math import atan, cos, degrees, floor, log, pi, radians, sinh, tan
from typing import List
from osgeo import gdal

from utils import order_coordinate
from pathlib import Path


def sec(x):
    return 1 / cos(x)


def latlon_to_xyz(lat, lon, z):
    tile_count = pow(2, z)
    x = (lon + 180) / 360
    y = (1 - log(tan(radians(lat)) + sec(radians(lat))) / pi) / 2
    return (tile_count * x, tile_count * y)


def bbox_to_xyz(lon_min, lon_max, lat_min, lat_max, z):
    x_min, y_max = latlon_to_xyz(lat_min, lon_min, z)
    x_max, y_min = latlon_to_xyz(lat_max, lon_max, z)
    return (floor(x_min), floor(x_max), floor(y_min), floor(y_max))


def mercatorToLat(mercatorY):
    return degrees(atan(sinh(mercatorY)))


def y_to_lat_edges(y, z):
    tile_count = pow(2, z)
    unit = 1 / tile_count
    relative_y1 = y * unit
    relative_y2 = relative_y1 + unit
    lat1 = mercatorToLat(pi * (1 - 2 * relative_y1))
    lat2 = mercatorToLat(pi * (1 - 2 * relative_y2))
    return (lat1, lat2)


def x_to_lon_edges(x, z):
    tile_count = pow(2, z)
    unit = 360 / tile_count
    lon1 = -180 + x * unit
    lon2 = lon1 + unit
    return (lon1, lon2)


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
            tile_source.replace("{x}", str(x)).replace("{y}", str(y)).replace("{z}", str(z))
        )

        if not tile_source.startswith("http"):
            return url.replace("file:///", "")

        path = f"{self.temp_dir}/{x}_{y}_{z}.png"
        urllib.request.urlretrieve(url, path)
        return path


    def merge_tiles(self, input_pattern, output_path):
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
    
    def convert(
        self,
        tile_source: str
    ):
        box = order_coordinate(self.bounding_box)
        lon_min = box.start_lon
        lat_min = box.end_lat
        lon_max = box.end_lon
        lat_max = box.start_lat

        # Script start:
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        x_min, x_max, y_min, y_max = bbox_to_xyz(lon_min, lon_max, lat_min, lat_max, self.zoom)
        print(
            f"Fetching & georeferencing {(x_max - x_min + 1) * (y_max - y_min + 1)} tiles for {tile_source}"
        )

        for x in range(x_min, x_max + 1):
            for y in range(y_min, y_max + 1):
                try:
                    png_path = self.fetch_tile(x, y, self.zoom, tile_source)
                    self.georeference_raster_tile(x, y, self.zoom, png_path)
                except OSError:
                    print(f"Error, failed to get {x},{y}")
                    pass

        print("Resolving and georeferencing of raster tiles complete")

        print("Merging tiles")
        self.merge_tiles((self.temp_dir / "*.tif").as_posix(), self.output_dir / f"{self.job_id}_merged.tif")
        print("Merge complete")

        shutil.rmtree(self.temp_dir)
