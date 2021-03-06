import glob
import shutil
import urllib.request

import dateutil.parser
import geopandas as gpd
import planet.api as api
from dateutil.relativedelta import relativedelta
from osgeo import gdal
from planet.api import ClientV1
from shapely.geometry import MultiPolygon, Polygon, mapping

from schemas import Coordinate
from tileserverutils import bbox_to_xyz, x_to_lon_edges, y_to_lat_edges


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
        tile_source: str,
        prepost: str
    ) -> int:
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
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        x_min, x_max, y_min, y_max = bbox_to_xyz(lon_min, lon_max, lat_min, lat_max, self.zoom)
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
        self.merge_tiles((self.temp_dir / "*.tif").as_posix(), self.output_dir / f"{self.job_id}_{prepost}_merged.tif")
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
                outers_lonlat.append(Polygon([(x["lon"], x["lat"]) for x in outer]))

            for inner in inners:
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


def get_planet_imagery(client: ClientV1, geom: Polygon, current_date: str) -> dict:
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
    # this will cause an exception if there are any API related errors
    results = client.quick_search(request)
    items = [i for i in results.items_iter(500)]

    # items_iter returns an iterator over API response pages
    return [
        {"image_id": i["id"], "timestamp": i["properties"]["published"], "asset": i} for i in items
    ]


def download_planet_imagery(converter: Converter, url: str, prepost: str):
    return converter.convert(
        url,
        prepost
    )
