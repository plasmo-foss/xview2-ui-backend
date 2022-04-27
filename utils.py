from schemas import Coordinate
import geopandas as gpd
from shapely.geometry import shape
from shapely.geometry.polygon import orient


def order_coordinate(coordinate: Coordinate) -> Coordinate:
    south = min(coordinate.start_lat, coordinate.end_lat)
    north = max(coordinate.start_lat, coordinate.end_lat)
    west = min(coordinate.start_lon, coordinate.end_lon)
    east = max(coordinate.start_lon, coordinate.end_lon)

    return Coordinate(start_lon=west, start_lat=north, end_lon=east, end_lat=south)

def osm_geom_to_poly_geojson(osm_data: dict):
    polys = []
    for element in osm_data["elements"]:
        geom = element["geometry"]
        # Change LineString to Polygon
        geom["type"] = "Polygon"
        # Wrap coordinates array in an additional list
        coords = [geom["coordinates"]]
        geom["coordinates"] = coords

        # Convert JSON to Shapely Polygon and orient it according to the right-hand rule
        polys.append(orient(shape(geom)))

    # Return GeoJSON
    return gpd.GeoSeries(polys).__geo_interface__