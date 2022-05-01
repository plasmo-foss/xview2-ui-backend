import dateutil.parser
import geopandas as gpd
import planet.api as api
from dateutil.relativedelta import relativedelta
from planet.api import ClientV1
from shapely.geometry import MultiPolygon, Polygon, mapping

from schemas import Coordinate


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
        api.filters.range_filter("cloud_cover", lte=0.5),
        api.filters.permission_filter("assets.ortho_pansharpened:download"),
        api.filters.string_filter("quality_category", "standard"),
    )

    request = api.filters.build_search_request(query, ["SkySatCollect"])
    # this will cause an exception if there are any API related errors
    results = client.quick_search(request)
    items = [i for i in results.items_iter(500)]

    # items_iter returns an iterator over API response pages
    return [
        {"image_id": i["id"], "timestamp": i["properties"]["published"]} for i in items
    ]
