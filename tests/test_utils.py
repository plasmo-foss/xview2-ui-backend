import pytest
import utils
import json
from schemas import coordinate

class TestOSMtoPoly:
    def test_osm_to_poly(self):
        coords = coordinate.Coordinate(
            start_lon=30.51312875997559,
            start_lat=50.456883437521434,
            end_lon=30.496226372379418,
            end_lat=50.45087351945944)

        result = utils.osm_geom_to_poly_geojson(coords)
        assert len(result['features']) == 434