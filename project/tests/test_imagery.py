import pytest
from imagery import MAXARIM, PlanetIM
import utils
import schemas

@pytest.fixture
def coordinate():
    return schemas.Coordinate(
            start_lon=-84.51018588514154,
            start_lat=39.13524611961671,
            end_lon=-84.5010154424076,
            end_lat=39.127504935072274,
        )


@pytest.fixture
def bbox_poly(coordinate):
    return utils.create_bounding_box_poly(coordinate)


class TestMaxar:
    def test_maxar(self):
        assert False


class TestPlanet:
    def test_url_list(self, bbox_poly):
        planet = PlanetIM('null')
        res = planet.get_url_list('20220605_234920_ssc16_u0001', bbox_poly)
        assert res == False
