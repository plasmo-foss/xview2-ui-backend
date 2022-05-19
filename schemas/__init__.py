"""Schemas"""

from .coordinate import Coordinate
from .osmgeojson import OsmGeoJson
from .planet import Planet
from .routes import SearchOsmPolygons, FetchPlanetImagery, LaunchAssessment

__all__ = (
    "Coordinate",
    "OsmGeoJson",
    "Planet",
    "SearchOsmPolygons",
    "FetchPlanetImagery",
    "LaunchAssessment"
)

# https://stackoverflow.com/questions/40018681
for module in __all__:
    globals()[module].__module__ = "xview2backend.schemas"
