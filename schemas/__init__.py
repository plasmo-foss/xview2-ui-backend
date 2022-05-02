"""Schemas"""

from .coordinate import Coordinate
from .osmgeojson import OsmGeoJson
from .planet import Planet

__all__ = (
    "Coordinate",
    "OsmGeoJson",
    "Planet"
)

# https://stackoverflow.com/questions/40018681
for module in __all__:
    globals()[module].__module__ = "xview2backend.schemas"