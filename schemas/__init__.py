"""Schemas"""

from .coordinate import Coordinate
from .osmgeojson import OsmGeoJson

__all__ = (
    "Coordinate",
    "OsmGeoJson"
)

# https://stackoverflow.com/questions/40018681
for module in __all__:
    globals()[module].__module__ = "xview2backend.schemas"