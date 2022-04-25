"""Schemas"""

from .coordinate import Coordinate

__all__ = (
    "Coordinate",
)

# https://stackoverflow.com/questions/40018681
for module in __all__:
    globals()[module].__module__ = "xview2backend.schemas"