"""Schemas"""

from .coordinate import Coordinate, JobId

__all__ = (
    "Coordinate",
    "JobId"
)

# https://stackoverflow.com/questions/40018681
for module in __all__:
    globals()[module].__module__ = "xview2backend.schemas"