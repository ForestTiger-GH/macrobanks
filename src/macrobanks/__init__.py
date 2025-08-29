from ._version import __version__

from . import cbr, routines  # позволяет писать: from macrobanks import commodities

__all__ = ["cbr", "routines", "__version__"]
