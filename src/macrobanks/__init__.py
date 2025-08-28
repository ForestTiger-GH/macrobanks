from ._version import __version__

from . import commodities  # позволяет писать: from macrobanks import commodities

__all__ = ["commodities", "__version__"]
