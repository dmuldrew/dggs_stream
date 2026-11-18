"""Deprecated alias of :mod:`dggs_stream`, the package's name since the project was renamed.

``import raster_h3`` returns the :mod:`dggs_stream` module itself, so every function,
class and submodule (``raster_h3.spark``) keeps working. Import ``dggs_stream`` instead;
this alias will be removed in a future release.
"""

import sys
import warnings

import dggs_stream as _dggs_stream

warnings.warn(
    "the raster_h3 package is now called dggs_stream; `import dggs_stream` instead "
    "(raster_h3 will be removed in a future release)",
    DeprecationWarning,
    stacklevel=2,
)

sys.modules[__name__] = _dggs_stream
