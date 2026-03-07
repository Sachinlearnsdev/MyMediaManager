#!/usr/bin/env python3
"""
tvprocessor.py -- DEPRECATED: Use seriesprocessor.py --type tv

This file is a thin wrapper kept for backwards compatibility.
"""

import os
import sys
import warnings

_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

warnings.warn(
    "tvprocessor.py is deprecated. Use: python3 seriesprocessor.py --type tv",
    DeprecationWarning, stacklevel=2
)

from bin.seriesprocessor import SeriesProcessor

if __name__ == "__main__":
    SeriesProcessor("tv").run()
