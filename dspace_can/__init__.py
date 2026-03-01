"""dSPACE Bus API CAN wrapper for Python (ctypes-based)."""

from .api import DsCanApi
from .constants import *
from .structures import (
    DSSCanChannelInfo,
    DSSCanChannelsSearchAttribute,
    DSSCanMessage,
    DSSCanBitTimingParameters,
)

__all__ = ["DsCanApi"]
