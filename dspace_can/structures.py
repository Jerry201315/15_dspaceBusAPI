"""
ctypes structure definitions matching the dSPACE CAN Bus API C headers.

Reference: dSPACE Bus API Manual, CAN API Reference
  - DSSCanChannelInfo (p.66)
  - DSSCanChannelsSearchAttribute (p.68)
  - DSSCanMessage (p.110)
  - DSSCanBitTimingParameters (p.77)
  - DSSCanBusStatistics (p.112)
"""

import ctypes
from .constants import DSCAN_MAX_NAME_LENGTH, DSCAN_MAX_DATA_LENGTH


class DSSCanChannelInfo(ctypes.Structure):
    """CAN channel information (returned by DSCAN_GetAvailableChannels)."""
    _fields_ = [
        ("szVendorName", ctypes.c_char * DSCAN_MAX_NAME_LENGTH),
        ("szInterfaceName", ctypes.c_char * DSCAN_MAX_NAME_LENGTH),
        ("szInterfaceSerialNumber", ctypes.c_char * DSCAN_MAX_NAME_LENGTH),
        ("szChannelIdentifier", ctypes.c_char * DSCAN_MAX_NAME_LENGTH),
        ("ulChannelCapabilities", ctypes.c_uint32),
    ]

    def __repr__(self):
        return (
            f"DSSCanChannelInfo("
            f"vendor={self.szVendorName.decode()!r}, "
            f"interface={self.szInterfaceName.decode()!r}, "
            f"serial={self.szInterfaceSerialNumber.decode()!r}, "
            f"channel={self.szChannelIdentifier.decode()!r}, "
            f"caps=0x{self.ulChannelCapabilities:08X})"
        )


class DSSCanChannelsSearchAttribute(ctypes.Structure):
    """Search attribute for filtering CAN channels (e.g. by IP address)."""
    _fields_ = [
        ("tSearchAttributeType", ctypes.c_int32),  # DSECanChannelsSearchAttributeType enum
        ("szSearchAttribute", ctypes.c_char * DSCAN_MAX_NAME_LENGTH),
    ]


class DSSCanMessage(ctypes.Structure):
    """A single CAN message (TX or RX).

    Fields:
      ulIdentifier  - CAN message identifier (11-bit or 29-bit)
      usFlags       - TX/RX flags bitmask
      ucDlc         - Data Length Code (0-8 for classic CAN, 0-15 for CAN FD)
      abData        - message payload (up to 64 bytes for CAN FD)
      dHardwareTime - hardware timestamp in seconds (double)
    """
    _fields_ = [
        ("ulIdentifier", ctypes.c_uint32),
        ("usFlags", ctypes.c_uint16),
        ("ucDlc", ctypes.c_uint8),
        ("ucReserved", ctypes.c_uint8),
        ("abData", ctypes.c_uint8 * DSCAN_MAX_DATA_LENGTH),
        ("dHardwareTime", ctypes.c_double),
    ]

    def __repr__(self):
        data_len = min(self.ucDlc, 8)  # classic CAN
        if self.usFlags & 0x0004:  # FD flag
            data_len = self.ucDlc
        hex_data = " ".join(f"{self.abData[i]:02X}" for i in range(data_len))
        return (
            f"CAN ID=0x{self.ulIdentifier:03X} "
            f"[{self.ucDlc}] {hex_data} "
            f"@{self.dHardwareTime:.6f}s "
            f"flags=0x{self.usFlags:04X}"
        )


class DSSCanBitTimingParameters(ctypes.Structure):
    """CAN bit timing parameters."""
    _fields_ = [
        ("ulClockFrequency", ctypes.c_uint32),
        ("ulBaudratePrescaler", ctypes.c_uint32),
        ("ulTSeg1", ctypes.c_uint32),
        ("ulTSeg2", ctypes.c_uint32),
        ("ulSJW", ctypes.c_uint32),
        ("ulSAM", ctypes.c_uint32),
    ]


class DSSCanBusStatistics(ctypes.Structure):
    """CAN bus statistics."""
    _fields_ = [
        ("ulFlags", ctypes.c_uint32),
        ("ulBusLoad", ctypes.c_uint32),
        ("ulStdDataFrameCount", ctypes.c_uint32),
        ("ulStdRemoteFrameCount", ctypes.c_uint32),
        ("ulXtdDataFrameCount", ctypes.c_uint32),
        ("ulXtdRemoteFrameCount", ctypes.c_uint32),
        ("ulErrorFrameCount", ctypes.c_uint32),
        ("dHardwareTime", ctypes.c_double),
    ]
