"""
ctypes structure definitions matching the dSPACE CAN Bus API C headers.

Reference: dSPACE Bus API Manual, CAN API Reference
  - DSSCanChannelInfo (p.66)
  - DSSCanChannelsSearchAttribute (p.68)
  - DSSCanBitTimingParameters (p.77)
  - DSSCanBusInfo (p.109)
  - DSSCanMessage (p.110)
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


class DSSCanBitTimingParameters(ctypes.Structure):
    """CAN bit timing parameters (p.78)."""
    _fields_ = [
        ("ulSJW", ctypes.c_uint32),    # synchronization jump width
        ("ulBRP", ctypes.c_uint32),    # baud rate prescaler
        ("ulSAM", ctypes.c_uint32),    # sample mode
        ("ulTSEG1", ctypes.c_uint32),  # bit time segment 1
        ("ulTSEG2", ctypes.c_uint32),  # bit time segment 2
    ]


class DSSCanBusInfo(ctypes.Structure):
    """CAN bus information embedded in DSSCanMessage (p.109)."""
    _fields_ = [
        ("tBusStatus", ctypes.c_int32),        # DSECanBusStatus enum
        ("usRxErrorCounter", ctypes.c_uint16),
        ("usTxErrorCounter", ctypes.c_uint16),
        ("ucBusLoad", ctypes.c_uint8),
    ]


class DSSCanMessage(ctypes.Structure):
    """A single CAN message (TX or RX) — matches C struct on p.110.

    Fields:
      tMessageType       - DSECanMessageType enum (1=Data, 2=Remote, 3=Error, ...)
      ui64Timestamp      - hardware timestamp (ticks)
      ulCanIdentifier    - CAN message identifier (11-bit or 29-bit)
      tCanIdentifierType - DSECanIdentifierType enum (0x01=STD, 0x02=XTD)
      ulFlags            - TX/RX flags bitmask
      usDLC              - Data Length Code
      ucData             - message payload (up to 64 bytes for CAN FD)
      tBusInfo           - CAN bus info (for bus info/statistics messages)
    """
    _fields_ = [
        ("tMessageType", ctypes.c_int32),           # DSECanMessageType enum
        ("ui64Timestamp", ctypes.c_uint64),          # hardware timestamp
        ("ulCanIdentifier", ctypes.c_uint32),        # CAN ID
        ("tCanIdentifierType", ctypes.c_int32),      # DSECanIdentifierType enum
        ("ulFlags", ctypes.c_uint32),                # TX/RX flags
        ("usDLC", ctypes.c_uint16),                  # data length code
        ("ucData", ctypes.c_uint8 * DSCAN_MAX_DATA_LENGTH),  # payload
        ("tBusInfo", DSSCanBusInfo),                 # bus info
    ]

    def __repr__(self):
        data_len = min(self.usDLC, 8)  # classic CAN
        if self.ulFlags & 0x0100:  # DSCAN_RX_MESSAGE_FLAG_FD
            data_len = self.usDLC
        hex_data = " ".join(f"{self.ucData[i]:02X}" for i in range(data_len))
        id_fmt = f"0x{self.ulCanIdentifier:08X}" if self.tCanIdentifierType == 0x02 else f"0x{self.ulCanIdentifier:03X}"
        return (
            f"CAN ID={id_fmt} "
            f"[{self.usDLC}] {hex_data} "
            f"@{self.ui64Timestamp} "
            f"flags=0x{self.ulFlags:08X}"
        )


class DSSCanBusStatistics(ctypes.Structure):
    """CAN bus statistics (p.112)."""
    _fields_ = [
        ("ulFlags", ctypes.c_uint32),
        ("ulErrorFrames", ctypes.c_uint32),
        ("ulRxStdFrames", ctypes.c_uint32),
        ("ulTxStdFrames", ctypes.c_uint32),
        ("ulRxExtFrames", ctypes.c_uint32),
        ("ulTxExtFrames", ctypes.c_uint32),
        ("ulRxStdFDFrames", ctypes.c_uint32),
        ("ulTxStdFDFrames", ctypes.c_uint32),
        ("ulRxExtFDFrames", ctypes.c_uint32),
        ("ulTxExtFDFrames", ctypes.c_uint32),
    ]
