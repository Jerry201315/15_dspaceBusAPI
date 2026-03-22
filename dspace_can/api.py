"""
Python wrapper around the dSPACE CAN Bus API (DSBusApiCan.dll on Windows).

This module loads the native DLL via ctypes and exposes a Pythonic interface
for discovering CAN channels, connecting to them, and reading/writing CAN
messages on dSPACE SCALEXIO (and other supported) hardware.

Target platform: Windows 11 x64 with dSPACE Bus API 2025-B installed.

Usage:
    from dspace_can import DsCanApi
    api = DsCanApi()                              # loads DLL from default path
    channels = api.get_available_channels("10.0.0.1")
    handle = api.register_channel(channels[0])
    api.init_channel(handle)
    api.activate_channel(handle)
    messages = api.read_messages(handle)
    api.deactivate_channel(handle)
    api.unregister_channel(handle)
"""

from __future__ import annotations

import ctypes
import os
import sys
from pathlib import Path
from typing import Optional

from .constants import (
    DSCAN_ERR_NO_ERROR,
    DSCAN_ERR_QUEUE_EMPTY,
    DSCAN_IDENTIFIER_TYPE_STD_XTD,
    DSCAN_INVALID_CAN_HANDLE,
    DSCAN_MAX_NAME_LENGTH,
    DSCAN_MAX_RX_QUEUE_SIZE,
    DSCAN_MAX_TEXT_LENGTH,
    DSCAN_SEARCH_ATTRIBUTE_TYPE_IP_V4_ADDRESS,
    DSCAN_BAUD_500K,
    ERROR_TEXT,
)
from .structures import (
    DSSCanChannelInfo,
    DSSCanChannelsSearchAttribute,
    DSSCanMessage,
    DSSCanBitTimingParameters,
)


class DsCanError(Exception):
    """Raised when a dSPACE CAN API call returns a non-zero error code."""

    def __init__(self, func_name: str, error_code: int, detail: str = ""):
        self.func_name = func_name
        self.error_code = error_code
        msg = f"{func_name} failed with error {error_code}"
        text = ERROR_TEXT.get(error_code)
        if text:
            msg += f" ({text})"
        if detail:
            msg += f": {detail}"
        super().__init__(msg)


# Default DLL search paths on Windows (dSPACE Bus API 2025-B)
_DEFAULT_DLL_PATHS = [
    # Standard dSPACE installation
    os.path.expandvars(
        r"%CommonProgramFiles%\dSPACE\dSPACE Bus API\2025-B\Deliverables\bin\DSBusApiCan.dll"
    ),
    # Fallback: DLL in same directory as this script (for portable deployments)
    str(Path(__file__).parent / "DSBusApiCan.dll"),
    # Fallback: DLL in project root
    str(Path(__file__).parent.parent / "DSBusApiCan.dll"),
]


class DsCanApi:
    """High-level Python interface to the dSPACE CAN Bus API."""

    def __init__(self, dll_path: Optional[str] = None):
        """Load the DSBusApiCan.dll.

        Args:
            dll_path: Explicit path to DSBusApiCan.dll. If None, searches
                      default dSPACE installation paths.
        """
        self._dll = self._load_dll(dll_path)
        self._setup_prototypes()

    # ------------------------------------------------------------------ #
    # DLL loading
    # ------------------------------------------------------------------ #

    @staticmethod
    def _load_dll(dll_path: Optional[str] = None) -> ctypes.WinDLL:
        if sys.platform != "win32":
            raise OSError(
                "dSPACE Bus API CAN DLL is only available on Windows. "
                "This code must run on the Win11 machine with dSPACE hardware."
            )

        paths = [dll_path] if dll_path else _DEFAULT_DLL_PATHS
        last_error = None
        for p in paths:
            if p and os.path.isfile(p):
                try:
                    return ctypes.WinDLL(p)
                except OSError as e:
                    last_error = e

        searched = "\n  ".join(paths)
        raise FileNotFoundError(
            f"Could not load DSBusApiCan.dll. Searched:\n  {searched}\n"
            f"Make sure dSPACE Bus API 2025-B is installed, or pass dll_path explicitly.\n"
            f"Last error: {last_error}"
        )

    def _setup_prototypes(self):
        """Declare ctypes argument/return types for each API function we use."""

        dll = self._dll

        # --- Vendor information ---
        dll.DSCAN_GetSupportedVendorsCount.argtypes = [ctypes.POINTER(ctypes.c_uint32)]
        dll.DSCAN_GetSupportedVendorsCount.restype = ctypes.c_int32

        # --- Channel discovery ---
        dll.DSCAN_GetAvailableChannelsCount.argtypes = [
            ctypes.POINTER(ctypes.c_uint32),       # pulChannelsCount (out)
            ctypes.c_uint32,                        # ulAdditionalSearchAttributesCount
            ctypes.POINTER(DSSCanChannelsSearchAttribute),  # ptAdditionalSearchAttributesArray
        ]
        dll.DSCAN_GetAvailableChannelsCount.restype = ctypes.c_int32

        dll.DSCAN_GetAvailableChannels.argtypes = [
            ctypes.POINTER(ctypes.c_uint32),       # pulChannelsCount (in/out)
            ctypes.POINTER(DSSCanChannelInfo),      # ptChannelsArray (out)
            ctypes.c_uint32,                        # ulAdditionalSearchAttributesCount
            ctypes.POINTER(DSSCanChannelsSearchAttribute),
        ]
        dll.DSCAN_GetAvailableChannels.restype = ctypes.c_int32

        dll.DSCAN_IsChannelAvailable.argtypes = [
            ctypes.c_char_p,  # szVendorName
            ctypes.c_char_p,  # szInterfaceName
            ctypes.c_char_p,  # szInterfaceSerialNumber
            ctypes.c_char_p,  # szChannelIdentifier
            ctypes.POINTER(ctypes.c_bool),  # pbIsAvailable (out)
        ]
        dll.DSCAN_IsChannelAvailable.restype = ctypes.c_int32

        # --- Registration ---
        dll.DSCAN_RegisterChannel.argtypes = [
            ctypes.c_char_p,  # szVendorName
            ctypes.c_char_p,  # szInterfaceName
            ctypes.c_char_p,  # szInterfaceSerialNumber
            ctypes.c_char_p,  # szChannelIdentifier
            ctypes.POINTER(ctypes.c_int32),  # ptHandle (out)
        ]
        dll.DSCAN_RegisterChannel.restype = ctypes.c_int32

        dll.DSCAN_UnregisterChannel.argtypes = [ctypes.c_int32]
        dll.DSCAN_UnregisterChannel.restype = ctypes.c_int32

        # --- Initialization & configuration ---
        dll.DSCAN_InitChannel.argtypes = [
            ctypes.c_int32,          # tHandle
            ctypes.c_int32,          # tIdentifierType (DSECanIdentifierType)
            ctypes.c_uint32,         # ulRxQueueSize
            ctypes.c_bool,           # bExclusive
            ctypes.POINTER(ctypes.c_bool),  # pbAccessPermission (out)
        ]
        dll.DSCAN_InitChannel.restype = ctypes.c_int32

    # --- Baud rate functions ---
        dll.DSCAN_GetBaudrate.argtypes = [
            ctypes.c_int32,                             # tHandle
            ctypes.POINTER(ctypes.c_uint32),            # pulClockFrequency
            ctypes.POINTER(DSSCanBitTimingParameters),  # ptBitTimingParameters
            ctypes.POINTER(ctypes.c_bool),              # pbFD
            ctypes.POINTER(DSSCanBitTimingParameters),  # ptBitTimingParameters_FD
        ]
        dll.DSCAN_GetBaudrate.restype = ctypes.c_int32

        dll.DSCAN_ConvertBitTimingParametersToBaudrate.argtypes = [
            ctypes.c_uint32,                            # ulClockFrequency
            ctypes.POINTER(DSSCanBitTimingParameters),  # ptBitTimingParameters
            ctypes.POINTER(ctypes.c_uint32),            # pulBaudrate
        ]
        dll.DSCAN_ConvertBitTimingParametersToBaudrate.restype = ctypes.c_int32

        # --- Activation ---
        dll.DSCAN_ActivateChannel.argtypes = [ctypes.c_int32]
        dll.DSCAN_ActivateChannel.restype = ctypes.c_int32

        dll.DSCAN_DeactivateChannel.argtypes = [ctypes.c_int32]
        dll.DSCAN_DeactivateChannel.restype = ctypes.c_int32

        # --- Communication ---
        dll.DSCAN_GetReceiveQueueLevel.argtypes = [
            ctypes.c_int32,
            ctypes.POINTER(ctypes.c_uint32),
        ]
        dll.DSCAN_GetReceiveQueueLevel.restype = ctypes.c_int32

        dll.DSCAN_ReadReceiveQueue.argtypes = [
            ctypes.c_int32,                      # tHandle
            ctypes.POINTER(ctypes.c_uint32),     # pulMsgCount (in/out)
            ctypes.POINTER(DSSCanMessage),       # ptMsgArray (out)
        ]
        dll.DSCAN_ReadReceiveQueue.restype = ctypes.c_int32

        dll.DSCAN_TransmitMessages.argtypes = [
            ctypes.c_int32,                      # tHandle
            ctypes.POINTER(ctypes.c_uint32),     # pulMsgCount (in/out)
            ctypes.POINTER(DSSCanMessage),       # ptMsgArray (in)
        ]
        dll.DSCAN_TransmitMessages.restype = ctypes.c_int32

        dll.DSCAN_FlushReceiveQueue.argtypes = [ctypes.c_int32]
        dll.DSCAN_FlushReceiveQueue.restype = ctypes.c_int32

        dll.DSCAN_FlushTransmitQueue.argtypes = [ctypes.c_int32]
        dll.DSCAN_FlushTransmitQueue.restype = ctypes.c_int32

        dll.DSCAN_GetHardwareTime.argtypes = [
            ctypes.c_int32,
            ctypes.POINTER(ctypes.c_double),
        ]
        dll.DSCAN_GetHardwareTime.restype = ctypes.c_int32

        # --- Error handling ---
        dll.DSCAN_GetErrorText.argtypes = [
            ctypes.c_int32,
            ctypes.c_char * DSCAN_MAX_TEXT_LENGTH,
        ]
        dll.DSCAN_GetErrorText.restype = ctypes.c_int32

        # --- Acceptance filter ---
# --- Acceptance filter ---
        dll.DSCAN_SetAcceptance.argtypes = [
            ctypes.c_int32,   # tHandle
            ctypes.c_uint32,  # ulStandardCanIdentifiersCode
            ctypes.c_uint32,  # ulStandardCanIdentifiersMask
            ctypes.c_uint32,  # ulExtendedCanIdentifiersCode
            ctypes.c_uint32,  # ulExtendedCanIdentifiersMask
        ]
        dll.DSCAN_SetAcceptance.restype = ctypes.c_int32

        # --- Transmit acknowledge ---
        dll.DSCAN_SetTransmitAcknowledge.argtypes = [
            ctypes.c_int32,  # tHandle
            ctypes.c_bool,   # bEnable
        ]
        dll.DSCAN_SetTransmitAcknowledge.restype = ctypes.c_int32

        # --- Channel output mode ---
        dll.DSCAN_SetChannelOutput.argtypes = [
            ctypes.c_int32,  # tHandle
            ctypes.c_int32,  # tOutputMode (DSECanChannelOutputMode)
        ]
        dll.DSCAN_SetChannelOutput.restype = ctypes.c_int32

        # --- Bus info ---
        # DSCAN_GetBusInfo defined but omitting detailed struct for now

    # ------------------------------------------------------------------ #
    # Helper: check error code
    # ------------------------------------------------------------------ #

    def _check(self, func_name: str, err: int, ignore: tuple[int, ...] = ()):
        if err != DSCAN_ERR_NO_ERROR and err not in ignore:
            raise DsCanError(func_name, err, self._get_error_text(err))

    def _get_error_text(self, error_code: int) -> str:
        buf = (ctypes.c_char * DSCAN_MAX_TEXT_LENGTH)()
        try:
            self._dll.DSCAN_GetErrorText(error_code, buf)
            return buf.value.decode("utf-8", errors="replace")
        except Exception:
            return ERROR_TEXT.get(error_code, "Unknown error")

    # ------------------------------------------------------------------ #
    # Channel discovery
    # ------------------------------------------------------------------ #

    def get_available_channels(
        self, ip_address: Optional[str] = None
    ) -> list[DSSCanChannelInfo]:
        """Discover available CAN channels.

        Args:
            ip_address: IPv4 address of the dSPACE platform (e.g. SCALEXIO).
                        If None, discovers PC-based interfaces only.

        Returns:
            List of DSSCanChannelInfo structures describing each channel.
        """
        count = ctypes.c_uint32(0)

        if ip_address:
            attr = DSSCanChannelsSearchAttribute()
            attr.tSearchAttributeType = DSCAN_SEARCH_ATTRIBUTE_TYPE_IP_V4_ADDRESS
            attr.szSearchAttribute = ip_address.encode("utf-8")
            attr_count = ctypes.c_uint32(1)

            err = self._dll.DSCAN_GetAvailableChannelsCount(
                ctypes.byref(count), 1, ctypes.byref(attr)
            )
            self._check("DSCAN_GetAvailableChannelsCount", err)

            if count.value == 0:
                return []

            channels = (DSSCanChannelInfo * count.value)()
            err = self._dll.DSCAN_GetAvailableChannels(
                ctypes.byref(count), channels, 1, ctypes.byref(attr)
            )
            self._check("DSCAN_GetAvailableChannels", err)
        else:
            err = self._dll.DSCAN_GetAvailableChannelsCount(
                ctypes.byref(count), 0, None
            )
            self._check("DSCAN_GetAvailableChannelsCount", err)

            if count.value == 0:
                return []

            channels = (DSSCanChannelInfo * count.value)()
            err = self._dll.DSCAN_GetAvailableChannels(
                ctypes.byref(count), channels, 0, None
            )
            self._check("DSCAN_GetAvailableChannels", err)

        return list(channels[: count.value])

    # ------------------------------------------------------------------ #
    # Channel registration / lifecycle
    # ------------------------------------------------------------------ #

    def register_channel(self, channel_info: DSSCanChannelInfo) -> int:
        """Register a CAN channel for data transmission/reception.

        Returns:
            Channel handle (int) used in all subsequent API calls.
        """
        handle = ctypes.c_int32(DSCAN_INVALID_CAN_HANDLE)
        err = self._dll.DSCAN_RegisterChannel(
            channel_info.szVendorName,
            channel_info.szInterfaceName,
            channel_info.szInterfaceSerialNumber,
            channel_info.szChannelIdentifier,
            ctypes.byref(handle),
        )
        self._check("DSCAN_RegisterChannel", err)
        return handle.value

    def unregister_channel(self, handle: int):
        """Unregister a CAN channel and invalidate the handle."""
        err = self._dll.DSCAN_UnregisterChannel(handle)
        self._check("DSCAN_UnregisterChannel", err)

    def init_channel(
        self,
        handle: int,
        identifier_type: int = DSCAN_IDENTIFIER_TYPE_STD_XTD,
        rx_queue_size: int = 1024,
        exclusive: bool = False,
    ) -> bool:
        """Initialize a CAN channel and request access permission.

        Args:
            handle: Channel handle from register_channel().
            identifier_type: DSCAN_IDENTIFIER_TYPE_STD, _XTD, or _STD_XTD.
            rx_queue_size: Receive queue size (max 32768).
            exclusive: If True, request exclusive access.

        Returns:
            True if access permission was granted.
        """
        access_perm = ctypes.c_bool(False)
        err = self._dll.DSCAN_InitChannel(
            handle, identifier_type, rx_queue_size, exclusive, ctypes.byref(access_perm)
        )
        self._check("DSCAN_InitChannel", err)
        return access_perm.value

    def set_baudrate(self, handle: int, baudrate: int = DSCAN_BAUD_500K, fd_baudrate: int = 0):
        """Set the CAN baud rate using the auxiliary conversion function.

        For simplicity this uses DSCAN_ConvertBaudrateToBitTimingParameters
        internally. For advanced usage, build DSSCanBitTimingParameters manually.

        Args:
            handle: Channel handle.
            baudrate: Arbitration phase baud rate in bit/s (default 500k).
            fd_baudrate: Data phase baud rate for CAN FD (0 = not CAN FD).
        """
        btp = DSSCanBitTimingParameters()
        btp_fd = DSSCanBitTimingParameters() if fd_baudrate else None

        # Use the API's conversion helper
        err = self._dll.DSCAN_ConvertBaudrateToBitTimingParameters(
            baudrate, ctypes.byref(btp)
        )
        self._check("DSCAN_ConvertBaudrateToBitTimingParameters", err)

        if fd_baudrate:
            err = self._dll.DSCAN_ConvertBaudrateToBitTimingParameters(
                fd_baudrate, ctypes.byref(btp_fd)
            )
            self._check("DSCAN_ConvertBaudrateToBitTimingParameters (FD)", err)

        err = self._dll.DSCAN_SetBaudrate(
            handle, ctypes.byref(btp), ctypes.byref(btp_fd) if btp_fd else None
        )
        self._check("DSCAN_SetBaudrate", err)

    def get_baudrate(self, handle: int) -> int:
        """Read the current physical baud rate configured on the hardware."""
        clock_freq = ctypes.c_uint32(0)
        btp = DSSCanBitTimingParameters()
        is_fd = ctypes.c_bool(False)
        btp_fd = DSSCanBitTimingParameters()

        # 1. Ask the hardware for its raw timing parameters
        err = self._dll.DSCAN_GetBaudrate(
            handle, 
            ctypes.byref(clock_freq), 
            ctypes.byref(btp), 
            ctypes.byref(is_fd), 
            ctypes.byref(btp_fd)
        )
        self._check("DSCAN_GetBaudrate", err)

        # 2. Convert the raw parameters into a readable integer (e.g., 500000)
        baudrate = ctypes.c_uint32(0)
        err = self._dll.DSCAN_ConvertBitTimingParametersToBaudrate(
            clock_freq.value, 
            ctypes.byref(btp), 
            ctypes.byref(baudrate)
        )
        self._check("DSCAN_ConvertBitTimingParametersToBaudrate", err)

        return baudrate.value

    def activate_channel(self, handle: int):
        """Activate a CAN channel so it can send/receive messages."""
        err = self._dll.DSCAN_ActivateChannel(handle)
        self._check("DSCAN_ActivateChannel", err)

    def deactivate_channel(self, handle: int):
        """Deactivate a CAN channel."""
        err = self._dll.DSCAN_DeactivateChannel(handle)
        self._check("DSCAN_DeactivateChannel", err)

    def set_acceptance(
            self, 
            handle: int, 
            std_code: int = 0, 
            std_mask: int = 0, 
            xtd_code: int = 0, 
            xtd_mask: int = 0
        ):
            """Sets the acceptance filter to allow specific CAN IDs. 0 means allow all."""
            err = self._dll.DSCAN_SetAcceptance(
                handle, 
                std_code, 
                std_mask, 
                xtd_code, 
                xtd_mask
            )
            self._check("DSCAN_SetAcceptance", err)
    
    def set_transmit_acknowledge(self, handle: int, enable: bool):
        """Allows the receive queue to see messages transmitted by this same CAN node."""
        err = self._dll.DSCAN_SetTransmitAcknowledge(handle, enable)
        self._check("DSCAN_SetTransmitAcknowledge", err)
    # ------------------------------------------------------------------ #
    # Reading messages
    # ------------------------------------------------------------------ #

    def get_receive_queue_level(self, handle: int) -> int:
        """Return the number of CAN messages waiting in the receive queue."""
        count = ctypes.c_uint32(0)
        err = self._dll.DSCAN_GetReceiveQueueLevel(handle, ctypes.byref(count))
        self._check("DSCAN_GetReceiveQueueLevel", err)
        return count.value

    def read_messages(self, handle: int, max_messages: int = 0) -> list[DSSCanMessage]:
        """Read CAN messages from the receive queue.

        Args:
            handle: Channel handle.
            max_messages: Max messages to read. 0 = read all available.

        Returns:
            List of DSSCanMessage objects.
        """
        queue_level = self.get_receive_queue_level(handle)
        if queue_level == 0:
            return []

        count = min(queue_level, max_messages) if max_messages > 0 else queue_level
        msg_count = ctypes.c_uint32(count)
        messages = (DSSCanMessage * count)()

        err = self._dll.DSCAN_ReadReceiveQueue(
            handle, ctypes.byref(msg_count), messages
        )
        self._check("DSCAN_ReadReceiveQueue", err, ignore=(DSCAN_ERR_QUEUE_EMPTY,))

        return list(messages[: msg_count.value])

    # ------------------------------------------------------------------ #
    # Transmitting messages
    # ------------------------------------------------------------------ #

    def transmit_message(
        self,
        handle: int,
        can_id: int,
        data: bytes,
        flags: int = 0,
    ):
        """Transmit a single CAN message.

        Args:
            handle: Channel handle.
            can_id: CAN message identifier.
            data: Payload bytes (max 8 for classic CAN, 64 for CAN FD).
            flags: TX flags bitmask (e.g. DSCAN_MSG_TX_FLAG_XTD).
        """
        msg = DSSCanMessage()
        msg.ulIdentifier = can_id
        msg.usFlags = flags
        msg.ucDlc = len(data)
        for i, b in enumerate(data):
            msg.abData[i] = b

        count = ctypes.c_uint32(1)
        err = self._dll.DSCAN_TransmitMessages(handle, ctypes.byref(count), ctypes.byref(msg))
        self._check("DSCAN_TransmitMessages", err)

    def transmit_messages(self, handle: int, messages: list[DSSCanMessage]):
        """Transmit multiple CAN messages at once."""
        count = ctypes.c_uint32(len(messages))
        msg_array = (DSSCanMessage * len(messages))(*messages)
        err = self._dll.DSCAN_TransmitMessages(handle, ctypes.byref(count), msg_array)
        self._check("DSCAN_TransmitMessages", err)

    # ------------------------------------------------------------------ #
    # Queue management
    # ------------------------------------------------------------------ #

    def flush_receive_queue(self, handle: int):
        """Clear all messages from the receive queue."""
        err = self._dll.DSCAN_FlushReceiveQueue(handle)
        self._check("DSCAN_FlushReceiveQueue", err)

    def flush_transmit_queue(self, handle: int):
        """Clear all messages from the transmit queue."""
        err = self._dll.DSCAN_FlushTransmitQueue(handle)
        self._check("DSCAN_FlushTransmitQueue", err)

    # ------------------------------------------------------------------ #
    # Timing
    # ------------------------------------------------------------------ #

    def get_hardware_time(self, handle: int) -> float:
        """Get the current hardware timestamp in seconds."""
        time_val = ctypes.c_double(0.0)
        err = self._dll.DSCAN_GetHardwareTime(handle, ctypes.byref(time_val))
        self._check("DSCAN_GetHardwareTime", err)
        return time_val.value
