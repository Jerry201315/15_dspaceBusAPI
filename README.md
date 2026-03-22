# dSPACE Bus API — CAN Message Read/Write (Python)

Python wrapper (ctypes) for the **dSPACE Bus API** CAN functions, enabling CAN message reading and writing on **dSPACE SCALEXIO** hardware from Python.

## Target Environment

| Item | Detail |
|------|--------|
| **OS** | Windows 11 x64 |
| **Hardware** | dSPACE SCALEXIO with CAN interface |
| **dSPACE Software** | dSPACE Bus API 2025-B |
| **Python** | 3.10+ (no extra pip packages needed — uses ctypes) |

## Prerequisites

1. **dSPACE Bus API 2025-B** installed on the Win11 machine.
   The DLL is expected at:
   `%CommonProgramFiles%\dSPACE\dSPACE Bus API\2025-B\Deliverables\bin\DSBusApiCan.dll`

2. **SCALEXIO** hardware connected and reachable via Ethernet (know its IP address).

3. Copy these licensing files to your working directory (or next to the DLL):
   - `dSPACE.Common.LHInternal.dll`
   - `dSPACE.Common.RHFoundationNative.dll`
   - `DsBusAccessManager.xml`

## Quick Start

```bash
# 1. Discover available CAN channels
python examples/01_discover_channels.py --ip <SCALEXIO_IP>

# 2. Read CAN messages (Ctrl+C to stop)
python examples/02_read_can_messages.py --ip <SCALEXIO_IP>

# 3. Send a CAN message
python examples/03_send_can_message.py --ip <SCALEXIO_IP> --id 0x100 --data "DE AD BE EF"
```

## Project Structure

```
dspace_can/
  __init__.py          # Package entry point
  api.py               # DsCanApi class — main wrapper around DSBusApiCan.dll
  constants.py         # All CAN API constants, error codes, flag definitions
  structures.py        # ctypes Structure definitions (DSSCanChannelInfo, DSSCanMessage, etc.)
examples/
  01_discover_channels.py    # List available CAN channels
  02_read_can_messages.py    # Connect and read CAN messages
  03_send_can_message.py     # Send a CAN message and listen for responses
```

## API Usage

```python
from dspace_can import DsCanApi

api = DsCanApi()  # loads DLL from default dSPACE installation path

# Discover channels on SCALEXIO at 10.0.0.1
channels = api.get_available_channels("10.0.0.1")

# Connect to the first channel
handle = api.register_channel(channels[0])
api.init_channel(handle)
api.activate_channel(handle)

# Read messages
messages = api.read_messages(handle)
for msg in messages:
    print(msg)  # CAN ID=0x123 [8] DE AD BE EF 01 02 03 04 @1.234567s

# Send a message
api.transmit_message(handle, can_id=0x100, data=b"\x01\x02\x03\x04")

# Cleanup
api.deactivate_channel(handle)
api.unregister_channel(handle)
```

## CAN API Call Flow

```
DSCAN_GetAvailableChannelsCount  →  DSCAN_GetAvailableChannels
        ↓
DSCAN_RegisterChannel  →  DSCAN_InitChannel  →  DSCAN_SetBaudrate
        ↓
DSCAN_ActivateChannel
        ↓
DSCAN_ReadReceiveQueue / DSCAN_TransmitMessages  (loop)
        ↓
DSCAN_DeactivateChannel  →  DSCAN_UnregisterChannel
```

## Development Notes


- **Test machine**: Win11 with SCALEXIO hardware
- The wrapper uses only Python standard library (`ctypes`) — no pip dependencies
- All dSPACE API error codes are translated into Python exceptions (`DsCanError`)
