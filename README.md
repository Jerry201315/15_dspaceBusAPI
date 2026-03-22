# dSPACE Bus API — CAN Message Read/Write (Python)

Python wrapper (ctypes) for the **dSPACE Bus API** CAN functions, enabling CAN and CAN FD message reading and writing on **dSPACE SCALEXIO** hardware from Python.

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

# 3. Read CAN FD messages
python examples/02_read_can_messages.py --ip <SCALEXIO_IP> --fd

# 4. Send a classic CAN message
python examples/03_send_can_message.py --ip <SCALEXIO_IP> --id 0x100 --data "DE AD BE EF"

# 5. Send a CAN FD message with baud rate switching
python examples/03_send_can_message.py --ip <SCALEXIO_IP> --id 0x100 --fd --brs --data "01 02 03 04 05 06 07 08 09 0A 0B 0C"
```

## CAN vs CAN FD

| Feature | Classic CAN | CAN FD |
|---------|-------------|--------|
| Max payload | 8 bytes | 64 bytes |
| DLC range | 0–8 (= byte count) | 0–15 (non-linear: DLC 9→12B, 10→16B, ..., 15→64B) |
| Baud rate | Up to 1 Mbit/s | Arbitration up to 1 Mbit/s + data phase up to 8 Mbit/s (BRS) |
| Init flag | `api.init_channel(handle)` | `api.init_channel(handle, fd=True)` |
| TX flag | — | `api.transmit_message(..., fd=True, brs=True)` |
| CLI flag | (default) | `--fd` (and `--brs` for send) |

To receive or send CAN FD frames, the channel **must** be initialized with `fd=True`. Classic CAN frames are always supported regardless of this flag.

## Project Structure

```
dspace_can/
  __init__.py          # Package entry point
  api.py               # DsCanApi class — main wrapper around DSBusApiCan.dll
  constants.py         # All CAN API constants, error codes, flag definitions, DLC conversion
  structures.py        # ctypes Structure definitions (DSSCanChannelInfo, DSSCanMessage, etc.)
examples/
  00_diagnose_connection.py    # Diagnose SCALEXIO connectivity and channel details
  01_discover_channels.py      # List available CAN channels
  02_read_can_messages.py      # Connect and read CAN/CAN FD messages
  03_send_can_message.py       # Send a CAN/CAN FD message and listen for responses
```

## API Usage

### Classic CAN

```python
from dspace_can import DsCanApi

api = DsCanApi()  # loads DLL from default dSPACE installation path

# Discover channels on SCALEXIO
channels = api.get_available_channels("192.168.0.10")

# Connect to the first channel
handle = api.register_channel(channels[0])
api.init_channel(handle)
api.activate_channel(handle)

# Read messages
messages = api.read_messages(handle)
for msg in messages:
    print(msg)

# Send a message
api.transmit_message(handle, can_id=0x100, data=b"\x01\x02\x03\x04")

# Cleanup
api.deactivate_channel(handle)
api.unregister_channel(handle)
```

### CAN FD

```python
# Initialize with CAN FD enabled
handle = api.register_channel(channels[0])
api.init_channel(handle, fd=True)
api.activate_channel(handle)

# Send a 32-byte CAN FD frame with baud rate switching
api.transmit_message(handle, can_id=0x200, data=bytes(range(32)), fd=True, brs=True)

# Read — FD frames are detected automatically via RX flags
messages = api.read_messages(handle)
```

## Performance & Reliability

The read example uses several optimizations to avoid message loss:

| Feature | Detail |
|---------|--------|
| **RX queue** | 32768 messages (max), ~8.5 seconds buffer at 500 kbit/s |
| **Single DLL call** | `ReadReceiveQueue` with pre-allocated buffer (no separate `GetQueueLevel` call) |
| **Event-driven** | Uses `DSCAN_SetEventNotification` + `WaitForSingleObject` instead of polling |
| **Polling fallback** | 1ms sleep (use `--poll` flag) |
| **Overrun detection** | Checks `RX_BUFFER_OVERRUN` flags and warns if messages were lost |
| **Throughput stats** | Shows msg/s rate and total count on exit |

```bash
# Event-driven mode (default, recommended)
python examples/02_read_can_messages.py --ip 192.168.0.10

# Polling mode with larger batch buffer
python examples/02_read_can_messages.py --ip 192.168.0.10 --poll --batch 1024
```

## CAN API Call Flow

```
DSCAN_GetAvailableChannelsCount  →  DSCAN_GetAvailableChannels
        ↓
DSCAN_RegisterChannel  →  DSCAN_InitChannel(fd=True/False)  →  DSCAN_SetBaudrate
        ↓
DSCAN_SetAcceptance  →  DSCAN_SetEventNotification (optional)
        ↓
DSCAN_ActivateChannel
        ↓
DSCAN_ReadReceiveQueue / DSCAN_TransmitMessages  (loop)
        ↓
DSCAN_DeactivateChannel  →  DSCAN_UnregisterChannel
```

## Development Notes

- **Test machine**: Win11 with SCALEXIO hardware at 192.168.0.10
- The wrapper uses only Python standard library (`ctypes`) — no pip dependencies
- All dSPACE API error codes are translated into Python exceptions (`DsCanError`)
- Struct definitions match the dSPACE Bus API Manual (Nov 2025) C headers exactly
