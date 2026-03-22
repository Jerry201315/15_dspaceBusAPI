import argparse
import ctypes
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

from dspace_can import DsCanApi
from dspace_can.constants import (
    DSCAN_BAUD_500K,
    DSCAN_BAUD_FD_2M,
    DSCAN_IDENTIFIER_TYPE_XTD,
    DSCAN_MESSAGE_TYPE_REMOTE,
    DSCAN_MESSAGE_TYPE_DATA,
    DSCAN_RX_MESSAGE_FLAG_TX_ACKNOWLEDGE,
    DSCAN_RX_MESSAGE_FLAG_FD,
    DSCAN_RX_MESSAGE_FLAG_FD_BAUDRATE_SWITCH,
    DSCAN_RX_MESSAGE_FLAG_RX_BUFFER_OVERRUN,
    DSCAN_RX_MESSAGE_FLAG_HW_RX_BUFFER_OVERRUN,
    dlc_to_byte_count,
)

# Windows constants for WaitForSingleObject
WAIT_OBJECT_0 = 0x00000000
WAIT_TIMEOUT = 0x00000102
INFINITE = 0xFFFFFFFF

OVERRUN_FLAGS = DSCAN_RX_MESSAGE_FLAG_RX_BUFFER_OVERRUN | DSCAN_RX_MESSAGE_FLAG_HW_RX_BUFFER_OVERRUN


def format_can_message(msg) -> str:
    """Format a CAN message for display."""
    is_xtd = (msg.tCanIdentifierType == DSCAN_IDENTIFIER_TYPE_XTD)
    is_rtr = (msg.tMessageType == DSCAN_MESSAGE_TYPE_REMOTE)
    is_fd = bool(msg.ulFlags & DSCAN_RX_MESSAGE_FLAG_FD)
    is_brs = bool(msg.ulFlags & DSCAN_RX_MESSAGE_FLAG_FD_BAUDRATE_SWITCH)
    is_tx_ack = bool(msg.ulFlags & DSCAN_RX_MESSAGE_FLAG_TX_ACKNOWLEDGE)

    id_fmt = f"0x{msg.ulCanIdentifier:08X}" if is_xtd else f"0x{msg.ulCanIdentifier:03X}"
    flags_parts = []
    if is_xtd: flags_parts.append("XTD")
    if is_fd: flags_parts.append("FD")
    if is_brs: flags_parts.append("BRS")
    if is_rtr: flags_parts.append("RTR")
    if is_tx_ack: flags_parts.append("TX")
    flags_str = " ".join(flags_parts) if flags_parts else "STD"

    # CAN FD: DLC 9-15 map to 12,16,20,24,32,48,64 bytes
    # Classic CAN: DLC = byte count (0-8)
    data_len = dlc_to_byte_count(msg.usDLC) if is_fd else min(msg.usDLC, 8)
    hex_data = " ".join(f"{msg.ucData[i]:02X}" for i in range(data_len))

    return f"[{msg.ui64Timestamp:12d}] ID={id_fmt}  DLC={msg.usDLC:<2d}  [{hex_data}]  ({flags_str})"


def main():
    parser = argparse.ArgumentParser(description="Read CAN messages from dSPACE hardware")
    parser.add_argument("--ip", type=str, required=True, help="SCALEXIO IPv4 address")
    parser.add_argument("--channel", type=int, default=0, help="Channel index to use (default: 0)")
    parser.add_argument("--baudrate", type=int, default=DSCAN_BAUD_500K, help="CAN baud rate in bit/s")
    parser.add_argument("--dll", type=str, default=None, help="Path to DSBusApiCan.dll")
    parser.add_argument("--fd", action="store_true", help="Enable CAN FD support (up to 64-byte frames)")
    parser.add_argument("--poll", action="store_true", help="Use polling instead of event-based waiting")
    parser.add_argument("--batch", type=int, default=512, help="Read buffer size (default: 512)")
    args = parser.parse_args()

    print("=== dSPACE CAN Message Reader ===\n")
    api = DsCanApi(dll_path=args.dll)
    print("[OK] DLL loaded.\n")

    print(f"Discovering CAN channels at {args.ip}...")
    channels = api.get_available_channels(ip_address=args.ip)

    if not channels:
        print("ERROR: No CAN channels found. Check hardware and IP.")
        sys.exit(1)

    if args.channel >= len(channels):
        print(f"ERROR: Channel index {args.channel} out of range (0-{len(channels)-1}).")
        sys.exit(1)

    target = channels[args.channel]
    print(f"\n---> USING CHANNEL {args.channel}: {target.szChannelIdentifier.decode()} <---")

    handle = api.register_channel(target)
    print(f"[OK] Channel registered (handle={handle}).")

    event_handle = None
    kernel32 = None

    try:
        access = api.init_channel(handle, rx_queue_size=32768, fd=args.fd)
        fd_str = ", CAN-FD" if args.fd else ""
        print(f"[OK] Channel initialized (access_permission={access}, rx_queue=32768{fd_str}).")

        try:
            current_baud = api.get_baudrate(handle)
            print(f"[OK] Hardware baud rate: {current_baud} bit/s.")
        except Exception as e:
            print(f"[WARN] Could not read baud rate: {e}")

        api.set_acceptance(handle, 0, 0, 0, 0)
        print("[OK] Acceptance filter opened.")

        try:
            api.set_transmit_acknowledge(handle, True)
            print("[OK] Transmit Acknowledge enabled.")
        except Exception as e:
            print(f"[WARN] Could not enable Tx Ack: {e}")

        # Set up event notification for blocking reads (instead of polling)
        if not args.poll:
            try:
                kernel32 = ctypes.windll.kernel32
                event_handle = kernel32.CreateEventW(None, False, False, None)
                api.set_event_notification(handle, event_handle, 1)
                print("[OK] Event notification enabled (blocking mode).")
            except Exception as e:
                print(f"[WARN] Event notification failed, falling back to polling: {e}")
                event_handle = None

        api.activate_channel(handle)
        print("[OK] Channel activated!\n")

        mode = "polling" if event_handle is None else "event-driven"
        print(f"Listening for CAN messages ({mode}, batch={args.batch})...")
        print("Press Ctrl+C to stop.\n")

        msg_total = 0
        overrun_count = 0
        t_start = time.perf_counter()

        while True:
            # Wait for messages: event-driven or polling
            if event_handle is not None:
                kernel32.WaitForSingleObject(event_handle, 100)  # 100ms timeout for Ctrl+C

            messages = api.read_messages(handle, max_messages=args.batch)

            for msg in messages:
                if msg.tMessageType != DSCAN_MESSAGE_TYPE_DATA:
                    continue
                if msg.ulFlags & OVERRUN_FLAGS:
                    overrun_count += 1
                    if overrun_count <= 5:
                        print(f"  *** BUFFER OVERRUN detected (#{overrun_count}) ***")
                print(format_can_message(msg))
                msg_total += 1

            if not messages and event_handle is None:
                time.sleep(0.001)  # 1ms poll (was 10ms)

    except KeyboardInterrupt:
        print("\n\n--- Interrupted ---")
    finally:
        elapsed = time.perf_counter() - t_start if 't_start' in dir() else 0
        print(f"\nReceived {msg_total} message(s) in {elapsed:.1f}s", end="")
        if elapsed > 0 and msg_total > 0:
            print(f" ({msg_total / elapsed:.0f} msg/s)", end="")
        print()
        if overrun_count:
            print(f"WARNING: {overrun_count} buffer overrun(s) detected — messages were lost!")

        if event_handle is not None:
            try:
                api.clear_event_notification(handle)
            except Exception:
                pass
            kernel32.CloseHandle(event_handle)

        api.deactivate_channel(handle)
        api.unregister_channel(handle)
        print("[OK] Channel deactivated and unregistered.")

if __name__ == "__main__":
    main()
