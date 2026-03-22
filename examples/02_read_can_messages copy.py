"""
Example 02: Connect to a CAN channel and read messages.

This example performs the full lifecycle:
  1. Discover CAN channels on the SCALEXIO
  2. Register & initialize the first channel found
  3. Activate the channel
  4. Continuously read and print received CAN messages
  5. Deactivate & unregister on Ctrl+C

Run on Win11 machine:
    python examples/02_read_can_messages.py --ip 10.0.0.1
    python examples/02_read_can_messages.py --ip 10.0.0.1 --channel 0 --baudrate 500000
"""

import argparse
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

from dspace_can import DsCanApi, api
from dspace_can.constants import DSCAN_BAUD_500K


def format_can_message(msg) -> str:
    """Format a CAN message for display using dSPACE Bus API structures."""
    
    # 1. Determine Identifier Type (1 = STD, 2 = XTD)
    is_xtd = (msg.tCanIdentifierType == 2)
    
    # 2. Determine Message Type (2 = REMOTE)
    is_rtr = (msg.tMessageType == 2)
    
    # 3. Read specific dSPACE Rx Flags
    # 0x0100 = CAN FD Message | 0x0001 = Tx Acknowledge (Loopback)
    is_fd = bool(msg.ulFlags & 0x0100)
    is_tx_ack = bool(msg.ulFlags & 0x0001)

    # Format the ID string
    id_fmt = f"0x{msg.ulCanIdentifier:08X}" if is_xtd else f"0x{msg.ulCanIdentifier:03X}"
    
    flags_parts = []
    if is_xtd:
        flags_parts.append("XTD")
    if is_fd:
        flags_parts.append("FD")
    if is_rtr:
        flags_parts.append("RTR")
    if is_tx_ack:
        flags_parts.append("TX")
    flags_str = " ".join(flags_parts) if flags_parts else "STD"

    # 4. Extract Data Payload
    data_len = msg.usDLC
    # The payload array in dSPACE is named ucData
    hex_data = " ".join(f"{msg.ucData[i]:02X}" for i in range(min(data_len, 64)))

    # 5. Extract Timestamp (Raw hardware ticks)
    timestamp_ticks = msg.ui64Timestamp

    return (
        f"[Tick: {timestamp_ticks:12d}] "
        f"ID={id_fmt}  DLC={msg.usDLC:<2d}  "
        f"Data=[{hex_data}]  "
        f"({flags_str})"
    )


def main():
    parser = argparse.ArgumentParser(description="Read CAN messages from dSPACE hardware")
    parser.add_argument("--ip", type=str, required=True, help="SCALEXIO IPv4 address")
    parser.add_argument("--channel", type=int, default=0, help="Channel index to use (default: 0)")
    parser.add_argument("--baudrate", type=int, default=DSCAN_BAUD_500K, help="CAN baud rate in bit/s (default: 500000)")
    parser.add_argument("--dll", type=str, default=None, help="Path to DSBusApiCan.dll")
    parser.add_argument("--duration", type=float, default=0, help="Duration in seconds (0 = run until Ctrl+C)")
    args = parser.parse_args()

    print("=== dSPACE CAN Message Reader ===\n")

    api = DsCanApi(dll_path=args.dll)
    print("[OK] DLL loaded.\n")

    # Step 1: Discover
    print(f"Discovering CAN channels at {args.ip}...")
    channels = api.get_available_channels(ip_address=args.ip)
    if not channels:
        print("ERROR: No CAN channels found. Check hardware and IP.")
        sys.exit(1)

    print(f"Found {len(channels)} channel(s).")
    if args.channel >= len(channels):
        print(f"ERROR: Channel index {args.channel} out of range (0-{len(channels)-1}).")
        sys.exit(1)

    target = channels[args.channel]
    print(f"Using: {target}\n")

    # Step 2: Register
    handle = api.register_channel(target)
    print(f"[OK] Channel registered (handle={handle}).")

    try:
        # Step 3: Initialize
        access = api.init_channel(handle)
        print(f"[OK] Channel initialized (access_permission={access}).")

        if not access:
            print("WARNING: No access permission. Another client may hold exclusive access.")

        # Step 4: Set baud rate (skip if fixed configuration)
        try:
            current_baud = api.get_baudrate(handle)
            print(f"[OK] Hardware is currently configured to {current_baud} bit/s.")
            
            if current_baud != args.baudrate:
                print(f"[WARN] Hardware is running at {current_baud}, but you requested {args.baudrate}!")
        except Exception as e:
            print(f"[WARN] Could not read baud rate: {e}")


        # Open the filter to receive everything
        api.set_acceptance(handle, 0, 0, 0, 0)
        print("[OK] Acceptance filter opened! Allowing all CAN IDs.")


        try:
            api.set_transmit_acknowledge(handle, True)
            print("[OK] Transmit Acknowledge enabled.")
        except Exception as e:
            print(f"[WARN] Could not enable Tx Acknowledge: {e}")

        # Step 5: Activate
        api.activate_channel(handle)
        print("[OK] Channel activated — listening for CAN messages...\n")

        # Step 6: Read loop
        msg_total = 0
        start_time = time.time()

        try:
            while True:
                messages = api.read_messages(handle)
                for msg in messages:
                    print(format_can_message(msg))
                    msg_total += 1

                if not messages:
                    time.sleep(0.01)  # 10ms poll interval

                if args.duration > 0 and (time.time() - start_time) >= args.duration:
                    break

        except KeyboardInterrupt:
            print("\n\n--- Interrupted by user ---")

        elapsed = time.time() - start_time
        print(f"\nReceived {msg_total} message(s) in {elapsed:.1f}s.")

        # Step 7: Deactivate
        api.deactivate_channel(handle)
        print("[OK] Channel deactivated.")

    finally:
        # Step 8: Unregister (always, even on error)
        api.unregister_channel(handle)
        print("[OK] Channel unregistered.")

    print("\nDone.")


if __name__ == "__main__":
    main()
