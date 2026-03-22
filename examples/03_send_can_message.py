"""
Example 03: Send a CAN message via dSPACE hardware.

Connects to the first CAN channel and transmits a single CAN message,
then reads back any responses for a short period.

Run on Win11 machine:
    python examples/03_send_can_message.py --ip 10.0.0.1 --id 0x100 --data "DE AD BE EF"
"""

import argparse
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

from dspace_can import DsCanApi
from dspace_can.constants import DSCAN_BAUD_500K


def parse_hex_data(hex_string: str) -> bytes:
    """Parse a hex string like 'DE AD BE EF' or 'DEADBEEF' into bytes."""
    cleaned = hex_string.replace(" ", "").replace("0x", "").replace(",", "")
    return bytes.fromhex(cleaned)


def main():
    parser = argparse.ArgumentParser(description="Send a CAN message via dSPACE hardware")
    parser.add_argument("--ip", type=str, required=True, help="SCALEXIO IPv4 address")
    parser.add_argument("--id", type=str, default="0x100", help="CAN message ID (hex, e.g. 0x100)")
    parser.add_argument("--data", type=str, default="01 02 03 04 05 06 07 08", help="Payload as hex bytes")
    parser.add_argument("--extended", action="store_true", help="Use 29-bit extended identifier")
    parser.add_argument("--channel", type=int, default=0, help="Channel index")
    parser.add_argument("--baudrate", type=int, default=DSCAN_BAUD_500K, help="Baud rate")
    parser.add_argument("--dll", type=str, default=None)
    args = parser.parse_args()

    can_id = int(args.id, 16) if args.id.startswith("0x") else int(args.id)
    payload = parse_hex_data(args.data)

    print("=== dSPACE CAN Message Transmitter ===\n")

    api = DsCanApi(dll_path=args.dll)

    channels = api.get_available_channels(ip_address=args.ip)
    if not channels:
        print("No CAN channels found.")
        sys.exit(1)

    target = channels[args.channel]
    print(f"Using: {target}\n")

    handle = api.register_channel(target)

    try:
        api.init_channel(handle)
        try:
            api.set_baudrate(handle, baudrate=args.baudrate)
        except Exception as e:
            print(f"[WARN] Could not set baud rate: {e}")

        api.activate_channel(handle)

        # Transmit
        hex_str = " ".join(f"{b:02X}" for b in payload)
        id_str = f"0x{can_id:08X}" if args.extended else f"0x{can_id:03X}"
        print(f"Sending: ID={id_str} Data=[{hex_str}]")

        api.transmit_message(handle, can_id, payload, extended=args.extended)
        print("[OK] Message sent.\n")

        # Listen briefly for responses
        print("Listening for responses (2 seconds)...")
        end_time = time.time() + 2.0
        rx_count = 0
        while time.time() < end_time:
            msgs = api.read_messages(handle)
            for msg in msgs:
                print(f"  RX: {msg}")
                rx_count += 1
            if not msgs:
                time.sleep(0.01)

        print(f"\nReceived {rx_count} response(s).")

        api.deactivate_channel(handle)
    finally:
        api.unregister_channel(handle)

    print("Done.")


if __name__ == "__main__":
    main()
