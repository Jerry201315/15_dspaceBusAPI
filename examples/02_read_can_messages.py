import argparse
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

from dspace_can import DsCanApi
from dspace_can.constants import (
    DSCAN_BAUD_500K,
    DSCAN_IDENTIFIER_TYPE_XTD,
    DSCAN_MESSAGE_TYPE_REMOTE,
    DSCAN_RX_MESSAGE_FLAG_TX_ACKNOWLEDGE,
    DSCAN_RX_MESSAGE_FLAG_FD,
)

def format_can_message(msg) -> str:
    """Format a CAN message for display using dSPACE Bus API structures."""
    is_xtd = (msg.tCanIdentifierType == DSCAN_IDENTIFIER_TYPE_XTD)
    is_rtr = (msg.tMessageType == DSCAN_MESSAGE_TYPE_REMOTE)
    is_fd = bool(msg.ulFlags & DSCAN_RX_MESSAGE_FLAG_FD)
    is_tx_ack = bool(msg.ulFlags & DSCAN_RX_MESSAGE_FLAG_TX_ACKNOWLEDGE)

    id_fmt = f"0x{msg.ulCanIdentifier:08X}" if is_xtd else f"0x{msg.ulCanIdentifier:03X}"
    flags_parts = []
    if is_xtd: flags_parts.append("XTD")
    if is_fd: flags_parts.append("FD")
    if is_rtr: flags_parts.append("RTR")
    if is_tx_ack: flags_parts.append("TX")
    flags_str = " ".join(flags_parts) if flags_parts else "STD"

    data_len = msg.usDLC
    hex_data = " ".join(f"{msg.ucData[i]:02X}" for i in range(min(data_len, 64)))

    return f"[Tick: {msg.ui64Timestamp:12d}] ID={id_fmt}  DLC={msg.usDLC:<2d}  Data=[{hex_data}]  ({flags_str})"

def main():
    parser = argparse.ArgumentParser(description="Read CAN messages from dSPACE hardware")
    parser.add_argument("--ip", type=str, required=True, help="SCALEXIO IPv4 address")
    parser.add_argument("--channel", type=int, default=0, help="Channel index to use (default: 0)")
    parser.add_argument("--baudrate", type=int, default=DSCAN_BAUD_500K, help="CAN baud rate in bit/s")
    parser.add_argument("--dll", type=str, default=None, help="Path to DSBusApiCan.dll")
    args = parser.parse_args()

    print("=== dSPACE CAN Message Reader ===\n")
    api = DsCanApi(dll_path=args.dll)
    print("[OK] DLL loaded.\n")

    print(f"Discovering CAN channels at {args.ip}...")
    channels = api.get_available_channels(ip_address=args.ip)

    #print("dspace channels", channels)
    if not channels:
        print("ERROR: No CAN channels found. Check hardware and IP.")
        sys.exit(1)

    if args.channel >= len(channels):
        print(f"ERROR: Channel index {args.channel} out of range (0-{len(channels)-1}).")
        sys.exit(1)

    target = channels[args.channel]
    print("target channel",target)
    print(f"\n---> USING CHANNEL {args.channel}: {target.szChannelIdentifier.decode()} <---")

    handle = api.register_channel(target)
    print(f"[OK] Channel registered (handle={handle}).")

    try:
        access = api.init_channel(handle)
        print(f"[OK] Channel initialized (access_permission={access}).")

        try:
            current_baud = api.get_baudrate(handle)
            print(f"[OK] Hardware baud rate: {current_baud} bit/s.")
        except Exception as e:
            print(f"[WARN] Could not read baud rate: {e}")

        api.set_acceptance(handle, 0, 0, 0, 0)
        print("[OK] Acceptance filter opened!")

        try:
            api.set_transmit_acknowledge(handle, True)
            print("[OK] Transmit Acknowledge enabled.")
        except Exception as e:
            print(f"[WARN] Could not enable Tx Ack: {e}")

        api.activate_channel(handle)
        print("[OK] Channel activated!\n")

        # --- THE TX LOOPBACK TEST ---
        #print("Sending test message (ID: 0x123) from Python...")
        #api.transmit_message(handle, can_id=0x123, data=b'\xAA\xBB\xCC', flags=0)

        print("Listening for CAN messages... (Press Ctrl+C to stop)\n")
        msg_total = 0
        while True:
            messages = api.read_messages(handle)
            for msg in messages:
                print(format_can_message(msg))
                msg_total += 1

            if not messages:
                time.sleep(0.01)

    except KeyboardInterrupt:
        print("\n\n--- Interrupted by user ---")
    finally:
        print(f"Received {msg_total} message(s).")
        api.deactivate_channel(handle)
        api.unregister_channel(handle)
        print("[OK] Channel deactivated and unregistered.")

if __name__ == "__main__":
    main()