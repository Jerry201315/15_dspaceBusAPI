"""
Example 01: Discover available CAN channels on a dSPACE SCALEXIO system.

This is the simplest starting point — it connects to the dSPACE platform
and lists all CAN channels without sending or receiving any data.

Run on Win11 machine with dSPACE Bus API installed and SCALEXIO connected:
    python examples/01_discover_channels.py --ip 10.0.0.1

If no --ip is given, it discovers PC-based CAN interfaces (DCI-CAN2, Vector, etc.)
"""

import argparse
import sys

# Add project root to path so we can import dspace_can
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

from dspace_can import DsCanApi
from dspace_can.constants import (
    DSCAN_CHANNEL_CAPABILITY_FD,
    DSCAN_CHANNEL_CAPABILITY_BUS_STATISTICS,
)


def main():
    parser = argparse.ArgumentParser(description="Discover dSPACE CAN channels")
    parser.add_argument(
        "--ip",
        type=str,
        default=None,
        help="IPv4 address of the dSPACE platform (e.g. SCALEXIO). "
             "Omit to discover PC-based interfaces only.",
    )
    parser.add_argument(
        "--dll",
        type=str,
        default=None,
        help="Explicit path to DSBusApiCan.dll (optional).",
    )
    args = parser.parse_args()

    print("=== dSPACE CAN Channel Discovery ===\n")

    # Load the API
    api = DsCanApi(dll_path=args.dll)
    print("[OK] DSBusApiCan.dll loaded successfully.\n")

    # Discover channels
    target = f"SCALEXIO at {args.ip}" if args.ip else "PC-based interfaces"
    print(f"Searching for CAN channels on {target}...\n")

    channels = api.get_available_channels(ip_address=args.ip)

    if not channels:
        print("No CAN channels found.")
        print("  - Check that the dSPACE hardware is powered on and connected.")
        if args.ip:
            print(f"  - Verify the IP address {args.ip} is reachable (ping it).")
        print("  - Ensure dSPACE Bus API 2025-B is installed.")
        return

    print(f"Found {len(channels)} CAN channel(s):\n")
    print(f"{'#':<4} {'Vendor':<20} {'Interface':<35} {'Channel':<15} {'Serial':<20} {'Capabilities'}")
    print("-" * 120)

    for i, ch in enumerate(channels):
        vendor = ch.szVendorName.decode()
        iface = ch.szInterfaceName.decode()
        chan_id = ch.szChannelIdentifier.decode()
        serial = ch.szInterfaceSerialNumber.decode()
        caps = ch.ulChannelCapabilities

        cap_flags = []
        if caps & DSCAN_CHANNEL_CAPABILITY_FD:
            cap_flags.append("CAN-FD")
        if caps & DSCAN_CHANNEL_CAPABILITY_BUS_STATISTICS:
            cap_flags.append("BusStat")
        cap_str = ", ".join(cap_flags) if cap_flags else f"0x{caps:08X}"

        print(f"{i:<4} {vendor:<20} {iface:<35} {chan_id:<15} {serial:<20} {cap_str}")

    print(f"\nDone. Use these channel details in the next examples.")


if __name__ == "__main__":
    main()
