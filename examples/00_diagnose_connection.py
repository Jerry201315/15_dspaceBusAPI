"""
Diagnostic script: troubleshoot dSPACE SCALEXIO CAN channel discovery.

Checks:
  1. DLL loading
  2. Supported vendors
  3. Channels with NO IP filter (PC-based + virtual)
  4. Channels WITH IP filter (SCALEXIO)
  5. Network reachability (ping)

Run:
    python examples/00_diagnose_connection.py --ip 192.168.0.10
"""

import argparse
import ctypes
import os
import subprocess
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

from dspace_can.api import DsCanApi, DsCanError, _DEFAULT_DLL_PATHS
from dspace_can.structures import DSSCanChannelInfo, DSSCanChannelsSearchAttribute
from dspace_can.constants import (
    DSCAN_SEARCH_ATTRIBUTE_TYPE_IP_V4_ADDRESS,
    DSCAN_MAX_TEXT_LENGTH,
    DSCAN_ERR_NO_ERROR,
)


def check_files():
    """Check that required support files are present in cwd."""
    print("--- Step 0: Check required files in working directory ---")
    cwd = os.getcwd()
    print(f"  Working directory: {cwd}")

    required = [
        "DsBusAccessManager.xml",
        "dSPACE.Common.LHInternal.dll",
        "dSPACE.Common.RHFoundationNative.dll",
    ]
    all_ok = True
    for f in required:
        path = os.path.join(cwd, f)
        exists = os.path.isfile(path)
        size = os.path.getsize(path) if exists else 0
        status = f"OK ({size:,} bytes)" if exists else "MISSING"
        print(f"  {f}: {status}")
        if not exists:
            all_ok = False

    if not all_ok:
        print("  WARNING: Missing files! Copy them from:")
        print(r"    %CommonProgramFiles%\dSPACE\dSPACE Bus API\2025-B\Deliverables\bin")
    print()
    return all_ok


def check_dll():
    """Try loading the DLL and print the path used."""
    print("--- Step 1: Load DSBusApiCan.dll ---")
    for p in _DEFAULT_DLL_PATHS:
        expanded = os.path.expandvars(p) if "%" in p else p
        exists = os.path.isfile(expanded)
        print(f"  Checking: {expanded} -> {'EXISTS' if exists else 'not found'}")

    try:
        api = DsCanApi()
        print("  [OK] DLL loaded successfully.")
        print()
        return api
    except Exception as e:
        print(f"  [FAIL] {e}")
        print()
        return None


def check_vendors(api):
    """List supported vendors."""
    print("--- Step 2: Supported vendors ---")
    try:
        count = ctypes.c_uint32(0)
        err = api._dll.DSCAN_GetSupportedVendorsCount(ctypes.byref(count))
        print(f"  Vendor count returned: {count.value} (error code: {err})")
    except Exception as e:
        print(f"  Could not query vendors: {e}")
    print()


def check_channels_no_filter(api):
    """Discover channels WITHOUT IP filter — shows PC-based + virtual."""
    print("--- Step 3: Discover channels (NO IP filter) ---")
    try:
        channels = api.get_available_channels(ip_address=None)
        print(f"  Found {len(channels)} channel(s):")
        for i, ch in enumerate(channels):
            print(f"    [{i}] vendor={ch.szVendorName.decode()!r} "
                  f"iface={ch.szInterfaceName.decode()!r} "
                  f"channel={ch.szChannelIdentifier.decode()!r} "
                  f"serial={ch.szInterfaceSerialNumber.decode()!r} "
                  f"caps=0x{ch.ulChannelCapabilities:08X}")
    except DsCanError as e:
        print(f"  [ERROR] {e}")
    print()


def check_channels_with_ip(api, ip):
    """Discover channels WITH IP filter — should show SCALEXIO channels."""
    print(f"--- Step 4: Discover channels (IP filter: {ip}) ---")
    try:
        channels = api.get_available_channels(ip_address=ip)
        print(f"  Found {len(channels)} channel(s):")
        for i, ch in enumerate(channels):
            print(f"    [{i}] vendor={ch.szVendorName.decode()!r} "
                  f"iface={ch.szInterfaceName.decode()!r} "
                  f"channel={ch.szChannelIdentifier.decode()!r} "
                  f"serial={ch.szInterfaceSerialNumber.decode()!r} "
                  f"caps=0x{ch.ulChannelCapabilities:08X}")
        if not channels:
            print("  No SCALEXIO channels found. Possible causes:")
            print("    - SCALEXIO not powered on or not connected via Ethernet")
            print("    - IP address incorrect (check in dSPACE ConfigurationDesk)")
            print("    - No real-time application deployed to SCALEXIO")
            print("    - CAN I/O board not configured in the platform")
            print("    - Firewall blocking dSPACE discovery protocol")
    except DsCanError as e:
        print(f"  [ERROR] {e}")
    print()

    # Also try with raw API call to see the actual error code
    print(f"  Raw API call test (IP={ip}):")
    try:
        attr = DSSCanChannelsSearchAttribute()
        attr.tSearchAttributeType = DSCAN_SEARCH_ATTRIBUTE_TYPE_IP_V4_ADDRESS
        attr.szSearchAttribute = ip.encode("utf-8")
        count = ctypes.c_uint32(0)

        err = api._dll.DSCAN_GetAvailableChannelsCount(
            ctypes.byref(count), 1, ctypes.byref(attr)
        )
        err_text = ""
        if err != DSCAN_ERR_NO_ERROR:
            buf = (ctypes.c_char * DSCAN_MAX_TEXT_LENGTH)()
            api._dll.DSCAN_GetErrorText(err, buf)
            err_text = buf.value.decode("utf-8", errors="replace")

        print(f"    DSCAN_GetAvailableChannelsCount -> error_code={err}, count={count.value}")
        if err_text:
            print(f"    Error text: {err_text}")
    except Exception as e:
        print(f"    Exception: {e}")
    print()


def check_ping(ip):
    """Ping the SCALEXIO to verify network connectivity."""
    print(f"--- Step 5: Ping {ip} ---")
    try:
        result = subprocess.run(
            ["ping", "-n", "2", "-w", "1000", ip],
            capture_output=True, text=True, timeout=10
        )
        print(result.stdout)
        if result.returncode == 0:
            print("  [OK] Host is reachable.")
        else:
            print("  [FAIL] Host unreachable. Check network cable and IP config.")
    except Exception as e:
        print(f"  Ping failed: {e}")
    print()


def check_xml_content():
    """Show DsBusAccessManager.xml content for inspection."""
    print("--- Step 6: DsBusAccessManager.xml content ---")
    xml_path = os.path.join(os.getcwd(), "DsBusAccessManager.xml")
    if os.path.isfile(xml_path):
        with open(xml_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
        # Show first 2000 chars
        print(content[:2000])
        if len(content) > 2000:
            print(f"  ... ({len(content)} total chars)")
    else:
        print("  File not found in working directory.")
    print()


def main():
    parser = argparse.ArgumentParser(description="Diagnose dSPACE CAN connection")
    parser.add_argument("--ip", type=str, default="192.168.0.10",
                        help="SCALEXIO IPv4 address")
    parser.add_argument("--dll", type=str, default=None)
    args = parser.parse_args()

    print("=" * 60)
    print("  dSPACE CAN Bus API — Connection Diagnostics")
    print("=" * 60)
    print()

    check_files()
    api = check_dll()
    if not api:
        print("Cannot proceed without DLL. Exiting.")
        sys.exit(1)

    check_vendors(api)
    check_channels_no_filter(api)
    check_channels_with_ip(api, args.ip)
    check_ping(args.ip)
    check_xml_content()

    print("=" * 60)
    print("  Diagnostics complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
