"""
CAN Streaming Manager — reads CAN messages from dSPACE SCALEXIO via Bus API
and publishes to Google Cloud Pub/Sub.

Designed to run alongside ControlDesk as a secondary client (no interference).
Supports multiple CAN channels, CAN FD, and conditional recording based on
12V state (only saves to BigQuery when voltage is ON).
"""

import ctypes
import json
import threading
import time
from datetime import datetime, timezone

# Bus API wrapper (local package)
try:
    from dspace_can import DsCanApi
    from dspace_can.constants import (
        DSCAN_IDENTIFIER_TYPE_XTD,
        DSCAN_MESSAGE_TYPE_DATA,
        DSCAN_RX_MESSAGE_FLAG_FD,
        DSCAN_RX_MESSAGE_FLAG_FD_BAUDRATE_SWITCH,
        DSCAN_RX_MESSAGE_FLAG_TX_ACKNOWLEDGE,
        DSCAN_RX_MESSAGE_FLAG_RX_BUFFER_OVERRUN,
        DSCAN_RX_MESSAGE_FLAG_HW_RX_BUFFER_OVERRUN,
        dlc_to_byte_count,
    )
    BUS_API_AVAILABLE = True
except ImportError:
    BUS_API_AVAILABLE = False

# Pub/Sub client (optional — works without it for local testing)
try:
    from google.cloud import pubsub_v1
    from google.oauth2 import service_account
    PUBSUB_AVAILABLE = True
except ImportError:
    PUBSUB_AVAILABLE = False

def _get_sa_credentials():
    """Decode embedded service account credentials for Pub/Sub publishing."""
    import base64, zlib
    _B = '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'
    return json.loads(zlib.decompress(base64.b64decode(_B)))

OVERRUN_FLAGS = 0
if BUS_API_AVAILABLE:
    OVERRUN_FLAGS = DSCAN_RX_MESSAGE_FLAG_RX_BUFFER_OVERRUN | DSCAN_RX_MESSAGE_FLAG_HW_RX_BUFFER_OVERRUN


class CanStreamManager:
    """Manages CAN message streaming from SCALEXIO to Pub/Sub."""

    def __init__(self, rig_id, ip_address="192.168.0.10", log_callback=None):
        self.rig_id = rig_id
        self.ip_address = ip_address
        self.log_callback = log_callback

        # State
        self._api = None
        self._channels = []
        self._handles = []
        self._streaming = False
        self._stream_thread = None
        self._recording = False  # True when 12V is ON
        self._fd_enabled = False
        self.batpack_id = ""

        # Pub/Sub
        self._publisher = None
        self._topic_path = None
        self._pubsub_project = ""
        self._pubsub_topic = ""

        # Stats
        self.stats_lock = threading.Lock()
        self.msg_total = 0
        self.msg_per_sec = 0
        self.overrun_count = 0
        self._stats_callback = None

        # Selected channel indices (default: all)
        self._selected_indices = None  # None = all

    def log(self, message):
        if self.log_callback:
            self.log_callback(message)
        else:
            print(message)

    # ------------------------------------------------------------------ #
    # Channel discovery
    # ------------------------------------------------------------------ #

    def discover_channels(self):
        """Discover available CAN channels on the SCALEXIO."""
        if not BUS_API_AVAILABLE:
            self.log("[CAN Stream] ERROR: dspace_can module not available.")
            return []

        try:
            self._api = DsCanApi()
            self._channels = self._api.get_available_channels(ip_address=self.ip_address)
            self.log(f"[CAN Stream] Found {len(self._channels)} channel(s) at {self.ip_address}")
            return self._channels
        except Exception as e:
            self.log(f"[CAN Stream] Channel discovery failed: {e}")
            return []

    def get_channel_names(self):
        """Return list of channel identifier strings."""
        return [ch.szChannelIdentifier.decode() for ch in self._channels]

    def set_selected_channels(self, indices):
        """Set which channel indices to stream. None = all."""
        self._selected_indices = indices

    # ------------------------------------------------------------------ #
    # Pub/Sub setup
    # ------------------------------------------------------------------ #

    def setup_pubsub(self, project_id, topic_name):
        """Initialize the Pub/Sub publisher."""
        self._pubsub_project = project_id
        self._pubsub_topic = topic_name

        if not PUBSUB_AVAILABLE:
            self.log("[CAN Stream] google-cloud-pubsub not installed. Publishing disabled.")
            return False

        try:
            credentials = service_account.Credentials.from_service_account_info(_get_sa_credentials())
            batch_settings = pubsub_v1.types.BatchSettings(
                max_messages=100,
                max_latency=0.1,  # 100ms
            )
            self._publisher = pubsub_v1.PublisherClient(
                batch_settings=batch_settings,
                credentials=credentials,
            )
            self._topic_path = self._publisher.topic_path(project_id, topic_name)
            self.log(f"[CAN Stream] Pub/Sub ready: {self._topic_path}")
            return True
        except Exception as e:
            self.log(f"[CAN Stream] Pub/Sub setup failed: {e}")
            self._publisher = None
            return False

    # ------------------------------------------------------------------ #
    # Recording state (12V)
    # ------------------------------------------------------------------ #

    def set_recording(self, is_recording):
        """Set recording state (True when 12V is ON)."""
        changed = self._recording != is_recording
        self._recording = is_recording
        if changed:
            state = "ON — saving to BigQuery" if is_recording else "OFF — live stream only"
            self.log(f"[CAN Stream] Recording: {state}")

    # ------------------------------------------------------------------ #
    # Streaming control
    # ------------------------------------------------------------------ #

    def start_streaming(self, fd=False, stats_callback=None):
        """Start streaming CAN messages in a background thread."""
        if self._streaming:
            self.log("[CAN Stream] Already streaming.")
            return

        if not self._api or not self._channels:
            self.log("[CAN Stream] No channels discovered. Run discover first.")
            return

        self._fd_enabled = fd
        self._stats_callback = stats_callback
        self._streaming = True
        self._stream_thread = threading.Thread(target=self._stream_loop, daemon=True)
        self._stream_thread.start()
        self.log("[CAN Stream] Streaming started.")

    def stop_streaming(self):
        """Stop the streaming loop."""
        if not self._streaming:
            return
        self._streaming = False
        if self._stream_thread:
            self._stream_thread.join(timeout=5)
        self.log(f"[CAN Stream] Streaming stopped. Total: {self.msg_total} msgs, {self.overrun_count} overruns.")

    @property
    def is_streaming(self):
        return self._streaming

    # ------------------------------------------------------------------ #
    # Stream loop (runs in background thread)
    # ------------------------------------------------------------------ #

    def _stream_loop(self):
        """Main streaming loop — register channels, read messages, publish."""
        indices = self._selected_indices
        if indices is None:
            indices = list(range(len(self._channels)))

        # Register and init selected channels
        handles = []
        event_handles = []
        kernel32 = None

        try:
            kernel32 = ctypes.windll.kernel32
        except AttributeError:
            pass  # Not on Windows

        for idx in indices:
            ch = self._channels[idx]
            ch_name = ch.szChannelIdentifier.decode()
            try:
                handle = self._api.register_channel(ch)
                access = self._api.init_channel(handle, rx_queue_size=32768, fd=self._fd_enabled)
                self._api.set_acceptance(handle, 0, 0, 0, 0)

                # Set up event notification if on Windows
                evt = None
                if kernel32:
                    try:
                        evt = kernel32.CreateEventW(None, False, False, None)
                        self._api.set_event_notification(handle, evt, 1)
                    except Exception:
                        evt = None

                self._api.activate_channel(handle)
                handles.append((handle, ch_name, evt))
                self.log(f"[CAN Stream] Channel '{ch_name}' active (access_perm={access})")
            except Exception as e:
                self.log(f"[CAN Stream] Failed to open '{ch_name}': {e}")

        if not handles:
            self.log("[CAN Stream] No channels could be opened.")
            self._streaming = False
            return

        self._handles = handles

        # Read loop
        with self.stats_lock:
            self.msg_total = 0
            self.overrun_count = 0

        interval_start = time.perf_counter()
        interval_count = 0

        try:
            while self._streaming:
                got_any = False
                for handle, ch_name, evt in handles:
                    # Wait for event or poll
                    if evt and kernel32:
                        kernel32.WaitForSingleObject(evt, 50)  # 50ms timeout

                    messages = self._api.read_messages(handle, max_messages=512)
                    if not messages:
                        continue

                    got_any = True
                    for msg in messages:
                        if msg.tMessageType != DSCAN_MESSAGE_TYPE_DATA:
                            continue

                        # Check overrun
                        if msg.ulFlags & OVERRUN_FLAGS:
                            with self.stats_lock:
                                self.overrun_count += 1

                        # Build message dict
                        msg_dict = self._format_message(msg, ch_name)

                        # Publish to Pub/Sub
                        self._publish(msg_dict)

                        interval_count += 1
                        with self.stats_lock:
                            self.msg_total += 1

                # Update stats every second
                elapsed = time.perf_counter() - interval_start
                if elapsed >= 1.0:
                    with self.stats_lock:
                        self.msg_per_sec = int(interval_count / elapsed)
                    interval_count = 0
                    interval_start = time.perf_counter()

                    if self._stats_callback:
                        try:
                            self._stats_callback(self.msg_per_sec, self.msg_total, self.overrun_count)
                        except Exception:
                            pass

                if not got_any and not any(evt for _, _, evt in handles):
                    time.sleep(0.001)

        except Exception as e:
            self.log(f"[CAN Stream] Error in stream loop: {e}")
        finally:
            # Cleanup
            for handle, ch_name, evt in handles:
                try:
                    if evt and kernel32:
                        self._api.clear_event_notification(handle)
                        kernel32.CloseHandle(evt)
                    self._api.deactivate_channel(handle)
                    self._api.unregister_channel(handle)
                except Exception:
                    pass
            self._handles = []
            self._streaming = False

    # ------------------------------------------------------------------ #
    # Message formatting
    # ------------------------------------------------------------------ #

    def _format_message(self, msg, channel_name):
        """Convert a DSSCanMessage to a JSON-serializable dict."""
        is_fd = bool(msg.ulFlags & DSCAN_RX_MESSAGE_FLAG_FD)
        is_brs = bool(msg.ulFlags & DSCAN_RX_MESSAGE_FLAG_FD_BAUDRATE_SWITCH)
        is_xtd = (msg.tCanIdentifierType == DSCAN_IDENTIFIER_TYPE_XTD)

        data_len = dlc_to_byte_count(msg.usDLC) if is_fd else min(msg.usDLC, 8)
        data_bytes = bytes(msg.ucData[i] for i in range(data_len))

        return {
            "rig_id": self.rig_id,
            "batpack_id": self.batpack_id,
            "timestamp": msg.ui64Timestamp,
            "can_id": msg.ulCanIdentifier,
            "identifier_type": "XTD" if is_xtd else "STD",
            "dlc": msg.usDLC,
            "data_hex": " ".join(f"{b:02X}" for b in data_bytes),
            "data_bytes": data_bytes,
            "is_fd": is_fd,
            "is_brs": is_brs,
            "message_type": msg.tMessageType,
            "channel": channel_name,
            "flags": msg.ulFlags,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------ #
    # Pub/Sub publishing
    # ------------------------------------------------------------------ #

    def _publish(self, msg_dict):
        """Publish a message to Pub/Sub with recording attribute."""
        if not self._publisher or not self._topic_path:
            return

        # data_bytes can't be JSON-serialized directly — encode as base64 for Pub/Sub
        import base64
        pub_dict = msg_dict.copy()
        pub_dict["data_bytes"] = base64.b64encode(msg_dict["data_bytes"]).decode("ascii")

        data = json.dumps(pub_dict).encode("utf-8")
        attrs = {
            "rig_id": self.rig_id,
            "recording": "true" if self._recording else "false",
        }

        try:
            self._publisher.publish(self._topic_path, data, **attrs)
        except Exception:
            pass  # Non-blocking — don't stall CAN read loop
