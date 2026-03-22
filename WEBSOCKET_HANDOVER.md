# WebSocket Integration Handover — CAN Live Streaming

## Overview

The SBTL HiL PCs stream real-time CAN bus data to Google Cloud Pub/Sub. This document explains how to integrate the **Django backend** to receive these messages and push them to the **React frontend (GEMINI Dashboard)** via WebSocket.

## Architecture

```
3x Win11 HiL PCs (Vib1, Vib2, MAST)
        │ publish JSON to Pub/Sub
        ↓
Google Cloud Pub/Sub (topic: sbtl-can-stream)
        │
        ├── Subscription: sbtl-can-stream-bq
        │   (BigQuery, filter: attributes.recording="true")
        │   → Only stores data when 12V is ON
        │   → Zero code, fully managed
        │
        └── Subscription: sbtl-can-stream-ws  ← YOU CREATE THIS
            (Pull subscription, no filter)
            → Django consumes ALL messages
            → Routes to WebSocket by rig_id
```

## GCP Resources (already created)

| Resource | Name | Project |
|----------|------|---------|
| Pub/Sub Topic | `sbtl-can-stream` | `jlr-eng-ftd-tool-prod` |
| BigQuery Subscription | `sbtl-can-stream-bq` | filter: `attributes.recording="true"` |
| BigQuery Table | `ddg_dev_table.sbtl_can_stream` | europe-west2, partitioned by day, clustered by rig_id + can_id |

## Step 1: Create the Django Pull Subscription

```bash
gcloud pubsub subscriptions create sbtl-can-stream-ws \
  --topic=sbtl-can-stream \
  --ack-deadline=10 \
  --message-retention-duration=10m \
  --project=jlr-eng-ftd-tool-prod
```

No filter — Django receives ALL messages (including when 12V is off) for live monitoring.

## Step 2: Message Format

Each Pub/Sub message body is UTF-8 JSON:

```json
{
  "rig_id": "vib1_horizontal",
  "batpack_id": "TestPack001",
  "timestamp": 123456789,
  "can_id": 256,
  "identifier_type": "STD",
  "dlc": 8,
  "data_hex": "DE AD BE EF 01 02 03 04",
  "data_bytes": "3q2+7wECAwQ=",
  "is_fd": false,
  "is_brs": false,
  "message_type": 1,
  "channel": "ApplicationProcess_1.CAN (1)",
  "flags": 0,
  "ingested_at": "2026-03-22T14:30:00.123456+00:00"
}
```

Message **attributes** (metadata, not in the JSON body):

| Attribute | Values | Purpose |
|-----------|--------|---------|
| `rig_id` | `vib1_horizontal`, `vib2_vertical`, `mast` | Route to correct WebSocket group |
| `recording` | `"true"` / `"false"` | Whether 12V is ON (BigQuery subscription filters on this) |

## Step 3: Django Channels Setup

### routing.py

```python
from django.urls import re_path
from . import consumers

websocket_urlpatterns = [
    re_path(r'ws/can-stream/(?P<rig_id>\w+)/$', consumers.CanStreamConsumer.as_asgi()),
]
```

### consumers.py

```python
import json
from channels.generic.websocket import AsyncWebsocketConsumer

class CanStreamConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.rig_id = self.scope['url_route']['kwargs']['rig_id']
        self.group_name = f'can_stream_{self.rig_id}'
        await self.channel_layer.group_add(self.group_name, self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(self.group_name, self.channel_name)

    async def can_message(self, event):
        """Receive batched messages from Pub/Sub subscriber and send to WebSocket."""
        await self.send(text_data=json.dumps(event['data']))
```

### pubsub_subscriber.py (Django management command or background task)

```python
"""
Run as: python manage.py run_can_subscriber
Or as a Celery/background task.
"""
import json
import time
from google.cloud import pubsub_v1
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

SUBSCRIPTION = 'projects/jlr-eng-ftd-tool-prod/subscriptions/sbtl-can-stream-ws'

def run_subscriber():
    channel_layer = get_channel_layer()
    subscriber = pubsub_v1.SubscriberClient()

    # Buffer messages and flush every 200ms for browser performance
    buffers = {}  # rig_id -> list of messages
    last_flush = time.time()

    def callback(message):
        nonlocal last_flush
        msg_data = json.loads(message.data.decode('utf-8'))
        rig_id = message.attributes.get('rig_id', 'unknown')

        buffers.setdefault(rig_id, []).append(msg_data)
        message.ack()

        # Flush every 200ms
        now = time.time()
        if now - last_flush >= 0.2:
            for rid, msgs in buffers.items():
                if msgs:
                    group_name = f'can_stream_{rid}'
                    async_to_sync(channel_layer.group_send)(
                        group_name,
                        {'type': 'can_message', 'data': {'messages': msgs, 'count': len(msgs)}}
                    )
            buffers.clear()
            last_flush = now

    streaming_pull = subscriber.subscribe(SUBSCRIPTION, callback=callback)
    print(f"Listening on {SUBSCRIPTION}...")

    try:
        streaming_pull.result()
    except KeyboardInterrupt:
        streaming_pull.cancel()
        streaming_pull.result()
```

## Step 4: React Integration

Each zone page in GEMINI Dashboard connects to its own WebSocket:

```jsx
// In the zone detail page (e.g., Vib1 Horizontal)
const rigId = 'vib1_horizontal';  // from route params

useEffect(() => {
  const ws = new WebSocket(`wss://${window.location.host}/ws/can-stream/${rigId}/`);

  ws.onmessage = (event) => {
    const { messages, count } = JSON.parse(event.data);
    // messages is an array of CAN message objects
    // Append to table, update charts, etc.
    setCanMessages(prev => [...prev.slice(-500), ...messages]);  // Keep last 500
  };

  return () => ws.close();
}, [rigId]);
```

## Step 5: Performance Notes

| Metric | Value |
|--------|-------|
| Max messages per rig | ~7,700 msg/s (2 CAN channels at 500kbit/s) |
| Typical messages per rig | ~3,000 msg/s (40% bus load) |
| WebSocket flush interval | 200ms (5 batches/sec to browser) |
| Messages per WebSocket batch | ~600 at typical load |
| Browser rendering | Show latest 200-500 messages in scrolling table |

**Do NOT send every individual message to the browser.** Batch server-side (200ms intervals) and limit the React table to the latest N rows.

## Rig IDs

| Rig ID | Zone | Description |
|--------|------|-------------|
| `vib1_horizontal` | Vib1 Horizontal | Horizontal Vibration Test Zone |
| `vib2_vertical` | Vib2 Vertical | Vertical Vibration Test Zone |
| `mast` | MAST | MAST Test Zone |

## Dependencies

```
# Django backend
pip install channels channels-redis google-cloud-pubsub

# Channel layer (Redis)
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {"hosts": [("redis", 6379)]},
    },
}
```
