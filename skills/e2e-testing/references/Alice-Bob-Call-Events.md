---
name: Bob starts the call in the room and Alice joins
---

Below are the events sent by Alice and Bob in a room where they are both joined and have the room call.


Bob starts the call in the room.

```json
{
    "event_id": "$_ErcrEWx3Hj77_wScF-U4e9aS6cVi37RvFUeq12BiaI",
    "room_id": "!RhkzuEOlOxpckXJkhY:synapse.m.localhost",
    "type": "org.matrix.msc4143.rtc.member",
    "sender": "@bob:synapse.othersite.m.localhost",
    "content": {
        "application": {
            "type": "m.call",
            "m.call.intent": "video"
        },
        "slot_id": "m.call#ROOM",
        "rtc_transports": [
            {
                "type": "livekit",
                "livekit_service_url": "https://matrix-rtc.othersite.m.localhost/livekit/jwt"
            }
        ],
        "member": {
            "device_id": "WDQHAPEYDK",
            "user_id": "@bob:synapse.othersite.m.localhost",
            "id": "bcab799f-abae-4d38-bf1b-77238346349a"
        },
        "versions": [],
        "msc4354_sticky_key": "bcab799f-abae-4d38-bf1b-77238346349a"
    },
    "msc4354_sticky": {
        "duration_ms": 3600000
    },
    "origin_server_ts": 1779030731296,
    "unsigned": {
        "age": 103,
        "transaction_id": "m1779030731272.11",
        "msc4354_sticky_duration_ttl_ms": 3599897,
        "membership": "join"
    }
}
```

There is also the notification event sent in the room to notify the call

```json
{
    "event_id": "$-J58IM_m8OFoIb96ynQJ09_6lGQhAwaREKY2TLFnki8",
    "room_id": "!RhkzuEOlOxpckXJkhY:synapse.m.localhost",
    "type": "org.matrix.msc4075.rtc.notification",
    "sender": "@bob:synapse.othersite.m.localhost",
    "content": {
        "m.mentions": {
            "user_ids": [],
            "room": true
        },
        "notification_type": "notification",
        "m.relates_to": {
            "event_id": "$_ErcrEWx3Hj77_wScF-U4e9aS6cVi37RvFUeq12BiaI",
            "rel_type": "m.reference"
        },
        "sender_ts": 1779030731419,
        "lifetime": 30000,
        "m.call.intent": "video"
    },
    "origin_server_ts": 1779030731475,
}
```

Now Alice joins

```json
{
  "event_id": "$imqekRtWGLcITMI6YuMF0xgpT4S8LMr78eseonO2_Nw",
  "room_id": "!RhkzuEOlOxpckXJkhY:synapse.m.localhost",
  "type": "org.matrix.msc4143.rtc.member",
  "sender": "@alice:synapse.m.localhost",
    "content": {
        "application": {
            "m.call.intent": "video",
            "type": "m.call"
        },
        "member": {
            "device_id": "VJHNJJCVOA",
            "id": "d50437bd-424a-498d-912f-b0f1d2ba7f18",
            "user_id": "@alice:synapse.m.localhost"
        },
        "msc4354_sticky_key": "d50437bd-424a-498d-912f-b0f1d2ba7f18",
        "rtc_transports": [
            {
                "livekit_service_url": "https://matrix-rtc.m.localhost/livekit/jwt",
                "type": "livekit"
            }
        ],
        "slot_id": "m.call#ROOM",
        "versions": []
    },
    "msc4354_sticky": {
        "duration_ms": 3600000
    },
    "origin_server_ts": 1779030745881,
}
```


Alice leaves

```json
{
    "event_id": "$4cN54YJqNgWjtv1g0U1kx_bRhu_kfGV5_6qIAzxUXMA",
    "room_id": "!RhkzuEOlOxpckXJkhY:synapse.m.localhost",
    "sender": "@alice:synapse.m.localhost",
    "type": "org.matrix.msc4143.rtc.member",
    "content": {
        "msc4354_sticky_key": "d50437bd-424a-498d-912f-b0f1d2ba7f18"
    },
    "msc4354_sticky": {
        "duration_ms": 3600000
    },
    "origin_server_ts": 1779030751040
}
```

Bob leaves

```json
{
    "event_id": "$aCtxanx0gjPUbgCORaZ5GyqekAIE4Zy0pCILriT8BBk",
    "room_id": "!RhkzuEOlOxpckXJkhY:synapse.m.localhost",
    "type": "org.matrix.msc4143.rtc.member",
    "sender": "@bob:synapse.othersite.m.localhost",
    "content": {
        "msc4354_sticky_key": "bcab799f-abae-4d38-bf1b-77238346349a"
    },
    "msc4354_sticky": {
        "duration_ms": 3600000
    },
    "origin_server_ts": 1779030753302
}
```