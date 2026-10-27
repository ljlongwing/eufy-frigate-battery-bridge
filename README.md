# eufy-frigate-battery-bridge

Get **eufy battery cameras** into **Frigate**: recordings and Frigate events when eufy sees motion
or a person, plus live view on demand.

eufy battery cameras sleep almost all the time, have no RTSP stream, and only stream when woken
over eufy's own P2P connection (the one the eufy app uses). Frigate needs a continuous stream. This
bridge sits in between:

- **Frigate sees a normal, always-on camera.** While the eufy camera sleeps, the stream is a static
  "camera asleep" card. It has no motion, so Frigate records nothing.
- **Motion wakes it.** When eufy reports motion, a person, a pet, etc., the bridge starts a P2P
  livestream through [eufy-security-ws](https://github.com/bropat/eufy-security-ws) and feeds the real
  video into the same stream for `hold_secs` after the last report. Frigate records it and runs
  detection on it like any other camera.
- **eufy's detection becomes a Frigate event.** A walk-past is often over before the video arrives,
  so the bridge also files the event in Frigate directly (`person`, `car`, `dog` or `motion`, sub
  label `eufy`), with the recording attached. It shows up in Review.
- **Opening the camera wakes it.** Opening its live view in Frigate (or anything else reading the
  go2rtc stream) wakes the camera for as long as someone is watching.
- **One stream at a time.** A HomeBase serves only one P2P livestream at a time, so the cameras take
  turns. Motion beats a viewer, newer motion on another camera takes over once the current one has
  shown `min_dwell_secs` of video, and a newer viewer beats an older one.
- **Optional Home Assistant webhook** for notifications (see below).

```
eufy event ──► eufy-security-ws ──► bridge ──(wake, P2P video)──► ffmpeg ──RTSP──► Frigate's go2rtc ──► Frigate
                                       └──► Frigate API (event)      └──► HA webhook (optional)
```

One ffmpeg encoder per camera runs all the time, so the output stream never breaks between asleep
and live and Frigate's recorder never has to reconnect at the start of an event. While a camera is
live, a second ffmpeg decodes the eufy video (H.265 or H.264, any size) and scales it to 1280×720.

## Requirements

- Frigate 0.14+ (uses its built-in go2rtc and the `/api/events/.../create` API)
- eufy-security-ws, already logged in to your eufy account and seeing the cameras
- Docker, on the same docker network as Frigate

## Setup

**1. Config.** Copy `config.example.json` to `config.json` and fill it in:

| Key | Required | Meaning |
|---|---|---|
| `eufy_ws` | yes | eufy-security-ws WebSocket URL |
| `go2rtc_api` | yes | Frigate's go2rtc API, normally `http://frigate:1984` |
| `rtsp_publish` | yes | Where to publish each camera. `{name}` is replaced by the camera name. If Frigate's go2rtc has `rtsp.username/password` set, put them in the URL. |
| `cameras` | yes | `[{ "name", "serial", "title" }]`. `name` must match the Frigate camera name. `title` is shown on the asleep card. |
| `frigate_api` | no | Frigate's internal API (`http://frigate:5000`). Without it, no Frigate events are filed. |
| `ha_webhook` | no | Home Assistant webhook URL, see below |
| `audio` | no | `true` (default) passes the camera's audio through |
| `hold_secs` | no | Stay live this long after the last eufy report (60) |
| `max_secs` | no | Hard cap for one live period (900) |
| `view_linger_secs` | no | Stay live this long after the last viewer leaves (20) |
| `view_max_secs` | no | Cap for a live period only viewers asked for (300) |
| `min_dwell_secs` | no | Video a motion-woken camera gets before motion elsewhere can take over (15) |
| `viewer_cooldown_secs` | no | Ignore viewers that appear this soon after the camera went to sleep (45) |
| `trigger_events` | no | eufy event names that wake the camera (motion, person, pet, vehicle, dog, stranger, sound) |
| `event_secs` | no | Length of the Frigate event (30) |
| `snapshot_delay_secs` | no | How long real video must be flowing before the event is filed, so its snapshot isn't the "waking up" card (3) |

**2. Frigate.** Each battery camera reads the stream the bridge publishes into Frigate's go2rtc.
Declare the stream in go2rtc **with no source**: go2rtc refuses to accept a published stream it
doesn't know (the bridge's encoder then exits and restarts every 3 s with "Failed reading RTSP data:
End of file").

```yaml
go2rtc:
  streams:
    shed:        # empty on purpose: the bridge publishes into it
cameras:
  shed:
    ffmpeg:
      inputs:
        - path: rtsp://127.0.0.1:8554/shed
          input_args: preset-rtsp-restream
          roles: [record, detect]
      output_args:
        record: preset-record-generic-audio-copy   # or preset-record-generic without audio
    detect:
      width: 1280
      height: 720
      fps: 5
    record:
      continuous:
        days: 0          # keep only motion/alerts/detections, not hours of "camera asleep"
    live:
      streams:
        Main: shed
```

**3. Run it:** `docker compose -f docker-compose.example.yml up -d --build`. The log shows every
wake, sleep, take-over and event.

**4. eufy camera settings.** Set the camera's streaming quality to **Medium (1280×720)**, either in
the eufy app or via eufy-security-ws (`videoStreamingQuality` 7). On "Auto", one of my cameras
picked 4K for a P2P session, which stalled after a few seconds.

## Home Assistant notifications (optional)

With `ha_webhook` set, the bridge POSTs JSON twice per eufy event:

1. **Right away:** `{"camera", "title", "label"}`. Good for a fast text alert.
2. **Once the Frigate event exists:** the same keys plus `event_id`, `review_id` and `no_video`
   (true if the camera never sent video, so the snapshot is a placeholder). Good for a follow-up
   with the snapshot (`/api/frigate/notifications/<event_id>/snapshot.jpg` via the Frigate
   integration) and a link to Frigate's review page.

Trigger an automation with `platform: webhook` and use `trigger.json.event_id` to tell the two apart.

## Troubleshooting

- **Encoder exits every 3 s with "Failed reading RTSP data: End of file":** the stream isn't declared
  in `go2rtc.streams` (step 2), or the RTSP user/password in `rtsp_publish` is wrong.
- **Nothing wakes on motion:** notifications must be on for the camera in the eufy app, otherwise
  eufy-security-ws gets no motion events. Mute them on the phone instead of turning them off.
- **Stuck on "waking up", then "no video for 15 s, asking eufy again":** check the camera's streaming
  quality (step 4), its battery and Wi-Fi signal, and that the eufy app can open it live.
- **"start_livestream failed":** eufy-security-ws lost its eufy session or the HomeBase is busy with
  another P2P stream (the eufy app counts too).

## Good to know

- **Battery.** Every wake costs battery, the same as opening the camera in the eufy app. Cameras that
  see a lot of motion will drain faster than on eufy alone.
- **Latency.** First video arrives about 4–5 s after the wake, so a quick walk-past may be half over.
  That's why the bridge files eufy's own detection as a Frigate event.
- **Missed events are eufy-side.** If eufy's own detection doesn't fire, nothing wakes the camera.
- **Asleep ↔ live switches look like motion to Frigate,** and Frigate's Live dashboard auto-opens
  cameras with activity. The bridge ignores viewers that connect while a camera is already live or
  just went to sleep, so they don't keep it awake.
- **CPU.** Idle ~3–4% of one core. While a camera is live, ~35–65% of one core (decode + x264 encode).
- **Test hook:** `docker kill -s USR2 eufy-frigate-battery-bridge` files a "test" event on the first
  camera without waking it.

## Tested with

Frigate 0.18.2, eufy-security-ws 3.1.0 (eufy-security-client 4.1.0, API schema 21), two eufy
`T8162` battery cameras (firmware 3.1.5.8) on a HomeBase 3.

Reported working by users (#1): eufyCam 2C `T8140-Z` (firmware 4.1.4, motion + live view wake) and
eufyCam 3 `T8160` (firmware 3.4.3.0, motion + live view wake), both on a
HomeBase 3.

Heads-up: eufy-security-client describes itself as a stopgap while eufy moves to a new platform,
so upstream changes may break this.

## Notes

Built with help from Claude (Anthropic) and tested on my own setup.

## License

MIT
