// eufy-frigate-battery-bridge: event-only Frigate feed for eufy battery cameras.
//
// Frigate sees each camera as a normal, always-on RTSP stream published by this bridge into
// Frigate's go2rtc. While the camera sleeps the stream is a static "camera asleep" card (no motion,
// so Frigate keeps nothing). The real camera is woken (eufy P2P livestream through eufy-security-ws,
// the same path the eufy app uses) when
//   - eufy reports motion/person/... on it          -> live until hold_secs after the last report
//   - someone opens the camera's live view          -> live while they watch (go2rtc shows a viewer)
// The HomeBase serves only ONE P2P livestream at a time, so the cameras take turns: motion beats a
// viewer, a newer viewer beats an older one, and newer motion on another camera takes over once the
// current one has shown min_dwell_secs of video. Viewers never block or outlast another camera's motion.
// Going asleep <-> live looks like motion to Frigate, and Frigate's Live dashboard auto-opens streams
// for cameras with activity. Such viewers must not keep a camera awake, so only a viewer that connected
// while the camera was asleep (and not right after it fell asleep) counts as someone asking for live view.
// One continuous encoder per camera keeps the output stream unbroken across asleep <-> live, so
// Frigate's recorder never has to reconnect at the start of an event.
'use strict';
const fs = require('fs');
const { spawn, spawnSync } = require('child_process');
const WebSocket = require('ws');

const cfg = JSON.parse(fs.readFileSync(process.env.BRIDGE_CONFIG || '/config/config.json', 'utf8'));
for (const k of ['eufy_ws', 'rtsp_publish', 'go2rtc_api']) if (!cfg[k]) throw new Error(`config.json: "${k}" is required`);
if (!Array.isArray(cfg.cameras) || !cfg.cameras.length || cfg.cameras.some((c) => !c.name || !c.serial))
  throw new Error('config.json: "cameras" must list at least one {"name", "serial"}');
if (!cfg.rtsp_publish.includes('{name}')) throw new Error('config.json: "rtsp_publish" must contain {name}');
const HOLD_MS = (cfg.hold_secs ?? 60) * 1000;          // stay live this long after the last motion report
const LINGER_MS = (cfg.view_linger_secs ?? 20) * 1000; // stay live this long after the last viewer left
const MAX_MS = (cfg.max_secs ?? 900) * 1000;           // hard cap for one continuous live period
const VIEW_MAX_MS = (cfg.view_max_secs ?? 300) * 1000;  // cap for a live period that only viewers asked for
const DWELL_MS = (cfg.min_dwell_secs ?? 15) * 1000;      // video a motion-woken camera gets before newer motion elsewhere may take over
const COOLDOWN_MS = (cfg.viewer_cooldown_secs ?? 45) * 1000; // after going to sleep, ignore viewers that appear this soon
const TRIGGERS = new Set(cfg.trigger_events ?? ['motion detected', 'person detected', 'pet detected',
  'vehicle detected', 'dog detected', 'stranger person detected', 'identity person detected', 'sound detected']);
const W = cfg.width ?? 1280, H = cfg.height ?? 720, FRAME = W * H * 3 / 2;
const LIVE_FPS = cfg.live_fps ?? 10, IDLE_FPS = cfg.idle_fps ?? 2;
const AUDIO = cfg.audio !== false, RATE = 16000, AUDIO_TICK = 100, AUDIO_CHUNK = RATE * 2 * AUDIO_TICK / 1000;
const SILENCE = Buffer.alloc(AUDIO_CHUNK);

const log = (...a) => console.log(new Date().toLocaleString('sv-SE'), ...a);   // local time (TZ env)

// ---------------------------------------------------------------- static cards
function card(line1, line2) {
  const font = ['/usr/share/fonts/dejavu/DejaVuSans.ttf', '/usr/share/fonts/ttf-dejavu/DejaVuSans.ttf'].find((f) => fs.existsSync(f));
  const esc = (t) => t.replace(/[:'\\,]/g, ' ');
  const text = font
    ? `,drawtext=fontfile=${font}:text='${esc(line1)}':fontcolor=0xd0d0d0:fontsize=54:x=(w-text_w)/2:y=h/2-70` +
      `,drawtext=fontfile=${font}:text='${esc(line2)}':fontcolor=0x909090:fontsize=30:x=(w-text_w)/2:y=h/2+20`
    : '';
  const r = spawnSync('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-f', 'lavfi', '-i', `color=c=0x1c1c1c:s=${W}x${H}`,
    '-vf', 'format=yuv420p' + text, '-frames:v', '1', '-f', 'rawvideo', 'pipe:1'], { maxBuffer: FRAME * 2 });
  if (r.status === 0 && r.stdout.length === FRAME) return r.stdout;
  log('could not render a text card, using a plain one:', String(r.stderr).trim().split('\n').pop());
  const b = Buffer.alloc(FRAME, 0x80); b.fill(0x1c, 0, W * H); return b;
}

// ---------------------------------------------------------------- eufy-security-ws
let ws = null, wsReady = false, seq = 0;
const waiting = new Map();
const call = (o) => new Promise((resolve) => {
  if (!ws || ws.readyState !== WebSocket.OPEN) return resolve({ success: false, errorCode: 'ws_closed' });
  const id = 'b' + (++seq);
  waiting.set(id, resolve);
  ws.send(JSON.stringify({ messageId: id, ...o }));
  setTimeout(() => { if (waiting.delete(id)) resolve({ success: false, errorCode: 'no_reply' }); }, 20000);
});

function connectWs() {
  ws = new WebSocket(cfg.eufy_ws);
  ws.on('open', () => log('eufy-security-ws connected'));
  ws.on('close', () => {
    wsReady = false;
    log('eufy-security-ws connection closed, retrying in 10 s');
    for (const c of cams) { c.motionUntil = 0; c.viewUntil = 0; if (c.active) stop(c, 'bridge lost eufy-security-ws'); }
    setTimeout(connectWs, 10000);
  });
  ws.on('error', (e) => log('eufy-security-ws error:', e.message));
  ws.on('message', async (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'version') {
      await call({ command: 'set_api_schema', schemaVersion: Math.min(m.maxSchemaVersion, 21) });
      const st = await call({ command: 'start_listening' });
      wsReady = !!st.success;
      log(wsReady ? 'listening for eufy events' : 'start_listening failed: ' + st.errorCode);
    } else if (m.type === 'result') {
      const r = waiting.get(m.messageId);
      if (r) { waiting.delete(m.messageId); r(m); }
    } else if (m.type === 'event') {
      onEvent(m.event || {});
    }
  });
}

function onEvent(e) {
  const cam = bySerial.get(e.serialNumber);
  if (!cam) return;
  switch (e.event) {
    case 'livestream video data': return onVideo(cam, e);
    case 'livestream audio data': return onAudio(cam, e);
    case 'livestream started':
      return log(`${cam.name}: eufy livestream started`);
    case 'livestream stopped':
      log(`${cam.name}: eufy livestream stopped`);
      stopDecoder(cam);
      if (cam.active && Date.now() < want(cam)) startLivestream(cam);     // eufy ends sessions after a few minutes
      return;
    default:
      if (TRIGGERS.has(e.event) && e.state === true) { trigger(cam, e.event, HOLD_MS, 'motion'); frigateEvent(cam, e.event); }
  }
}

// ---------------------------------------------------------------- cameras
const cams = cfg.cameras.map((c) => ({
  ...c, active: false, motionUntil: 0, lastMotionAt: 0, viewUntil: 0, viewSince: 0, reason: '', sleptAt: Date.now(), firstVideoAt: 0,
  startedAt: 0, lastStartAt: 0, lastVideoAt: 0, restarts: 0, gotVideo: false,
  enc: null, dec: null, frame: null, frameAt: 0, frameNew: false, lastWrite: 0, lastSent: null,
  pcm: [], pcmLen: 0, viewers: 0, viewerIds: new Map(),   // go2rtc consumer id -> asked for live view?
  asleep: card(`${c.title || c.name} - camera asleep`, 'Open this camera to wake it, or wait for motion'),
  waking: card(`${c.title || c.name} - waking up...`, 'Live video starts in a few seconds'),
  busy: card(`${c.title || c.name} - waiting`, 'Another camera is live. Only one can stream at a time'),
}));
// ---------------------------------------------------------------- Frigate events
// A walk-past is often over before the camera's video arrives, so Frigate's own detector may see nothing.
// eufy already classified the event, so file it in Frigate directly (shows up in Review with the recording).
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const EVENT_SECS = cfg.event_secs ?? 30, SNAP_DELAY_MS = (cfg.snapshot_delay_secs ?? 3) * 1000;
const LABELS = [['person', 'person'], ['vehicle', 'car'], ['dog', 'dog'], ['pet', 'dog']];
function notifyHA(cam, extra) {
  if (!cfg.ha_webhook) return;
  fetch(cfg.ha_webhook, { method: 'POST', headers: { 'Content-Type': 'application/json' }, signal: AbortSignal.timeout(5000),
    body: JSON.stringify({ camera: cam.name, title: cam.title || cam.name, label: cam.evLabel, ...extra }) })
    .then((r) => { if (!r.ok) log(`${cam.name}: HA webhook failed (${r.status})`); })
    .catch((err) => log(`${cam.name}: HA webhook failed: ${err.message}`));
}
function frigateEvent(cam, ev) {
  if (!cfg.frigate_api) return;
  const label = LABELS.find(([k]) => ev.includes(k))?.[1] || (ev.startsWith('test') ? 'test' : 'motion');
  if (cam.evTimer) { if (cam.evLabel === 'motion') cam.evLabel = label; return; }   // eufy sends "person" and "motion" together
  if (Date.now() < (cam.evUntil || 0)) return;
  cam.evLabel = label;
  cam.evTimer = setTimeout(async () => {
    // tell Home Assistant right away (text alert); the Frigate event with a real picture follows once video is up
    notifyHA(cam, {});
    // Frigate takes the event's snapshot when it is created: wait until decoded pictures have been flowing for
    // SNAP_DELAY_MS (encoder + go2rtc + Frigate latency), 25 s at most. A first packet is not enough: a stalled
    // stream (seen with 4K) leaves Frigate on the "waking up" card.
    const showing = () => cam.active && cam.frame && Date.now() - cam.frameAt < 1500 && Date.now() - cam.firstFrameAt > SNAP_DELAY_MS;
    for (let i = 0; i < 50 && cam.evLabel !== 'test' && !showing(); i++) await sleep(500);
    const noVideo = cam.evLabel !== 'test' && !showing();
    cam.evTimer = null;
    cam.evUntil = Date.now() + EVENT_SECS * 1000;
    try {
      const r = await fetch(`${cfg.frigate_api}/api/events/${cam.name}/${cam.evLabel}/create`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, signal: AbortSignal.timeout(5000),
        body: JSON.stringify({ source_type: 'api', sub_label: 'eufy', duration: EVENT_SECS, include_recording: true }),
      });
      if (!r.ok) return log(`${cam.name}: Frigate event "${cam.evLabel}" failed (${r.status} ${(await r.text()).slice(0, 200)})`);
      const eventId = (await r.json()).event_id;
      log(`${cam.name}: Frigate event "${cam.evLabel}" created${noVideo ? ' (camera sent no live video, snapshot is a placeholder)' : ''}`);
      await sleep(1500);
      let reviewId = '';
      try {
        const rv = await (await fetch(`${cfg.frigate_api}/api/review?cameras=${cam.name}&limit=5`, { signal: AbortSignal.timeout(5000) })).json();
        reviewId = rv.find((x) => x.data?.detections?.includes(eventId))?.id || '';
      } catch { /* link falls back to the review page */ }
      notifyHA(cam, { event_id: eventId, review_id: reviewId, no_video: noVideo });
    } catch (err) { log(`${cam.name}: Frigate event failed: ${err.message}`); }
  }, 1500);
}
// test hook: `docker kill -s USR2 eufy-frigate-battery-bridge` files a "test" event on the first camera (does not wake it)
process.on('SIGUSR2', () => frigateEvent(cams[0], 'test detected'));

const bySerial = new Map(cams.map((c) => [c.serial, c]));

const want = (c) => Math.max(c.motionUntil, c.viewUntil);
const inMotion = (c) => Date.now() < c.motionUntil;

function trigger(cam, reason, ms, kind) {
  const now = Date.now();
  if (!wsReady) return;
  if (now >= want(cam)) cam.reason = reason;
  if (kind === 'motion') {
    if (!inMotion(cam) && !cam.active) cam.reason = reason;
    cam.motionUntil = Math.max(cam.motionUntil, now + ms); cam.lastMotionAt = now;
    if (cam.active) log(`${cam.name}: ${reason} (already live)`);
  } else {
    if (now >= cam.viewUntil) cam.viewSince = now;
    cam.viewUntil = Math.max(cam.viewUntil, now + ms);
  }
  arbitrate();
}

// decide which camera gets the single P2P livestream
function arbitrate() {
  const now = Date.now();
  let cur = cams.find((c) => c.active);
  if (cur && now >= want(cur)) { stop(cur, 'no more activity or viewers'); cur = null; }
  const wanting = cams.filter((c) => !c.active && now < want(c));
  if (!wanting.length || !wsReady) return;
  const pick = wanting.sort((a, b) => inMotion(b) - inMotion(a) || (inMotion(a) ? b.lastMotionAt - a.lastMotionAt : b.viewSince - a.viewSince))[0];
  if (cur) {
    const ok = inMotion(pick)
      ? !inMotion(cur) || (pick.lastMotionAt > cur.lastMotionAt && cur.gotVideo && now - cur.firstVideoAt >= DWELL_MS)
      : !inMotion(cur) && pick.viewSince > cur.viewSince;
    if (!ok) return;
    stop(cur, `${pick.name} takes over`);
  }
  pick.active = true; pick.startedAt = now; pick.gotVideo = false; pick.restarts = 0; pick.lastVideoAt = 0;
  log(`${pick.name}: WAKE (${pick.reason})`);
  startLivestream(pick);
}

async function startLivestream(cam) {
  cam.lastStartAt = Date.now();
  let r = await call({ command: 'device.start_livestream', serialNumber: cam.serial });
  if (!r.success && /already_running/.test(r.errorCode || '')) {
    // eufy-security-ws still holds an old session that sends nothing: drop it and start a fresh one
    log(`${cam.name}: eufy-security-ws reports a stale livestream, restarting it`);
    await call({ command: 'device.stop_livestream', serialNumber: cam.serial });
    await new Promise((res) => setTimeout(res, 1500));
    if (!cam.active) return;
    cam.lastStartAt = Date.now();
    r = await call({ command: 'device.start_livestream', serialNumber: cam.serial });
  }
  if (!r.success) log(`${cam.name}: start_livestream failed: ${r.errorCode}`);
}

function stop(cam, why) {
  if (!cam.active) return;
  cam.active = false; cam.sleptAt = Date.now();
  cam.viewUntil = 0;
  for (const id of cam.viewerIds.keys()) cam.viewerIds.set(id, false);   // still-open viewers must reconnect to wake it again
  log(`${cam.name}: SLEEP (${why}) after ${Math.round((Date.now() - cam.startedAt) / 1000)} s live`);
  call({ command: 'device.stop_livestream', serialNumber: cam.serial });
  stopDecoder(cam);
}

// ---- encoder: one per camera, runs all the time, fed raw frames + PCM, publishes H.264 + AAC to go2rtc
function startEncoder(cam) {
  const args = ['-hide_banner', '-loglevel', 'warning',
    '-use_wallclock_as_timestamps', '1', '-f', 'rawvideo', '-pix_fmt', 'yuv420p', '-s', `${W}x${H}`, '-i', 'pipe:0'];
  if (AUDIO) args.push('-use_wallclock_as_timestamps', '1', '-f', 's16le', '-ar', String(RATE), '-ac', '1', '-i', 'pipe:3', '-map', '0:v', '-map', '1:a');
  args.push('-fps_mode', 'passthrough', '-c:v', 'libx264', '-preset', 'veryfast', '-tune', 'zerolatency', '-pix_fmt', 'yuv420p',
    '-crf', '23', '-maxrate', '2500k', '-bufsize', '5000k', '-force_key_frames', 'expr:gte(t,n_forced*2)');
  if (AUDIO) args.push('-af', 'aresample=async=1', '-c:a', 'aac', '-b:a', '32k');
  args.push('-f', 'rtsp', '-rtsp_transport', 'tcp', cfg.rtsp_publish.replace('{name}', cam.name));
  const enc = spawn('ffmpeg', args, { stdio: ['pipe', 'ignore', 'pipe', AUDIO ? 'pipe' : 'ignore'] });
  cam.enc = enc; cam.lastSent = null;
  enc.stdin.on('error', () => {});
  if (AUDIO) enc.stdio[3].on('error', () => {});
  let tail = '';
  enc.stderr.on('data', (d) => { tail = (tail + d).slice(-500); });
  enc.on('exit', (code) => {
    if (cam.enc === enc) cam.enc = null;
    log(`${cam.name}: encoder exited (${code}) ${tail.trim().split('\n').slice(-2).join(' | ')} - restarting in 3 s`);
    setTimeout(() => { if (!cam.enc && !shuttingDown) startEncoder(cam); }, 3000);
  });
  log(`${cam.name}: encoder publishing to go2rtc`);
}

// ---- decoder: one per eufy session, turns the P2P H.265/H.264 + AAC into raw frames + PCM
function startDecoder(cam, codec) {
  const vfmt = /265|hevc/i.test(codec) ? 'hevc' : 'h264';
  const args = ['-hide_banner', '-loglevel', 'error', '-fflags', '+genpts', '-use_wallclock_as_timestamps', '1',
    '-analyzeduration', '1500000', '-f', vfmt, '-i', 'pipe:0'];
  if (AUDIO) args.push('-use_wallclock_as_timestamps', '1', '-f', 'aac', '-i', 'pipe:3');
  args.push('-map', '0:v', '-vf', `scale=${W}:${H}`, '-pix_fmt', 'yuv420p', '-fps_mode', 'passthrough', '-f', 'rawvideo', 'pipe:1');
  if (AUDIO) args.push('-map', '1:a', '-ar', String(RATE), '-ac', '1', '-f', 's16le', 'pipe:4');
  const dec = spawn('ffmpeg', args, { stdio: ['pipe', 'pipe', 'pipe', AUDIO ? 'pipe' : 'ignore', AUDIO ? 'pipe' : 'ignore'] });
  cam.dec = dec;
  dec.stdin.on('error', () => {});
  if (AUDIO) dec.stdio[3].on('error', () => {});
  let buf = Buffer.allocUnsafe(FRAME), fill = 0;
  dec.stdout.on('data', (d) => {
    let off = 0;
    while (off < d.length) {
      const n = Math.min(FRAME - fill, d.length - off);
      d.copy(buf, fill, off, off + n); fill += n; off += n;
      if (fill === FRAME) {
        if (cam.dec === dec) { if (!cam.firstFrameAt) cam.firstFrameAt = Date.now(); cam.frame = buf; cam.frameAt = Date.now(); cam.frameNew = true; }
        buf = Buffer.allocUnsafe(FRAME); fill = 0;
      }
    }
  });
  if (AUDIO) dec.stdio[4].on('data', (d) => {
    if (cam.dec !== dec) return;
    cam.pcm.push(d); cam.pcmLen += d.length;
    while (cam.pcmLen > RATE * 2 && cam.pcm.length > 1) cam.pcmLen -= cam.pcm.shift().length;   // keep at most ~1 s
  });
  let tail = '';
  dec.stderr.on('data', (d) => { tail = (tail + d).slice(-400); });
  dec.on('exit', (code) => {
    if (cam.dec === dec) { cam.dec = null; if (cam.active) log(`${cam.name}: decoder exited (${code}) ${tail.trim().split('\n').pop()}`); }
  });
}

function stopDecoder(cam) {
  const dec = cam.dec;
  cam.dec = null; cam.frame = null; cam.firstFrameAt = 0; cam.frameNew = false; cam.pcm = []; cam.pcmLen = 0;
  if (!dec) return;
  try { dec.stdin.end(); } catch {}
  setTimeout(() => { try { dec.kill('SIGKILL'); } catch {} }, 2000);
}

const bytes = (e) => Buffer.from(e.buffer?.data ?? e.buffer ?? []);

// true when the packet starts with parameter sets (HEVC VPS/SPS or H.264 SPS), i.e. a keyframe ffmpeg can start on
function isKeyPacket(buf, codec) {
  const i = buf[2] === 1 ? 3 : buf[3] === 1 ? 4 : -1;
  if (i < 0 || buf[0] !== 0 || buf[1] !== 0) return false;
  if (/265|hevc/i.test(codec)) { const t = (buf[i] >> 1) & 0x3f; return t === 32 || t === 33; }
  return (buf[i] & 0x1f) === 7;
}

function onVideo(cam, e) {
  if (!cam.active) return;
  cam.lastVideoAt = Date.now();
  const buf = bytes(e), codec = e.metadata?.videoCodec || 'H265';
  if (!cam.dec) {
    if (!isKeyPacket(buf, codec)) return;               // a decoder must begin on a keyframe
    startDecoder(cam, codec);
  }
  cam.dec.stdin.write(buf);
  if (!cam.gotVideo) {
    cam.gotVideo = true; cam.firstVideoAt = Date.now();
    log(`${cam.name}: first video (${codec} ${e.metadata?.videoWidth}x${e.metadata?.videoHeight}) ${((Date.now() - cam.startedAt) / 1000).toFixed(1)} s after the wake`);
  }
}

function onAudio(cam, e) {
  if (cam.active && cam.dec && AUDIO) cam.dec.stdio[3].write(bytes(e));
}

// ---- feed the encoders
setInterval(() => {                                       // video
  const now = Date.now();
  for (const cam of cams) {
    const enc = cam.enc;
    if (!enc || enc.stdin.writableNeedDrain) continue;
    const live = cam.active && cam.frame && now - cam.frameAt < 2000;
    const frame = live ? cam.frame : cam.active ? cam.waking : now < want(cam) ? cam.busy : cam.asleep;
    if (live ? (cam.frameNew || now - cam.lastWrite >= 500) : (frame !== cam.lastSent || now - cam.lastWrite >= 1000 / IDLE_FPS)) {
      enc.stdin.write(frame);
      cam.lastWrite = now; cam.lastSent = frame; cam.frameNew = false;
    }
  }
}, Math.round(1000 / LIVE_FPS));

if (AUDIO) setInterval(() => {                            // audio: steady 100 ms chunks, silence when there is none
  for (const cam of cams) {
    const enc = cam.enc;
    if (!enc || enc.stdio[3].writableNeedDrain) continue;
    let out = SILENCE;
    if (cam.pcmLen >= AUDIO_CHUNK) {
      const all = cam.pcm.length === 1 ? cam.pcm[0] : Buffer.concat(cam.pcm);
      out = all.subarray(0, AUDIO_CHUNK);
      const rest = all.subarray(AUDIO_CHUNK);
      cam.pcm = rest.length ? [rest] : []; cam.pcmLen = rest.length;
    }
    enc.stdio[3].write(out);
  }
}, AUDIO_TICK);

// ---- live viewers: anything reading the go2rtc stream other than Frigate's own recorder
async function pollViewers() {
  for (const cam of cams) {
    try {
      const r = await fetch(`${cfg.go2rtc_api}/api/streams?src=${cam.name}`, { signal: AbortSignal.timeout(3000) });
      const d = await r.json();
      const viewers = (d.consumers || []).filter((c) => !(/^FFmpeg Frigate/.test(c.user_agent || '') && /^127\.0\.0\.1:\d+$/.test(c.remote_addr || '')));
      const now = Date.now(), seen = new Set();
      for (const v of viewers) {
        seen.add(v.id);
        if (cam.viewerIds.has(v.id)) continue;
        const asked = !cam.active && now - cam.sleptAt >= COOLDOWN_MS;
        cam.viewerIds.set(v.id, asked);
        log(`${cam.name}: viewer connected (${v.format_name || v.protocol} ${v.remote_addr || ''}) - ${asked ? 'waking the camera' : cam.active ? 'joined while live, does not extend it' : 'ignored, camera just went to sleep'}`);
      }
      for (const id of [...cam.viewerIds.keys()]) if (!seen.has(id)) cam.viewerIds.delete(id);
      cam.viewers = viewers.length;
      if ([...cam.viewerIds.values()].some(Boolean)) trigger(cam, 'live view opened', LINGER_MS, 'view');
    } catch { /* Frigate restarting */ }
  }
}
setInterval(pollViewers, 2000);

// ---- housekeeping
setInterval(() => {
  const now = Date.now();
  arbitrate();
  for (const cam of cams) {
    if (!cam.active) continue;
    if (now - cam.startedAt >= MAX_MS) { cam.motionUntil = 0; stop(cam, `reached the ${MAX_MS / 60000}-minute limit`); continue; }
    if (!inMotion(cam) && now - Math.max(cam.startedAt, cam.motionUntil) >= VIEW_MAX_MS) { stop(cam, `viewer-only for ${VIEW_MAX_MS / 60000} minutes`); continue; }
    const quietSince = Math.max(cam.lastVideoAt, cam.lastStartAt);
    if (now - quietSince > 15000) {                       // camera did not wake, or the session died silently
      if (++cam.restarts > 3) { cam.motionUntil = 0; stop(cam, 'camera is not sending video'); continue; }
      log(`${cam.name}: no video for 15 s, asking eufy again (try ${cam.restarts})`);
      stopDecoder(cam);
      startLivestream(cam);
    }
  }
}, 1000);

let shuttingDown = false;
const shutdown = () => {
  shuttingDown = true;
  for (const c of cams) { c.motionUntil = 0; stop(c, 'bridge shutting down'); try { c.enc?.kill('SIGTERM'); } catch {} }
  setTimeout(() => process.exit(0), 1500);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

log(`eufy-frigate-battery-bridge starting: ${cams.map((c) => c.name).join(', ')}; hold ${HOLD_MS / 1000}s, viewer linger ${LINGER_MS / 1000}s, max ${MAX_MS / 1000}s`);
for (const c of cams) startEncoder(c);
connectWs();
