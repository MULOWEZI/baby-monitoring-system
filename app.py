#!/usr/bin/env python3

Must run before ANY other import — requests/supabase/ssl/socket/threading

all need to be patched before they're first imported, or gevent silently

mixes patched and unpatched networking primitives (no exception raised,

calls just vanish). This must stay the first code in the file.

from gevent import monkey
monkey.patch_all()

import os
import sys
import time
import queue
import threading
import logging
import hmac
from datetime import datetime

import requests
from flask import Flask, render_template, request, jsonify, Response
from flask_socketio import SocketIO
from dotenv import load_dotenv

============================================================

SETUP

============================================================

logging.basicConfig(level=logging.INFO, stream=sys.stdout,
format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(name)

load_dotenv()

app = Flask(name)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "baby-monitor-secret-key")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="gevent")

============================================================

CONFIG (read once, not per-request)

============================================================

TEMP_MIN = float(os.getenv("TEMP_MIN", 20))
TEMP_MAX = float(os.getenv("TEMP_MAX", 25))
HUMIDITY_MIN = float(os.getenv("HUMIDITY_MIN", 40))
HUMIDITY_MAX = float(os.getenv("HUMIDITY_MAX", 60))
DASHBOARD_PUSH_INTERVAL = float(os.getenv("DASHBOARD_PUSH_INTERVAL", 5))

Debounce: a reading must be abnormal for this many CONSECUTIVE ingests

before it's treated as a real state change. Filters single noisy/flickering

sensor readings so they don't fire an alert on their own.

ALERT_DEBOUNCE_READINGS = int(os.getenv("ALERT_DEBOUNCE_READINGS", 2))

Email policy: one email for each confirmed event, with a 10-minute

safety cooldown. The event must recover before another email of the

same type can be generated.

ALERT_EMAIL_COOLDOWN_SECONDS = float(
os.getenv("ALERT_EMAIL_COOLDOWN_SECONDS", 600)
)

BIRD_API_KEY = os.getenv("BIRD_API_KEY", "").strip()
BIRD_SENDER = os.getenv("BIRD_SENDER", "onboarding@messagebird.dev").strip()
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "").strip()
EMAIL_TEST_TOKEN = os.getenv("EMAIL_TEST_TOKEN", "").strip()

============================================================

SUPABASE

============================================================

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
supabase = None

if SUPABASE_URL and SUPABASE_KEY:
try:
from supabase import create_client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
log.info("Supabase client connected: %s...", SUPABASE_URL[:30])
except Exception as e:
log.warning("Supabase initialization failed: %s", e)
else:
log.warning("SUPABASE_URL/KEY not set — running without database")

============================================================

SHARED STATE

============================================================

current_data = {
"temperature": 0, "humidity": 0, "motion": False,
"sound": 0, "wetness": False, "last_update": None,
}
_data_lock = threading.Lock()
_last_broadcast_update = None  # tracks last_update value already pushed to dashboard

Video: latest frame + live subscriber queues (unchanged — this stays event-driven)

latest_frame = None
_frame_lock = threading.Lock()
_frame_subscribers = []
_subscribers_lock = threading.Lock()

Reading processing: single worker consumes a queue instead of spawning

a new thread per ingest call, so alerts/DB writes stay immediate without

risking thread explosion under high-frequency sensor posts.

_reading_queue = queue.Queue()

alert_state_lock = threading.Lock()
previous_wetness = False               # debounced/confirmed state
previous_temperature_abnormal = False  # debounced/confirmed state
_wetness_streak = 0                    # consecutive readings disagreeing with confirmed state
_temp_streak = 0
_last_email_sent = {}                  # alert_type -> last successful email timestamp
_email_alert_active = {
"temperature": False,
"wetness": False,
}

def _broadcast_frame(frame):
with _subscribers_lock:
for q in _frame_subscribers:
try:
q.put_nowait(frame)
except queue.Full:
try:
q.get_nowait()
except queue.Empty:
pass
try:
q.put_nowait(frame)
except queue.Full:
pass

============================================================

EMAIL ALERTS

============================================================

def bird_host():
"""
Bird API keys contain their region:
bk_us1_... -> https://us1.platform.bird.com
bk_eu1_... -> https://eu1.platform.bird.com
"""

parts = BIRD_API_KEY.split("_")

if len(parts) >= 2 and parts[0] == "bk" and parts[1]:
    return f"https://{parts[1]}.platform.bird.com"

# Safe fallback. The normal production key should always contain
# the region in the bk_<region>_... format.
return "https://us1.platform.bird.com"

def _validate_email_config():
"""Log configuration status without exposing secrets."""

if not BIRD_API_KEY:
    log.error("EMAIL CONFIG ERROR: BIRD_API_KEY is missing")

else:
    masked = (
        BIRD_API_KEY[:8] + "..." + BIRD_API_KEY[-4:]
        if len(BIRD_API_KEY) > 12
        else "***"
    )
    log.info(
        "Bird API key configured: %s | host=%s",
        masked,
        bird_host()
    )

if not ALERT_EMAIL:
    log.error("EMAIL CONFIG ERROR: ALERT_EMAIL is missing")
else:
    log.info(
        "Alert recipient configured: %s",
        ALERT_EMAIL
    )

log.info(
    "Bird sender configured: %s",
    BIRD_SENDER
)

def send_alert_email(alerts, recipient=None):
"""
Send an email through Bird.

Returns:
    True  = Bird accepted the message (HTTP 202/200)
    False = Bird rejected it or configuration is missing

The cooldown timestamp is deliberately NOT updated here. The caller
updates it only after this function returns True.
"""

if not alerts:
    log.warning("EMAIL SKIPPED: no alerts supplied")
    return False

to_email = (recipient or ALERT_EMAIL).strip()

if not BIRD_API_KEY:
    log.error(
        "EMAIL NOT SENT: BIRD_API_KEY is not configured"
    )
    return False

if not to_email:
    log.error(
        "EMAIL NOT SENT: ALERT_EMAIL is not configured"
    )
    return False

alert_types = {
    a.get("alert_type")
    for a in alerts
}

if "wetness" in alert_types:
    subject = "Wet Diaper Detected"
elif "temperature" in alert_types:
    subject = "Temperature Alert"
else:
    subject = "Baby Monitoring Alert"

items = "".join(
    f"<li><strong>{a.get('severity', 'warning').upper()}</strong> "
    f"— {a.get('message', '')}</li>"
    for a in alerts
)

timestamp = datetime.now().strftime(
    "%Y-%m-%d %H:%M:%S"
)

html = f"""
<html>
  <body>
    <h2>Baby Cradle Monitoring Alert</h2>
    <p>A new condition requiring attention was detected.</p>
    <p><strong>Time:</strong> {timestamp}</p>
    <ul>{items}</ul>
    <p>Please check the baby monitoring dashboard.</p>
    <p>
      <a href="https://baby-monitoring-system.onrender.com">
        Open Baby Monitoring Dashboard
      </a>
    </p>
  </body>
</html>
"""

text_body = (
    "Baby Cradle Monitoring Alert\n\n"
    f"Time: {timestamp}\n\n"
    + "\n".join(
        f"- {a.get('message', '')}"
        for a in alerts
    )
    + "\n\nOpen the dashboard: "
    "https://baby-monitoring-system.onrender.com"
)

payload = {
    "from": BIRD_SENDER,
    "to": [to_email],
    "subject": subject,
    "html": html,
    "text": text_body,
    # Alerts are operational/transactional messages.
    "category": "transactional",
}

url = f"{bird_host()}/v1/email/messages"

log.warning(
    "BIRD EMAIL ATTEMPT: host=%s sender=%s recipient=%s",
    bird_host(),
    BIRD_SENDER,
    to_email
)

try:

    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {BIRD_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=20,
    )

    log.warning(
        "BIRD RESPONSE: status=%s body=%s",
        response.status_code,
        response.text[:1000]
    )

    if response.status_code in (200, 202):

        try:
            body = response.json()
            message_id = body.get("id")
            status = body.get("status")
        except Exception:
            message_id = None
            status = None

        log.warning(
            "BIRD EMAIL ACCEPTED: message_id=%s status=%s",
            message_id,
            status
        )

        return True

    log.error(
        "BIRD EMAIL REJECTED: HTTP %s",
        response.status_code
    )

    return False

except requests.RequestException as e:

    log.exception(
        "BIRD EMAIL NETWORK ERROR: %s",
        e
    )

    return False

except Exception as e:

    log.exception(
        "BIRD EMAIL UNEXPECTED ERROR: %s",
        e
    )

    return False

def _send_email_background(alerts, alert_types=None):
"""
Send the email outside the sensor worker.

The last-email timestamp is updated ONLY when Bird accepts the
request. This means a failed send does not consume the cooldown.
"""

log.warning(
    "EMAIL WORKER STARTED: %s",
    [a.get("alert_type") for a in alerts]
)

success = send_alert_email(alerts)

if success:

    now = time.time()

    with alert_state_lock:

        for alert in alerts:

            alert_type = alert.get("alert_type")

            if alert_type:
                _last_email_sent[alert_type] = now

    log.warning(
        "EMAIL DELIVERY HANDOFF SUCCESSFUL"
    )

else:

    log.error(
        "EMAIL DELIVERY HANDOFF FAILED — cooldown was NOT consumed"
    )

_validate_email_config()

============================================================

THRESHOLD / ALERT LOGIC

============================================================

def check_abnormal(temp, hum):
if temp is not None and (temp < TEMP_MIN or temp > TEMP_MAX):
return True
if hum is not None and (hum < HUMIDITY_MIN or hum > HUMIDITY_MAX):
return True
return False

def check_alerts(temp, hum, wetness, sound):
"""
Alert logic:

  - A state transition must be confirmed by two consecutive readings.
  - One email is generated when a NEW abnormal event starts.
  - No repeated email while the event remains active.
  - Recovery resets the event.
  - A later event can generate a new email.
  - A 10-minute safety cooldown protects against rapid oscillation.
"""

global previous_wetness, previous_temperature_abnormal
global _wetness_streak, _temp_streak

raw_temperature_abnormal = (
    temp is not None
    and (temp < TEMP_MIN or temp > TEMP_MAX)
)

raw_wetness = bool(wetness)

alerts = []
now = time.time()

with alert_state_lock:

    # --------------------------------------------------------
    # TEMPERATURE
    # --------------------------------------------------------

    old_temperature_state = previous_temperature_abnormal

    if raw_temperature_abnormal != old_temperature_state:
        _temp_streak += 1
    else:
        _temp_streak = 0

    if _temp_streak >= ALERT_DEBOUNCE_READINGS:

        previous_temperature_abnormal = raw_temperature_abnormal
        _temp_streak = 0

        # NEW abnormal event only.
        if (
            not old_temperature_state
            and raw_temperature_abnormal
        ):

            last_sent = _last_email_sent.get(
                "temperature",
                0
            )

            if now - last_sent >= ALERT_EMAIL_COOLDOWN_SECONDS:

                if temp > TEMP_MAX:
                    message = (
                        f"Temperature is too high: {temp}°C. "
                        f"Configured maximum is {TEMP_MAX}°C."
                    )

                else:
                    message = (
                        f"Temperature is too low: {temp}°C. "
                        f"Configured minimum is {TEMP_MIN}°C."
                    )

                alerts.append({
                    "alert_type": "temperature",
                    "severity": "critical",
                    "message": message,
                })

                log.warning(
                    "NEW TEMPERATURE EVENT CONFIRMED: %s",
                    message
                )

            else:

                log.warning(
                    "Temperature event confirmed but safety "
                    "cooldown is active."
                )

        elif (
            old_temperature_state
            and not raw_temperature_abnormal
        ):

            log.info(
                "Temperature recovered."
            )

    # --------------------------------------------------------
    # WETNESS
    # --------------------------------------------------------

    old_wetness_state = previous_wetness

    if raw_wetness != old_wetness_state:
        _wetness_streak += 1
    else:
        _wetness_streak = 0

    if _wetness_streak >= ALERT_DEBOUNCE_READINGS:

        previous_wetness = raw_wetness
        _wetness_streak = 0

        # NEW wetness event only.
        if not old_wetness_state and raw_wetness:

            last_sent = _last_email_sent.get(
                "wetness",
                0
            )

            if now - last_sent >= ALERT_EMAIL_COOLDOWN_SECONDS:

                alerts.append({
                    "alert_type": "wetness",
                    "severity": "critical",
                    "message": (
                        "Diaper is wet! "
                        "Please change the diaper."
                    ),
                })

                log.warning(
                    "NEW WETNESS EVENT CONFIRMED"
                )

            else:

                log.warning(
                    "Wetness event confirmed but safety "
                    "cooldown is active."
                )

        elif old_wetness_state and not raw_wetness:

            log.info(
                "Wetness recovered."
            )

if not alerts:
    return

# Dashboard + Supabase alert record.
for alert in alerts:

    log.warning(
        "NEW ALERT: %s",
        alert["message"]
    )

    socketio.emit(
        "new_alert",
        alert
    )

    if supabase is not None:

        try:

            supabase.table(
                "alerts"
            ).insert(
                alert
            ).execute()

        except Exception as e:

            log.error(
                "Supabase alert insert error: %s",
                e
            )

# Flask-SocketIO/gevent-compatible background task.
socketio.start_background_task(
    _send_email_background,
    alerts
)

log.warning(
    "EMAIL QUEUED FOR ALERT EVENT"
)

============================================================

READING WORKER (single background consumer)

============================================================

def _reading_worker():
while True:
temp, hum, motion, sound, wetness = _reading_queue.get()
try:
if supabase is not None:
reading = {
"temperature": temp, "humidity": hum,
"motion_detected": motion, "sound_level": sound,
"wetness_detected": wetness,
"is_abnormal": check_abnormal(temp, hum),
}
try:
supabase.table("sensor_readings").insert(reading).execute()
except Exception as e:
log.error("DB insert error: %s", e)

        check_alerts(temp, hum, wetness, sound)
    finally:
        _reading_queue.task_done()

threading.Thread(target=_reading_worker, daemon=True).start()

============================================================

DASHBOARD BROADCASTER — pushes sensor_update at most every

DASHBOARD_PUSH_INTERVAL seconds, regardless of ingest rate.

Video is intentionally NOT throttled — it stays event-driven/live.

============================================================

def _dashboard_broadcaster():
global _last_broadcast_update
while True:
socketio.sleep(DASHBOARD_PUSH_INTERVAL)
with _data_lock:
if current_data["last_update"] == _last_broadcast_update:
continue  # no new reading since last push, skip
snapshot = dict(current_data)
_last_broadcast_update = current_data["last_update"]
socketio.emit("sensor_update", snapshot)

============================================================

PAGES

============================================================

@app.route("/")
def index():
return render_template("index.html")

@app.route("/live")
def live():
return render_template("live.html")

@app.route("/history")
def history():
return render_template("history.html")

============================================================

API — CURRENT DATA / HISTORY / ALERTS

============================================================

@app.route("/api/current_data")
def api_current_data():
with _data_lock:
return jsonify(current_data)

@app.route("/api/history")
def api_history():
if supabase is None:
return jsonify([])
limit = request.args.get("limit", 100, type=int)
try:
response = (supabase.table("sensor_readings").select("*")
.order("created_at", desc=True).limit(limit).execute())
return jsonify(response.data)
except Exception as e:
log.error("History error: %s", e)
return jsonify({"error": str(e)}), 500

@app.route("/api/alerts")
def api_alerts():
if supabase is None:
return jsonify([])
limit = request.args.get("limit", 50, type=int)
try:
response = (supabase.table("alerts").select("*")
.order("created_at", desc=True).limit(limit).execute())
return jsonify(response.data)
except Exception as e:
log.error("Alert history error: %s", e)
return jsonify({"error": str(e)}), 500

@app.route("/api/clear_alerts", methods=["POST"])
def clear_alerts():
if supabase is None:
return jsonify({"success": True})
try:
supabase.table("alerts").update({"is_read": True}).neq("is_read", True).execute()
return jsonify({"success": True})
except Exception as e:
return jsonify({"error": str(e)}), 500

============================================================

EMAIL TEST

============================================================

@app.route("/api/test-email", methods=["POST"])
def api_test_email():

"""
Test Bird directly from Render.

Render environment variable required:
    EMAIL_TEST_TOKEN=<your secret>

Header:
    X-Email-Test-Token: <your secret>

Optional JSON:
    {"recipient": "delivered@messagebird.dev"}

The Bird sandbox recipient delivered@messagebird.dev is useful
for proving that the API pipeline works without depending on
the user's mailbox.
"""

if not EMAIL_TEST_TOKEN:

    log.error(
        "TEST EMAIL DISABLED: EMAIL_TEST_TOKEN is not configured"
    )

    return jsonify({
        "ok": False,
        "error": "EMAIL_TEST_TOKEN is not configured on Render."
    }), 503

supplied_token = request.headers.get(
    "X-Email-Test-Token",
    ""
)

if not hmac.compare_digest(
    supplied_token,
    EMAIL_TEST_TOKEN
):

    return jsonify({
        "ok": False,
        "error": "Unauthorized."
    }), 401

data = request.get_json(
    silent=True
) or {}

recipient = (
    data.get("recipient")
    or ALERT_EMAIL
).strip()

if not recipient:

    return jsonify({
        "ok": False,
        "error": "No recipient configured."
    }), 400

test_alert = [{
    "alert_type": "test",
    "severity": "info",
    "message": "Baby Monitor email test from Render."
}]

socketio.start_background_task(
    _test_email_background,
    test_alert,
    recipient
)

log.warning(
    "TEST EMAIL QUEUED: recipient=%s",
    recipient
)

return jsonify({
    "ok": True,
    "message": "Test email queued. Check Render logs."
}), 202

def _test_email_background(alerts, recipient):

log.warning(
    "TEST EMAIL WORKER STARTED: recipient=%s",
    recipient
)

success = send_alert_email(
    alerts,
    recipient=recipient
)

if success:

    log.warning(
        "TEST EMAIL ACCEPTED BY BIRD"
    )

else:

    log.error(
        "TEST EMAIL FAILED"
    )

============================================================

RASPBERRY PI INGEST

current_data is updated immediately (so /api/current_data is

always fresh), DB write + alert checks happen on the worker

thread immediately, but the dashboard socket push is

throttled to once every DASHBOARD_PUSH_INTERVAL seconds by

_dashboard_broadcaster above.

============================================================

@app.route("/api/ingest", methods=["POST"])
def api_ingest():
data = request.get_json(silent=True) or {}

if "temperature" not in data or "humidity" not in data:
    return jsonify({"error": "Missing required fields: temperature, humidity"}), 400

temp = data.get("temperature")
hum = data.get("humidity")
motion = data.get("motion_detected", False)
sound = data.get("sound_level", 0)
wetness = data.get("wetness_detected", False)

with _data_lock:
    current_data.update({
        "temperature": temp, "humidity": hum, "motion": motion,
        "sound": sound, "wetness": wetness,
        "last_update": datetime.now().isoformat(),
    })

_reading_queue.put((temp, hum, motion, sound, wetness))

return jsonify({"status": "ok", "abnormal": check_abnormal(temp, hum)})

============================================================

VIDEO — FRAME UPLOAD (unthrottled, immediate broadcast)

============================================================

@app.route("/api/upload_frame", methods=["POST"])
def api_upload_frame():
global latest_frame

data = request.get_data()
if not data or len(data) < 100:
    return jsonify({"error": "Empty or invalid frame"}), 400

with _frame_lock:
    latest_frame = data

_broadcast_frame(data)
return jsonify({"status": "ok"})

============================================================

VIDEO — LIVE MJPEG STREAM (event-driven, stays real-time)

============================================================

@app.route("/video_feed")
def video_feed():
def generate():
q = queue.Queue(maxsize=1)
with _subscribers_lock:
_frame_subscribers.append(q)
try:
with _frame_lock:
if latest_frame is not None:
yield (b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + latest_frame + b"\r\n")
while True:
frame = q.get()
yield (b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + frame + b"\r\n")
finally:
with _subscribers_lock:
if q in _frame_subscribers:
_frame_subscribers.remove(q)

return Response(generate(), mimetype="multipart/x-mixed-replace; boundary=frame")

============================================================

CHATBOT

============================================================

@app.route("/api/chat", methods=["POST"])
def api_chat():
data = request.get_json(silent=True) or {}
msg = (data.get("message") or "").lower().strip()

with _data_lock:
    d = dict(current_data)

temp = d.get("temperature") if d.get("temperature") is not None else "--"
hum = d.get("humidity") if d.get("humidity") is not None else "--"
motion_str = "moving" if d.get("motion") else "quiet/sleeping"
diaper_str = "wet — needs changing" if d.get("wetness") else "dry"

if any(w in msg for w in ["temp", "hot", "cold", "warm"]):
    reply = f"The current temperature is {temp}°C. Safe range is {TEMP_MIN}–{TEMP_MAX}°C."
    if temp != "--":
        if temp < TEMP_MIN:
            reply += " It's **below** the minimum."
        elif temp > TEMP_MAX:
            reply += " It's **above** the maximum."
        else:
            reply += " This is within the normal range."

elif any(w in msg for w in ["humid", "moist"]):
    reply = f"The current humidity is {hum}%. Safe range is {HUMIDITY_MIN}–{HUMIDITY_MAX}%."
    if hum != "--":
        if hum < HUMIDITY_MIN:
            reply += " It's **below** the minimum."
        elif hum > HUMIDITY_MAX:
            reply += " It's **above** the maximum."
        else:
            reply += " This is within the normal range."

elif any(w in msg for w in ["motion", "move", "moving", "activity", "active"]):
    reply = f"Baby is currently **{motion_str}**."
    reply += " Recent motion was detected." if d.get("motion") else " No recent motion was detected."

elif any(w in msg for w in ["sound", "noise", "loud", "cry", "crying"]):
    if d.get("sound"):
        reply = "Sound level is currently **loud/noisy**. This may indicate crying or a loud environment."
    else:
        reply = "Sound level is currently **quiet**. No loud sounds detected."

elif any(w in msg for w in ["diaper", "wet", "wee", "nappy", "change"]):
    reply = f"Diaper is **{diaper_str}**."
    reply += " It's time for a change!" if d.get("wetness") else " All good, no change needed."

elif any(w in msg for w in ["hi", "hello", "hey", "help"]):
    reply = ("Hello! I'm your Baby Cradle Monitoring assistant. Ask about "
             "**temperature**, **humidity**, **motion**, **sound**, or **diaper**.")

elif any(w in msg for w in ["status", "summary", "all", "overview"]):
    flags = []
    if d.get("motion"):
        flags.append("motion detected")
    if d.get("wetness"):
        flags.append("wet diaper")
    if check_abnormal(temp if temp != "--" else None, hum if hum != "--" else None):
        flags.append("⚠️ abnormal readings")

    reply = (f"**Temperature:** {temp}°C  |  **Humidity:** {hum}%  |  "
             f"**Motion:** {motion_str}  |  "
             f"**Sound:** {'loud' if d.get('sound') else 'quiet'}  |  "
             f"**Diaper:** {diaper_str}")
    if flags:
        reply += f"\n\nNotable: {' · '.join(flags)}"

else:
    reply = ("I can answer about: **temperature**, **humidity**, **motion**, "
             "**sound**, **diaper**, or say **status** for a full summary.")

return jsonify({"reply": reply})

============================================================

START SERVER

============================================================

socketio.start_background_task(_dashboard_broadcaster)

if name == "main":
port = int(os.getenv("PORT", 5000))
debug = os.getenv("RENDER") is None
log.info(
"Baby Cradle Monitoring Server starting on port %s...",
port
)
_validate_email_config()
socketio.run(app, host="0.0.0.0", port=port, debug=debug, allow_unsafe_werkzeug=True)
