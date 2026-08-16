#!/usr/bin/env python3
# Must run before ANY other import — requests/supabase/ssl/socket/threading
# all need to be patched before they're first imported, or gevent silently
# mixes patched and unpatched networking primitives (no exception raised,
# calls just vanish). This must stay the first code in the file.
from gevent import monkey
monkey.patch_all()

import os
import sys
import time
import queue
import threading
import logging
from datetime import datetime

import requests
from flask import Flask, render_template, request, jsonify, Response
from flask_socketio import SocketIO
from dotenv import load_dotenv

# ============================================================
# SETUP
# ============================================================
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                     format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

load_dotenv()

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "baby-monitor-secret-key")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="gevent")

# ============================================================
# CONFIG (read once, not per-request)
# ============================================================
TEMP_MIN = float(os.getenv("TEMP_MIN", 20))
TEMP_MAX = float(os.getenv("TEMP_MAX", 25))
HUMIDITY_MIN = float(os.getenv("HUMIDITY_MIN", 40))
HUMIDITY_MAX = float(os.getenv("HUMIDITY_MAX", 60))
DASHBOARD_PUSH_INTERVAL = float(os.getenv("DASHBOARD_PUSH_INTERVAL", 5))

# Debounce: a reading must be abnormal for this many CONSECUTIVE ingests
# before it's treated as a real state change. Filters single noisy/flickering
# sensor readings so they don't fire an alert on their own.
ALERT_DEBOUNCE_READINGS = int(os.getenv("ALERT_DEBOUNCE_READINGS", 2))

# Cooldown: hard floor on how often an email can go out for the SAME alert
# type, regardless of how the state flips. Protects against rapid toggling
# and against Render restarts resetting in-memory state and re-firing.
ALERT_EMAIL_COOLDOWN_SECONDS = float(os.getenv("ALERT_EMAIL_COOLDOWN_SECONDS", 180))

BIRD_API_KEY = os.getenv("BIRD_API_KEY", "")
BIRD_SENDER = os.getenv("BIRD_SENDER", "onboarding@messagebird.dev")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")

# ============================================================
# SUPABASE
# ============================================================
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

# ============================================================
# SHARED STATE
# ============================================================
current_data = {
    "temperature": 0, "humidity": 0, "motion": False,
    "sound": 0, "wetness": False, "last_update": None,
}
_data_lock = threading.Lock()
_last_broadcast_update = None  # tracks last_update value already pushed to dashboard

# Video: latest frame + live subscriber queues (unchanged — this stays event-driven)
latest_frame = None
_frame_lock = threading.Lock()
_frame_subscribers = []
_subscribers_lock = threading.Lock()

# Reading processing: single worker consumes a queue instead of spawning
# a new thread per ingest call, so alerts/DB writes stay immediate without
# risking thread explosion under high-frequency sensor posts.
_reading_queue = queue.Queue()

alert_state_lock = threading.Lock()
previous_wetness = False               # debounced/confirmed state
previous_temperature_abnormal = False  # debounced/confirmed state
_wetness_streak = 0                    # consecutive readings disagreeing with confirmed state
_temp_streak = 0
_last_email_sent = {}                  # alert_type -> unix timestamp of last email sent


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


# ============================================================
# EMAIL ALERTS
# ============================================================
def bird_host():
    parts = BIRD_API_KEY.split("_")
    region = parts[1] if len(parts) > 1 and parts[1] else "us1"
    return f"https://{region}.platform.bird.com"


def send_alert_email(alerts):
    if not alerts or not BIRD_API_KEY or not ALERT_EMAIL:
        if alerts and not BIRD_API_KEY:
            log.warning("BIRD_API_KEY not set — skipping email notification")
        if alerts and not ALERT_EMAIL:
            log.warning("ALERT_EMAIL not set — skipping email notification")
        return False

    alert_types = {a.get("alert_type") for a in alerts}
    if "wetness" in alert_types:
        subject = "💧 Wet Diaper Detected"
    elif "temperature" in alert_types:
        subject = "🌡️ Temperature Alert"
    else:
        subject = "🚼 Baby Monitoring Alert"

    items = "".join(
        f"<li><strong>{a.get('severity', 'warning').upper()}</strong> — {a.get('message', '')}</li>"
        for a in alerts
    )
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html = f"""
    <html><body>
        <h2>🚼 Baby Cradle Monitoring Alert</h2>
        <p>A new condition requiring attention was detected.</p>
        <p><strong>Time:</strong> {timestamp}</p>
        <ul>{items}</ul>
        <p>Please check the baby monitoring dashboard.</p>
        <p><a href="https://baby-monitoring-system.onrender.com">Open Baby Monitoring Dashboard</a></p>
    </body></html>
    """
    payload = {"from": BIRD_SENDER, "to": [ALERT_EMAIL], "subject": subject, "html": html}

    try:
        response = requests.post(
            f"{bird_host()}/v1/email/messages",
            headers={"Authorization": f"Bearer {BIRD_API_KEY}", "Content-Type": "application/json"},
            json=payload, timeout=15,
        )
        if response.status_code in (200, 202):
            log.info("Alert email sent to %s (%s)", ALERT_EMAIL, response.status_code)
            return True
        log.error("Bird email failed %s: %s", response.status_code, response.text[:300])
    except Exception as e:
        log.error("Bird email exception: %s", e)
    return False


# ============================================================
# THRESHOLD / ALERT LOGIC
# ============================================================
def check_abnormal(temp, hum):
    if temp is not None and (temp < TEMP_MIN or temp > TEMP_MAX):
        return True
    if hum is not None and (hum < HUMIDITY_MIN or hum > HUMIDITY_MAX):
        return True
    return False


def check_alerts(temp, hum, wetness, sound):
    """
    Alerting model:
      - A state change (normal->abnormal or abnormal->normal) is only
        confirmed after ALERT_DEBOUNCE_READINGS consecutive raw readings
        agree — filters a single noisy/flickering reading.
      - Once confirmed abnormal, an alert fires immediately, then fires
        again every ALERT_EMAIL_COOLDOWN_SECONDS for as long as the
        condition remains abnormal (a repeating reminder, not one-and-done).
      - The cooldown is a strict timer with no early reset: even if the
        condition briefly recovers and re-triggers (sensor noise around
        the threshold), the next alert still waits out the full cooldown
        from the last one sent, so boundary jitter can't cause rapid-fire
        emails.
    Same logic applies independently to temperature and wetness.
    """
    global previous_wetness, previous_temperature_abnormal
    global _wetness_streak, _temp_streak

    raw_temperature_abnormal = temp is not None and (temp < TEMP_MIN or temp > TEMP_MAX)
    raw_wetness = bool(wetness)
    alerts = []
    now = time.time()

    with alert_state_lock:
        # ---- temperature: debounce raw reading against confirmed state ----
        if raw_temperature_abnormal != previous_temperature_abnormal:
            _temp_streak += 1
        else:
            _temp_streak = 0

        if _temp_streak >= ALERT_DEBOUNCE_READINGS:
            previous_temperature_abnormal = raw_temperature_abnormal
            _temp_streak = 0

        if previous_temperature_abnormal:
            last_sent = _last_email_sent.get("temperature", 0)
            if now - last_sent >= ALERT_EMAIL_COOLDOWN_SECONDS:
                if temp > TEMP_MAX:
                    message = f"🌡️ Temperature is too high: {temp}°C. Configured maximum is {TEMP_MAX}°C."
                elif temp < TEMP_MIN:
                    message = f"🌡️ Temperature is too low: {temp}°C. Configured minimum is {TEMP_MIN}°C."
                else:
                    message = f"🌡️ Abnormal temperature detected: {temp}°C."
                alerts.append({"alert_type": "temperature", "severity": "critical", "message": message})
                _last_email_sent["temperature"] = now

        # ---- wetness: same debounce + repeat pattern ----
        if raw_wetness != previous_wetness:
            _wetness_streak += 1
        else:
            _wetness_streak = 0

        if _wetness_streak >= ALERT_DEBOUNCE_READINGS:
            previous_wetness = raw_wetness
            _wetness_streak = 0

        if previous_wetness:
            last_sent = _last_email_sent.get("wetness", 0)
            if now - last_sent >= ALERT_EMAIL_COOLDOWN_SECONDS:
                alerts.append({
                    "alert_type": "wetness", "severity": "critical",
                    "message": "💧 Diaper is wet! Please change the diaper.",
                })
                _last_email_sent["wetness"] = now

    if not alerts:
        return

    for alert in alerts:
        log.info("NEW ALERT: %s", alert["message"])
        socketio.emit("new_alert", alert)

        if supabase is not None:
            try:
                supabase.table("alerts").insert(alert).execute()
            except Exception as e:
                log.error("Supabase alert insert error: %s", e)

    send_alert_email(alerts)


# ============================================================
# SUPABASE SENSOR READING
# ============================================================
def save_sensor_reading(temp, hum, motion, sound, wetness):
    """
    Save one complete sensor reading to Supabase synchronously.

    The ingest endpoint uses this function before returning success.
    This prevents the API from reporting success when the database
    write has actually failed in a background thread.
    """
    if supabase is None:
        raise RuntimeError(
            "Supabase is not configured. Set SUPABASE_URL and SUPABASE_KEY."
        )

    reading = {
        "temperature": temp,
        "humidity": hum,
        "motion_detected": bool(motion),
        "sound_level": sound,
        "wetness_detected": bool(wetness),
        "is_abnormal": check_abnormal(temp, hum),
    }

    response = supabase.table("sensor_readings").insert(reading).execute()

    if not response.data:
        raise RuntimeError("Supabase did not return the inserted sensor reading.")

    log.info(
        "Sensor reading saved to Supabase: temp=%s, humidity=%s, "
        "motion=%s, sound=%s, wetness=%s",
        temp, hum, motion, sound, wetness
    )

    return response.data[0]


# ============================================================
# READING WORKER (alert processing)
# ============================================================
def _reading_worker():
    while True:
        temp, hum, motion, sound, wetness = _reading_queue.get()
        try:
            # The sensor reading has already been saved synchronously
            # by /api/ingest. The worker is only responsible for alerts.
            check_alerts(temp, hum, wetness, sound)
        except Exception as e:
            log.error("Reading/alert worker error: %s", e)
        finally:
            _reading_queue.task_done()


threading.Thread(target=_reading_worker, daemon=True).start()


# ============================================================
# DASHBOARD BROADCASTER — pushes sensor_update at most every
# DASHBOARD_PUSH_INTERVAL seconds, regardless of ingest rate.
# Video is intentionally NOT throttled — it stays event-driven/live.
# ============================================================
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


# ============================================================
# PAGES
# ============================================================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/live")
def live():
    return render_template("live.html")


@app.route("/history")
def history():
    return render_template("history.html")


# ============================================================
# API — DATABASE HEALTH CHECK
# ============================================================
@app.route("/api/db_health")
def api_db_health():
    """Verify that the backend can communicate with Supabase."""
    if supabase is None:
        return jsonify({
            "status": "error",
            "database": "not_configured",
            "message": "SUPABASE_URL or SUPABASE_KEY is missing"
        }), 503

    try:
        response = (
            supabase.table("sensor_readings")
            .select("id")
            .limit(1)
            .execute()
        )

        return jsonify({
            "status": "ok",
            "database": "connected",
            "table": "sensor_readings",
            "message": "Backend can communicate with Supabase"
        }), 200

    except Exception as e:
        log.error("Supabase health check failed: %s", e)
        return jsonify({
            "status": "error",
            "database": "unavailable",
            "message": str(e)
        }), 503


# ============================================================
# API — CURRENT DATA / HISTORY / ALERTS
# ============================================================
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


# ============================================================
# RASPBERRY PI INGEST
# current_data is updated immediately (so /api/current_data is
# always fresh), DB write + alert checks happen on the worker
# thread immediately, but the *dashboard socket push* is
# throttled to once every DASHBOARD_PUSH_INTERVAL seconds by
# _dashboard_broadcaster above.
# ============================================================
@app.route("/api/ingest", methods=["POST"])
def api_ingest():
    """
    Receive one complete sensor reading.

    Flow:
        Raspberry Pi -> Flask -> validate -> Supabase -> dashboard
                                      |
                                      +-> alert worker

    The endpoint does NOT return success until Supabase has accepted
    the sensor reading. This makes database communication reliable.
    """
    data = request.get_json(silent=True) or {}

    if "temperature" not in data or "humidity" not in data:
        return jsonify({
            "status": "error",
            "error": "Missing required fields: temperature, humidity"
        }), 400

    try:
        temp = float(data.get("temperature"))
        hum = float(data.get("humidity"))
    except (TypeError, ValueError):
        return jsonify({
            "status": "error",
            "error": "temperature and humidity must be numeric"
        }), 400

    motion = bool(data.get("motion_detected", False))
    sound = data.get("sound_level", 0)
    wetness = bool(data.get("wetness_detected", False))

    # Keep sound numeric when possible, but don't reject an otherwise
    # valid reading if the sensor sends a value that is already usable.
    try:
        sound = float(sound)
    except (TypeError, ValueError):
        return jsonify({
            "status": "error",
            "error": "sound_level must be numeric"
        }), 400

    # ------------------------------------------------------------
    # 1. SAVE TO SUPABASE FIRST
    # ------------------------------------------------------------
    try:
        saved_reading = save_sensor_reading(
            temp, hum, motion, sound, wetness
        )
    except Exception as e:
        log.error("Supabase sensor reading save failed: %s", e)

        # Do not tell the Raspberry Pi/dashboard that the reading was
        # successfully stored when the database write failed.
        return jsonify({
            "status": "error",
            "error": "Sensor reading could not be saved to Supabase",
            "details": str(e)
        }), 503

    # ------------------------------------------------------------
    # 2. UPDATE CURRENT DASHBOARD STATE
    # ------------------------------------------------------------
    now_iso = datetime.now().isoformat()

    with _data_lock:
        current_data.update({
            "temperature": temp,
            "humidity": hum,
            "motion": motion,
            "sound": sound,
            "wetness": wetness,
            "last_update": now_iso,
        })
        dashboard_snapshot = dict(current_data)

    # ------------------------------------------------------------
    # 3. PUSH THE SAVED READING TO CONNECTED DASHBOARDS
    # ------------------------------------------------------------
    socketio.emit("sensor_update", dashboard_snapshot)

    # ------------------------------------------------------------
    # 4. PROCESS ALERTS
    # ------------------------------------------------------------
    _reading_queue.put((temp, hum, motion, sound, wetness))

    return jsonify({
        "status": "ok",
        "message": "Sensor reading saved to Supabase",
        "abnormal": check_abnormal(temp, hum),
        "reading": saved_reading
    }), 200


# ============================================================
# VIDEO — FRAME UPLOAD (unthrottled, immediate broadcast)
# ============================================================
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


# ============================================================
# VIDEO — LIVE MJPEG STREAM (event-driven, stays real-time)
# ============================================================
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


# ============================================================
# CHATBOT
# ============================================================
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


# ============================================================
# START SERVER
# ============================================================
socketio.start_background_task(_dashboard_broadcaster)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("RENDER") is None
    log.info("Baby Cradle Monitoring Server starting on port %s...", port)
    socketio.run(app, host="0.0.0.0", port=port, debug=debug, allow_unsafe_werkzeug=True)
