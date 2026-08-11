#!/usr/bin/env python3
import os
import sys
import time
import threading
import logging
import decimal
from datetime import datetime

import requests
import psycopg2

from flask import Flask, render_template, request, jsonify, Response
from flask_socketio import SocketIO
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
app.config["SECRET_KEY"] = "baby-monitor-secret-key"
socketio = SocketIO(app, cors_allowed_origins="*")

# Database via the Supabase connection pooler (direct Postgres)
DATABASE_URL = os.getenv("DATABASE_URL", "")


def db_query(sql, params=()):
    """Run a query against the Postgres pooler.

    Returns a list of dicts for SELECT, True for writes, None if DB is
    unavailable. A fresh connection per call keeps things thread-safe and
    plays well with Supabase's transaction pooler.
    """
    if not DATABASE_URL:
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if cur.description:
                    cols = [d[0] for d in cur.description]
                    return [
                        {k: (float(v) if isinstance(v, decimal.Decimal) else v)
                         for k, v in dict(zip(cols, row)).items()}
                        for row in cur.fetchall()
                    ]
                return True
        finally:
            conn.close()
    except Exception as e:
        log.error("DB error: %s", e)
        return None

# ---------------------------------------------------------------------------
# Global state — data is ONLY populated by POSTs from the Raspberry Pi
# ---------------------------------------------------------------------------
current_data = {
    "temperature": 0,
    "humidity": 0,
    "motion": False,
    "sound": 0,
    "wetness": False,
    "last_update": None,
}

# Latest JPEG frame uploaded by the Pi (served on /video_feed)
latest_frame = None
_frame_lock = threading.Lock()

# HLS segments uploaded by the Pi (served under /hls/<filename>)
hls_segments = {}
_hls_lock = threading.Lock()
HLS_MAX_SEGMENTS = 60


# ---------------------------------------------------------------------------
# Alert helpers
# ---------------------------------------------------------------------------
# Bird (MessageBird) email notifications
BIRD_API_KEY = os.getenv("BIRD_API_KEY", "")
BIRD_SENDER = "onboarding@messagebird.dev"
_last_email_ts = 0
EMAIL_COOLDOWN = 300  # seconds — at most one email per alert burst


def bird_host():
    """Derive the Bird platform host from the key's region prefix (bk_<region>_...)."""
    parts = BIRD_API_KEY.split("_")
    region = parts[1] if len(parts) > 1 and parts[1] else "us1"
    return f"https://{region}.platform.bird.com"


def send_alert_email(alerts):
    """Send an alert summary email via the Bird Email REST API (throttled)."""
    global _last_email_ts
    if not BIRD_API_KEY:
        log.warning("BIRD_API_KEY not set — skipping email notification")
        return False
    recipient = os.getenv("ALERT_EMAIL", "")
    if not recipient:
        log.warning("ALERT_EMAIL not set — skipping email notification")
        return False

    now = time.time()
    if now - _last_email_ts < EMAIL_COOLDOWN:
        return False

    items = "".join(
        f"<li><b>{a['severity'].upper()}</b> — {a['message']}</li>" for a in alerts
    )
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = {
        "from": BIRD_SENDER,
        "to": [recipient],
        "subject": f"🚼 Cradle Alert ({len(alerts)})",
        "html": (
            "<h2>🚼 Baby Cradle Alert</h2>"
            f"<p>Detected {len(alerts)} issue(s) at <b>{ts}</b>:</p>"
            f"<ul>{items}</ul>"
            f"<p>Live dashboard: <a href='https://baby-monitoring-system.onrender.com'>"
            "https://baby-monitoring-system.onrender.com</a></p>"
        ),
    }
    try:
        r = requests.post(
            f"{bird_host()}/v1/email/messages",
            headers={"Authorization": f"Bearer {BIRD_API_KEY}",
                     "Content-Type": "application/json"},
            json=payload,
            timeout=15,
        )
        if r.status_code in (200, 202):
            _last_email_ts = now
            log.info("Alert email sent to %s (%s)", recipient, r.status_code)
            return True
        log.error("Bird email failed %s: %s", r.status_code, r.text[:300])
    except Exception as e:
        log.error("Bird email exception: %s", e)
    return False


def check_abnormal(temp, hum):
    temp_min = float(os.getenv("TEMP_MIN", 20))
    temp_max = float(os.getenv("TEMP_MAX", 25))
    hum_min = float(os.getenv("HUMIDITY_MIN", 40))
    hum_max = float(os.getenv("HUMIDITY_MAX", 60))

    if temp and (temp < temp_min or temp > temp_max):
        return True
    if hum and (hum < hum_min or hum > hum_max):
        return True
    return False


def check_alerts(temp, hum, wetness):
    temp_min = float(os.getenv("TEMP_MIN", 20))
    temp_max = float(os.getenv("TEMP_MAX", 25))
    hum_min = float(os.getenv("HUMIDITY_MIN", 40))
    hum_max = float(os.getenv("HUMIDITY_MAX", 60))

    alerts = []

    if temp:
        if temp < temp_min:
            alerts.append({"alert_type": "temperature", "severity": "critical",
                           "message": f"⚠️ Temperature too low: {temp}°C"})
        elif temp > temp_max:
            alerts.append({"alert_type": "temperature", "severity": "critical",
                           "message": f"⚠️ Temperature too high: {temp}°C"})

    if hum:
        if hum < hum_min:
            alerts.append({"alert_type": "humidity", "severity": "warning",
                           "message": f"💧 Humidity too low: {hum}%"})
        elif hum > hum_max:
            alerts.append({"alert_type": "humidity", "severity": "warning",
                           "message": f"💧 Humidity too high: {hum}%"})

    if wetness:
        alerts.append({"alert_type": "wetness", "severity": "warning",
                       "message": "🚼 Diaper wetness detected!"})

    if not alerts:
        return

    for alert in alerts:
        db_query(
            "insert into alerts (alert_type, severity, message) values (%s, %s, %s)",
            (alert["alert_type"], alert["severity"], alert["message"]),
        )
        socketio.emit("new_alert", alert)
        log.info("Alert: %s", alert["message"])

    send_alert_email(alerts)


# ---------------------------------------------------------------------------
# Flask Routes
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/live")
def live():
    return render_template("live.html")


@app.route("/history")
def history():
    return render_template("history.html")


@app.route("/api/current_data")
def api_current_data():
    return jsonify(current_data)


@app.route("/api/history")
def api_history():
    limit = request.args.get("limit", 100, type=int)
    rows = db_query(
        "select * from sensor_readings order by created_at desc limit %s", (limit,)
    )
    if rows is None:
        return jsonify([])
    return jsonify(rows)


@app.route("/api/alerts")
def api_alerts():
    limit = request.args.get("limit", 50, type=int)
    rows = db_query(
        "select * from alerts order by created_at desc limit %s", (limit,)
    )
    if rows is None:
        return jsonify([])
    return jsonify(rows)


@app.route("/api/clear_alerts", methods=["POST"])
def clear_alerts():
    db_query("update alerts set is_read = true where is_read = false")
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Raspberry Pi ingestion endpoint — THE ONLY data source
# ---------------------------------------------------------------------------
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

    current_data["temperature"] = temp
    current_data["humidity"] = hum
    current_data["motion"] = motion
    current_data["sound"] = sound
    current_data["wetness"] = wetness
    current_data["last_update"] = datetime.now().isoformat()

    db_query(
        "insert into sensor_readings "
        "(temperature, humidity, motion_detected, sound_level, wetness_detected, is_abnormal) "
        "values (%s, %s, %s, %s, %s, %s)",
        (
            temp,
            hum,
            motion,
            sound,
            wetness,
            check_abnormal(temp, hum),
        ),
    )

    check_alerts(temp, hum, wetness)
    socketio.emit("sensor_update", current_data)

    return jsonify({"status": "ok", "abnormal": check_abnormal(temp, hum)})


# ---------------------------------------------------------------------------
# Video frame upload from the Pi + MJPEG streaming
# ---------------------------------------------------------------------------
@app.route("/api/upload_frame", methods=["POST"])
def api_upload_frame():
    """Receive a JPEG frame from the Pi camera."""
    global latest_frame
    data = request.get_data()
    if not data or len(data) < 100:
        return jsonify({"error": "Empty or invalid frame"}), 400
    with _frame_lock:
        latest_frame = data
    return jsonify({"status": "ok"})


@app.route("/video_feed")
def video_feed():
    def generate():
        last_served = None
        while True:
            with _frame_lock:
                frame = latest_frame
            if frame is not None and frame != last_served:
                last_served = frame
                yield (b"--frame\r\n"
                       b"Content-Type: image/jpeg\r\n\r\n" + frame + b"\r\n")
            time.sleep(0.2)
    return Response(generate(), mimetype="multipart/x-mixed-replace; boundary=frame")


# ---------------------------------------------------------------------------
# HLS live video (segments + playlist uploaded by the Pi)
# ---------------------------------------------------------------------------
@app.route("/api/hls_upload", methods=["POST"])
def api_hls_upload():
    """Store an HLS segment/playlist pushed by the Pi."""
    filename = request.args.get("filename", "")
    if not filename or ".." in filename or "/" in filename or "\\" in filename:
        return jsonify({"error": "bad filename"}), 400
    if not (filename.endswith(".m3u8") or filename.endswith(".ts")):
        return jsonify({"error": "unsupported file type"}), 400
    data = request.get_data()
    if not data:
        return jsonify({"error": "empty"}), 400
    with _hls_lock:
        hls_segments[filename] = data
        if len(hls_segments) > HLS_MAX_SEGMENTS:
            for k in [k for k in hls_segments if k.endswith(".ts")][: len(hls_segments) - HLS_MAX_SEGMENTS]:
                hls_segments.pop(k, None)
    return jsonify({"status": "ok"})


@app.route("/hls/<path:filename>")
def api_hls(filename):
    """Serve an HLS segment/playlist to the browser."""
    with _hls_lock:
        content = hls_segments.get(filename)
    if content is None:
        return ("", 404)
    ctype = ("application/vnd.apple.mpegurl"
             if filename.endswith(".m3u8") else "video/mp2t")
    return Response(content, mimetype=ctype)


# ---------------------------------------------------------------------------
# Chatbot / assistant endpoint
# ---------------------------------------------------------------------------
@app.route("/api/chat", methods=["POST"])
def api_chat():
    data = request.get_json(silent=True) or {}
    msg = (data.get("message") or "").lower().strip()
    d = current_data

    temp = d.get("temperature") or "--"
    hum = d.get("humidity") or "--"
    tmin = os.getenv("TEMP_MIN", "20")
    tmax = os.getenv("TEMP_MAX", "25")
    hmin = os.getenv("HUMIDITY_MIN", "40")
    hmax = os.getenv("HUMIDITY_MAX", "60")
    motion_str = "moving" if d.get("motion") else "quiet/sleeping"
    diaper_str = "wet — needs changing" if d.get("wetness") else "dry"

    if any(w in msg for w in ["temp", "hot", "cold", "warm"]):
        reply = f"The current temperature is {temp}°C. Safe range is {tmin}–{tmax}°C."
        if temp != "--":
            if temp < float(tmin):
                reply += " It's **below** the minimum — consider warming the room."
            elif temp > float(tmax):
                reply += " It's **above** the maximum — consider cooling the room."
            else:
                reply += " This is within the normal range."
    elif any(w in msg for w in ["humid", "moist"]):
        reply = f"The current humidity is {hum}%. Safe range is {hmin}–{hmax}%."
        if hum != "--":
            if hum < float(hmin):
                reply += " It's **below** the minimum — consider using a humidifier."
            elif hum > float(hmax):
                reply += " It's **above** the maximum — consider using a dehumidifier."
            else:
                reply += " This is within the normal range."
    elif any(w in msg for w in ["motion", "move", "moving", "activity", "active"]):
        reply = f"Baby is currently **{motion_str}**."
        reply += " Recent motion was detected." if d.get("motion") else " No recent motion — baby may be sleeping."
    elif any(w in msg for w in ["sound", "noise", "loud", "cry", "crying"]):
        reply = "Sound level is currently **loud/noisy**." if d.get("sound") else " Sound level is currently **quiet**."
        reply += " This may indicate crying or a loud environment." if d.get("sound") else " No loud sounds detected."
    elif any(w in msg for w in ["diaper", "wet", "wee", "nappy", "change"]):
        reply = f"Diaper is **{diaper_str}**."
        reply += " It's time for a change!" if d.get("wetness") else " All good, no change needed."
    elif any(w in msg for w in ["hi", "hello", "hey", "help"]):
        reply = "Hello! I'm your Baby Cradle Monitoring assistant. Ask about **temperature**, **humidity**, **motion**, **sound**, or **diaper**."
    elif any(w in msg for w in ["status", "summary", "all", "overview"]):
        flags = []
        if d.get("motion"):
            flags.append("motion detected")
        if d.get("wetness"):
            flags.append("wet diaper")
        if check_abnormal(temp if temp != "--" else None, hum if hum != "--" else None):
            flags.append("⚠️ abnormal readings")
        reply = f"**Temperature:** {temp}°C  |  **Humidity:** {hum}%  |  **Motion:** {motion_str}  |  **Sound:** {'loud' if d.get('sound') else 'quiet'}  |  **Diaper:** {diaper_str}"
        if flags:
            reply += f"\n\nNotable: {' · '.join(flags)}"
    else:
        reply = ("I can answer about: **temperature**, **humidity**, **motion**, **sound**, **diaper**, "
                 "or say **status** for a full summary. Type **help** for options.")

    return jsonify({"reply": reply})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("RENDER") is None
    log.info("Baby Cradle Monitoring Server starting on port %s...", port)
    socketio.run(app, host="0.0.0.0", port=port, debug=debug,
                 allow_unsafe_werkzeug=True)
