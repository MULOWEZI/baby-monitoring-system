#!/usr/bin/env python3

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


# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger(__name__)


# ============================================================================
# ENVIRONMENT
# ============================================================================

load_dotenv()


# ============================================================================
# FLASK / SOCKET.IO
# ============================================================================

app = Flask(__name__)

app.config["SECRET_KEY"] = os.getenv(
    "SECRET_KEY",
    "baby-monitor-secret-key"
)

# Explicit threading mode.
# This works better with Gunicorn gthread than trying to use
# eventlet/gevent unnecessarily.
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading"
)


# ============================================================================
# SUPABASE
# ============================================================================

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

supabase = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client

        supabase = create_client(
            SUPABASE_URL,
            SUPABASE_KEY
        )

        log.info(
            "Supabase client connected: %s",
            SUPABASE_URL[:30] + "..."
        )

    except Exception as e:
        log.warning(
            "Supabase initialization failed: %s",
            e
        )

else:
    log.warning(
        "SUPABASE_URL / SUPABASE_KEY not set. "
        "Running without database."
    )


# ============================================================================
# CONFIGURATION
# ============================================================================

# How often sensor readings should be written to Supabase.
#
# The Raspberry Pi can send data much faster than this.
# The dashboard still receives every reading immediately.
#
# Set to 1 for one database write per second.
# Set to 2 for one database write every two seconds, etc.
DB_SAVE_INTERVAL = float(
    os.getenv("DB_SAVE_INTERVAL", "1.0")
)

# How long the same alert type must wait before another email.
ALERT_COOLDOWN = int(
    os.getenv("ALERT_COOLDOWN", "300")
)

# Camera frame queue size.
FRAME_QUEUE_SIZE = 1


# ============================================================================
# CURRENT SENSOR STATE
# ============================================================================

current_data = {
    "temperature": 0,
    "humidity": 0,
    "motion": False,
    "sound": 0,
    "wetness": False,
    "last_update": None,
}

_current_data_lock = threading.Lock()


# ============================================================================
# SENSOR DATABASE QUEUE
# ============================================================================

# Only a small number of sensor readings are allowed to wait.
#
# This prevents the server from creating thousands of threads when
# the Raspberry Pi sends data rapidly.
sensor_queue = queue.Queue(maxsize=5)


def enqueue_sensor_reading(reading):
    """
    Put a reading into the DB queue.

    If the queue is full, discard an old reading and keep
    the newest one.
    """

    try:
        sensor_queue.put_nowait(reading)
        return True

    except queue.Full:

        try:
            sensor_queue.get_nowait()
        except queue.Empty:
            pass

        try:
            sensor_queue.put_nowait(reading)
            return True
        except queue.Full:
            return False


# ============================================================================
# DATABASE WORKER
# ============================================================================

_last_db_save_time = 0.0
_db_save_lock = threading.Lock()


def database_worker():
    """
    Single background worker responsible for Supabase writes.

    This is deliberately ONE worker instead of creating a new
    thread for every /api/ingest request.
    """

    global _last_db_save_time

    log.info("Database worker started")

    while True:

        try:
            reading = sensor_queue.get()

            if reading is None:
                sensor_queue.task_done()
                continue

            # ---------------------------------------------------------------
            # Rate limit database writes
            # ---------------------------------------------------------------

            now = time.monotonic()

            wait_time = (
                DB_SAVE_INTERVAL
                - (now - _last_db_save_time)
            )

            if wait_time > 0:
                time.sleep(wait_time)

            # ---------------------------------------------------------------
            # Save to Supabase
            # ---------------------------------------------------------------

            if supabase is not None:

                try:

                    supabase.table(
                        "sensor_readings"
                    ).insert(reading).execute()

                    _last_db_save_time = time.monotonic()

                    log.info(
                        "Sensor reading saved to Supabase"
                    )

                except Exception as e:

                    log.error(
                        "Supabase sensor insert error: %s",
                        e
                    )

            sensor_queue.task_done()

        except Exception as e:

            log.exception(
                "Database worker error: %s",
                e
            )

            time.sleep(1)


# Start exactly ONE database worker.
_db_worker = threading.Thread(
    target=database_worker,
    name="supabase-db-worker",
    daemon=True
)

_db_worker.start()


# ============================================================================
# CAMERA FRAME MANAGEMENT
# ============================================================================

latest_frame = None

_frame_lock = threading.Lock()

_frame_subscribers = []

_subscribers_lock = threading.Lock()


def _broadcast_frame(frame):
    """
    Send the newest frame to all connected browsers.

    Each browser has a queue of size 1, meaning old frames
    are discarded instead of building a backlog.
    """

    with _subscribers_lock:

        dead_subscribers = []

        for q in _frame_subscribers:

            try:
                q.put_nowait(frame)

            except queue.Full:

                # Drop stale frame.
                try:
                    q.get_nowait()
                except queue.Empty:
                    pass

                try:
                    q.put_nowait(frame)
                except queue.Full:
                    pass

            except Exception:
                dead_subscribers.append(q)

        for q in dead_subscribers:

            if q in _frame_subscribers:
                _frame_subscribers.remove(q)


# ============================================================================
# EMAIL ALERT SYSTEM
# ============================================================================

BIRD_API_KEY = os.getenv("BIRD_API_KEY", "")

BIRD_SENDER = os.getenv(
    "BIRD_SENDER",
    "onboarding@messagebird.dev"
)

ALERT_EMAIL = os.getenv(
    "ALERT_EMAIL",
    ""
)


# Per-alert cooldown.
_last_alert_email = {}

_alert_email_lock = threading.Lock()


def bird_host():

    """
    Derive Bird platform host from the API key region.
    """

    parts = BIRD_API_KEY.split("_")

    region = (
        parts[1]
        if len(parts) > 1 and parts[1]
        else "us1"
    )

    return f"https://{region}.platform.bird.com"


def send_alert_email(alerts):
    """
    Send alert email.

    Uses a global cooldown to prevent email flooding.
    """

    if not BIRD_API_KEY:

        log.warning(
            "BIRD_API_KEY not set - skipping email."
        )

        return False

    if not ALERT_EMAIL:

        log.warning(
            "ALERT_EMAIL not set - skipping email."
        )

        return False

    if not alerts:
        return False

    now = time.time()

    # Determine alert types.
    alert_types = set(
        a.get("alert_type")
        for a in alerts
    )

    # ---------------------------------------------------------------
    # Check cooldown
    # ---------------------------------------------------------------

    with _alert_email_lock:

        allowed = False

        for alert_type in alert_types:

            last_time = _last_alert_email.get(
                alert_type,
                0
            )

            if now - last_time >= ALERT_COOLDOWN:
                allowed = True
                break

        if not allowed:
            return False

        for alert_type in alert_types:
            _last_alert_email[alert_type] = now

    # ---------------------------------------------------------------
    # Email content
    # ---------------------------------------------------------------

    items = "".join(
        f"<li><b>{a['severity'].upper()}</b> "
        f"— {a['message']}</li>"
        for a in alerts
    )

    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    subject = "🚼 Baby Monitoring Alert"

    if any(
        a["alert_type"] == "crying"
        for a in alerts
    ):
        subject = "👶 Baby Crying Detected"

    elif any(
        a["alert_type"] == "wetness"
        for a in alerts
    ):
        subject = "💧 Wet Diaper Detected"

    elif any(
        a["alert_type"] == "temperature"
        for a in alerts
    ):
        subject = "🌡️ Temperature Alert"

    elif any(
        a["alert_type"] == "humidity"
        for a in alerts
    ):
        subject = "💦 Humidity Alert"

    payload = {

        "from": BIRD_SENDER,

        "to": [
            ALERT_EMAIL
        ],

        "subject": subject,

        "html": (
            "<h2>🚼 Baby Cradle Alert</h2>"

            f"<p>Detected {len(alerts)} "
            f"issue(s) at <b>{timestamp}</b>:</p>"

            f"<ul>{items}</ul>"

            "<p>Live dashboard:</p>"

            "<a href='https://baby-monitoring-system-7.onrender.com'>"
            "Open Baby Monitoring Dashboard"
            "</a>"
        )
    }

    try:

        response = requests.post(

            f"{bird_host()}/v1/email/messages",

            headers={
                "Authorization":
                    f"Bearer {BIRD_API_KEY}",

                "Content-Type":
                    "application/json"
            },

            json=payload,

            timeout=15
        )

        if response.status_code in (200, 202):

            log.info(
                "Alert email sent to %s (%s)",
                ALERT_EMAIL,
                response.status_code
            )

            return True

        log.error(
            "Bird email failed %s: %s",
            response.status_code,
            response.text[:300]
        )

    except Exception as e:

        log.error(
            "Bird email exception: %s",
            e
        )

    return False


# ============================================================================
# SENSOR LIMIT CHECKS
# ============================================================================

def check_abnormal(temp, hum):

    temp_min = float(
        os.getenv("TEMP_MIN", "20")
    )

    temp_max = float(
        os.getenv("TEMP_MAX", "25")
    )

    hum_min = float(
        os.getenv("HUMIDITY_MIN", "40")
    )

    hum_max = float(
        os.getenv("HUMIDITY_MAX", "60")
    )

    if temp is not None:

        if temp < temp_min or temp > temp_max:
            return True

    if hum is not None:

        if hum < hum_min or hum > hum_max:
            return True

    return False


def check_alerts(
    temp,
    hum,
    wetness,
    sound
):

    temp_min = float(
        os.getenv("TEMP_MIN", "20")
    )

    temp_max = float(
        os.getenv("TEMP_MAX", "25")
    )

    hum_min = float(
        os.getenv("HUMIDITY_MIN", "40")
    )

    hum_max = float(
        os.getenv("HUMIDITY_MAX", "60")
    )

    alerts = []

    # ---------------------------------------------------------------
    # Temperature
    # ---------------------------------------------------------------

    if temp is not None:

        if temp < temp_min:

            alerts.append({

                "alert_type": "temperature",

                "severity": "critical",

                "message":
                    f"⚠️ Temperature too low: {temp}°C"
            })

        elif temp > temp_max:

            alerts.append({

                "alert_type": "temperature",

                "severity": "critical",

                "message":
                    f"⚠️ Temperature too high: {temp}°C"
            })

    # ---------------------------------------------------------------
    # Humidity
    # ---------------------------------------------------------------

    if hum is not None:

        if hum < hum_min:

            alerts.append({

                "alert_type": "humidity",

                "severity": "warning",

                "message":
                    f"💧 Humidity too low: {hum}%"
            })

        elif hum > hum_max:

            alerts.append({

                "alert_type": "humidity",

                "severity": "warning",

                "message":
                    f"💧 Humidity too high: {hum}%"
            })

    # ---------------------------------------------------------------
    # Wetness
    # ---------------------------------------------------------------

    if wetness:

        alerts.append({

            "alert_type": "wetness",

            "severity": "critical",

            "message":
                "🚼 Diaper is wet! Please change the diaper."
        })

    # ---------------------------------------------------------------
    # Sound
    # ---------------------------------------------------------------

    if sound:

        alerts.append({

            "alert_type": "crying",

            "severity": "critical",

            "message":
                "👶 Baby is crying!"
        })

    if not alerts:
        return

    # ---------------------------------------------------------------
    # Save alerts
    # ---------------------------------------------------------------

    if supabase is not None:

        for alert in alerts:

            try:

                supabase.table(
                    "alerts"
                ).insert(alert).execute()

                socketio.emit(
                    "new_alert",
                    alert
                )

                log.info(
                    "NEW ALERT: %s",
                    alert["message"]
                )

            except Exception as e:

                log.error(
                    "Supabase alert insert error: %s",
                    e
                )

    # ---------------------------------------------------------------
    # Email
    # ---------------------------------------------------------------

    send_alert_email(alerts)


# ============================================================================
# PROCESS SENSOR DATA
# ============================================================================

def process_sensor_reading(reading):

    """
    Process alert conditions.

    Database insertion is handled by the single database worker.
    """

    temp = reading.get("temperature")
    hum = reading.get("humidity")
    motion = reading.get("motion_detected", False)
    sound = reading.get("sound_level", 0)
    wetness = reading.get("wetness_detected", False)

    check_alerts(
        temp,
        hum,
        wetness,
        sound
    )


# ============================================================================
# FLASK ROUTES
# ============================================================================

@app.route("/")
def index():

    return render_template(
        "index.html"
    )


@app.route("/live")
def live():

    return render_template(
        "live.html"
    )


@app.route("/history")
def history():

    return render_template(
        "history.html"
    )


# ============================================================================
# CURRENT DATA
# ============================================================================

@app.route("/api/current_data")
def api_current_data():

    with _current_data_lock:

        return jsonify(
            dict(current_data)
        )


# ============================================================================
# HISTORY
# ============================================================================

@app.route("/api/history")
def api_history():

    if supabase is None:

        return jsonify([])

    limit = request.args.get(
        "limit",
        100,
        type=int
    )

    # Prevent accidentally requesting thousands of rows.
    limit = max(
        1,
        min(limit, 200)
    )

    try:

        response = (

            supabase

            .table("sensor_readings")

            .select("*")

            .order(
                "created_at",
                desc=True
            )

            .limit(limit)

            .execute()
        )

        return jsonify(
            response.data
        )

    except Exception as e:

        log.error(
            "History query error: %s",
            e
        )

        return jsonify({
            "error": str(e)
        }), 500


# ============================================================================
# ALERTS
# ============================================================================

@app.route("/api/alerts")
def api_alerts():

    if supabase is None:

        return jsonify([])

    limit = request.args.get(
        "limit",
        50,
        type=int
    )

    limit = max(
        1,
        min(limit, 100)
    )

    try:

        response = (

            supabase

            .table("alerts")

            .select("*")

            .order(
                "created_at",
                desc=True
            )

            .limit(limit)

            .execute()
        )

        return jsonify(
            response.data
        )

    except Exception as e:

        log.error(
            "Alerts query error: %s",
            e
        )

        return jsonify({
            "error": str(e)
        }), 500


# ============================================================================
# CLEAR ALERTS
# ============================================================================

@app.route(
    "/api/clear_alerts",
    methods=["POST"]
)
def clear_alerts():

    if supabase is None:

        return jsonify({
            "success": True
        })

    try:

        (
            supabase

            .table("alerts")

            .update({
                "is_read": True
            })

            .neq(
                "is_read",
                True
            )

            .execute()
        )

        return jsonify({
            "success": True
        })

    except Exception as e:

        log.error(
            "Clear alerts error: %s",
            e
        )

        return jsonify({
            "error": str(e)
        }), 500


# ============================================================================
# RASPBERRY PI SENSOR INGESTION
# ============================================================================

@app.route(
    "/api/ingest",
    methods=["POST"]
)
def api_ingest():

    data = request.get_json(
        silent=True
    ) or {}

    if (
        "temperature" not in data
        or
        "humidity" not in data
    ):

        return jsonify({

            "error":
                "Missing required fields: "
                "temperature, humidity"

        }), 400

    try:

        temp = data.get(
            "temperature"
        )

        hum = data.get(
            "humidity"
        )

        motion = data.get(
            "motion_detected",
            False
        )

        sound = data.get(
            "sound_level",
            0
        )

        wetness = data.get(
            "wetness_detected",
            False
        )

        # ---------------------------------------------------------------
        # Update current state immediately.
        # ---------------------------------------------------------------

        with _current_data_lock:

            current_data["temperature"] = temp

            current_data["humidity"] = hum

            current_data["motion"] = motion

            current_data["sound"] = sound

            current_data["wetness"] = wetness

            current_data["last_update"] = (
                datetime.now().isoformat()
            )

            dashboard_data = dict(
                current_data
            )

        # ---------------------------------------------------------------
        # Immediately update dashboards.
        # ---------------------------------------------------------------

        socketio.emit(
            "sensor_update",
            dashboard_data
        )

        # ---------------------------------------------------------------
        # Prepare DB record.
        # ---------------------------------------------------------------

        reading = {

            "temperature": temp,

            "humidity": hum,

            "motion_detected": motion,

            "sound_level": sound,

            "wetness_detected": wetness,

            "is_abnormal":
                check_abnormal(
                    temp,
                    hum
                )
        }

        # ---------------------------------------------------------------
        # Queue DB write.
        #
        # IMPORTANT:
        # We DO NOT create a new thread here.
        # ---------------------------------------------------------------

        enqueue_sensor_reading(
            reading
        )

        # ---------------------------------------------------------------
        # Alert processing.
        #
        # Alert processing itself is lightweight compared with
        # database insertion. Run it using SocketIO background task
        # rather than spawning unlimited raw threads.
        # ---------------------------------------------------------------

        socketio.start_background_task(
            process_sensor_reading,
            reading
        )

        return jsonify({

            "status": "ok",

            "abnormal":
                check_abnormal(
                    temp,
                    hum
                )

        })

    except Exception as e:

        log.exception(
            "Ingest error: %s",
            e
        )

        return jsonify({
            "error": str(e)
        }), 500


# ============================================================================
# CAMERA FRAME UPLOAD
# ============================================================================

@app.route(
    "/api/upload_frame",
    methods=["POST"]
)
def api_upload_frame():

    global latest_frame

    data = request.get_data()

    if (
        not data
        or
        len(data) < 100
    ):

        return jsonify({
            "error":
                "Empty or invalid frame"
        }), 400

    # ---------------------------------------------------------------
    # Replace old frame with newest frame.
    # ---------------------------------------------------------------

    with _frame_lock:

        latest_frame = data

    # ---------------------------------------------------------------
    # Send newest frame to viewers.
    # ---------------------------------------------------------------

    _broadcast_frame(
        data
    )

    return jsonify({
        "status": "ok"
    })


# ============================================================================
# LIVE VIDEO STREAM
# ============================================================================

@app.route("/video_feed")
def video_feed():

    def generate():

        # Each browser gets a queue containing only
        # the newest frame.
        q = queue.Queue(
            maxsize=FRAME_QUEUE_SIZE
        )

        # -----------------------------------------------------------
        # Register subscriber.
        # -----------------------------------------------------------

        with _subscribers_lock:

            _frame_subscribers.append(
                q
            )

        try:

            # -------------------------------------------------------
            # Send latest frame immediately if available.
            # -------------------------------------------------------

            with _frame_lock:

                first_frame = latest_frame

            if first_frame is not None:

                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Cache-Control: no-cache\r\n"
                    b"\r\n"
                    + first_frame
                    + b"\r\n"
                )

            # -------------------------------------------------------
            # Continue receiving newest frames.
            # -------------------------------------------------------

            while True:

                frame = q.get()

                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Cache-Control: no-cache\r\n"
                    b"\r\n"
                    + frame
                    + b"\r\n"
                )

        except GeneratorExit:
            pass

        except Exception as e:

            log.warning(
                "Video stream ended: %s",
                e
            )

        finally:

            with _subscribers_lock:

                if q in _frame_subscribers:

                    _frame_subscribers.remove(
                        q
                    )

    response = Response(

        generate(),

        mimetype=(
            "multipart/"
            "x-mixed-replace;"
            " boundary=frame"
        )
    )

    response.headers[
        "Cache-Control"
    ] = "no-cache, no-store, must-revalidate"

    response.headers[
        "Pragma"
    ] = "no-cache"

    response.headers[
        "Connection"
    ] = "keep-alive"

    return response


# ============================================================================
# CHATBOT
# ============================================================================

@app.route(
    "/api/chat",
    methods=["POST"]
)
def api_chat():

    data = request.get_json(
        silent=True
    ) or {}

    msg = (
        data.get("message")
        or ""
    ).lower().strip()

    with _current_data_lock:

        d = dict(
            current_data
        )

    temp = (
        d.get("temperature")
        if d.get("temperature") is not None
        else "--"
    )

    hum = (
        d.get("humidity")
        if d.get("humidity") is not None
        else "--"
    )

    tmin = os.getenv(
        "TEMP_MIN",
        "20"
    )

    tmax = os.getenv(
        "TEMP_MAX",
        "25"
    )

    hmin = os.getenv(
        "HUMIDITY_MIN",
        "40"
    )

    hmax = os.getenv(
        "HUMIDITY_MAX",
        "60"
    )

    motion_str = (
        "moving"
        if d.get("motion")
        else "quiet/sleeping"
    )

    diaper_str = (
        "wet — needs changing"
        if d.get("wetness")
        else "dry"
    )

    # ---------------------------------------------------------------
    # Temperature
    # ---------------------------------------------------------------

    if any(
        w in msg
        for w in [
            "temp",
            "hot",
            "cold",
            "warm"
        ]
    ):

        reply = (
            f"The current temperature is "
            f"{temp}°C. Safe range is "
            f"{tmin}–{tmax}°C."
        )

        if temp != "--":

            if temp < float(tmin):

                reply += (
                    " It's **below** the minimum."
                )

            elif temp > float(tmax):

                reply += (
                    " It's **above** the maximum."
                )

            else:

                reply += (
                    " This is within the normal range."
                )

    # ---------------------------------------------------------------
    # Humidity
    # ---------------------------------------------------------------

    elif any(
        w in msg
        for w in [
            "humid",
            "moist"
        ]
    ):

        reply = (
            f"The current humidity is "
            f"{hum}%. Safe range is "
            f"{hmin}–{hmax}%."
        )

        if hum != "--":

            if hum < float(hmin):

                reply += (
                    " It's **below** the minimum."
                )

            elif hum > float(hmax):

                reply += (
                    " It's **above** the maximum."
                )

            else:

                reply += (
                    " This is within the normal range."
                )

    # ---------------------------------------------------------------
    # Motion
    # ---------------------------------------------------------------

    elif any(
        w in msg
        for w in [
            "motion",
            "move",
            "moving",
            "activity",
            "active"
        ]
    ):

        reply = (
            f"Baby is currently **{motion_str}**."
        )

        reply += (
            " Recent motion was detected."
            if d.get("motion")
            else
            " No recent motion — baby may be sleeping."
        )

    # ---------------------------------------------------------------
    # Sound
    # ---------------------------------------------------------------

    elif any(
        w in msg
        for w in [
            "sound",
            "noise",
            "loud",
            "cry",
            "crying"
        ]
    ):

        reply = (
            "Sound level is currently "
            "**loud/noisy**."
            if d.get("sound")
            else
            "Sound level is currently **quiet**."
        )

        reply += (
            " This may indicate crying or a loud environment."
            if d.get("sound")
            else
            " No loud sounds detected."
        )

    # ---------------------------------------------------------------
    # Diaper
    # ---------------------------------------------------------------

    elif any(
        w in msg
        for w in [
            "diaper",
            "wet",
            "wee",
            "nappy",
            "change"
        ]
    ):

        reply = (
            f"Diaper is **{diaper_str}**."
        )

        reply += (
            " It's time for a change!"
            if d.get("wetness")
            else
            " All good, no change needed."
        )

    # ---------------------------------------------------------------
    # Greeting
    # ---------------------------------------------------------------

    elif any(
        w in msg
        for w in [
            "hi",
            "hello",
            "hey",
            "help"
        ]
    ):

        reply = (
            "Hello! I'm your Baby Cradle "
            "Monitoring assistant. Ask about "
            "**temperature**, **humidity**, "
            "**motion**, **sound**, or **diaper**."
        )

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------

    elif any(
        w in msg
        for w in [
            "status",
            "summary",
            "all",
            "overview"
        ]
    ):

        flags = []

        if d.get("motion"):

            flags.append(
                "motion detected"
            )

        if d.get("wetness"):

            flags.append(
                "wet diaper"
            )

        if check_abnormal(
            temp if temp != "--" else None,
            hum if hum != "--" else None
        ):

            flags.append(
                "⚠️ abnormal readings"
            )

        reply = (

            f"**Temperature:** {temp}°C  |  "

            f"**Humidity:** {hum}%  |  "

            f"**Motion:** {motion_str}  |  "

            f"**Sound:** "
            f"{'loud' if d.get('sound') else 'quiet'}  |  "

            f"**Diaper:** {diaper_str}"
        )

        if flags:

            reply += (
                f"\n\nNotable: "
                f"{' · '.join(flags)}"
            )

    else:

        reply = (
            "I can answer about: "
            "**temperature**, **humidity**, "
            "**motion**, **sound**, **diaper**, "
            "or say **status** for a full summary. "
            "Type **help** for options."
        )

    return jsonify({
        "reply": reply
    })


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.route("/health")
def health():

    return jsonify({

        "status": "ok",

        "database":
            "connected"
            if supabase is not None
            else "disabled",

        "queue_size":
            sensor_queue.qsize(),

        "timestamp":
            datetime.now().isoformat()
    })


# ============================================================================
# LOCAL DEVELOPMENT
# ============================================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "5000"
        )
    )

    debug = (
        os.getenv("RENDER") is None
    )

    log.info(
        "Baby Cradle Monitoring Server "
        "starting on port %s...",
        port
    )

    socketio.run(

        app,

        host="0.0.0.0",

        port=port,

        debug=debug,

        allow_unsafe_werkzeug=True
    )
