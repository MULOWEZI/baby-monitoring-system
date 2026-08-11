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


# ============================================================
# ENVIRONMENT
# ============================================================

load_dotenv()


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "WARNING"),
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger(__name__)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)

app.config["SECRET_KEY"] = os.getenv(
    "SECRET_KEY",
    "baby-monitor-secret-key"
)


# ============================================================
# SOCKET.IO
# ============================================================

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading",
    logger=False,
    engineio_logger=False,

    # Keep connections alive.
    ping_interval=25,
    ping_timeout=60,

    # Avoid unnecessary Socket.IO compression overhead.
    compression_threshold=1024
)


# ============================================================
# SUPABASE
# ============================================================

SUPABASE_URL = os.getenv(
    "SUPABASE_URL",
    ""
).rstrip("/")

SUPABASE_KEY = os.getenv(
    "SUPABASE_KEY",
    ""
)

supabase = None

if SUPABASE_URL and SUPABASE_KEY:

    try:

        from supabase import create_client

        supabase = create_client(
            SUPABASE_URL,
            SUPABASE_KEY
        )

        log.warning("Supabase connected")

    except Exception as e:

        log.exception(
            "Supabase initialization failed: %s",
            e
        )

else:

    log.warning(
        "Supabase disabled: missing SUPABASE_URL or SUPABASE_KEY"
    )


# ============================================================
# CURRENT SENSOR DATA
# ============================================================

current_data = {
    "temperature": 0,
    "humidity": 0,
    "motion": False,
    "sound": 0,
    "wetness": False,
    "last_update": None
}

current_data_lock = threading.Lock()


# ============================================================
# VIDEO
# ============================================================

latest_frame = None

_frame_lock = threading.Lock()

_frame_subscribers = []

_subscribers_lock = threading.Lock()


def _broadcast_frame(frame):

    with _subscribers_lock:

        subscribers = list(
            _frame_subscribers
        )

    for q in subscribers:

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
# DATABASE QUEUE
# ============================================================

# Sensor processing and database writing are separated.
#
# This prevents Supabase from slowing down:
#
#   /api/ingest
#   dashboard navigation
#   Socket.IO
#   video
#
# ============================================================

DB_QUEUE_SIZE = int(
    os.getenv(
        "DB_QUEUE_SIZE",
        "200"
    )
)

db_queue = queue.Queue(
    maxsize=DB_QUEUE_SIZE
)


# ============================================================
# ALERT QUEUE
# ============================================================

ALERT_QUEUE_SIZE = int(
    os.getenv(
        "ALERT_QUEUE_SIZE",
        "100"
    )
)

alert_queue = queue.Queue(
    maxsize=ALERT_QUEUE_SIZE
)


# ============================================================
# EMAIL QUEUE
# ============================================================

EMAIL_QUEUE_SIZE = int(
    os.getenv(
        "EMAIL_QUEUE_SIZE",
        "50"
    )
)

email_queue = queue.Queue(
    maxsize=EMAIL_QUEUE_SIZE
)


# ============================================================
# CACHE
# ============================================================

history_cache = {
    "data": [],
    "time": 0
}

alerts_cache = {
    "data": [],
    "time": 0
}

cache_lock = threading.Lock()

CACHE_SECONDS = float(
    os.getenv(
        "CACHE_SECONDS",
        "3"
    )
)


def invalidate_history_cache():

    with cache_lock:

        history_cache["time"] = 0


def invalidate_alerts_cache():

    with cache_lock:

        alerts_cache["time"] = 0


# ============================================================
# DATABASE WRITE RATE LIMIT
# ============================================================

# Do not write every single Raspberry Pi reading to Supabase.
#
# The Pi can still send data very frequently.
# The dashboard gets the latest data immediately.
#
# Supabase receives samples at a controlled rate.

DB_SAVE_INTERVAL = float(
    os.getenv(
        "DB_SAVE_INTERVAL",
        "1.0"
    )
)

last_db_save = 0

last_db_save_lock = threading.Lock()


def should_save_to_database():

    global last_db_save

    now = time.monotonic()

    with last_db_save_lock:

        if (
            now - last_db_save
            >= DB_SAVE_INTERVAL
        ):

            last_db_save = now

            return True

    return False


# ============================================================
# EMAIL CONFIGURATION
# ============================================================

BIRD_API_KEY = os.getenv(
    "BIRD_API_KEY",
    ""
)

BIRD_SENDER = os.getenv(
    "BIRD_SENDER",
    "onboarding@messagebird.dev"
)

ALERT_EMAIL = os.getenv(
    "ALERT_EMAIL",
    ""
)


# ============================================================
# ALERT STATE
# ============================================================

alert_state_lock = threading.Lock()

previous_wetness = False

previous_temperature_abnormal = False


# ============================================================
# BIRD HOST
# ============================================================

def bird_host():

    parts = BIRD_API_KEY.split("_")

    region = (
        parts[1]
        if len(parts) > 1 and parts[1]
        else "us1"
    )

    return (
        f"https://{region}.platform.bird.com"
    )


# ============================================================
# SEND EMAIL
# ============================================================

def send_alert_email(alerts):

    if not alerts:
        return False

    if not BIRD_API_KEY:

        return False

    if not ALERT_EMAIL:

        return False


    alert_types = {
        alert.get("alert_type")
        for alert in alerts
    }


    if "wetness" in alert_types:

        subject = "💧 Wet Diaper Detected"

    elif "temperature" in alert_types:

        subject = "🌡️ Temperature Alert"

    else:

        subject = "🚼 Baby Monitoring Alert"


    items = ""

    for alert in alerts:

        items += (
            "<li>"
            f"<strong>"
            f"{alert.get('severity', 'warning').upper()}"
            f"</strong> — "
            f"{alert.get('message', '')}"
            "</li>"
        )


    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


    dashboard_url = os.getenv(
        "DASHBOARD_URL",
        "https://baby-monitoring-system-7.onrender.com"
    )


    html = f"""
    <html>
    <body>

        <h2>🚼 Baby Cradle Monitoring Alert</h2>

        <p>
            A new condition requiring attention
            was detected.
        </p>

        <p>
            <strong>Time:</strong>
            {timestamp}
        </p>

        <ul>
            {items}
        </ul>

        <p>
            Please check the baby monitoring dashboard.
        </p>

        <p>
            <a href="{dashboard_url}">
                Open Baby Monitoring Dashboard
            </a>
        </p>

    </body>
    </html>
    """


    payload = {
        "from": BIRD_SENDER,
        "to": [
            ALERT_EMAIL
        ],
        "subject": subject,
        "html": html
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

            timeout=10
        )


        if response.status_code in (
            200,
            202
        ):

            log.warning(
                "Alert email sent"
            )

            return True


        log.error(
            "Bird email failed: %s",
            response.status_code
        )


    except Exception as e:

        log.error(
            "Bird email exception: %s",
            e
        )


    return False


# ============================================================
# THRESHOLDS
# ============================================================

def get_thresholds():

    return (
        float(
            os.getenv(
                "TEMP_MIN",
                "20"
            )
        ),

        float(
            os.getenv(
                "TEMP_MAX",
                "25"
            )
        ),

        float(
            os.getenv(
                "HUMIDITY_MIN",
                "40"
            )
        ),

        float(
            os.getenv(
                "HUMIDITY_MAX",
                "60"
            )
        )
    )


# ============================================================
# ABNORMAL SENSOR CHECK
# ============================================================

def check_abnormal(
    temp,
    hum
):

    temp_min, temp_max, hum_min, hum_max = (
        get_thresholds()
    )


    if temp is not None:

        if (
            temp < temp_min
            or
            temp > temp_max
        ):

            return True


    if hum is not None:

        if (
            hum < hum_min
            or
            hum > hum_max
        ):

            return True


    return False


# ============================================================
# CREATE ALERTS
# ============================================================

def detect_alerts(
    temp,
    hum,
    wetness,
    sound
):

    global previous_wetness
    global previous_temperature_abnormal


    temp_min, temp_max, _, _ = (
        get_thresholds()
    )


    alerts = []


    temperature_abnormal = False


    if temp is not None:

        temperature_abnormal = (
            temp < temp_min
            or
            temp > temp_max
        )


    current_wetness = bool(
        wetness
    )


    with alert_state_lock:

        new_temperature_event = (
            temperature_abnormal
            and
            not previous_temperature_abnormal
        )


        if new_temperature_event:

            if temp > temp_max:

                message = (
                    f"🌡️ Temperature is too high: "
                    f"{temp}°C. "
                    f"Configured maximum is "
                    f"{temp_max}°C."
                )

            elif temp < temp_min:

                message = (
                    f"🌡️ Temperature is too low: "
                    f"{temp}°C. "
                    f"Configured minimum is "
                    f"{temp_min}°C."
                )

            else:

                message = (
                    f"🌡️ Abnormal temperature detected: "
                    f"{temp}°C."
                )


            alerts.append({
                "alert_type": "temperature",
                "severity": "critical",
                "message": message
            })


        previous_temperature_abnormal = (
            temperature_abnormal
        )


        # ----------------------------------------------------
        # WETNESS
        # ----------------------------------------------------

        new_wetness_event = (
            current_wetness
            and
            not previous_wetness
        )


        if new_wetness_event:

            alerts.append({
                "alert_type": "wetness",
                "severity": "critical",
                "message":
                    "💧 Diaper is wet! "
                    "Please change the diaper."
            })


        previous_wetness = (
            current_wetness
        )


    return alerts


# ============================================================
# SAVE SENSOR READING
# ============================================================

def save_sensor_reading(
    temp,
    hum,
    motion,
    sound,
    wetness
):

    if supabase is None:

        return


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


    try:

        (
            supabase
            .table("sensor_readings")
            .insert(reading)
            .execute()
        )


        invalidate_history_cache()


    except Exception as e:

        log.error(
            "Sensor database error: %s",
            e
        )


# ============================================================
# SAVE ALERT
# ============================================================

def save_alert(alert):

    if supabase is None:

        return


    try:

        (
            supabase
            .table("alerts")
            .insert(alert)
            .execute()
        )


        invalidate_alerts_cache()


    except Exception as e:

        log.error(
            "Alert database error: %s",
            e
        )


# ============================================================
# DATABASE WORKER
# ============================================================

def database_worker():

    log.warning(
        "Database worker started"
    )


    while True:

        item = db_queue.get()

        try:

            save_sensor_reading(
                *item
            )

        except Exception as e:

            log.error(
                "Database worker error: %s",
                e
            )

        finally:

            db_queue.task_done()


# ============================================================
# ALERT WORKER
# ============================================================

def alert_worker():

    log.warning(
        "Alert worker started"
    )


    while True:

        alerts = alert_queue.get()

        try:

            for alert in alerts:

                save_alert(
                    alert
                )

                try:

                    socketio.emit(
                        "new_alert",
                        alert
                    )

                except Exception:
                    pass


            # Email is placed into a different queue.

            try:

                email_queue.put_nowait(
                    alerts
                )

            except queue.Full:

                log.warning(
                    "Email queue full"
                )


        except Exception as e:

            log.error(
                "Alert worker error: %s",
                e
            )

        finally:

            alert_queue.task_done()


# ============================================================
# EMAIL WORKER
# ============================================================

def email_worker():

    log.warning(
        "Email worker started"
    )


    while True:

        alerts = email_queue.get()

        try:

            send_alert_email(
                alerts
            )

        except Exception as e:

            log.error(
                "Email worker error: %s",
                e
            )

        finally:

            email_queue.task_done()


# ============================================================
# SENSOR PROCESSING
# ============================================================

def process_sensor_reading(
    temp,
    hum,
    motion,
    sound,
    wetness
):

    # --------------------------------------------------------
    # DATABASE
    # --------------------------------------------------------

    if should_save_to_database():

        item = (
            temp,
            hum,
            motion,
            sound,
            wetness
        )


        try:

            db_queue.put_nowait(
                item
            )

        except queue.Full:

            # Drop database sample rather than blocking
            # the live monitoring system.

            log.warning(
                "Database queue full; dropping sample"
            )


    # --------------------------------------------------------
    # ALERT DETECTION
    # --------------------------------------------------------

    alerts = detect_alerts(
        temp,
        hum,
        wetness,
        sound
    )


    if alerts:

        try:

            alert_queue.put_nowait(
                alerts
            )

        except queue.Full:

            log.warning(
                "Alert queue full"
            )


# ============================================================
# SENSOR WORKER
# ============================================================

def sensor_worker():

    log.warning(
        "Sensor worker started"
    )


    while True:

        item = reading_queue.get()

        try:

            process_sensor_reading(
                *item
            )

        except Exception as e:

            log.error(
                "Sensor processing error: %s",
                e
            )

        finally:

            reading_queue.task_done()


# ============================================================
# START BACKGROUND WORKERS
# ============================================================

def start_workers():

    # Only start once per process.

    if getattr(
        start_workers,
        "_started",
        False
    ):

        return


    start_workers._started = True


    sensor_thread = threading.Thread(
        target=sensor_worker,
        daemon=True,
        name="sensor-worker"
    )

    sensor_thread.start()


    database_thread = threading.Thread(
        target=database_worker,
        daemon=True,
        name="database-worker"
    )

    database_thread.start()


    alert_thread = threading.Thread(
        target=alert_worker,
        daemon=True,
        name="alert-worker"
    )

    alert_thread.start()


    email_thread = threading.Thread(
        target=email_worker,
        daemon=True,
        name="email-worker"
    )

    email_thread.start()


# Start workers when module loads.

start_workers()


# ============================================================
# HOME
# ============================================================

@app.route("/")
def index():

    return render_template(
        "index.html"
    )


# ============================================================
# LIVE
# ============================================================

@app.route("/live")
def live():

    return render_template(
        "live.html"
    )


# ============================================================
# HISTORY
# ============================================================

@app.route("/history")
def history():

    return render_template(
        "history.html"
    )


# ============================================================
# CURRENT DATA
# ============================================================

@app.route("/api/current_data")
def api_current_data():

    with current_data_lock:

        data = dict(
            current_data
        )


    return jsonify(data)


# ============================================================
# SENSOR HISTORY
# ============================================================

@app.route("/api/history")
def api_history():

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


    now = time.monotonic()


    # --------------------------------------------------------
    # CACHE
    # --------------------------------------------------------

    with cache_lock:

        if (
            history_cache["data"]
            and
            now - history_cache["time"]
            < CACHE_SECONDS
        ):

            return jsonify(
                history_cache["data"][:limit]
            )


    # --------------------------------------------------------
    # SUPABASE
    # --------------------------------------------------------

    try:

        response = (
            supabase
            .table("sensor_readings")
            .select("*")
            .order(
                "created_at",
                desc=True
            )
            .limit(100)
            .execute()
        )


        data = response.data or []


        with cache_lock:

            history_cache["data"] = data

            history_cache["time"] = (
                time.monotonic()
            )


        return jsonify(
            data[:limit]
        )


    except Exception as e:

        log.error(
            "History error: %s",
            e
        )


        # If database temporarily fails,
        # return cached data if available.

        with cache_lock:

            cached = list(
                history_cache["data"]
            )


        if cached:

            return jsonify(
                cached[:limit]
            )


        return jsonify({
            "error":
                "Unable to load history"
        }), 500


# ============================================================
# ALERT HISTORY
# ============================================================

@app.route("/api/alerts")
def api_alerts():

    if supabase is None:

        return jsonify([])


    limit = request.args.get(
        "limit",
        20,
        type=int
    )


    limit = max(
        1,
        min(limit, 50)
    )


    now = time.monotonic()


    # --------------------------------------------------------
    # CACHE
    # --------------------------------------------------------

    with cache_lock:

        if (
            alerts_cache["data"]
            and
            now - alerts_cache["time"]
            < CACHE_SECONDS
        ):

            return jsonify(
                alerts_cache["data"][:limit]
            )


    # --------------------------------------------------------
    # DATABASE
    # --------------------------------------------------------

    try:

        response = (
            supabase
            .table("alerts")
            .select("*")
            .order(
                "created_at",
                desc=True
            )
            .limit(50)
            .execute()
        )


        data = response.data or []


        with cache_lock:

            alerts_cache["data"] = data

            alerts_cache["time"] = (
                time.monotonic()
            )


        return jsonify(
            data[:limit]
        )


    except Exception as e:

        log.error(
            "Alert history error: %s",
            e
        )


        with cache_lock:

            cached = list(
                alerts_cache["data"]
            )


        if cached:

            return jsonify(
                cached[:limit]
            )


        return jsonify({
            "error":
                "Unable to load alerts"
        }), 500


# ============================================================
# CLEAR ALERTS
# ============================================================

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


        invalidate_alerts_cache()


        return jsonify({
            "success": True
        })


    except Exception as e:

        log.error(
            "Clear alerts error: %s",
            e
        )


        return jsonify({
            "error":
                "Unable to clear alerts"
        }), 500


# ============================================================
# RASPBERRY PI INGEST
# ============================================================

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

        temp = float(
            data["temperature"]
        )

        hum = float(
            data["humidity"]
        )

    except (
        TypeError,
        ValueError
    ):

        return jsonify({
            "error":
                "Temperature and humidity "
                "must be numbers"
        }), 400


    motion = bool(
        data.get(
            "motion_detected",
            False
        )
    )


    sound = data.get(
        "sound_level",
        0
    )


    wetness = bool(
        data.get(
            "wetness_detected",
            False
        )
    )


    timestamp = (
        datetime.now().isoformat()
    )


    # ========================================================
    # UPDATE MEMORY IMMEDIATELY
    # ========================================================

    with current_data_lock:

        current_data["temperature"] = temp
        current_data["humidity"] = hum
        current_data["motion"] = motion
        current_data["sound"] = sound
        current_data["wetness"] = wetness
        current_data["last_update"] = timestamp


        socket_data = dict(
            current_data
        )


    # ========================================================
    # SOCKET.IO
    # ========================================================

    try:

        socketio.emit(
            "sensor_update",
            socket_data
        )

    except Exception:
        pass


    # ========================================================
    # BACKGROUND PROCESSING
    # ========================================================

    item = (
        temp,
        hum,
        motion,
        sound,
        wetness
    )


    try:

        reading_queue.put_nowait(
            item
        )

    except queue.Full:

        # Remove oldest item.

        try:

            reading_queue.get_nowait()

            reading_queue.task_done()

        except queue.Empty:
            pass


        try:

            reading_queue.put_nowait(
                item
            )

        except queue.Full:
            pass


    # ========================================================
    # RETURN IMMEDIATELY
    # ========================================================

    return jsonify({
        "status": "ok",
        "abnormal":
            check_abnormal(
                temp,
                hum
            )
    })


# ============================================================
# UPLOAD VIDEO FRAME
# ============================================================

@app.route(
    "/api/upload_frame",
    methods=["POST"]
)
def api_upload_frame():

    global latest_frame


    data = request.get_data(
        cache=False
    )


    if (
        not data
        or
        len(data) < 100
    ):

        return jsonify({
            "error":
                "Empty or invalid frame"
        }), 400


    # --------------------------------------------------------
    # Replace old frame immediately.
    # --------------------------------------------------------

    with _frame_lock:

        latest_frame = data


    # --------------------------------------------------------
    # Broadcast only newest frame.
    # --------------------------------------------------------

    _broadcast_frame(
        data
    )


    return jsonify({
        "status": "ok"
    })


# ============================================================
# VIDEO STREAM
# ============================================================

@app.route(
    "/video_feed"
)
def video_feed():

    def generate():

        # ----------------------------------------------------
        # One frame maximum.
        # ----------------------------------------------------

        q = queue.Queue(
            maxsize=1
        )


        with _subscribers_lock:

            _frame_subscribers.append(
                q
            )


        try:

            # ------------------------------------------------
            # Send current frame immediately.
            # ------------------------------------------------

            with _frame_lock:

                frame = latest_frame


            if frame is not None:

                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Cache-Control: no-cache, "
                    b"no-store, must-revalidate\r\n"
                    b"Pragma: no-cache\r\n"
                    b"\r\n"
                    +
                    frame
                    +
                    b"\r\n"
                )


            # ------------------------------------------------
            # Continue with newest frames.
            # ------------------------------------------------

            while True:

                frame = q.get()


                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Cache-Control: no-cache, "
                    b"no-store, must-revalidate\r\n"
                    b"Pragma: no-cache\r\n"
                    b"\r\n"
                    +
                    frame
                    +
                    b"\r\n"
                )


        except GeneratorExit:

            pass


        except Exception as e:

            log.debug(
                "Video stream ended: %s",
                e
            )


        finally:

            with _subscribers_lock:

                try:

                    _frame_subscribers.remove(
                        q
                    )

                except ValueError:

                    pass


    return Response(
        generate(),

        mimetype=(
            "multipart/x-mixed-replace;"
            " boundary=frame"
        ),

        headers={
            "Cache-Control":
                "no-cache, no-store, "
                "must-revalidate",

            "Pragma":
                "no-cache",

            "Expires":
                "0",

            "X-Accel-Buffering":
                "no",

            "Connection":
                "keep-alive"
        }
    )


# ============================================================
# CHATBOT
# ============================================================

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


    with current_data_lock:

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
        else
        "quiet/sleeping"
    )


    diaper_str = (
        "wet — needs changing"
        if d.get("wetness")
        else
        "dry"
    )


    # ========================================================
    # TEMPERATURE
    # ========================================================

    if any(
        word in msg
        for word in [
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


    # ========================================================
    # HUMIDITY
    # ========================================================

    elif any(
        word in msg
        for word in [
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


    # ========================================================
    # MOTION
    # ========================================================

    elif any(
        word in msg
        for word in [
            "motion",
            "move",
            "moving",
            "activity",
            "active"
        ]
    ):

        reply = (
            f"Baby is currently **"
            f"{motion_str}**."
        )


        if d.get("motion"):

            reply += (
                " Recent motion was detected."
            )

        else:

            reply += (
                " No recent motion was detected."
            )


    # ========================================================
    # SOUND
    # ========================================================

    elif any(
        word in msg
        for word in [
            "sound",
            "noise",
            "loud",
            "cry",
            "crying"
        ]
    ):

        if d.get("sound"):

            reply = (
                "Sound level is currently "
                "**loud/noisy**."
            )

            reply += (
                " This may indicate crying "
                "or a loud environment."
            )

        else:

            reply = (
                "Sound level is currently "
                "**quiet**."
            )

            reply += (
                " No loud sounds detected."
            )


    # ========================================================
    # DIAPER
    # ========================================================

    elif any(
        word in msg
        for word in [
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


        if d.get("wetness"):

            reply += (
                " It's time for a change!"
            )

        else:

            reply += (
                " All good, no change needed."
            )


    # ========================================================
    # GREETING
    # ========================================================

    elif any(
        word in msg
        for word in [
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


    # ========================================================
    # STATUS
    # ========================================================

    elif any(
        word in msg
        for word in [
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
            temp
            if temp != "--"
            else None,

            hum
            if hum != "--"
            else None
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


    # ========================================================
    # UNKNOWN
    # ========================================================

    else:

        reply = (
            "I can answer about: "
            "**temperature**, **humidity**, "
            "**motion**, **sound**, **diaper**, "
            "or say **status** for a full summary."
        )


    return jsonify({
        "reply": reply
    })


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    return jsonify({

        "status": "healthy",

        "database":
            supabase is not None,

        "sensor_queue":
            reading_queue.qsize(),

        "database_queue":
            db_queue.qsize(),

        "alert_queue":
            alert_queue.qsize(),

        "email_queue":
            email_queue.qsize(),

        "video_clients":
            len(_frame_subscribers)

    })


# ============================================================
# RUN LOCALLY
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "5000"
        )
    )


    log.warning(
        "Baby Cradle Monitoring Server "
        "starting on port %s",
        port
    )


    socketio.run(

        app,

        host="0.0.0.0",

        port=port,

        debug=False,

        allow_unsafe_werkzeug=True

    )
