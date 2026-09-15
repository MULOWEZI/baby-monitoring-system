#!/usr/bin/env python3

import os
import sys
import time
import threading
import logging
from collections import deque
from datetime import datetime

import requests
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify, Response
from flask_socketio import SocketIO

load_dotenv()


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger(__name__)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)

app.config["SECRET_KEY"] = os.getenv(
    "SECRET_KEY",
    "baby-monitor-secret-key",
)


# ============================================================
# SOCKET.IO
# ============================================================
#
# Render start command:
#
# gunicorn --worker-class geventwebsocket.gunicorn.workers.GeventWebSocketWorker \
#          --workers 1 --timeout 120 app:app
#
# One worker is intentional because current sensor data and the
# video buffer are stored in this process.
# ============================================================

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False,
    ping_interval=25,
    ping_timeout=60,
)


# ============================================================
# SUPABASE
# ============================================================

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

supabase = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client

        supabase = create_client(
            SUPABASE_URL,
            SUPABASE_KEY,
        )

        log.info("Supabase connected")

    except Exception as exc:
        log.exception(
            "Supabase initialization failed: %s",
            exc,
        )
else:
    log.warning(
        "SUPABASE_URL/SUPABASE_KEY not set - "
        "database features disabled"
    )


# ============================================================
# SENSOR UPDATE SETTINGS
# ============================================================

# The browser receives sensor updates at most once every 5 seconds.
# The Raspberry Pi can POST more often; the latest values are kept.
SENSOR_UPDATE_INTERVAL = 5.0

last_sensor_emit = 0.0
last_sensor_db_write = 0.0

sensor_timing_lock = threading.Lock()


# ============================================================
# CURRENT SENSOR DATA
# ============================================================

current_data = {
    "temperature": 0,
    "humidity": 0,
    "motion": False,
    "sound": 0,
    "wetness": False,
    "last_update": None,
}

current_data_lock = threading.Lock()


# ============================================================
# HISTORY / ALERT CACHE
# ============================================================

CACHE_TTL = 5.0

history_cache = {
    "data": [],
    "timestamp": 0.0,
    "limit": 50,
}

alerts_cache = {
    "data": [],
    "timestamp": 0.0,
    "limit": 20,
}

cache_lock = threading.Lock()


def invalidate_history_cache():
    with cache_lock:
        history_cache["timestamp"] = 0.0


def invalidate_alerts_cache():
    with cache_lock:
        alerts_cache["timestamp"] = 0.0


def get_cached(cache, limit):
    now = time.monotonic()

    with cache_lock:
        if (
            cache["limit"] == limit
            and cache["timestamp"] > 0
            and now - cache["timestamp"] < CACHE_TTL
        ):
            return list(cache["data"])

    return None


def set_cached(cache, data, limit):
    with cache_lock:
        cache["data"] = list(data)
        cache["limit"] = limit
        cache["timestamp"] = time.monotonic()


# ============================================================
# VIDEO BUFFER
# ============================================================
#
# A tiny ring buffer is used instead of one queue per browser.
#
# Why maxlen=3?
#
#   Camera -> [frame][frame][frame] -> Browser
#
# If the browser is slower than the camera, old frames are dropped.
# This keeps the stream close to real time instead of building a
# large latency-inducing queue.
#
# This is similar to the "latest frames only" buffering strategy
# used in real-time streaming systems.
# ============================================================

VIDEO_BUFFER_SIZE = 3

video_buffer = deque(maxlen=VIDEO_BUFFER_SIZE)

video_condition = threading.Condition(
    threading.Lock()
)

latest_frame = None
latest_frame_lock = threading.Lock()


def push_video_frame(frame):
    """
    Add a JPEG frame to the small live buffer.

    Old frames are automatically discarded when the buffer is full.
    The newest frames therefore have priority.
    """
    global latest_frame

    with latest_frame_lock:
        latest_frame = frame

    with video_condition:
        video_buffer.append(frame)
        video_condition.notify_all()


def get_latest_video_frame():
    with latest_frame_lock:
        return latest_frame


# ============================================================
# EMAIL CONFIGURATION
# ============================================================

BIRD_API_KEY = os.getenv(
    "BIRD_API_KEY",
    "",
)

BIRD_SENDER = os.getenv(
    "BIRD_SENDER",
    "onboarding@messagebird.dev",
)

ALERT_EMAIL = os.getenv(
    "ALERT_EMAIL",
    "",
)

EMAIL_COOLDOWN = 300.0
last_email_sent = 0.0
email_lock = threading.Lock()


def bird_host():
    parts = BIRD_API_KEY.split("_")

    region = (
        parts[1]
        if len(parts) > 1 and parts[1]
        else "us1"
    )

    return f"https://{region}.platform.bird.com"


def send_alert_email(alerts):
    global last_email_sent

    if not alerts:
        return False

    if not BIRD_API_KEY:
        log.warning(
            "BIRD_API_KEY not set - email skipped"
        )
        return False

    if not ALERT_EMAIL:
        log.warning(
            "ALERT_EMAIL not set - email skipped"
        )
        return False

    with email_lock:
        now = time.monotonic()

        if now - last_email_sent < EMAIL_COOLDOWN:
            return False

        last_email_sent = now

    alert_types = {
        alert.get("alert_type")
        for alert in alerts
    }

    if "wetness" in alert_types:
        subject = "Wet Diaper Detected"
    elif "temperature" in alert_types:
        subject = "Temperature Alert"
    else:
        subject = "Baby Monitoring Alert"

    items = "".join(
        "<li>"
        f"<strong>{alert.get('severity', 'warning').upper()}</strong> - "
        f"{alert.get('message', '')}"
        "</li>"
        for alert in alerts
    )

    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    dashboard_url = os.getenv(
        "DASHBOARD_URL",
        "https://baby-monitoring-system-7.onrender.com",
    )

    html = f"""
    <html>
    <body>
        <h2>Baby Cradle Monitoring Alert</h2>

        <p>
            A new condition requiring attention was detected.
        </p>

        <p>
            <strong>Time:</strong> {timestamp}
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
        "to": [ALERT_EMAIL],
        "subject": subject,
        "html": html,
    }

    try:
        response = requests.post(
            f"{bird_host()}/v1/email/messages",
            headers={
                "Authorization":
                    f"Bearer {BIRD_API_KEY}",
                "Content-Type":
                    "application/json",
            },
            json=payload,
            timeout=15,
        )

        if response.status_code in (200, 202):
            log.info("Alert email sent")
            return True

        log.error(
            "Bird email failed %s: %s",
            response.status_code,
            response.text[:300],
        )

    except Exception as exc:
        log.exception(
            "Bird email exception: %s",
            exc,
        )

    return False


# ============================================================
# SENSOR / ALERT LOGIC
# ============================================================

def check_abnormal(temp, hum):
    try:
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

    except (TypeError, ValueError):
        return False

    if temp is not None:
        if temp < temp_min or temp > temp_max:
            return True

    if hum is not None:
        if hum < hum_min or hum > hum_max:
            return True

    return False


alert_state_lock = threading.Lock()

previous_wetness = False
previous_temperature_abnormal = False


def check_alerts(
    temp,
    hum,
    wetness,
    sound,
):
    global previous_wetness
    global previous_temperature_abnormal

    try:
        temp_min = float(
            os.getenv("TEMP_MIN", "20")
        )
        temp_max = float(
            os.getenv("TEMP_MAX", "25")
        )

    except (TypeError, ValueError):
        temp_min = 20
        temp_max = 25

    alerts = []

    temperature_abnormal = False

    if temp is not None:
        temperature_abnormal = (
            temp < temp_min
            or
            temp > temp_max
        )

    current_wetness = bool(wetness)

    with alert_state_lock:

        new_temperature_event = (
            temperature_abnormal
            and
            not previous_temperature_abnormal
        )

        if new_temperature_event:

            if temp > temp_max:
                message = (
                    f"Temperature is too high: "
                    f"{temp}°C. Configured maximum "
                    f"is {temp_max}°C."
                )

            elif temp < temp_min:
                message = (
                    f"Temperature is too low: "
                    f"{temp}°C. Configured minimum "
                    f"is {temp_min}°C."
                )

            else:
                message = (
                    f"Abnormal temperature detected: "
                    f"{temp}°C."
                )

            alerts.append({
                "alert_type":
                    "temperature",

                "severity":
                    "critical",

                "message":
                    message,
            })

        previous_temperature_abnormal = (
            temperature_abnormal
        )

        new_wetness_event = (
            current_wetness
            and
            not previous_wetness
        )

        if new_wetness_event:

            alerts.append({
                "alert_type":
                    "wetness",

                "severity":
                    "critical",

                "message":
                    "Diaper is wet! "
                    "Please change the diaper.",
            })

        previous_wetness = current_wetness

    if not alerts:
        return []

    # Real-time alert to connected browsers.
    for alert in alerts:

        try:
            socketio.emit(
                "new_alert",
                alert,
            )

        except Exception as exc:
            log.debug(
                "Socket alert failed: %s",
                exc,
            )

    # Persist alerts in the background.
    if supabase is not None:

        def save_alerts():
            for alert in alerts:
                try:
                    (
                        supabase
                        .table("alerts")
                        .insert({
                            "alert_type":
                                alert["alert_type"],

                            "severity":
                                alert["severity"],

                            "message":
                                alert["message"],
                        })
                        .execute()
                    )

                except Exception as exc:
                    log.error(
                        "Alert database insert failed: %s",
                        exc,
                    )

            invalidate_alerts_cache()

        threading.Thread(
            target=save_alerts,
            daemon=True,
            name="alert-db",
        ).start()

    # Email never blocks the sensor request.
    if BIRD_API_KEY and ALERT_EMAIL:

        threading.Thread(
            target=send_alert_email,
            args=(alerts,),
            daemon=True,
            name="alert-email",
        ).start()

    return alerts


# ============================================================
# SENSOR DATABASE WRITE
# ============================================================

def save_sensor_reading(
    temp,
    hum,
    motion,
    sound,
    wetness,
    is_abnormal,
):
    if supabase is None:
        return

    try:

        (
            supabase
            .table("sensor_readings")
            .insert({
                "temperature":
                    temp,

                "humidity":
                    hum,

                "motion_detected":
                    motion,

                "sound_level":
                    sound,

                "wetness_detected":
                    wetness,

                "is_abnormal":
                    is_abnormal,
            })
            .execute()
        )

        invalidate_history_cache()

    except Exception as exc:

        log.error(
            "Sensor database insert failed: %s",
            exc,
        )


# ============================================================
# PAGE ROUTES
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
# CURRENT SENSOR DATA
# ============================================================

@app.route("/api/current_data")
def api_current_data():

    with current_data_lock:
        data = dict(current_data)

    response = jsonify(data)

    response.headers[
        "Cache-Control"
    ] = "no-store"

    return response


# ============================================================
# HISTORY
# ============================================================

@app.route("/api/history")
def api_history():

    if supabase is None:
        return jsonify([])

    limit = request.args.get(
        "limit",
        50,
        type=int,
    )

    limit = max(
        1,
        min(limit, 100),
    )

    cached = get_cached(
        history_cache,
        limit,
    )

    if cached is not None:
        return jsonify(cached)

    try:

        result = (
            supabase
            .table("sensor_readings")
            .select("*")
            .order(
                "created_at",
                desc=True,
            )
            .limit(limit)
            .execute()
        )

        data = result.data or []

        set_cached(
            history_cache,
            data,
            limit,
        )

        return jsonify(data)

    except Exception as exc:

        log.error(
            "History error: %s",
            exc,
        )

        return jsonify({
            "error":
                "Unable to load history",
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
        type=int,
    )

    limit = max(
        1,
        min(limit, 50),
    )

    cached = get_cached(
        alerts_cache,
        limit,
    )

    if cached is not None:
        return jsonify(cached)

    try:

        result = (
            supabase
            .table("alerts")
            .select("*")
            .order(
                "created_at",
                desc=True,
            )
            .limit(limit)
            .execute()
        )

        data = result.data or []

        set_cached(
            alerts_cache,
            data,
            limit,
        )

        return jsonify(data)

    except Exception as exc:

        log.error(
            "Alert history error: %s",
            exc,
        )

        return jsonify({
            "error":
                "Unable to load alerts",
        }), 500


# ============================================================
# CLEAR ALERTS
# ============================================================

@app.route(
    "/api/clear_alerts",
    methods=["POST"],
)
def clear_alerts():

    if supabase is None:
        return jsonify({
            "success": True,
        })

    try:

        (
            supabase
            .table("alerts")
            .update({
                "is_read": True,
            })
            .neq(
                "is_read",
                True,
            )
            .execute()
        )

        invalidate_alerts_cache()

        return jsonify({
            "success": True,
        })

    except Exception as exc:

        log.error(
            "Clear alerts error: %s",
            exc,
        )

        return jsonify({
            "error":
                "Unable to clear alerts",
        }), 500


# ============================================================
# RASPBERRY PI SENSOR INGEST
# ============================================================

@app.route(
    "/api/ingest",
    methods=["POST"],
)
def api_ingest():

    global last_sensor_emit
    global last_sensor_db_write

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
                "temperature, humidity",
        }), 400

    try:

        temp = float(
            data.get("temperature")
        )

        hum = float(
            data.get("humidity")
        )

    except (
        TypeError,
        ValueError,
    ):

        return jsonify({
            "error":
                "Temperature and humidity "
                "must be numbers",
        }), 400

    motion = bool(
        data.get(
            "motion_detected",
            False,
        )
    )

    sound = data.get(
        "sound_level",
        0,
    )

    wetness = bool(
        data.get(
            "wetness_detected",
            False,
        )
    )

    # --------------------------------------------------------
    # Update server-side current state immediately.
    # --------------------------------------------------------

    with current_data_lock:

        current_data[
            "temperature"
        ] = temp

        current_data[
            "humidity"
        ] = hum

        current_data[
            "motion"
        ] = motion

        current_data[
            "sound"
        ] = sound

        current_data[
            "wetness"
        ] = wetness

        current_data[
            "last_update"
        ] = datetime.now().isoformat()

        data_for_socket = dict(
            current_data
        )

    # --------------------------------------------------------
    # Alerts are checked immediately.
    # --------------------------------------------------------

    check_alerts(
        temp,
        hum,
        wetness,
        sound,
    )

    is_abnormal = check_abnormal(
        temp,
        hum,
    )

    # --------------------------------------------------------
    # Browser sensor update: maximum once per 5 seconds.
    # --------------------------------------------------------

    now = time.monotonic()

    should_emit = False
    should_save = False

    with sensor_timing_lock:

        if (
            now - last_sensor_emit
            >= SENSOR_UPDATE_INTERVAL
        ):
            last_sensor_emit = now
            should_emit = True

        if (
            now - last_sensor_db_write
            >= SENSOR_UPDATE_INTERVAL
        ):
            last_sensor_db_write = now
            should_save = True

    if should_emit:

        try:

            socketio.emit(
                "sensor_update",
                data_for_socket,
            )

        except Exception as exc:

            log.debug(
                "Sensor socket emit failed: %s",
                exc,
            )

    # --------------------------------------------------------
    # Save at most one reading every 5 seconds.
    # --------------------------------------------------------

    if should_save and supabase is not None:

        threading.Thread(
            target=save_sensor_reading,
            args=(
                temp,
                hum,
                motion,
                sound,
                wetness,
                is_abnormal,
            ),
            daemon=True,
            name="sensor-db-write",
        ).start()

    return jsonify({
        "status": "ok",
        "abnormal": is_abnormal,
        "sensor_update_interval":
            SENSOR_UPDATE_INTERVAL,
    })


# ============================================================
# CAMERA FRAME UPLOAD
# ============================================================

@app.route(
    "/api/upload_frame",
    methods=["POST"],
)
def api_upload_frame():

    data = request.get_data()

    if (
        not data
        or
        len(data) < 100
    ):
        return jsonify({
            "error":
                "Empty or invalid frame",
        }), 400

    # Only JPEG data is expected from the Raspberry Pi.
    # Reject obviously invalid payloads.
    if not (
        data.startswith(b"\xff\xd8")
        and
        data.endswith(b"\xff\xd9")
    ):
        return jsonify({
            "error":
                "Frame must be JPEG data",
        }), 400

    push_video_frame(data)

    return jsonify({
        "status": "ok",
    })


# ============================================================
# LIVE MJPEG VIDEO
# ============================================================
#
# Browser:
#
#   <img src="/video_feed">
#
# The server sends:
#
#   JPEG -> JPEG -> JPEG -> JPEG ...
#
# The three-frame buffer keeps the stream smooth while dropping
# old frames to prevent latency from continuously increasing.
# ============================================================

@app.route("/video_feed")
def video_feed():

    def generate():

        # Send the latest frame immediately when available.
        first_frame = get_latest_video_frame()

        if first_frame is not None:

            yield (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n"
                b"Content-Length: "
                + str(len(first_frame)).encode()
                + b"\r\n"
                b"Cache-Control: no-cache\r\n"
                b"Pragma: no-cache\r\n"
                b"\r\n"
                + first_frame
                + b"\r\n"
            )

        last_frame = first_frame

        while True:

            # Wait until the camera provides another frame.
            with video_condition:

                video_condition.wait(
                    timeout=2.0
                )

                if video_buffer:

                    # Drain old frames and keep the newest one.
                    # This is what prevents stream latency.
                    frame = video_buffer[-1]

                    video_buffer.clear()

                else:

                    frame = None

            if frame is None:
                continue

            if frame is last_frame:
                continue

            last_frame = frame

            yield (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n"
                b"Content-Length: "
                + str(len(frame)).encode()
                + b"\r\n"
                b"Cache-Control: no-cache\r\n"
                b"Pragma: no-cache\r\n"
                b"\r\n"
                + frame
                + b"\r\n"
            )

    return Response(
        generate(),
        mimetype=(
            "multipart/x-mixed-replace; "
            "boundary=frame"
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
        },
    )


# ============================================================
# CHATBOT
# ============================================================

@app.route(
    "/api/chat",
    methods=["POST"],
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
        d = dict(current_data)

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
        "20",
    )

    tmax = os.getenv(
        "TEMP_MAX",
        "25",
    )

    hmin = os.getenv(
        "HUMIDITY_MIN",
        "40",
    )

    hmax = os.getenv(
        "HUMIDITY_MAX",
        "60",
    )

    motion_str = (
        "moving"
        if d.get("motion")
        else "quiet/sleeping"
    )

    diaper_str = (
        "wet - needs changing"
        if d.get("wetness")
        else "dry"
    )

    # --------------------------------------------------------
    # Temperature
    # --------------------------------------------------------

    if any(
        word in msg
        for word in [
            "temp",
            "hot",
            "cold",
            "warm",
        ]
    ):

        reply = (
            f"The current temperature is "
            f"{temp}°C. Safe range is "
            f"{tmin}-{tmax}°C."
        )

        if temp != "--":

            if temp < float(tmin):

                reply += (
                    " It is below the minimum."
                )

            elif temp > float(tmax):

                reply += (
                    " It is above the maximum."
                )

            else:

                reply += (
                    " This is within the normal range."
                )

    # --------------------------------------------------------
    # Humidity
    # --------------------------------------------------------

    elif any(
        word in msg
        for word in [
            "humid",
            "moist",
        ]
    ):

        reply = (
            f"The current humidity is "
            f"{hum}%. Safe range is "
            f"{hmin}-{hmax}%."
        )

        if hum != "--":

            if hum < float(hmin):

                reply += (
                    " It is below the minimum."
                )

            elif hum > float(hmax):

                reply += (
                    " It is above the maximum."
                )

            else:

                reply += (
                    " This is within the normal range."
                )

    # --------------------------------------------------------
    # Motion
    # --------------------------------------------------------

    elif any(
        word in msg
        for word in [
            "motion",
            "move",
            "moving",
            "activity",
            "active",
        ]
    ):

        reply = (
            f"Baby is currently "
            f"{motion_str}."
        )

    # --------------------------------------------------------
    # Sound
    # --------------------------------------------------------

    elif any(
        word in msg
        for word in [
            "sound",
            "noise",
            "loud",
            "cry",
            "crying",
        ]
    ):

        if d.get("sound"):

            reply = (
                "Sound level is currently "
                "loud/noisy. This may indicate "
                "crying or a loud environment."
            )

        else:

            reply = (
                "Sound level is currently quiet. "
                "No loud sounds detected."
            )

    # --------------------------------------------------------
    # Diaper
    # --------------------------------------------------------

    elif any(
        word in msg
        for word in [
            "diaper",
            "wet",
            "wee",
            "nappy",
            "change",
        ]
    ):

        reply = (
            f"Diaper is {diaper_str}."
        )

        if d.get("wetness"):

            reply += (
                " It is time for a change!"
            )

        else:

            reply += (
                " No change needed."
            )

    # --------------------------------------------------------
    # Greeting
    # --------------------------------------------------------

    elif any(
        word in msg
        for word in [
            "hi",
            "hello",
            "hey",
            "help",
        ]
    ):

        reply = (
            "Hello! I am your Baby Cradle "
            "Monitoring assistant. Ask about "
            "temperature, humidity, motion, "
            "sound, or diaper."
        )

    # --------------------------------------------------------
    # Status
    # --------------------------------------------------------

    elif any(
        word in msg
        for word in [
            "status",
            "summary",
            "all",
            "overview",
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
            else None,
        ):
            flags.append(
                "abnormal readings"
            )

        reply = (
            f"Temperature: {temp}°C | "
            f"Humidity: {hum}% | "
            f"Motion: {motion_str} | "
            f"Sound: "
            f"{'loud' if d.get('sound') else 'quiet'} | "
            f"Diaper: {diaper_str}"
        )

        if flags:

            reply += (
                "\n\nNotable: "
                +
                " · ".join(flags)
            )

    # --------------------------------------------------------
    # Unknown
    # --------------------------------------------------------

    else:

        reply = (
            "I can answer about temperature, "
            "humidity, motion, sound, diaper, "
            "or status."
        )

    return jsonify({
        "reply": reply,
    })


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    with video_condition:

        video_buffer_size = len(
            video_buffer
        )

    return jsonify({
        "status":
            "healthy",

        "database":
            supabase is not None,

        "sensor_update_interval":
            SENSOR_UPDATE_INTERVAL,

        "video_buffer_size":
            video_buffer_size,

        "video_buffer_capacity":
            VIDEO_BUFFER_SIZE,

        "timestamp":
            datetime.now().isoformat(),
    })


# ============================================================
# LOCAL DEVELOPMENT
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "5000",
        )
    )

    log.info(
        "Baby Cradle Monitoring Server "
        "starting on port %s...",
        port,
    )

    socketio.run(
        app,
        host="0.0.0.0",
        port=port,
        debug=False,
        allow_unsafe_werkzeug=True,
    )
