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
    level=logging.INFO,
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
    ping_interval=25,
    ping_timeout=60,
    max_http_buffer_size=1_000_000
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
            SUPABASE_KEY
        )

        log.info("Supabase connected")

    except Exception as e:

        log.exception(
            "Supabase initialization failed: %s",
            e
        )

else:

    log.warning(
        "SUPABASE_URL or SUPABASE_KEY missing"
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

frame_lock = threading.Lock()

frame_subscribers = []

subscribers_lock = threading.Lock()


def broadcast_frame(frame):
    """
    Broadcast only the newest frame.

    Every client has a queue of size 1.
    Therefore old frames are discarded.
    """

    with subscribers_lock:
        subscribers = list(frame_subscribers)

    for q in subscribers:

        try:

            q.put_nowait(frame)

        except queue.Full:

            # Throw away old frame.
            try:
                q.get_nowait()
            except queue.Empty:
                pass

            try:
                q.put_nowait(frame)
            except queue.Full:
                pass


# ============================================================
# SENSOR QUEUE
# ============================================================

SENSOR_QUEUE_SIZE = int(
    os.getenv(
        "SENSOR_QUEUE_SIZE",
        "30"
    )
)

sensor_queue = queue.Queue(
    maxsize=SENSOR_QUEUE_SIZE
)


# ============================================================
# EMAIL QUEUE
# ============================================================

EMAIL_QUEUE_SIZE = 10

email_queue = queue.Queue(
    maxsize=EMAIL_QUEUE_SIZE
)


# ============================================================
# BIRD EMAIL
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


# Reuse HTTP connection.
bird_session = requests.Session()


# ============================================================
# ALERT STATE
# ============================================================

alert_state_lock = threading.Lock()

previous_wetness = False

previous_temperature_abnormal = False


# ============================================================
# SIMPLE CACHE
# ============================================================

history_cache = None
history_cache_time = 0

alerts_cache = None
alerts_cache_time = 0

cache_lock = threading.Lock()

CACHE_SECONDS = float(
    os.getenv(
        "CACHE_SECONDS",
        "2"
    )
)


def invalidate_history_cache():

    global history_cache
    global history_cache_time

    with cache_lock:

        history_cache = None
        history_cache_time = 0


def invalidate_alert_cache():

    global alerts_cache
    global alerts_cache_time

    with cache_lock:

        alerts_cache = None
        alerts_cache_time = 0


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

    return f"https://{region}.platform.bird.com"


# ============================================================
# EMAIL
# ============================================================

def send_alert_email(alerts):

    if not alerts:
        return False

    if not BIRD_API_KEY:
        log.warning("BIRD_API_KEY not configured")
        return False

    if not ALERT_EMAIL:
        log.warning("ALERT_EMAIL not configured")
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
        "to": [ALERT_EMAIL],
        "subject": subject,
        "html": html
    }


    try:

        response = bird_session.post(
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


        if response.status_code in (200, 202):

            log.info(
                "Alert email sent (%s)",
                response.status_code
            )

            return True


        log.error(
            "Bird email failed: %s %s",
            response.status_code,
            response.text[:200]
        )


    except Exception as e:

        log.error(
            "Bird email exception: %s",
            e
        )


    return False


# ============================================================
# EMAIL WORKER
# ============================================================

def email_worker():

    log.info("Email worker started")

    while True:

        try:

            alerts = email_queue.get()

            try:
                send_alert_email(alerts)

            except Exception:

                log.exception(
                    "Email worker error"
                )

            finally:

                email_queue.task_done()

        except Exception:

            log.exception(
                "Email queue error"
            )


# Only ONE email worker.
email_thread = threading.Thread(
    target=email_worker,
    daemon=True
)

email_thread.start()


# ============================================================
# ABNORMAL SENSOR CHECK
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

    except ValueError:

        return False


    if temp is not None:

        if temp < temp_min or temp > temp_max:
            return True


    if hum is not None:

        if hum < hum_min or hum > hum_max:
            return True


    return False


# ============================================================
# ALERT DETECTION
# ============================================================

def check_alerts(
    temp,
    hum,
    wetness,
    sound
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

    except ValueError:

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
                    f"🌡️ Temperature is too high: "
                    f"{temp}°C. Configured maximum is "
                    f"{temp_max}°C."
                )

            else:

                message = (
                    f"🌡️ Temperature is too low: "
                    f"{temp}°C. Configured minimum is "
                    f"{temp_min}°C."
                )


            alerts.append({
                "alert_type": "temperature",
                "severity": "critical",
                "message": message
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
                "alert_type": "wetness",
                "severity": "critical",
                "message":
                    "💧 Diaper is wet! "
                    "Please change the diaper."
            })


        previous_wetness = current_wetness


    if not alerts:
        return


    # ========================================================
    # DATABASE
    # ========================================================

    if supabase is not None:

        for alert in alerts:

            try:

                supabase.table(
                    "alerts"
                ).insert(
                    alert
                ).execute()

                log.info(
                    "NEW ALERT: %s",
                    alert["message"]
                )

                invalidate_alert_cache()

            except Exception as e:

                log.error(
                    "Alert DB error: %s",
                    e
                )


    # ========================================================
    # SOCKET
    # ========================================================

    for alert in alerts:

        try:

            socketio.emit(
                "new_alert",
                alert
            )

        except Exception as e:

            log.debug(
                "Socket alert emit failed: %s",
                e
            )


    # ========================================================
    # EMAIL
    #
    # DO NOT SEND EMAIL HERE.
    #
    # Put it in a separate queue.
    # ========================================================

    try:

        email_queue.put_nowait(
            alerts
        )

    except queue.Full:

        log.warning(
            "Email queue full; skipping email"
        )


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

    # ========================================================
    # DATABASE
    # ========================================================

    if supabase is not None:

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

            supabase.table(
                "sensor_readings"
            ).insert(
                reading
            ).execute()

            invalidate_history_cache()

        except Exception as e:

            log.error(
                "Sensor DB error: %s",
                e
            )


    # ========================================================
    # ALERT
    # ========================================================

    check_alerts(
        temp,
        hum,
        wetness,
        sound
    )


# ============================================================
# SENSOR WORKER
# ============================================================

def sensor_worker():

    log.info(
        "Sensor worker started"
    )

    while True:

        item = sensor_queue.get()

        try:

            process_sensor_reading(
                *item
            )

        except Exception:

            log.exception(
                "Sensor worker error"
            )

        finally:

            sensor_queue.task_done()


# IMPORTANT:
# Only ONE database worker by default.
#
# This prevents multiple simultaneous Supabase
# connections from overwhelming a small Render instance.

SENSOR_WORKERS = int(
    os.getenv(
        "SENSOR_WORKERS",
        "1"
    )
)


for _ in range(
    max(1, min(SENSOR_WORKERS, 2))
):

    threading.Thread(
        target=sensor_worker,
        daemon=True
    ).start()


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

        return jsonify(
            dict(current_data)
        )


# ============================================================
# HISTORY API
# ============================================================

@app.route("/api/history")
def api_history():

    global history_cache
    global history_cache_time


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


    # ========================================================
    # CACHE
    # ========================================================

    with cache_lock:

        if (
            history_cache is not None
            and
            now - history_cache_time < CACHE_SECONDS
        ):

            return jsonify(
                history_cache[:limit]
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


        data = response.data or []


        with cache_lock:

            history_cache = data
            history_cache_time = time.monotonic()


        return jsonify(data)


    except Exception as e:

        log.error(
            "History error: %s",
            e
        )

        return jsonify({
            "error": "Unable to load history"
        }), 500


# ============================================================
# ALERT HISTORY
# ============================================================

@app.route("/api/alerts")
def api_alerts():

    global alerts_cache
    global alerts_cache_time


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


    # ========================================================
    # CACHE
    # ========================================================

    with cache_lock:

        if (
            alerts_cache is not None
            and
            now - alerts_cache_time < CACHE_SECONDS
        ):

            return jsonify(
                alerts_cache[:limit]
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


        data = response.data or []


        with cache_lock:

            alerts_cache = data
            alerts_cache_time = time.monotonic()


        return jsonify(data)


    except Exception as e:

        log.error(
            "Alert history error: %s",
            e
        )

        return jsonify({
            "error": "Unable to load alerts"
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


        invalidate_alert_cache()


        try:

            socketio.emit(
                "alerts_cleared",
                {
                    "success": True
                }
            )

        except Exception:
            pass


        return jsonify({
            "success": True
        })


    except Exception as e:

        log.error(
            "Clear alerts error: %s",
            e
        )

        return jsonify({
            "error": "Unable to clear alerts"
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
                "Missing required fields"
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


    # ========================================================
    # UPDATE MEMORY IMMEDIATELY
    # ========================================================

    with current_data_lock:

        current_data.update({

            "temperature": temp,

            "humidity": hum,

            "motion": motion,

            "sound": sound,

            "wetness": wetness,

            "last_update":
                datetime.now().isoformat()
        })


        socket_data = dict(
            current_data
        )


    # ========================================================
    # SEND REAL-TIME SENSOR UPDATE
    # ========================================================

    try:

        socketio.emit(
            "sensor_update",
            socket_data
        )

    except Exception as e:

        log.debug(
            "Sensor socket emit failed: %s",
            e
        )


    # ========================================================
    # QUEUE DATABASE WORK
    # ========================================================

    item = (
        temp,
        hum,
        motion,
        sound,
        wetness
    )


    try:

        sensor_queue.put_nowait(
            item
        )

    except queue.Full:

        # Remove the oldest item.

        try:

            sensor_queue.get_nowait()

            sensor_queue.task_done()

        except queue.Empty:

            pass


        try:

            sensor_queue.put_nowait(
                item
            )

        except queue.Full:

            log.warning(
                "Sensor queue full; "
                "reading discarded"
            )


    # ========================================================
    # RESPOND IMMEDIATELY
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
# VIDEO FRAME UPLOAD
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
            "error": "Invalid frame"
        }), 400


    # ========================================================
    # KEEP ONLY NEWEST FRAME
    # ========================================================

    with frame_lock:

        latest_frame = data


    # ========================================================
    # BROADCAST
    # ========================================================

    broadcast_frame(
        data
    )


    return jsonify({
        "status": "ok"
    })


# ============================================================
# VIDEO FEED
# ============================================================

@app.route(
    "/video_feed"
)
def video_feed():

    def generate():

        # ONE FRAME ONLY.
        #
        # This is the key to preventing video latency.

        q = queue.Queue(
            maxsize=1
        )


        with subscribers_lock:

            frame_subscribers.append(
                q
            )


        try:

            # =================================================
            # SEND CURRENT FRAME
            # =================================================

            with frame_lock:

                frame = latest_frame


            if frame is not None:

                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Cache-Control: no-cache\r\n"
                    b"Pragma: no-cache\r\n"
                    b"\r\n"
                    + frame
                    + b"\r\n"
                )


            # =================================================
            # NEW FRAMES
            # =================================================

            while True:

                frame = q.get()


                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Cache-Control: no-cache\r\n"
                    b"Pragma: no-cache\r\n"
                    b"\r\n"
                    + frame
                    + b"\r\n"
                )


        except GeneratorExit:

            pass


        except Exception as e:

            log.debug(
                "Video connection ended: %s",
                e
            )


        finally:

            with subscribers_lock:

                try:

                    frame_subscribers.remove(
                        q
                    )

                except ValueError:

                    pass


    return Response(
        generate(),

        mimetype=
            "multipart/x-mixed-replace; "
            "boundary=frame",

        headers={

            "Cache-Control":
                "no-cache, no-store, must-revalidate",

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
            f"Baby is currently "
            f"**{motion_str}**."
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

        else:

            reply = (
                "Sound level is currently "
                "**quiet**."
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
                " It is time for a change."
            )

        else:

            reply += (
                " No wetness detected."
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
            "temperature, humidity, motion, "
            "sound, diaper, or status."
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
            temp if temp != "--" else None,
            hum if hum != "--" else None
        ):

            flags.append(
                "abnormal readings"
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
                "\n\nNotable: "
                + " · ".join(flags)
            )


    else:

        reply = (
            "I can answer about temperature, "
            "humidity, motion, sound, diaper, "
            "or status."
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
            sensor_queue.qsize(),

        "email_queue":
            email_queue.qsize(),

        "video_clients":
            len(frame_subscribers)

    })


# ============================================================
# SOCKET.IO EVENTS
# ============================================================

@socketio.on("connect")
def socket_connect():

    log.info(
        "Socket.IO client connected"
    )


@socketio.on("disconnect")
def socket_disconnect():

    log.info(
        "Socket.IO client disconnected"
    )


# ============================================================
# LOCAL DEVELOPMENT
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "5000"
        )
    )


    log.info(
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
