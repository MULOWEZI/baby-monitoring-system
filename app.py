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
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger(__name__)


# ============================================================
# ENVIRONMENT
# ============================================================

load_dotenv()


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
#
# IMPORTANT:
# This application uses normal Python threads and blocking
# Supabase/HTTP requests.
#
# Therefore use:
#
# gunicorn --worker-class gthread --workers 1 --threads 8 app:app
#
# NOT gevent.
# ============================================================

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading",
    logger=False,
    engineio_logger=False,
    ping_interval=25,
    ping_timeout=60
)


# ============================================================
# SUPABASE
# ============================================================

SUPABASE_URL = os.getenv(
    "SUPABASE_URL",
    ""
)

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
        "SUPABASE_URL/KEY not set - database disabled"
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
# DATABASE CACHE
# ============================================================
#
# Dashboard navigation can repeatedly request history/alerts.
# Instead of hitting Supabase every single time, keep a small
# cache for a few seconds.
# ============================================================

CACHE_TTL = 5.0

history_cache = {
    "data": [],
    "timestamp": 0,
    "limit": 50
}

alerts_cache = {
    "data": [],
    "timestamp": 0,
    "limit": 20
}

cache_lock = threading.Lock()


def invalidate_history_cache():

    with cache_lock:

        history_cache["timestamp"] = 0


def invalidate_alerts_cache():

    with cache_lock:

        alerts_cache["timestamp"] = 0


# ============================================================
# VIDEO STREAM
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
# SENSOR READING QUEUE
# ============================================================
#
# IMPORTANT:
# This is defined BEFORE sensor_worker().
#
# This fixes:
#
# NameError: name 'reading_queue' is not defined
# ============================================================

READING_QUEUE_SIZE = 100

reading_queue = queue.Queue(
    maxsize=READING_QUEUE_SIZE
)


# ============================================================
# DATABASE QUEUE
# ============================================================

DATABASE_QUEUE_SIZE = 200

database_queue = queue.Queue(
    maxsize=DATABASE_QUEUE_SIZE
)


# ============================================================
# EMAIL QUEUE
# ============================================================

EMAIL_QUEUE_SIZE = 20

email_queue = queue.Queue(
    maxsize=EMAIL_QUEUE_SIZE
)


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

        log.warning(
            "BIRD_API_KEY not set - email skipped"
        )

        return False

    if not ALERT_EMAIL:

        log.warning(
            "ALERT_EMAIL not set - email skipped"
        )

        return False


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


    items = ""

    for alert in alerts:

        items += (
            "<li>"
            f"<strong>"
            f"{alert.get('severity', 'warning').upper()}"
            f"</strong> - "
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

        <h2>Baby Cradle Monitoring Alert</h2>

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

            timeout=15
        )


        if response.status_code in (
            200,
            202
        ):

            log.info(
                "Alert email sent"
            )

            return True


        log.error(
            "Bird email failed %s: %s",
            response.status_code,
            response.text[:300]
        )


    except Exception as e:

        log.exception(
            "Bird email exception: %s",
            e
        )


    return False


# ============================================================
# ABNORMAL SENSOR CHECK
# ============================================================

def check_abnormal(
    temp,
    hum
):

    try:

        temp_min = float(
            os.getenv(
                "TEMP_MIN",
                "20"
            )
        )

        temp_max = float(
            os.getenv(
                "TEMP_MAX",
                "25"
            )
        )

        hum_min = float(
            os.getenv(
                "HUMIDITY_MIN",
                "40"
            )
        )

        hum_max = float(
            os.getenv(
                "HUMIDITY_MAX",
                "60"
            )
        )

    except ValueError:

        return False


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
            os.getenv(
                "TEMP_MIN",
                "20"
            )
        )

        temp_max = float(
            os.getenv(
                "TEMP_MAX",
                "25"
            )
        )

    except ValueError:

        temp_min = 20
        temp_max = 25


    alerts = []


    # ========================================================
    # TEMPERATURE
    # ========================================================

    temperature_abnormal = False

    if temp is not None:

        temperature_abnormal = (

            temp < temp_min

            or

            temp > temp_max
        )


    # ========================================================
    # WETNESS
    # ========================================================

    current_wetness = bool(
        wetness
    )


    # ========================================================
    # UPDATE STATE
    # ========================================================

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
                    f"{temp}°C. "
                    f"Configured maximum is "
                    f"{temp_max}°C."
                )

            elif temp < temp_min:

                message = (
                    f"Temperature is too low: "
                    f"{temp}°C. "
                    f"Configured minimum is "
                    f"{temp_min}°C."
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
                    message
            })


        previous_temperature_abnormal = (
            temperature_abnormal
        )


        # ====================================================
        # WETNESS
        # ====================================================

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
                    "Please change the diaper."
            })


        previous_wetness = (
            current_wetness
        )


    if not alerts:

        return


    # ========================================================
    # SEND ALERT TO DASHBOARD IMMEDIATELY
    # ========================================================

    for alert in alerts:

        try:

            socketio.emit(
                "new_alert",
                alert
            )

        except Exception as e:

            log.debug(
                "Socket alert failed: %s",
                e
            )


    # ========================================================
    # DATABASE ALERT INSERT
    # ========================================================

    for alert in alerts:

        try:

            database_queue.put_nowait({

                "type":
                    "alert",

                "data":
                    alert
            })

        except queue.Full:

            log.warning(
                "Database queue full - alert dropped"
            )


    # ========================================================
    # EMAIL
    # ========================================================
    #
    # Email is NEVER sent inside the HTTP request.
    # ========================================================

    try:

        email_queue.put_nowait(
            alerts
        )

    except queue.Full:

        log.warning(
            "Email queue full - email skipped"
        )


# ============================================================
# PROCESS SENSOR READING
# ============================================================

def process_sensor_reading(
    temp,
    hum,
    motion,
    sound,
    wetness
):

    # ========================================================
    # DETECT ALERTS FIRST
    # ========================================================
    #
    # This is fast and does not wait for Supabase.
    # ========================================================

    check_alerts(
        temp,
        hum,
        wetness,
        sound
    )


    # ========================================================
    # QUEUE DATABASE WRITE
    # ========================================================

    reading = {

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
            check_abnormal(
                temp,
                hum
            )
    }


    try:

        database_queue.put_nowait({

            "type":
                "sensor",

            "data":
                reading
        })


    except queue.Full:

        # Drop old database data rather than blocking
        # incoming sensor requests.

        try:

            database_queue.get_nowait()

            database_queue.task_done()

        except queue.Empty:

            pass


        try:

            database_queue.put_nowait({

                "type":
                    "sensor",

                "data":
                    reading
            })

        except queue.Full:

            log.warning(
                "Database queue full - reading dropped"
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

        except Exception:

            log.exception(
                "Sensor processing error"
            )

        finally:

            reading_queue.task_done()


# ============================================================
# DATABASE WORKER
# ============================================================

def database_worker():

    log.warning(
        "Database worker started"
    )


    while True:

        job = database_queue.get()

        try:

            if supabase is None:

                continue


            job_type = job.get(
                "type"
            )

            data = job.get(
                "data"
            )


            # =================================================
            # SENSOR READING
            # =================================================

            if job_type == "sensor":

                supabase.table(
                    "sensor_readings"
                ).insert(
                    data
                ).execute()


                invalidate_history_cache()


            # =================================================
            # ALERT
            # =================================================

            elif job_type == "alert":

                supabase.table(
                    "alerts"
                ).insert(
                    data
                ).execute()


                invalidate_alerts_cache()


        except Exception as e:

            log.error(
                "Database worker error: %s",
                e
            )

        finally:

            database_queue.task_done()


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

        except Exception:

            log.exception(
                "Email worker error"
            )

        finally:

            email_queue.task_done()


# ============================================================
# START BACKGROUND WORKERS
# ============================================================
#
# IMPORTANT:
# Queues are already defined above.
# ============================================================

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


email_thread = threading.Thread(
    target=email_worker,
    daemon=True,
    name="email-worker"
)

email_thread.start()


# ============================================================
# HOME
# ============================================================

@app.route("/")
def index():

    response = render_template(
        "index.html"
    )

    return Response(
        response,
        headers={
            "Cache-Control":
                "no-cache"
        }
    )


# ============================================================
# LIVE
# ============================================================

@app.route("/live")
def live():

    response = render_template(
        "live.html"
    )

    return Response(
        response,
        headers={
            "Cache-Control":
                "no-cache"
        }
    )


# ============================================================
# HISTORY
# ============================================================

@app.route("/history")
def history():

    response = render_template(
        "history.html"
    )

    return Response(
        response,
        headers={
            "Cache-Control":
                "no-cache"
        }
    )


# ============================================================
# CURRENT DATA
# ============================================================

@app.route(
    "/api/current_data"
)
def api_current_data():

    with current_data_lock:

        data = dict(
            current_data
        )


    response = jsonify(
        data
    )

    response.headers[
        "Cache-Control"
    ] = "no-store"


    return response


# ============================================================
# SENSOR HISTORY
# ============================================================

@app.route(
    "/api/history"
)
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


    # ========================================================
    # CACHE
    # ========================================================

    with cache_lock:

        cached_data = history_cache[
            "data"
        ]

        cached_time = history_cache[
            "timestamp"
        ]

        cached_limit = history_cache[
            "limit"
        ]


        if (

            cached_limit == limit

            and

            cached_data

            and

            now - cached_time < CACHE_TTL

        ):

            return jsonify(
                cached_data
            )


    # ========================================================
    # DATABASE
    # ========================================================

    try:

        response = (

            supabase

            .table(
                "sensor_readings"
            )

            .select(
                "*"
            )

            .order(
                "created_at",
                desc=True
            )

            .limit(
                limit
            )

            .execute()
        )


        data = (
            response.data
            or []
        )


        with cache_lock:

            history_cache[
                "data"
            ] = data

            history_cache[
                "timestamp"
            ] = time.monotonic()

            history_cache[
                "limit"
            ] = limit


        return jsonify(
            data
        )


    except Exception as e:

        log.error(
            "History error: %s",
            e
        )


        return jsonify({

            "error":
                "Unable to load history"

        }), 500


# ============================================================
# ALERT HISTORY
# ============================================================

@app.route(
    "/api/alerts"
)
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


    # ========================================================
    # CACHE
    # ========================================================

    with cache_lock:

        cached_data = alerts_cache[
            "data"
        ]

        cached_time = alerts_cache[
            "timestamp"
        ]

        cached_limit = alerts_cache[
            "limit"
        ]


        if (

            cached_limit == limit

            and

            cached_data

            and

            now - cached_time < CACHE_TTL

        ):

            return jsonify(
                cached_data
            )


    # ========================================================
    # DATABASE
    # ========================================================

    try:

        response = (

            supabase

            .table(
                "alerts"
            )

            .select(
                "*"
            )

            .order(
                "created_at",
                desc=True
            )

            .limit(
                limit
            )

            .execute()
        )


        data = (
            response.data
            or []
        )


        with cache_lock:

            alerts_cache[
                "data"
            ] = data

            alerts_cache[
                "timestamp"
            ] = time.monotonic()

            alerts_cache[
                "limit"
            ] = limit


        return jsonify(
            data
        )


    except Exception as e:

        log.error(
            "Alert history error: %s",
            e
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

            .table(
                "alerts"
            )

            .update({
                "is_read":
                    True
            })

            .neq(
                "is_read",
                True
            )

            .execute()
        )


        invalidate_alerts_cache()


        return jsonify({

            "success":
                True
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


    # ========================================================
    # REQUIRED DATA
    # ========================================================

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


    # ========================================================
    # CONVERT VALUES
    # ========================================================

    try:

        temp = float(
            data.get(
                "temperature"
            )
        )

        hum = float(
            data.get(
                "humidity"
            )
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
    # UPDATE CURRENT DATA
    # ========================================================
    #
    # This happens immediately.
    #
    # No Supabase request.
    # No email request.
    # No database request.
    # ========================================================

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


    # ========================================================
    # SEND LIVE SENSOR UPDATE
    # ========================================================

    try:

        socketio.emit(
            "sensor_update",
            data_for_socket
        )

    except Exception as e:

        log.debug(
            "Sensor socket emit failed: %s",
            e
        )


    # ========================================================
    # QUEUE PROCESSING
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

            log.warning(
                "Sensor queue full - "
                "dropping reading"
            )


    # ========================================================
    # RETURN IMMEDIATELY
    # ========================================================

    return jsonify({

        "status":
            "ok",

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


    # ========================================================
    # REPLACE OLD FRAME
    # ========================================================

    with _frame_lock:

        latest_frame = data


    # ========================================================
    # BROADCAST NEWEST FRAME
    # ========================================================

    _broadcast_frame(
        data
    )


    return jsonify({

        "status":
            "ok"

    })


# ============================================================
# VIDEO FEED
# ============================================================

@app.route(
    "/video_feed"
)
def video_feed():

    def generate():

        # ====================================================
        # ONE FRAME BUFFER ONLY
        # ====================================================

        q = queue.Queue(
            maxsize=1
        )


        with _subscribers_lock:

            _frame_subscribers.append(
                q
            )


        try:

            # =================================================
            # SEND CURRENT FRAME
            # =================================================

            with _frame_lock:

                frame = latest_frame


            if frame is not None:

                yield (

                    b"--frame\r\n"

                    b"Content-Type: image/jpeg\r\n"

                    b"Cache-Control: no-cache\r\n"

                    b"Pragma: no-cache\r\n"

                    b"\r\n"

                    +

                    frame

                    +

                    b"\r\n"
                )


            # =================================================
            # WAIT FOR NEW FRAMES
            # =================================================

            while True:

                frame = q.get()


                yield (

                    b"--frame\r\n"

                    b"Content-Type: image/jpeg\r\n"

                    b"Cache-Control: no-cache\r\n"

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

                if q in _frame_subscribers:

                    _frame_subscribers.remove(
                        q
                    )


    return Response(

        generate(),

        mimetype=
            "multipart/x-mixed-replace; "
            "boundary=frame",

        headers={

            "Cache-Control":
                "no-cache, no-store, "
                "must-revalidate",

            "Pragma":
                "no-cache",

            "Expires":
                "0",

            "X-Accel-Buffering":
                "no"
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

        data.get(
            "message"
        )

        or ""

    ).lower().strip()


    with current_data_lock:

        d = dict(
            current_data
        )


    temp = (

        d.get(
            "temperature"
        )

        if d.get(
            "temperature"
        ) is not None

        else "--"
    )


    hum = (

        d.get(
            "humidity"
        )

        if d.get(
            "humidity"
        ) is not None

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

        if d.get(
            "motion"
        )

        else

        "quiet/sleeping"
    )


    diaper_str = (

        "wet - needs changing"

        if d.get(
            "wetness"
        )

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
            f"{motion_str}."
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
                "loud/noisy. This may indicate "
                "crying or a loud environment."
            )

        else:

            reply = (
                "Sound level is currently quiet. "
                "No loud sounds detected."
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

            "Hello! I am your Baby Cradle "
            "Monitoring assistant. Ask about "
            "temperature, humidity, motion, "
            "sound, or diaper."
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

                " · ".join(
                    flags
                )
            )


    # ========================================================
    # UNKNOWN
    # ========================================================

    else:

        reply = (

            "I can answer about temperature, "
            "humidity, motion, sound, diaper, "
            "or status."
        )


    return jsonify({

        "reply":
            reply

    })


# ============================================================
# HEALTH CHECK
# ============================================================

@app.route(
    "/health"
)
def health():

    return jsonify({

        "status":
            "healthy",

        "database":
            supabase is not None,

        "sensor_queue":
            reading_queue.qsize(),

        "database_queue":
            database_queue.qsize(),

        "email_queue":
            email_queue.qsize(),

        "timestamp":
            datetime.now().isoformat()

    })


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
        "starting on port %s...",
        port
    )


    socketio.run(

        app,

        host="0.0.0.0",

        port=port,

        debug=False,

        allow_unsafe_werkzeug=True
    )
