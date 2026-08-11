#!/usr/bin/env python3

import os
import sys
import time
import queue
import threading
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

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

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading"
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
        "SUPABASE_URL / SUPABASE_KEY not set"
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
    "last_update": None,
}

_data_lock = threading.Lock()


# ============================================================
# VIDEO FRAME STORAGE
# ============================================================

latest_frame = None

_frame_lock = threading.Lock()

_frame_subscribers = []

_subscribers_lock = threading.Lock()


def _broadcast_frame(frame):
    """
    Send the newest frame to all connected browsers.

    Each browser has a queue of size 1.
    Old frames are discarded if the browser is slow.
    """

    with _subscribers_lock:

        dead_subscribers = []

        for q in _frame_subscribers:

            try:

                # If queue is full, remove old frame.
                if q.full():

                    try:
                        q.get_nowait()
                    except queue.Empty:
                        pass

                q.put_nowait(frame)

            except Exception:

                dead_subscribers.append(q)

        for q in dead_subscribers:

            try:
                _frame_subscribers.remove(q)
            except ValueError:
                pass


# ============================================================
# CONTROLLED BACKGROUND WORKERS
# ============================================================

# IMPORTANT:
# Do NOT create unlimited threads for every sensor request.
#
# The old implementation did:
#
# threading.Thread(...).start()
#
# on every /api/ingest request.
#
# With frequent Raspberry Pi readings this can exhaust
# resources on a small Render instance.

MAX_BACKGROUND_WORKERS = int(
    os.getenv("MAX_BACKGROUND_WORKERS", "2")
)

executor = ThreadPoolExecutor(
    max_workers=MAX_BACKGROUND_WORKERS,
    thread_name_prefix="sensor-worker"
)


# ============================================================
# ALERT CONFIGURATION
# ============================================================

BIRD_API_KEY = os.getenv("BIRD_API_KEY", "")

BIRD_SENDER = os.getenv(
    "BIRD_SENDER",
    "onboarding@messagebird.dev"
)

ALERT_EMAIL = os.getenv(
    "ALERT_EMAIL",
    ""
)


# Email cooldown.
EMAIL_COOLDOWN = int(
    os.getenv("EMAIL_COOLDOWN", "300")
)

_last_email_ts = 0

_email_lock = threading.Lock()


# Database alert cooldown.
#
# Prevents the same abnormal condition from creating
# hundreds of rows per minute.

ALERT_DB_COOLDOWN = int(
    os.getenv("ALERT_DB_COOLDOWN", "60")
)

_last_alert_ts = {}

_alert_lock = threading.Lock()


# ============================================================
# BIRD EMAIL
# ============================================================

def bird_host():

    parts = BIRD_API_KEY.split("_")

    region = (
        parts[1]
        if len(parts) > 1 and parts[1]
        else "us1"
    )

    return f"https://{region}.platform.bird.com"


def send_alert_email(alerts):

    global _last_email_ts

    if not BIRD_API_KEY:

        log.warning(
            "BIRD_API_KEY not set - email disabled"
        )

        return False

    if not ALERT_EMAIL:

        log.warning(
            "ALERT_EMAIL not set - email disabled"
        )

        return False

    now = time.time()

    # Thread-safe email cooldown
    with _email_lock:

        if now - _last_email_ts < EMAIL_COOLDOWN:

            return False

        # Reserve cooldown immediately.
        # This prevents several worker threads from
        # simultaneously sending the same email.

        _last_email_ts = now

    items = "".join(
        f"<li><b>{a['severity'].upper()}</b> - "
        f"{a['message']}</li>"
        for a in alerts
    )

    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    subject = "Baby Monitoring Alert"

    if any(
        a["alert_type"] == "crying"
        for a in alerts
    ):
        subject = "Baby Crying Detected"

    elif any(
        a["alert_type"] == "wetness"
        for a in alerts
    ):
        subject = "Wet Diaper Detected"

    elif any(
        a["alert_type"] == "temperature"
        for a in alerts
    ):
        subject = "Temperature Alert"

    elif any(
        a["alert_type"] == "humidity"
        for a in alerts
    ):
        subject = "Humidity Alert"

    payload = {

        "from": BIRD_SENDER,

        "to": [
            ALERT_EMAIL
        ],

        "subject": subject,

        "html": (
            "<h2>Baby Cradle Alert</h2>"

            f"<p>Detected {len(alerts)} "
            f"issue(s) at "
            f"<b>{timestamp}</b>:</p>"

            f"<ul>{items}</ul>"

            "<p>Open the Baby Monitoring "
            "dashboard to view the current "
            "status.</p>"
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

            timeout=10
        )

        if response.status_code in (200, 202):

            log.info(
                "Alert email sent to %s (%s)",
                ALERT_EMAIL,
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
# THRESHOLDS
# ============================================================

def get_thresholds():

    temp_min = float(
        os.getenv("TEMP_MIN", "20")
    )

    temp_max = float(
        os.getenv("TEMP_MAX", "25")
    )

    humidity_min = float(
        os.getenv("HUMIDITY_MIN", "40")
    )

    humidity_max = float(
        os.getenv("HUMIDITY_MAX", "60")
    )

    return (
        temp_min,
        temp_max,
        humidity_min,
        humidity_max
    )


# ============================================================
# ABNORMAL CHECK
# ============================================================

def check_abnormal(temp, hum):

    (
        temp_min,
        temp_max,
        hum_min,
        hum_max
    ) = get_thresholds()

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
# ALERT GENERATION
# ============================================================

def check_alerts(
    temp,
    hum,
    wetness,
    sound
):

    (
        temp_min,
        temp_max,
        hum_min,
        hum_max
    ) = get_thresholds()

    alerts = []

    # --------------------------------------------------------
    # TEMPERATURE
    # --------------------------------------------------------

    if temp is not None:

        if temp < temp_min:

            alerts.append({

                "alert_type":
                    "temperature",

                "severity":
                    "critical",

                "message":
                    f"Temperature too low: "
                    f"{temp}°C"
            })

        elif temp > temp_max:

            alerts.append({

                "alert_type":
                    "temperature",

                "severity":
                    "critical",

                "message":
                    f"Temperature too high: "
                    f"{temp}°C"
            })

    # --------------------------------------------------------
    # HUMIDITY
    # --------------------------------------------------------

    if hum is not None:

        if hum < hum_min:

            alerts.append({

                "alert_type":
                    "humidity",

                "severity":
                    "warning",

                "message":
                    f"Humidity too low: "
                    f"{hum}%"
            })

        elif hum > hum_max:

            alerts.append({

                "alert_type":
                    "humidity",

                "severity":
                    "warning",

                "message":
                    f"Humidity too high: "
                    f"{hum}%"
            })

    # --------------------------------------------------------
    # WETNESS
    # --------------------------------------------------------

    if wetness:

        alerts.append({

            "alert_type":
                "wetness",

            "severity":
                "critical",

            "message":
                "Diaper is wet! "
                "Please change the diaper."
        })

    # --------------------------------------------------------
    # SOUND
    # --------------------------------------------------------

    if sound:

        alerts.append({

            "alert_type":
                "crying",

            "severity":
                "critical",

            "message":
                "Baby is crying!"
        })

    return alerts


# ============================================================
# ALERT DATABASE INSERT
# ============================================================

def save_alerts(alerts):

    if not alerts:

        return

    if supabase is None:

        return

    now = time.time()

    for alert in alerts:

        alert_type = alert["alert_type"]

        # Prevent repeated inserts of the exact same
        # alert every time the Pi sends sensor data.

        with _alert_lock:

            previous = _last_alert_ts.get(
                alert_type,
                0
            )

            if (
                now - previous
                < ALERT_DB_COOLDOWN
            ):
                continue

            _last_alert_ts[
                alert_type
            ] = now

        try:

            supabase.table(
                "alerts"
            ).insert({

                "alert_type":
                    alert["alert_type"],

                "severity":
                    alert["severity"],

                "message":
                    alert["message"]

            }).execute()

            # Notify browser immediately.

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


# ============================================================
# PROCESS SENSOR READING
# ============================================================

def _process_reading(
    temp,
    hum,
    motion,
    sound,
    wetness
):

    # --------------------------------------------------------
    # SAVE SENSOR DATA
    # --------------------------------------------------------

    if supabase is not None:

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

            supabase.table(
                "sensor_readings"
            ).insert(
                reading
            ).execute()

            log.info(
                "Sensor reading saved to Supabase"
            )

        except Exception as e:

            log.error(
                "Sensor DB insert error: %s",
                e
            )

    # --------------------------------------------------------
    # ALERTS
    # --------------------------------------------------------

    alerts = check_alerts(
        temp,
        hum,
        wetness,
        sound
    )

    if alerts:

        save_alerts(alerts)

        # Email is handled after database processing.
        # Email has its own global cooldown.

        send_alert_email(alerts)


# ============================================================
# FLASK ROUTES
# ============================================================

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


# ============================================================
# CURRENT DATA
# ============================================================

@app.route("/api/current_data")
def api_current_data():

    with _data_lock:

        data = dict(
            current_data
        )

    return jsonify(data)


# ============================================================
# HISTORY
# ============================================================

@app.route("/api/history")
def api_history():

    if supabase is None:

        return jsonify([])

    limit = request.args.get(
        "limit",
        100,
        type=int
    )

    # Protect the database from huge requests.

    limit = max(
        1,
        min(limit, 100)
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
        50,
        type=int
    )

    limit = max(
        1,
        min(limit, 50)
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
# SENSOR INGESTION
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

    # --------------------------------------------------------
    # UPDATE MEMORY
    # --------------------------------------------------------

    with _data_lock:

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

        dashboard_data = dict(
            current_data
        )

    # --------------------------------------------------------
    # UPDATE DASHBOARD
    # --------------------------------------------------------

    try:

        socketio.emit(
            "sensor_update",
            dashboard_data
        )

    except Exception as e:

        log.warning(
            "Socket emit failed: %s",
            e
        )

    # --------------------------------------------------------
    # BACKGROUND PROCESSING
    # --------------------------------------------------------

    try:

        executor.submit(
            _process_reading,
            temp,
            hum,
            motion,
            sound,
            wetness
        )

    except RuntimeError:

        log.warning(
            "Background worker unavailable"
        )

    # --------------------------------------------------------
    # RETURN IMMEDIATELY
    # --------------------------------------------------------

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
# CAMERA FRAME UPLOAD
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

    # Store newest frame only.

    with _frame_lock:

        latest_frame = data

    # Broadcast immediately.

    _broadcast_frame(data)

    return jsonify({
        "status":
            "ok"
    })


# ============================================================
# VIDEO STREAM
# ============================================================

@app.route("/video_feed")
def video_feed():

    def generate():

        q = queue.Queue(
            maxsize=1
        )

        # Register browser.

        with _subscribers_lock:

            _frame_subscribers.append(q)

        try:

            # Send current frame immediately.

            with _frame_lock:

                frame = latest_frame

            if frame is not None:

                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Cache-Control: no-cache\r\n\r\n"
                    + frame
                    + b"\r\n"
                )

            # Continue receiving newest frames.

            while True:

                frame = q.get()

                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Cache-Control: no-cache\r\n\r\n"
                    + frame
                    + b"\r\n"
                )

        except GeneratorExit:

            pass

        except Exception as e:

            log.debug(
                "Video client disconnected: %s",
                e
            )

        finally:

            with _subscribers_lock:

                try:
                    _frame_subscribers.remove(q)
                except ValueError:
                    pass

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
        "X-Accel-Buffering"
    ] = "no"

    return response


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

    with _data_lock:

        d = dict(
            current_data
        )

    temp = (
        d.get("temperature")
        if d.get("temperature")
        is not None
        else "--"
    )

    hum = (
        d.get("humidity")
        if d.get("humidity")
        is not None
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
        "wet - needs changing"
        if d.get("wetness")
        else "dry"
    )

    # --------------------------------------------------------
    # TEMPERATURE
    # --------------------------------------------------------

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
            f"{tmin}-{tmax}°C."
        )

        if temp != "--":

            if temp < float(tmin):

                reply += (
                    " It is below the "
                    "minimum."
                )

            elif temp > float(tmax):

                reply += (
                    " It is above the "
                    "maximum."
                )

            else:

                reply += (
                    " This is within "
                    "the normal range."
                )

    # --------------------------------------------------------
    # HUMIDITY
    # --------------------------------------------------------

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
            f"{hmin}-{hmax}%."
        )

        if hum != "--":

            if hum < float(hmin):

                reply += (
                    " It is below "
                    "the minimum."
                )

            elif hum > float(hmax):

                reply += (
                    " It is above "
                    "the maximum."
                )

            else:

                reply += (
                    " This is within "
                    "the normal range."
                )

    # --------------------------------------------------------
    # MOTION
    # --------------------------------------------------------

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
            f"Baby is currently "
            f"{motion_str}."
        )

    # --------------------------------------------------------
    # SOUND
    # --------------------------------------------------------

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

        if d.get("sound"):

            reply = (
                "Sound level is currently "
                "loud/noisy. This may "
                "indicate crying or a "
                "loud environment."
            )

        else:

            reply = (
                "Sound level is currently "
                "quiet. No loud sound "
                "detected."
            )

    # --------------------------------------------------------
    # DIAPER
    # --------------------------------------------------------

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
            f"Diaper is {diaper_str}."
        )

        if d.get("wetness"):

            reply += (
                " It is time for a change."
            )

        else:

            reply += (
                " No change needed."
            )

    # --------------------------------------------------------
    # GREETING
    # --------------------------------------------------------

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
            "Hello! I'm your Baby "
            "Cradle Monitoring assistant. "
            "Ask about temperature, "
            "humidity, motion, sound, "
            "or diaper status."
        )

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

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
                "abnormal readings"
            )

        reply = (

            f"Temperature: {temp}°C\n"

            f"Humidity: {hum}%\n"

            f"Motion: {motion_str}\n"

            f"Sound: "
            f"{'loud' if d.get('sound') else 'quiet'}\n"

            f"Diaper: {diaper_str}"
        )

        if flags:

            reply += (
                "\n\nNotable: "
                + " - ".join(flags)
            )

    # --------------------------------------------------------
    # UNKNOWN
    # --------------------------------------------------------

    else:

        reply = (
            "I can answer about "
            "temperature, humidity, "
            "motion, sound, or diaper "
            "status. Type 'status' "
            "for a full summary."
        )

    return jsonify({
        "reply": reply
    })


# ============================================================
# HEALTH CHECK
# ============================================================

@app.route("/health")
def health():

    return jsonify({

        "status":
            "healthy",

        "supabase":
            supabase is not None,

        "timestamp":
            datetime.now().isoformat()

    })


# ============================================================
# SHUTDOWN
# ============================================================

def shutdown_executor():

    try:

        executor.shutdown(
            wait=False,
            cancel_futures=True
        )

    except Exception:

        pass


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

    debug = (
        os.getenv("RENDER")
        is None
    )

    log.info(
        "Baby Cradle Monitoring Server "
        "starting on port %s...",
        port
    )

    try:

        socketio.run(

            app,

            host="0.0.0.0",

            port=port,

            debug=debug,

            allow_unsafe_werkzeug=True
        )

    finally:

        shutdown_executor()
