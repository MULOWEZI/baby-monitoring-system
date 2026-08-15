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
# LOAD ENVIRONMENT VARIABLES
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
    cors_allowed_origins="*"
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
        "SUPABASE_URL/KEY not set — running without database"
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

# ============================================================
# SENSOR BUFFERING / RATE LIMITING
# ============================================================
#
# Sensor data can arrive much faster than the dashboard needs.
# Keep the newest reading in memory and publish it every 5 seconds.
#
# IMPORTANT:
# - Alert detection still runs on every incoming reading so an
#   abnormal event is not delayed by the dashboard refresh interval.
# - Database writes are also batched and flushed every 5 seconds
#   to reduce Supabase traffic and Render CPU/thread overhead.
#
SENSOR_UPDATE_INTERVAL = float(
    os.getenv("SENSOR_UPDATE_INTERVAL", "5")
)

_sensor_buffer_lock = threading.Lock()
_latest_sensor_reading = None

# Alert processing is kept separate from the 5-second dashboard/DB
# interval so abnormal conditions can still be detected immediately.
_alert_queue = queue.Queue(maxsize=200)


# ============================================================
# VIDEO STREAMING
# ============================================================

latest_frame = None

_frame_lock = threading.Lock()

_frame_subscribers = []

_subscribers_lock = threading.Lock()


def _broadcast_frame(frame):

    # Latest-frame-wins buffering:
    # never allow old camera frames to build up and create latency.
    with _subscribers_lock:

        for q in list(_frame_subscribers):

            try:
                q.put_nowait(frame)

            except queue.Full:

                # Drop the stale frame and replace it with the newest one.
                try:
                    q.get_nowait()
                except queue.Empty:
                    pass

                try:
                    q.put_nowait(frame)
                except queue.Full:
                    pass


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
#
# These variables prevent repeated alerts while the SAME
# condition remains active.
#
# Example:
#
# dry -> wet       = alert
# wet -> wet       = nothing
# wet -> dry       = reset
# dry -> wet       = alert again
#
# Same principle for temperature.
# ============================================================

alert_state_lock = threading.Lock()

previous_wetness = False

previous_temperature_abnormal = False


# ============================================================
# BIRD HOST
# ============================================================

def bird_host():

    """
    Derive Bird platform host from the API key region.

    Example:
        bk_us1_xxxxx
        -> https://us1.platform.bird.com
    """

    parts = BIRD_API_KEY.split("_")

    region = (
        parts[1]
        if len(parts) > 1 and parts[1]
        else "us1"
    )

    return f"https://{region}.platform.bird.com"


# ============================================================
# SEND EMAIL
# ============================================================

def send_alert_email(alerts):

    """
    Sends one email containing the supplied alerts.

    This function is only called when a NEW alert event
    occurs.
    """

    if not alerts:

        return False


    if not BIRD_API_KEY:

        log.warning(
            "BIRD_API_KEY not set — skipping email notification"
        )

        return False


    if not ALERT_EMAIL:

        log.warning(
            "ALERT_EMAIL not set — skipping email notification"
        )

        return False


    # --------------------------------------------------------
    # Determine email subject
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # Create HTML alert list
    # --------------------------------------------------------

    items = ""

    for alert in alerts:

        items += (
            "<li>"
            f"<strong>{alert.get('severity', 'warning').upper()}</strong>"
            " — "
            f"{alert.get('message', '')}"
            "</li>"
        )


    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
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
            <strong>Time:</strong> {timestamp}
        </p>

        <ul>
            {items}
        </ul>

        <p>
            Please check the baby monitoring dashboard.
        </p>

        <p>
            <a href="https://baby-monitoring-system.onrender.com">
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


# ============================================================
# TEMPERATURE / HUMIDITY CHECK
# ============================================================

def check_abnormal(temp, hum):

    temp_min = float(
        os.getenv("TEMP_MIN", 20)
    )

    temp_max = float(
        os.getenv("TEMP_MAX", 25)
    )

    hum_min = float(
        os.getenv("HUMIDITY_MIN", 40)
    )

    hum_max = float(
        os.getenv("HUMIDITY_MAX", 60)
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
# EVENT-BASED ALERT LOGIC
# ============================================================

def check_alerts(
    temp,
    hum,
    wetness,
    sound
):

    """
    Detect NEW alert events.

    WETNESS:

        False -> True
        = NEW EVENT

        True -> True
        = NO NEW EVENT

        True -> False
        = RESET

    TEMPERATURE:

        Normal -> Abnormal
        = NEW EVENT

        Abnormal -> Abnormal
        = NO NEW EVENT

        Abnormal -> Normal
        = RESET
    """

    global previous_wetness
    global previous_temperature_abnormal


    # ========================================================
    # THRESHOLDS
    # ========================================================

    temp_min = float(
        os.getenv("TEMP_MIN", 20)
    )

    temp_max = float(
        os.getenv("TEMP_MAX", 25)
    )


    alerts = []


    # ========================================================
    # CURRENT TEMPERATURE STATE
    # ========================================================

    temperature_abnormal = False


    if temp is not None:

        temperature_abnormal = (

            temp < temp_min

            or

            temp > temp_max
        )


    # ========================================================
    # WETNESS STATE
    # ========================================================

    current_wetness = bool(
        wetness
    )


    # ========================================================
    # LOCK STATE CHANGES
    # ========================================================

    with alert_state_lock:


        # ----------------------------------------------------
        # TEMPERATURE
        # ----------------------------------------------------

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


        # Save current temperature state

        previous_temperature_abnormal = (
            temperature_abnormal
        )


        # ----------------------------------------------------
        # WET DIAPER
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


        # Save current wetness state

        previous_wetness = (
            current_wetness
        )


    # ========================================================
    # NO NEW EVENTS
    # ========================================================

    if not alerts:

        return


    # ========================================================
    # SAVE ALERTS TO SUPABASE
    # ========================================================

    if supabase is not None:

        for alert in alerts:

            try:

                supabase.table(
                    "alerts"
                ).insert(
                    alert
                ).execute()


                # Immediately update dashboard

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


    # ========================================================
    # SEND ONE EMAIL FOR THE NEW EVENT
    # ========================================================

    send_alert_email(
        alerts
    )


# ============================================================
# PROCESS SENSOR READING
# ============================================================

def _process_reading_async(
    temp,
    hum,
    motion,
    sound,
    wetness
):

    """
    Process an incoming sensor sample.

    Alert detection remains immediate, while the actual dashboard
    update and database write are rate-limited/batched elsewhere.
    """

    # Alerts must not wait 5 seconds. This preserves event detection.
    check_alerts(
        temp,
        hum,
        wetness,
        sound
    )


def _alert_worker():

    """
    Process alert checks in one dedicated background thread.

    This keeps network/database/email work out of the Raspberry Pi
    request handler and avoids creating one thread per sensor packet.
    """

    while True:

        temp, hum, wetness, sound = _alert_queue.get()

        try:

            check_alerts(
                temp,
                hum,
                wetness,
                sound
            )

        except Exception as e:

            log.error(
                "Alert worker error: %s",
                e
            )

        finally:

            _alert_queue.task_done()


threading.Thread(
    target=_alert_worker,
    daemon=True,
    name="alert-worker"
).start()


def _sensor_flush_worker():

    """
    Runs continuously in one background thread.

    Every SENSOR_UPDATE_INTERVAL seconds:
      1. Publishes only the newest sensor reading to the dashboard.
      2. Writes the newest reading to Supabase.

    This avoids creating one Python thread and one DB request for
    every sensor packet.
    """

    global _latest_sensor_reading

    while True:

        # Strict 5-second cadence. We intentionally do not wake this
        # worker for every incoming sensor packet.
        time.sleep(SENSOR_UPDATE_INTERVAL)

        with _sensor_buffer_lock:

            reading = _latest_sensor_reading

        if reading is None:
            continue

        # --------------------------------------------------------
        # Update dashboard every 5 seconds
        # --------------------------------------------------------

        current_data["temperature"] = reading["temperature"]
        current_data["humidity"] = reading["humidity"]
        current_data["motion"] = reading["motion_detected"]
        current_data["sound"] = reading["sound_level"]
        current_data["wetness"] = reading["wetness_detected"]
        current_data["last_update"] = datetime.now().isoformat()

        socketio.emit(
            "sensor_update",
            current_data
        )

        # --------------------------------------------------------
        # Persist only the latest sample in this 5-second window
        # --------------------------------------------------------

        if supabase is not None:

            db_reading = {
                "temperature": reading["temperature"],
                "humidity": reading["humidity"],
                "motion_detected": reading["motion_detected"],
                "sound_level": reading["sound_level"],
                "wetness_detected": reading["wetness_detected"],
                "is_abnormal": check_abnormal(
                    reading["temperature"],
                    reading["humidity"]
                )
            }

            try:

                supabase.table(
                    "sensor_readings"
                ).insert(
                    db_reading
                ).execute()

                log.info(
                    "Sensor reading saved to Supabase "
                    "(batched every %.1fs)",
                    SENSOR_UPDATE_INTERVAL
                )

            except Exception as e:

                log.error(
                    "DB insert error: %s",
                    e
                )


# Start exactly one sensor worker instead of creating a new
# background thread for every Raspberry Pi request.
threading.Thread(
    target=_sensor_flush_worker,
    daemon=True,
    name="sensor-flush-worker"
).start()


# ============================================================
# HOME PAGE
# ============================================================

@app.route("/")
def index():

    return render_template(
        "index.html"
    )


# ============================================================
# LIVE PAGE
# ============================================================

@app.route("/live")
def live():

    return render_template(
        "live.html"
    )


# ============================================================
# HISTORY PAGE
# ============================================================

@app.route("/history")
def history():

    return render_template(
        "history.html"
    )


# ============================================================
# CURRENT SENSOR DATA
# ============================================================

@app.route(
    "/api/current_data"
)
def api_current_data():

    return jsonify(
        current_data
    )


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
        100,
        type=int
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
            "History error: %s",
            e
        )

        return jsonify({
            "error": str(e)
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
        50,
        type=int
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
            "Alert history error: %s",
            e
        )

        return jsonify({
            "error": str(e)
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

        supabase.table(
            "alerts"
        ).update({

            "is_read": True

        }).neq(
            "is_read",
            True
        ).execute()


        return jsonify({
            "success": True
        })


    except Exception as e:

        return jsonify({
            "error": str(e)
        }), 500


# ============================================================
# RASPBERRY PI INGEST
# ============================================================

@app.route(
    "/api/ingest",
    methods=["POST"]
)
def api_ingest():

    global _latest_sensor_reading

    data = request.get_json(
        silent=True
    ) or {}


    # --------------------------------------------------------
    # Required fields
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # Read values
    # --------------------------------------------------------

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
    # Store ONLY the newest sensor reading.
    #
    # The dashboard is deliberately updated every 5 seconds by
    # _sensor_flush_worker(). This prevents rapid sensor traffic
    # from flooding Socket.IO clients and Supabase.
    # --------------------------------------------------------

    reading = {
        "temperature": temp,
        "humidity": hum,
        "motion_detected": motion,
        "sound_level": sound,
        "wetness_detected": wetness
    }

    with _sensor_buffer_lock:
        _latest_sensor_reading = reading

    # --------------------------------------------------------
    # Queue alert processing immediately.
    #
    # This is intentionally separate from the 5-second dashboard
    # update interval. The event-state logic prevents repeated
    # alerts while the same abnormal condition remains active.
    # --------------------------------------------------------

    try:

        _alert_queue.put_nowait(
            (
                temp,
                hum,
                wetness,
                sound
            )
        )

    except queue.Full:

        # Keep the request path fast if the alert worker is temporarily
        # busy. The newest reading is already retained in the sensor
        # buffer and will be used for the next dashboard update.
        log.warning(
            "Alert queue full; skipping one intermediate alert sample"
        )


    # --------------------------------------------------------
    # Return immediately to Raspberry Pi
    # --------------------------------------------------------

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


    with _frame_lock:

        latest_frame = data


    _broadcast_frame(
        data
    )


    return jsonify({

        "status": "ok"

    })


# ============================================================
# LIVE VIDEO STREAM
# ============================================================

@app.route(
    "/video_feed"
)
def video_feed():

    """
    Event-driven MJPEG stream.

    New frames are pushed immediately.
    Stale frames are discarded.
    """


    def generate():

        q = queue.Queue(
            maxsize=1
        )


        with _subscribers_lock:

            _frame_subscribers.append(
                q
            )


        try:

            # Send current frame immediately

            with _frame_lock:

                if latest_frame is not None:

                    yield (
                        b"--frame\r\n"
                        b"Content-Type: image/jpeg\r\n\r\n"
                        +
                        latest_frame
                        +
                        b"\r\n"
                    )


            # Wait for new frames

            while True:

                frame = q.get()


                yield (

                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n\r\n"
                    +
                    frame
                    +
                    b"\r\n"
                )


        finally:

            with _subscribers_lock:

                if q in _frame_subscribers:

                    _frame_subscribers.remove(
                        q
                    )


    response = Response(

        generate(),

        mimetype=
            "multipart/x-mixed-replace; "
            "boundary=frame"
    )

    # Prevent proxies/browser layers from buffering the MJPEG stream.
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["X-Accel-Buffering"] = "no"

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


    d = current_data


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


    # ========================================================
    # HUMIDITY
    # ========================================================

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


    # ========================================================
    # MOTION
    # ========================================================

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


    # ========================================================
    # STATUS
    # ========================================================

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
    # UNKNOWN QUESTION
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
# START SERVER
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            5000
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

    log.info(
        "Sensor dashboard update interval: %.1f seconds",
        SENSOR_UPDATE_INTERVAL
    )


    socketio.run(

        app,

        host="0.0.0.0",

        port=port,

        debug=debug,

        allow_unsafe_werkzeug=True
    )
