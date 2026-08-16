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

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger(__name__)

load_dotenv()

app = Flask(__name__)

app.config["SECRET_KEY"] = os.getenv(
    "SECRET_KEY",
    "baby-monitor-secret-key"
)

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="gevent"
)


# ============================================================
# CONFIG
# ============================================================

TEMP_MIN = float(os.getenv("TEMP_MIN", 20))
TEMP_MAX = float(os.getenv("TEMP_MAX", 25))

HUMIDITY_MIN = float(os.getenv("HUMIDITY_MIN", 40))
HUMIDITY_MAX = float(os.getenv("HUMIDITY_MAX", 60))

# Dashboard updates at most every 5 seconds
DASHBOARD_PUSH_INTERVAL = float(
    os.getenv("DASHBOARD_PUSH_INTERVAL", 5)
)

# Number of consecutive abnormal readings required
# before an alert is confirmed.
#
# Example:
# Reading 1 = abnormal
# Reading 2 = abnormal
# -> Alert is triggered
#
ALERT_DEBOUNCE_READINGS = int(
    os.getenv("ALERT_DEBOUNCE_READINGS", 2)
)


# ============================================================
# BIRD EMAIL CONFIGURATION
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

        log.info(
            "Supabase client connected: %s...",
            SUPABASE_URL[:30]
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
# SHARED STATE
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

_last_broadcast_update = None


# ============================================================
# VIDEO STATE
# ============================================================

latest_frame = None

_frame_lock = threading.Lock()

_frame_subscribers = []

_subscribers_lock = threading.Lock()


# ============================================================
# SENSOR READING QUEUE
# ============================================================

_reading_queue = queue.Queue()


# ============================================================
# ALERT STATE
# ============================================================

alert_state_lock = threading.Lock()

# Confirmed states
previous_wetness = False

previous_temperature_abnormal = False

# Consecutive-reading counters
_wetness_streak = 0

_temp_streak = 0


# ============================================================
# VIDEO BROADCAST
# ============================================================

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

    region = (
        parts[1]
        if len(parts) > 1 and parts[1]
        else "us1"
    )

    return f"https://{region}.platform.bird.com"


def send_alert_email(alerts):

    # Check required configuration
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


    # Determine subject
    alert_types = {
        a.get("alert_type")
        for a in alerts
    }

    if "wetness" in alert_types:

        subject = "💧 Wet Diaper Detected"

    elif "temperature" in alert_types:

        subject = "🌡️ Temperature Alert"

    else:

        subject = "🚼 Baby Monitoring Alert"


    # Build alert list
    items = "".join(
        f"""
        <li>
            <strong>
                {a.get('severity', 'warning').upper()}
            </strong>
            —
            {a.get('message', '')}
        </li>
        """
        for a in alerts
    )


    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


    html = f"""
    <html>
    <body>

        <h2>🚼 Baby Cradle Monitoring Alert</h2>

        <p>
            A new condition requiring attention was detected.
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
            <a href="https://baby-monitoring-system.onrender.com">
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

        log.info(
            "Sending alert email to %s...",
            ALERT_EMAIL
        )

        response = requests.post(

            f"{bird_host()}/v1/email/messages",

            headers={
                "Authorization": f"Bearer {BIRD_API_KEY}",
                "Content-Type": "application/json"
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
            response.text[:500]
        )

    except Exception as e:

        log.error(
            "Bird email exception: %s",
            e
        )


    return False


# ============================================================
# THRESHOLD CHECK
# ============================================================

def check_abnormal(temp, hum):

    if temp is not None:

        if temp < TEMP_MIN or temp > TEMP_MAX:

            return True


    if hum is not None:

        if hum < HUMIDITY_MIN or hum > HUMIDITY_MAX:

            return True


    return False


# ============================================================
# ALERT LOGIC
# ============================================================
#
# IMPORTANT:
#
# There is NO email countdown/cooldown anymore.
#
# Alert behavior:
#
# Normal
#   ↓
# Abnormal reading #1
#   ↓
# Abnormal reading #2
#   ↓
# ALERT + EMAIL
#
# If condition stays abnormal:
#   No additional emails are sent.
#
# When condition becomes normal:
#   Alert state resets.
#
# If it becomes abnormal again:
#   Two consecutive abnormal readings are required again.
#
# ============================================================

def check_alerts(temp, hum, wetness, sound):

    global previous_wetness
    global previous_temperature_abnormal

    global _wetness_streak
    global _temp_streak


    # Raw states
    raw_temperature_abnormal = (
        temp is not None
        and (
            temp < TEMP_MIN
            or temp > TEMP_MAX
        )
    )

    raw_wetness = bool(wetness)


    alerts = []


    with alert_state_lock:

        # ====================================================
        # TEMPERATURE ALERT
        # ====================================================

        if (
            raw_temperature_abnormal
            != previous_temperature_abnormal
        ):

            # Reading disagrees with confirmed state
            _temp_streak += 1

        else:

            # Reading agrees with confirmed state
            _temp_streak = 0


        # Confirm state after required consecutive readings
        if _temp_streak >= ALERT_DEBOUNCE_READINGS:

            old_temperature_state = (
                previous_temperature_abnormal
            )

            previous_temperature_abnormal = (
                raw_temperature_abnormal
            )

            _temp_streak = 0


            # -----------------------------------------------
            # NORMAL -> ABNORMAL
            # -----------------------------------------------

            if (
                not old_temperature_state
                and previous_temperature_abnormal
            ):

                if temp > TEMP_MAX:

                    message = (
                        f"🌡️ Temperature is too high: "
                        f"{temp}°C. "
                        f"Configured maximum is "
                        f"{TEMP_MAX}°C."
                    )

                elif temp < TEMP_MIN:

                    message = (
                        f"🌡️ Temperature is too low: "
                        f"{temp}°C. "
                        f"Configured minimum is "
                        f"{TEMP_MIN}°C."
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


                log.info(
                    "TEMPERATURE ALERT CONFIRMED: %s",
                    message
                )


            # -----------------------------------------------
            # ABNORMAL -> NORMAL
            # -----------------------------------------------

            elif (
                old_temperature_state
                and not previous_temperature_abnormal
            ):

                log.info(
                    "Temperature returned to normal: %s°C",
                    temp
                )


        # ====================================================
        # WETNESS ALERT
        # ====================================================

        if raw_wetness != previous_wetness:

            _wetness_streak += 1

        else:

            _wetness_streak = 0


        # Confirm wetness state
        if _wetness_streak >= ALERT_DEBOUNCE_READINGS:

            old_wetness_state = previous_wetness

            previous_wetness = raw_wetness

            _wetness_streak = 0


            # -----------------------------------------------
            # DRY -> WET
            # -----------------------------------------------

            if (
                not old_wetness_state
                and previous_wetness
            ):

                message = (
                    "💧 Diaper is wet! "
                    "Please change the diaper."
                )


                alerts.append({
                    "alert_type": "wetness",
                    "severity": "critical",
                    "message": message
                })


                log.info(
                    "WETNESS ALERT CONFIRMED: %s",
                    message
                )


            # -----------------------------------------------
            # WET -> DRY
            # -----------------------------------------------

            elif (
                old_wetness_state
                and not previous_wetness
            ):

                log.info(
                    "Wetness condition cleared."
                )


    # ========================================================
    # NO NEW ALERT
    # ========================================================

    if not alerts:

        return


    # ========================================================
    # PROCESS NEW ALERT
    # ========================================================

    for alert in alerts:

        log.info(
            "NEW ALERT: %s",
            alert["message"]
        )


        # Send realtime alert to dashboard
        socketio.emit(
            "new_alert",
            alert
        )


        # Save alert to Supabase
        if supabase is not None:

            try:

                supabase.table(
                    "alerts"
                ).insert(alert).execute()

            except Exception as e:

                log.error(
                    "Supabase alert insert error: %s",
                    e
                )


    # ========================================================
    # SEND EMAIL IMMEDIATELY
    # ========================================================

    send_alert_email(alerts)


# ============================================================
# SENSOR READING WORKER
# ============================================================

def _reading_worker():

    while True:

        temp, hum, motion, sound, wetness = (
            _reading_queue.get()
        )

        try:

            # ================================================
            # SAVE SENSOR READING
            # ================================================

            if supabase is not None:

                reading = {
                    "temperature": temp,
                    "humidity": hum,
                    "motion_detected": motion,
                    "sound_level": sound,
                    "wetness_detected": wetness,
                    "is_abnormal": check_abnormal(
                        temp,
                        hum
                    )
                }


                try:

                    supabase.table(
                        "sensor_readings"
                    ).insert(reading).execute()


                    log.info(
                        "Sensor reading saved to Supabase"
                    )

                except Exception as e:

                    log.error(
                        "DB insert error: %s",
                        e
                    )


            # ================================================
            # CHECK ALERTS
            # ================================================

            check_alerts(
                temp,
                hum,
                wetness,
                sound
            )


        finally:

            _reading_queue.task_done()


# Start one background worker
threading.Thread(
    target=_reading_worker,
    daemon=True
).start()


# ============================================================
# DASHBOARD BROADCASTER
# ============================================================

def _dashboard_broadcaster():

    global _last_broadcast_update


    while True:

        socketio.sleep(
            DASHBOARD_PUSH_INTERVAL
        )


        with _data_lock:

            # Don't push if there is no new reading
            if (
                current_data["last_update"]
                == _last_broadcast_update
            ):

                continue


            snapshot = dict(current_data)

            _last_broadcast_update = (
                current_data["last_update"]
            )


        # Push latest sensor data
        socketio.emit(
            "sensor_update",
            snapshot
        )


# ============================================================
# PAGES
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
# API — CURRENT DATA
# ============================================================

@app.route("/api/current_data")
def api_current_data():

    with _data_lock:

        return jsonify(
            current_data
        )


# ============================================================
# API — HISTORY
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
# API — ALERTS
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
# API — CLEAR ALERTS
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

    data = request.get_json(
        silent=True
    ) or {}


    # Required values
    if (
        "temperature" not in data
        or "humidity" not in data
    ):

        return jsonify({
            "error": (
                "Missing required fields: "
                "temperature, humidity"
            )
        }), 400


    # Get sensor values
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


    # ========================================================
    # UPDATE CURRENT DATA IMMEDIATELY
    # ========================================================

    with _data_lock:

        current_data.update({

            "temperature": temp,

            "humidity": hum,

            "motion": motion,

            "sound": sound,

            "wetness": wetness,

            "last_update": (
                datetime.now().isoformat()
            )

        })


    # ========================================================
    # SEND READING TO WORKER
    # ========================================================

    _reading_queue.put(
        (
            temp,
            hum,
            motion,
            sound,
            wetness
        )
    )


    # Return immediately to Raspberry Pi
    return jsonify({

        "status": "ok",

        "abnormal": check_abnormal(
            temp,
            hum
        )

    })


# ============================================================
# VIDEO — FRAME UPLOAD
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
        or len(data) < 100
    ):

        return jsonify({
            "error": (
                "Empty or invalid frame"
            )
        }), 400


    with _frame_lock:

        latest_frame = data


    # Immediately broadcast frame
    _broadcast_frame(
        data
    )


    return jsonify({
        "status": "ok"
    })


# ============================================================
# VIDEO — LIVE MJPEG STREAM
# ============================================================

@app.route("/video_feed")
def video_feed():

    def generate():

        q = queue.Queue(
            maxsize=1
        )


        with _subscribers_lock:

            _frame_subscribers.append(
                q
            )


        try:

            # Send latest frame immediately
            with _frame_lock:

                if latest_frame is not None:

                    yield (
                        b"--frame\r\n"
                        b"Content-Type: image/jpeg\r\n\r\n"
                        + latest_frame
                        + b"\r\n"
                    )


            # Continue receiving frames
            while True:

                frame = q.get()


                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n\r\n"
                    + frame
                    + b"\r\n"
                )


        finally:

            with _subscribers_lock:

                if q in _frame_subscribers:

                    _frame_subscribers.remove(
                        q
                    )


    return Response(
        generate(),
        mimetype=(
            "multipart/x-mixed-replace; "
            "boundary=frame"
        )
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


    with _data_lock:

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
            f"{temp}°C. "
            f"Safe range is "
            f"{TEMP_MIN}–{TEMP_MAX}°C."
        )


        if temp != "--":

            if temp < TEMP_MIN:

                reply += (
                    " It's **below** the minimum."
                )

            elif temp > TEMP_MAX:

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
            f"{hum}%. "
            f"Safe range is "
            f"{HUMIDITY_MIN}–{HUMIDITY_MAX}%."
        )


        if hum != "--":

            if hum < HUMIDITY_MIN:

                reply += (
                    " It's **below** the minimum."
                )

            elif hum > HUMIDITY_MAX:

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
            f"Baby is currently "
            f"**{motion_str}**."
        )


        reply += (
            " Recent motion was detected."
            if d.get("motion")
            else " No recent motion was detected."
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
                "**loud/noisy**. "
                "This may indicate crying "
                "or a loud environment."
            )

        else:

            reply = (
                "Sound level is currently "
                "**quiet**. "
                "No loud sounds detected."
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


        reply += (
            " It's time for a change!"
            if d.get("wetness")
            else " All good, no change needed."
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
    # UNKNOWN REQUEST
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

socketio.start_background_task(
    _dashboard_broadcaster
)


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


    socketio.run(

        app,

        host="0.0.0.0",

        port=port,

        debug=debug,

        allow_unsafe_werkzeug=True
    )
