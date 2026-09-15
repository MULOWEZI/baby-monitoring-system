

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
# VIDEO STREAMING
# ============================================================

latest_frame = None

_frame_lock = threading.Lock()

_frame_subscribers = []

_subscribers_lock = threading.Lock()


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

# Timestamp of the last temperature alert.
# Default cooldown is 5 minutes; this prevents email spam.
last_temperature_alert_time = None


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
    Check temperature and wetness independently.

    TEMPERATURE:
        - Normal -> abnormal: alert immediately.
        - Abnormal -> abnormal: repeat after TEMP_ALERT_COOLDOWN.
        - Abnormal -> normal: reset the cooldown.
        - If the first reading after server startup is abnormal,
          it is treated as a new alert.

    WETNESS:
        - Dry -> wet: alert immediately.
        - Wet -> wet: no repeated alert.
        - Wet -> dry: reset.
    """

    global previous_wetness
    global previous_temperature_abnormal
    global last_temperature_alert_time

    # ========================================================
    # THRESHOLDS
    # ========================================================

    try:
        temp_min = float(os.getenv("TEMP_MIN", "20"))
    except (TypeError, ValueError):
        temp_min = 20.0

    try:
        temp_max = float(os.getenv("TEMP_MAX", "25"))
    except (TypeError, ValueError):
        temp_max = 25.0

    try:
        temperature_cooldown = float(
            os.getenv("TEMP_ALERT_COOLDOWN", "300")
        )
    except (TypeError, ValueError):
        temperature_cooldown = 300.0

    if temperature_cooldown < 0:
        temperature_cooldown = 0.0

    # ========================================================
    # NORMALIZE TEMPERATURE
    # ========================================================

    try:
        temp_value = float(temp)
    except (TypeError, ValueError):
        temp_value = None

    temperature_abnormal = False

    if temp_value is not None:
        temperature_abnormal = (
            temp_value < temp_min
            or
            temp_value > temp_max
        )

    # ========================================================
    # NORMALIZE WETNESS
    # ========================================================

    # Handle both real booleans and common string values.
    if isinstance(wetness, str):
        current_wetness = wetness.strip().lower() in (
            "true",
            "1",
            "yes",
            "wet",
            "on"
        )
    else:
        current_wetness = bool(wetness)

    # ========================================================
    # DEBUG LOG
    # ========================================================

    log.info(
        "TEMPERATURE CHECK: %s°C | SAFE RANGE: %.1f–%.1f°C | "
        "ABNORMAL: %s",
        (
            f"{temp_value:.1f}"
            if temp_value is not None
            else "INVALID"
        ),
        temp_min,
        temp_max,
        temperature_abnormal
    )

    # ========================================================
    # COLLECT NEW ALERT EVENTS
    # ========================================================

    temperature_alert = None
    wetness_alert = None

    with alert_state_lock:

        # ----------------------------------------------------
        # TEMPERATURE ALERT
        # ----------------------------------------------------

        now = time.time()

        cooldown_expired = (
            last_temperature_alert_time is None
            or
            (now - last_temperature_alert_time)
            >= temperature_cooldown
        )

        temperature_event = (
            temperature_abnormal
            and
            (
                not previous_temperature_abnormal
                or
                cooldown_expired
            )
        )

        if temperature_event:

            if temp_value > temp_max:
                message = (
                    f"🌡️ Temperature is too high: "
                    f"{temp_value:.1f}°C. "
                    f"Configured maximum is "
                    f"{temp_max:.1f}°C."
                )

            elif temp_value < temp_min:
                message = (
                    f"🌡️ Temperature is too low: "
                    f"{temp_value:.1f}°C. "
                    f"Configured minimum is "
                    f"{temp_min:.1f}°C."
                )

            else:
                message = (
                    f"🌡️ Abnormal temperature detected: "
                    f"{temp_value:.1f}°C."
                )

            temperature_alert = {
                "alert_type": "temperature",
                "severity": "critical",
                "message": message
            }

            last_temperature_alert_time = now

            log.warning(
                "TEMPERATURE ALERT CREATED: %s",
                message
            )

        elif not temperature_abnormal:
            # Returning to normal resets the repeat-alert timer.
            last_temperature_alert_time = None

        previous_temperature_abnormal = temperature_abnormal

        # ----------------------------------------------------
        # WET DIAPER ALERT
        # ----------------------------------------------------

        wetness_event = (
            current_wetness
            and
            not previous_wetness
        )

        if wetness_event:

            wetness_alert = {
                "alert_type": "wetness",
                "severity": "critical",
                "message":
                    "💧 Diaper is wet! "
                    "Please change the diaper."
            }

            log.warning(
                "WETNESS ALERT CREATED: Diaper is wet."
            )

        previous_wetness = current_wetness

    # ========================================================
    # PROCESS TEMPERATURE ALERT SEPARATELY
    # ========================================================

    if temperature_alert is not None:

        # Save temperature alert to Supabase.
        if supabase is not None:

            try:
                supabase.table(
                    "alerts"
                ).insert(
                    temperature_alert
                ).execute()

                log.info(
                    "Temperature alert saved to Supabase."
                )

            except Exception as e:

                log.error(
                    "Temperature alert DB error: %s",
                    e
                )

        # Send temperature email independently.
        email_sent = send_alert_email(
            [temperature_alert]
        )

        if email_sent:
            log.info(
                "Temperature alert email sent successfully."
            )
        else:
            log.error(
                "Temperature alert email was NOT sent."
            )

        # Update dashboard independently.
        try:
            socketio.emit(
                "new_alert",
                temperature_alert
            )
        except Exception as e:
            log.error(
                "Temperature Socket.IO alert error: %s",
                e
            )

    # ========================================================
    # PROCESS WETNESS ALERT SEPARATELY
    # ========================================================

    if wetness_alert is not None:

        # Save wetness alert to Supabase.
        if supabase is not None:

            try:
                supabase.table(
                    "alerts"
                ).insert(
                    wetness_alert
                ).execute()

                log.info(
                    "Wetness alert saved to Supabase."
                )

            except Exception as e:

                log.error(
                    "Wetness alert DB error: %s",
                    e
                )

        # Send wetness email independently.
        email_sent = send_alert_email(
            [wetness_alert]
        )

        if email_sent:
            log.info(
                "Wetness alert email sent successfully."
            )
        else:
            log.error(
                "Wetness alert email was NOT sent."
            )

        # Update dashboard independently.
        try:
            socketio.emit(
                "new_alert",
                wetness_alert
            )
        except Exception as e:
            log.error(
                "Wetness Socket.IO alert error: %s",
                e
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
    Saves the sensor reading to Supabase and checks
    for new alert events.
    """


    # ========================================================
    # SAVE SENSOR READING
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


            log.info(
                "Sensor reading saved to Supabase"
            )


        except Exception as e:

            log.error(
                "DB insert error: %s",
                e
            )


    # ========================================================
    # CHECK ALERTS
    # ========================================================

    check_alerts(

        temp,

        hum,

        wetness,

        sound
    )


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

    try:
        temp = float(temp)
    except (TypeError, ValueError):
        return jsonify({
            "error": "Invalid temperature value"
        }), 400

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
    # Update current dashboard data
    # --------------------------------------------------------

    current_data["temperature"] = temp

    current_data["humidity"] = hum

    current_data["motion"] = motion

    current_data["sound"] = sound

    current_data["wetness"] = wetness

    current_data["last_update"] = (
        datetime.now().isoformat()
    )


    # --------------------------------------------------------
    # Push sensor update to dashboard
    # --------------------------------------------------------

    socketio.emit(
        "sensor_update",
        current_data
    )


    # --------------------------------------------------------
    # Process database + alerts in background
    # --------------------------------------------------------

    threading.Thread(

        target=_process_reading_async,

        args=(
            temp,
            hum,
            motion,
            sound,
            wetness
        ),

        daemon=True

    ).start()


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


    return Response(

        generate(),

        mimetype=
            "multipart/x-mixed-replace; "
            "boundary=frame"
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


    # Keep the monitoring server stable on the Raspberry Pi.
    debug = False


    try:
        startup_temp_min = float(
            os.getenv("TEMP_MIN", "20")
        )
    except (TypeError, ValueError):
        startup_temp_min = 20.0

    try:
        startup_temp_max = float(
            os.getenv("TEMP_MAX", "25")
        )
    except (TypeError, ValueError):
        startup_temp_max = 25.0

    try:
        startup_temp_cooldown = float(
            os.getenv("TEMP_ALERT_COOLDOWN", "300")
        )
    except (TypeError, ValueError):
        startup_temp_cooldown = 300.0

    log.info(
        "Baby Cradle Monitoring Server "
        "starting on port %s...",
        port
    )

    log.info(
        "Temperature safe range: %.1f°C to %.1f°C",
        startup_temp_min,
        startup_temp_max
    )

    log.info(
        "Temperature alert cooldown: %.0f seconds",
        startup_temp_cooldown
    )


    socketio.run(

        app,

        host="0.0.0.0",

        port=port,

        debug=False,
        use_reloader=False,
        allow_unsafe_werkzeug=True
