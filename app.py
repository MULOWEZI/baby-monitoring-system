#!/usr/bin/env python3
"""
Baby Cradle Monitoring Server - optimized version

Main goals:
- Keep the functionality of the existing application.
- Use the simpler/faster architecture of the older working version.
- Keep sensor ingestion responsive.
- Keep alerts, email, Supabase/Postgres storage, Socket.IO, video,
  history, chatbot and health endpoints.
- Avoid unnecessary page-level no-cache headers.
- Keep the video implementation simple: one latest JPEG frame.
"""

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
# ENVIRONMENT
# ============================================================

load_dotenv()


# ============================================================
# FLASK / SOCKET.IO
# ============================================================

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv(
    "SECRET_KEY",
    "baby-monitor-secret-key",
)

# Let Flask-SocketIO choose its normal async mode instead of
# forcing threading mode. This follows the faster version.
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False,
    ping_interval=25,
    ping_timeout=60,
)


# ============================================================
# DATABASE
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL", "")


def db_query(sql, params=()):
    """
    Execute a PostgreSQL query through the Supabase transaction pooler.

    SELECT:
        returns a list of dictionaries.

    INSERT/UPDATE/etc:
        returns True.

    Failure:
        returns None.

    A connection is created per operation, matching the simpler
    architecture of the faster version.
    """
    if not DATABASE_URL:
        return None

    conn = None

    try:
        conn = psycopg2.connect(
            DATABASE_URL,
            connect_timeout=5,
        )
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute(sql, params)

            if cur.description:
                columns = [col[0] for col in cur.description]
                rows = cur.fetchall()

                return [
                    {
                        key: (
                            float(value)
                            if isinstance(value, decimal.Decimal)
                            else value
                        )
                        for key, value in zip(columns, row)
                    }
                    for row in rows
                ]

            return True

    except Exception as exc:
        log.error("Database error: %s", exc)
        return None

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


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
# LIGHTWEIGHT HISTORY CACHE
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
    """Return cached data if it is still valid."""
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
# VIDEO
# ============================================================

# Keep only the newest frame.
# This is intentionally much simpler than the subscriber/queue
# implementation in the slower version.
latest_frame = None
_frame_lock = threading.Lock()


# ============================================================
# ALERT STATE
# ============================================================

alert_state_lock = threading.Lock()

previous_wetness = False
previous_temperature_abnormal = False


# ============================================================
# EMAIL
# ============================================================

BIRD_API_KEY = os.getenv("BIRD_API_KEY", "")
BIRD_SENDER = os.getenv(
    "BIRD_SENDER",
    "onboarding@messagebird.dev",
)
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")

_last_email_ts = 0.0
EMAIL_COOLDOWN = 300


def bird_host():
    parts = BIRD_API_KEY.split("_")

    region = (
        parts[1]
        if len(parts) > 1 and parts[1]
        else "us1"
    )

    return f"https://{region}.platform.bird.com"


def send_alert_email(alerts):
    """Send one alert summary email, throttled by EMAIL_COOLDOWN."""
    global _last_email_ts

    if not alerts:
        return False

    if not BIRD_API_KEY:
        log.warning("BIRD_API_KEY not set - email skipped")
        return False

    if not ALERT_EMAIL:
        log.warning("ALERT_EMAIL not set - email skipped")
        return False

    now = time.time()

    if now - _last_email_ts < EMAIL_COOLDOWN:
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
        <p>A new condition requiring attention was detected.</p>
        <p><strong>Time:</strong> {timestamp}</p>
        <ul>{items}</ul>
        <p>Please check the baby monitoring dashboard.</p>
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
                "Authorization": f"Bearer {BIRD_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=15,
        )

        if response.status_code in (200, 202):
            _last_email_ts = now
            log.info("Alert email sent")
            return True

        log.error(
            "Bird email failed %s: %s",
            response.status_code,
            response.text[:300],
        )

    except Exception as exc:
        log.error("Bird email exception: %s", exc)

    return False


# ============================================================
# SENSOR HELPERS
# ============================================================

def check_abnormal(temp, hum):
    try:
        temp_min = float(os.getenv("TEMP_MIN", "20"))
        temp_max = float(os.getenv("TEMP_MAX", "25"))
        hum_min = float(os.getenv("HUMIDITY_MIN", "40"))
        hum_max = float(os.getenv("HUMIDITY_MAX", "60"))
    except (TypeError, ValueError):
        return False

    if temp is not None:
        if temp < temp_min or temp > temp_max:
            return True

    if hum is not None:
        if hum < hum_min or hum > hum_max:
            return True

    return False


def check_alerts(temp, hum, wetness, sound=0):
    """
    Detect new alert events.

    Temperature alerts are generated only when the temperature
    crosses from normal to abnormal.

    Wetness alerts are generated only when wetness changes from
    false to true.
    """
    global previous_wetness
    global previous_temperature_abnormal

    try:
        temp_min = float(os.getenv("TEMP_MIN", "20"))
        temp_max = float(os.getenv("TEMP_MAX", "25"))
    except (TypeError, ValueError):
        temp_min = 20
        temp_max = 25

    alerts = []

    temperature_abnormal = False

    if temp is not None:
        temperature_abnormal = (
            temp < temp_min or temp > temp_max
        )

    current_wetness = bool(wetness)

    with alert_state_lock:
        new_temperature_event = (
            temperature_abnormal
            and not previous_temperature_abnormal
        )

        if new_temperature_event:
            if temp > temp_max:
                message = (
                    f"Temperature is too high: {temp}°C. "
                    f"Configured maximum is {temp_max}°C."
                )
            elif temp < temp_min:
                message = (
                    f"Temperature is too low: {temp}°C. "
                    f"Configured minimum is {temp_min}°C."
                )
            else:
                message = (
                    f"Abnormal temperature detected: {temp}°C."
                )

            alerts.append({
                "alert_type": "temperature",
                "severity": "critical",
                "message": message,
            })

        previous_temperature_abnormal = temperature_abnormal

        new_wetness_event = (
            current_wetness
            and not previous_wetness
        )

        if new_wetness_event:
            alerts.append({
                "alert_type": "wetness",
                "severity": "critical",
                "message": "Diaper is wet! Please change the diaper.",
            })

        previous_wetness = current_wetness

    if not alerts:
        return []

    # Notify the connected dashboard immediately.
    for alert in alerts:
        try:
            socketio.emit("new_alert", alert)
        except Exception as exc:
            log.debug("Socket alert failed: %s", exc)

    # Persist alerts without delaying the sensor response.
    for alert in alerts:
        try:
            db_query(
                """
                INSERT INTO alerts
                    (alert_type, severity, message)
                VALUES (%s, %s, %s)
                """,
                (
                    alert["alert_type"],
                    alert["severity"],
                    alert["message"],
                ),
            )
        except Exception as exc:
            log.error("Alert database insert failed: %s", exc)

    invalidate_alerts_cache()

    # Email is deliberately outside the HTTP response path.
    # Run it in a small daemon thread so a slow Bird request cannot
    # delay Raspberry Pi ingestion.
    if BIRD_API_KEY and ALERT_EMAIL:
        threading.Thread(
            target=send_alert_email,
            args=(alerts,),
            daemon=True,
            name="alert-email",
        ).start()

    return alerts


def update_current_data(
    temp,
    hum,
    motion,
    sound,
    wetness,
):
    """Update shared live sensor state and return a safe copy."""
    with current_data_lock:
        current_data["temperature"] = temp
        current_data["humidity"] = hum
        current_data["motion"] = motion
        current_data["sound"] = sound
        current_data["wetness"] = wetness
        current_data["last_update"] = datetime.now().isoformat()

        return dict(current_data)


# ============================================================
# PAGE ROUTES
# ============================================================

@app.route("/")
def index():
    # Do not force no-cache on the HTML page.
    # Browser caching makes navigation faster.
    return render_template("index.html")


@app.route("/live")
def live():
    return render_template("live.html")


@app.route("/history")
def history():
    return render_template("history.html")


# ============================================================
# CURRENT DATA API
# ============================================================

@app.route("/api/current_data")
def api_current_data():
    with current_data_lock:
        data = dict(current_data)

    response = jsonify(data)
    response.headers["Cache-Control"] = "no-store"
    return response


# ============================================================
# HISTORY API
# ============================================================

@app.route("/api/history")
def api_history():
    if not DATABASE_URL:
        return jsonify([])

    limit = request.args.get(
        "limit",
        50,
        type=int,
    )

    limit = max(1, min(limit, 100))

    cached = get_cached(history_cache, limit)

    if cached is not None:
        return jsonify(cached)

    rows = db_query(
        """
        SELECT *
        FROM sensor_readings
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    )

    if rows is None:
        return jsonify({
            "error": "Unable to load history"
        }), 500

    set_cached(history_cache, rows, limit)

    return jsonify(rows)


# ============================================================
# ALERT HISTORY API
# ============================================================

@app.route("/api/alerts")
def api_alerts():
    if not DATABASE_URL:
        return jsonify([])

    limit = request.args.get(
        "limit",
        20,
        type=int,
    )

    limit = max(1, min(limit, 50))

    cached = get_cached(alerts_cache, limit)

    if cached is not None:
        return jsonify(cached)

    rows = db_query(
        """
        SELECT *
        FROM alerts
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    )

    if rows is None:
        return jsonify({
            "error": "Unable to load alerts"
        }), 500

    set_cached(alerts_cache, rows, limit)

    return jsonify(rows)


# ============================================================
# CLEAR ALERTS
# ============================================================

@app.route(
    "/api/clear_alerts",
    methods=["POST"],
)
def clear_alerts():
    if not DATABASE_URL:
        return jsonify({"success": True})

    result = db_query(
        """
        UPDATE alerts
        SET is_read = TRUE
        WHERE is_read IS DISTINCT FROM TRUE
        """
    )

    if result is None:
        return jsonify({
            "error": "Unable to clear alerts"
        }), 500

    invalidate_alerts_cache()

    return jsonify({"success": True})


# ============================================================
# RASPBERRY PI SENSOR INGEST
# ============================================================

@app.route(
    "/api/ingest",
    methods=["POST"],
)
def api_ingest():
    data = request.get_json(silent=True) or {}

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

    try:
        temp = float(data.get("temperature"))
        hum = float(data.get("humidity"))
    except (TypeError, ValueError):
        return jsonify({
            "error": (
                "Temperature and humidity "
                "must be numbers"
            )
        }), 400

    motion = bool(
        data.get("motion_detected", False)
    )

    sound = data.get(
        "sound_level",
        0,
    )

    wetness = bool(
        data.get("wetness_detected", False)
    )

    # Update live state first.
    data_for_socket = update_current_data(
        temp,
        hum,
        motion,
        sound,
        wetness,
    )

    # Push the live value immediately.
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

    # Keep the HTTP response fast.
    # Database persistence happens in a background thread.
    is_abnormal = check_abnormal(temp, hum)

    def persist_sensor_reading():
        result = db_query(
            """
            INSERT INTO sensor_readings
                (
                    temperature,
                    humidity,
                    motion_detected,
                    sound_level,
                    wetness_detected,
                    is_abnormal
                )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                temp,
                hum,
                motion,
                sound,
                wetness,
                is_abnormal,
            ),
        )

        if result is not None:
            invalidate_history_cache()

    threading.Thread(
        target=persist_sensor_reading,
        daemon=True,
        name="sensor-db-write",
    ).start()

    # Detect and broadcast alerts without waiting for the DB write.
    check_alerts(
        temp,
        hum,
        wetness,
        sound,
    )

    return jsonify({
        "status": "ok",
        "abnormal": is_abnormal,
    })


# ============================================================
# VIDEO FRAME UPLOAD
# ============================================================

@app.route(
    "/api/upload_frame",
    methods=["POST"],
)
def api_upload_frame():
    global latest_frame

    data = request.get_data()

    if not data or len(data) < 100:
        return jsonify({
            "error": "Empty or invalid frame"
        }), 400

    # Replace the previous frame.
    # No subscriber queues are required.
    with _frame_lock:
        latest_frame = data

    return jsonify({"status": "ok"})


# ============================================================
# SIMPLE MJPEG VIDEO FEED
# ============================================================

@app.route("/video_feed")
def video_feed():
    def generate():
        last_served = None

        while True:
            with _frame_lock:
                frame = latest_frame

            if frame is not None and frame != last_served:
                last_served = frame

                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Cache-Control: no-cache\r\n"
                    b"\r\n"
                    + frame
                    + b"\r\n"
                )

            # Same lightweight polling strategy as the faster version.
            time.sleep(0.2)

    return Response(
        generate(),
        mimetype=(
            "multipart/x-mixed-replace; "
            "boundary=frame"
        ),
        headers={
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
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

    tmin = os.getenv("TEMP_MIN", "20")
    tmax = os.getenv("TEMP_MAX", "25")
    hmin = os.getenv("HUMIDITY_MIN", "40")
    hmax = os.getenv("HUMIDITY_MAX", "60")

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
                reply += " It is below the minimum."
            elif temp > float(tmax):
                reply += " It is above the maximum."
            else:
                reply += " This is within the normal range."

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
                reply += " It is below the minimum."
            elif hum > float(hmax):
                reply += " It is above the maximum."
            else:
                reply += " This is within the normal range."

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
        reply = f"Diaper is {diaper_str}."

        if d.get("wetness"):
            reply += " It is time for a change!"
        else:
            reply += " No change needed."

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
            flags.append("motion detected")

        if d.get("wetness"):
            flags.append("wet diaper")

        if check_abnormal(
            temp if temp != "--" else None,
            hum if hum != "--" else None,
        ):
            flags.append("abnormal readings")

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
                + " · ".join(flags)
            )

    else:
        reply = (
            "I can answer about temperature, "
            "humidity, motion, sound, diaper, "
            "or status."
        )

    return jsonify({"reply": reply})


# ============================================================
# HEALTH CHECK
# ============================================================

@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "database": bool(DATABASE_URL),
        "timestamp": datetime.now().isoformat(),
    })


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    port = int(
        os.getenv("PORT", "5000")
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
