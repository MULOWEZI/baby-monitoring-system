#!/usr/bin/env python3

import os
import sys
import time
import queue
import threading
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

# IMPORTANT:
# This application runs under Gunicorn's geventwebsocket worker.
# Patch gevent BEFORE importing requests, Supabase/httpx, or other
# networking libraries. Late SSL/socket patching can cause recursive
# networking failures such as:
#   "maximum recursion depth exceeded"
try:
    from gevent import monkey
    monkey.patch_all()
except Exception as e:
    # Keep local/non-gevent execution possible.
    logging.getLogger(__name__).warning(
        "gevent monkey patch could not be applied early: %s",
        e
    )

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
# SENSOR SAMPLING / DATABASE UPDATE INTERVAL
# ============================================================

# Raspberry Pi may send readings much faster than we want to store/display.
# Only one reading every 5 seconds is committed to Supabase and broadcast
# to the dashboard. The first reading is accepted immediately.
SENSOR_UPDATE_INTERVAL = 5.0

_sensor_update_lock = threading.Lock()
_last_sensor_update_time = 0.0


def should_commit_sensor_reading():
    """
    Return True only once every SENSOR_UPDATE_INTERVAL seconds.

    This throttles BOTH:
      1. Supabase sensor_readings inserts
      2. Dashboard sensor_update Socket.IO events

    The first reading after server startup is accepted immediately.
    """
    global _last_sensor_update_time

    now = time.monotonic()

    with _sensor_update_lock:
        if (
            _last_sensor_update_time == 0.0
            or now - _last_sensor_update_time >= SENSOR_UPDATE_INTERVAL
        ):
            _last_sensor_update_time = now
            return True

        return False


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
# ============================================================
# EMAIL CONFIGURATION
# ============================================================

BIRD_API_KEY = os.getenv("BIRD_API_KEY", "").strip()
BIRD_SENDER = os.getenv(
    "BIRD_SENDER",
    "onboarding@messagebird.dev"
).strip()
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "").strip()


def bird_host():
    """
    Bird API region is encoded in the API key:
        bk_us1_... -> https://us1.platform.bird.com
        bk_eu1_... -> https://eu1.platform.bird.com

    The host must match the key region.
    """
    if not BIRD_API_KEY:
        return "https://us1.platform.bird.com"

    parts = BIRD_API_KEY.split("_")

    if len(parts) >= 2 and parts[0] == "bk" and parts[1]:
        region = parts[1]
    else:
        region = "us1"

    return f"https://{region}.platform.bird.com"


def email_configuration_status():
    """
    Return safe email configuration diagnostics.
    Never logs the actual API key.
    """
    return {
        "bird_api_key_configured": bool(BIRD_API_KEY),
        "bird_api_key_prefix": (
            BIRD_API_KEY[:8] + "..."
            if BIRD_API_KEY
            else None
        ),
        "bird_host": bird_host(),
        "bird_sender": BIRD_SENDER or None,
        "alert_email_configured": bool(ALERT_EMAIL),
        "alert_email": ALERT_EMAIL or None,
    }


# ============================================================
# EMAIL DELIVERY
# ============================================================

def send_alert_email(alerts):
    """
    Send one transactional email for a newly detected alert event.

    Bird returns 202 when the message has been accepted for
    asynchronous delivery. That is treated as a successful send
    request. Actual delivery can subsequently be checked in Bird.
    """

    if not alerts:
        log.warning("EMAIL: no alerts supplied")
        return False

    # --------------------------------------------------------
    # Validate configuration
    # --------------------------------------------------------

    if not BIRD_API_KEY:
        log.error(
            "EMAIL: BIRD_API_KEY is missing"
        )
        return False

    if not BIRD_SENDER:
        log.error(
            "EMAIL: BIRD_SENDER is missing"
        )
        return False

    if not ALERT_EMAIL:
        log.error(
            "EMAIL: ALERT_EMAIL is missing"
        )
        return False

    host = bird_host()
    endpoint = f"{host}/v1/email/messages"

    log.info(
        "EMAIL: preparing Bird send | host=%s | from=%s | to=%s",
        host,
        BIRD_SENDER,
        ALERT_EMAIL
    )

    # --------------------------------------------------------
    # Determine subject
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Build HTML + plain text
    # --------------------------------------------------------

    html_items = []
    text_items = []

    for alert in alerts:
        severity = str(
            alert.get("severity", "warning")
        ).upper()

        message = str(
            alert.get("message", "")
        )

        html_items.append(
            f"<li><strong>{severity}</strong> — {message}</li>"
        )

        text_items.append(
            f"{severity} — {message}"
        )

    # Use Lusaka time for the email timestamp.
    timestamp = datetime.now(
        ZoneInfo("Africa/Lusaka")
    ).strftime(
        "%Y-%m-%d %H:%M:%S CAT"
    )

    html = f"""
    <!doctype html>
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
          {''.join(html_items)}
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

    plain_text = (
        "Baby Cradle Monitoring Alert\n\n"
        "A new condition requiring attention was detected.\n\n"
        f"Time: {timestamp}\n\n"
        + "\n".join(text_items)
        + "\n\nPlease check the baby monitoring dashboard.\n"
    )

    # --------------------------------------------------------
    # Bird payload
    # --------------------------------------------------------

    payload = {
        "from": BIRD_SENDER,
        "to": [ALERT_EMAIL],
        "subject": subject,
        "html": html,
        "text": plain_text,
        # Alerts are transactional messages, not marketing.
        "category": "transactional"
    }

    # --------------------------------------------------------
    # Send
    # --------------------------------------------------------

    try:

        log.info(
            "EMAIL: POST %s",
            endpoint
        )

        log.info(
            "EMAIL: starting outbound Bird HTTP request"
        )

        response = requests.post(
            endpoint,
            headers={
                "Authorization": f"Bearer {BIRD_API_KEY}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=20
        )

        log.info(
            "EMAIL: outbound Bird HTTP request completed"
        )

        response_text = response.text[:1000]

        log.info(
            "EMAIL: Bird response status=%s body=%s",
            response.status_code,
            response_text
        )

        if response.status_code == 202:

            try:
                result = response.json()
            except ValueError:
                result = {}

            message_id = result.get("id")

            log.info(
                "EMAIL: accepted by Bird | message_id=%s | recipient=%s",
                message_id,
                ALERT_EMAIL
            )

            return True

        # Bird documents field-validation failures as 422 and
        # authentication failures as 401/403. Log them explicitly.
        if response.status_code == 401:
            log.error(
                "EMAIL: Bird rejected the API key (401). "
                "Check BIRD_API_KEY and its region."
            )

        elif response.status_code == 403:
            log.error(
                "EMAIL: Bird denied the API operation (403). "
                "Check the API key permissions/scopes."
            )

        elif response.status_code == 421:
            log.error(
                "EMAIL: wrong Bird regional host (421). "
                "Check the region encoded in BIRD_API_KEY."
            )

        elif response.status_code == 422:
            log.error(
                "EMAIL: Bird rejected the email request (422). "
                "Check sender verification, recipient restrictions, "
                "and payload fields."
            )

        elif response.status_code == 429:
            log.error(
                "EMAIL: Bird rate/usage limit reached (429)."
            )

        else:
            log.error(
                "EMAIL: Bird send failed with HTTP %s",
                response.status_code
            )

    except requests.Timeout:
        log.error(
            "EMAIL: Bird request timed out after 20 seconds"
        )

    except requests.RequestException as e:
        log.error(
            "EMAIL: network error while contacting Bird: %s",
            e
        )

    except Exception as e:
        log.exception(
            "EMAIL: unexpected email error: %s",
            e
        )

    return False


def send_alert_email_async(alerts):
    """
    Run email delivery outside the sensor request/alert logic so
    a slow or failed email provider never blocks sensor ingestion.
    """

    try:
        success = send_alert_email(alerts)

        if success:
            log.info(
                "EMAIL: alert email processing completed successfully"
            )
        else:
            log.error(
                "EMAIL: alert email was NOT accepted by Bird"
            )

    except Exception as e:
        log.exception(
            "EMAIL: background worker failed: %s",
            e
        )


# ============================================================
# ALERT STATE
# ============================================================

# Email behavior:
# - Temperature email is sent immediately when temperature goes
#   outside TEMP_MIN / TEMP_MAX.
# - Wet-diaper email is sent immediately when wetness is detected.
# - No consecutive-reading requirement.
# - No humidity emails.
# - No motion emails.
# - No sound emails.
# - The same active condition is latched so it does not generate
#   a new email every 5 seconds.
# - The condition resets when temperature returns to range or
#   the diaper becomes dry.

_alert_lock = threading.Lock()

_condition_latched = {
    "temperature": False,
    "wetness": False,
}


def reset_condition(condition):
    """Reset one alert condition so a future occurrence can email again."""
    with _alert_lock:
        _condition_latched[condition] = False


# ============================================================
# EVENT-BASED ALERT LOGIC
# ============================================================

def check_alerts(temp, hum, wetness, sound):
    """
    Generate alerts only for the two conditions requested:

    1. Temperature is outside TEMP_MIN <= temperature <= TEMP_MAX.
    2. Diaper is wet.

    There is NO consecutive-reading requirement.

    A condition sends one email when it first becomes active.
    While that condition remains active, additional 5-second
    readings do not send duplicate emails.

    Once the condition returns to normal, its latch is reset.
    A later occurrence can then send another email.
    """

    confirmed = []

    # ========================================================
    # TEMPERATURE
    # ========================================================

    temperature_abnormal = (
        temp < TEMP_MIN or
        temp > TEMP_MAX
    )

    if temperature_abnormal:

        with _alert_lock:

            if not _condition_latched["temperature"]:

                _condition_latched["temperature"] = True

                if temp > TEMP_MAX:

                    confirmed.append({
                        "alert_type": "temperature",
                        "severity": "critical",
                        "message": (
                            f"Temperature is too high: {temp:.1f}°C. "
                            f"Configured maximum is {TEMP_MAX:.1f}°C."
                        )
                    })

                elif temp < TEMP_MIN:

                    confirmed.append({
                        "alert_type": "temperature",
                        "severity": "critical",
                        "message": (
                            f"Temperature is too low: {temp:.1f}°C. "
                            f"Configured minimum is {TEMP_MIN:.1f}°C."
                        )
                    )

                log.warning(
                    "ALERT DETECTED: temperature outside range: %.1f°C",
                    temp
                )

    else:

        if _condition_latched["temperature"]:
            log.info(
                "ALERT RESET: temperature returned to normal range: %.1f°C",
                temp
            )

        reset_condition("temperature")


    # ========================================================
    # WET DIAPER
    # ========================================================

    if bool(wetness):

        with _alert_lock:

            if not _condition_latched["wetness"]:

                _condition_latched["wetness"] = True

                confirmed.append({
                    "alert_type": "wetness",
                    "severity": "critical",
                    "message": (
                        "Diaper is wet! Please change the diaper."
                    )
                })

                log.warning(
                    "ALERT DETECTED: wet diaper"
                )

    else:

        if _condition_latched["wetness"]:
            log.info(
                "ALERT RESET: diaper is dry"
            )

        reset_condition("wetness")


    # ========================================================
    # IMPORTANT:
    # Humidity, motion and sound intentionally do NOT create
    # email alerts.
    # ========================================================

    if not confirmed:
        return []


    # ========================================================
    # SAVE ALERTS TO SUPABASE
    # ========================================================

    saved_alerts = []

    if supabase is None:

        log.error(
            "ALERT: Supabase is not configured; alert cannot be saved."
        )

    else:

        for alert in confirmed:

            try:

                response = (
                    supabase
                    .table("alerts")
                    .insert(alert)
                    .execute()
                )

                saved = (
                    response.data[0]
                    if response.data
                    else alert
                )

                saved_alerts.append(saved)

                log.warning(
                    "ALERT SAVED: %s",
                    alert["message"]
                )

            except Exception as e:

                log.error(
                    "Failed to save alert: %s",
                    e
                )

                # Still keep the alert available for email.
                # This prevents a temporary alerts-table problem
                # from completely suppressing the email.
                saved_alerts.append(alert)


    # ========================================================
    # SEND EMAIL IMMEDIATELY
    # ========================================================

    if confirmed:

        threading.Thread(
            target=send_alert_email_async,
            args=(list(confirmed),),
            daemon=True
        ).start()

        log.info(
            "EMAIL QUEUED: temperature/wetness condition detected"
        )


    return saved_alerts


# ============================================================
# PROCESS SENSOR READING
# ============================================================


def save_sensor_reading_to_supabase(
    temp,
    hum,
    motion,
    sound,
    wetness
):
    """
    Save one sensor reading using the exact PostgreSQL types
    defined in the sensor_readings table.
    """

    if supabase is None:
        raise RuntimeError(
            "Supabase is not configured. Check SUPABASE_URL and SUPABASE_KEY."
        )

    # sensor_readings.sound_level is INTEGER.
    # This guarantees that 0.0 / "0.0" becomes integer 0.
    reading = {
        "temperature": float(temp),
        "humidity": float(hum),
        "motion_detected": bool(motion),
        "sound_level": int(float(sound)),
        "wetness_detected": bool(wetness),
        "is_abnormal": bool(check_abnormal(float(temp), float(hum)))
    }

    log.info("Supabase sensor payload: %s", reading)

    response = (
        supabase
        .table("sensor_readings")
        .insert(reading)
        .execute()
    )

    if not response.data:
        raise RuntimeError(
            "Supabase returned no inserted sensor reading."
        )

    log.info(
        "Sensor reading saved to Supabase: id=%s",
        response.data[0].get("id")
    )

    return response.data[0]


def _process_reading_async(
    temp,
    hum,
    motion,
    sound,
    wetness
):
    """
    Process alerts after the sensor reading has already been
    successfully saved to Supabase.
    """

    try:
        check_alerts(
            temp,
            hum,
            wetness,
            sound
        )
    except Exception as e:
        log.error(
            "Alert processing error: %s",
            e
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
# SUPABASE DATABASE HEALTH CHECK
# ============================================================

@app.route(
    "/api/db_health",
    methods=["GET"]
)
def api_db_health():

    if supabase is None:
        return jsonify({
            "status": "error",
            "database": "not_configured",
            "message": "SUPABASE_URL or SUPABASE_KEY is missing"
        }), 503

    try:

        supabase.table(
            "sensor_readings"
        ).select(
            "id"
        ).limit(
            1
        ).execute()

        return jsonify({
            "status": "ok",
            "database": "connected",
            "table": "sensor_readings"
        }), 200

    except Exception as e:

        log.error(
            "Supabase health check failed: %s",
            e
        )

        return jsonify({
            "status": "error",
            "database": "unavailable",
            "details": str(e)
        }), 503


# ============================================================
# EMAIL CONFIGURATION / TEST
# ============================================================

@app.route(
    "/api/email_status",
    methods=["GET"]
)
def api_email_status():

    """
    Safe diagnostic endpoint. It reports configuration status
    without exposing the Bird API key.
    """

    status = email_configuration_status()

    return jsonify({
        "status": "ok",
        "email": status
    }), 200


@app.route(
    "/api/test_email",
    methods=["POST"]
)
def api_test_email():

    """
    Send a controlled test email without requiring a sensor alert.

    This is intended for debugging the Bird configuration.
    """

    test_alert = [{
        "alert_type": "test",
        "severity": "warning",
        "message": "This is a test email from the Baby Monitoring System."
    }]

    success = send_alert_email(
        test_alert
    )

    if success:
        return jsonify({
            "status": "ok",
            "message": "Bird accepted the test email",
            "recipient": ALERT_EMAIL
        }), 200

    return jsonify({
        "status": "error",
        "message": "Bird did not accept the test email. Check Render logs.",
        "configuration": email_configuration_status()
    }), 502


# ============================================================
# BASIC ENVIRONMENTAL FORECASTING
# ============================================================

# Forecasts use the most recent stored readings. Because the system
# stores one reading every 5 seconds, 60 samples represent 5 minutes.
FORECAST_SAMPLE_COUNT = 60
FORECAST_HORIZONS_MINUTES = (5, 10, 15)

# Use the same environmental limits as the alert system.
# These were previously read directly inside alert functions, but the
# forecasting code also needs them when deciding whether a prediction
# is outside the normal range.
TEMP_MIN = float(os.getenv("TEMP_MIN", "20"))
TEMP_MAX = float(os.getenv("TEMP_MAX", "25"))
HUM_MIN = float(os.getenv("HUM_MIN", "40"))
HUM_MAX = float(os.getenv("HUM_MAX", "60"))


# ============================================================
# TEMPERATURE / HUMIDITY ABNORMALITY CHECK
# ============================================================

def check_abnormal(temp, hum):
    """
    Return True when temperature or humidity is outside the
    configured safe operating range.

    Uses the same thresholds as the alert and forecasting systems.
    None values are ignored so status/forecast calls do not crash.
    """

    if temp is not None:
        try:
            temp_value = float(temp)
            if temp_value < TEMP_MIN or temp_value > TEMP_MAX:
                return True
        except (TypeError, ValueError):
            pass

    if hum is not None:
        try:
            hum_value = float(hum)
            if hum_value < HUM_MIN or hum_value > HUM_MAX:
                return True
        except (TypeError, ValueError):
            pass

    return False


def linear_forecast(points, horizon_seconds):
    """
    Simple least-squares linear trend forecast.

    points:
        list of (timestamp_seconds, value)

    Returns the predicted value at the requested future horizon.
    """
    if len(points) < 2:
        return None

    xs = [float(p[0]) for p in points]
    ys = [float(p[1]) for p in points]

    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n

    denominator = sum(
        (x - mean_x) ** 2
        for x in xs
    )

    if denominator == 0:
        return mean_y

    slope = sum(
        (x - mean_x) * (y - mean_y)
        for x, y in zip(xs, ys)
    ) / denominator

    intercept = mean_y - slope * mean_x

    future_x = xs[-1] + float(horizon_seconds)

    return intercept + slope * future_x


def get_environment_forecast():
    """
    Retrieve recent environmental readings from Supabase and
    produce basic linear forecasts for temperature and humidity.

    This is intentionally a simple baseline forecast suitable for
    a project-level 'basic forecasting analytics' requirement.
    """

    if supabase is None:
        raise RuntimeError("Supabase is not configured.")

    response = (
        supabase
        .table("sensor_readings")
        .select(
            "temperature,humidity,created_at"
        )
        .order(
            "created_at",
            desc=True
        )
        .limit(
            FORECAST_SAMPLE_COUNT
        )
        .execute()
    )

    rows = list(reversed(response.data or []))

    if len(rows) < 2:
        return {
            "status": "insufficient_data",
            "samples": len(rows),
            "required_samples": 2,
            "forecast_minutes": list(
                FORECAST_HORIZONS_MINUTES
            ),
            "temperature": [],
            "humidity": []
        }

    from datetime import timezone

    def timestamp_seconds(value):
        dt = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.timestamp()

    temp_points = []
    humidity_points = []

    for row in rows:
        try:
            ts = timestamp_seconds(
                row["created_at"]
            )

            temp_points.append(
                (
                    ts,
                    float(row["temperature"])
                )
            )

            humidity_points.append(
                (
                    ts,
                    float(row["humidity"])
                )
            )

        except (KeyError, TypeError, ValueError):
            continue

    if len(temp_points) < 2:
        return {
            "status": "insufficient_data",
            "samples": len(temp_points),
            "required_samples": 2,
            "forecast_minutes": list(
                FORECAST_HORIZONS_MINUTES
            ),
            "temperature": [],
            "humidity": []
        }

    latest_temp = temp_points[-1][1]
    latest_humidity = humidity_points[-1][1]

    forecasts_temperature = []
    forecasts_humidity = []

    for minutes in FORECAST_HORIZONS_MINUTES:

        seconds = minutes * 60

        predicted_temp = linear_forecast(
            temp_points,
            seconds
        )

        predicted_humidity = linear_forecast(
            humidity_points,
            seconds
        )

        forecasts_temperature.append({
            "minutes_ahead": minutes,
            "value": round(
                float(predicted_temp),
                2
            )
        })

        forecasts_humidity.append({
            "minutes_ahead": minutes,
            "value": round(
                float(predicted_humidity),
                2
            )
        })

    # Determine simple warnings based on the same environmental
    # thresholds already used by the alert system.
    temp_warning = any(
        item["value"] < TEMP_MIN or
        item["value"] > TEMP_MAX
        for item in forecasts_temperature
    )

    humidity_warning = any(
        item["value"] < HUM_MIN or
        item["value"] > HUM_MAX
        for item in forecasts_humidity
    )

    return {
        "status": "ok",
        "samples": len(temp_points),
        "sample_window_minutes": round(
            (
                temp_points[-1][0] -
                temp_points[0][0]
            ) / 60,
            2
        ),
        "generated_at": datetime.now(ZoneInfo("Africa/Lusaka")).isoformat(),
        "current": {
            "temperature": round(
                latest_temp,
                2
            ),
            "humidity": round(
                latest_humidity,
                2
            )
        },
        "forecast": {
            "temperature": forecasts_temperature,
            "humidity": forecasts_humidity
        },
        "warnings": {
            "temperature": temp_warning,
            "humidity": humidity_warning
        }
    }


@app.route(
    "/api/forecast",
    methods=["GET"]
)
def api_forecast():

    try:
        result = get_environment_forecast()

        return jsonify(
            result
        ), 200

    except Exception as e:

        log.exception(
            "Forecast generation failed: %s",
            e
        )

        return jsonify({
            "status": "error",
            "error": "Could not generate environmental forecast",
            "details": str(e)
        }), 503


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

    """
    Raspberry Pi -> Flask -> 5-second sampler -> Supabase -> Dashboard.

    The Raspberry Pi may send data continuously, but only one reading
    every 5 seconds is:
        - inserted into Supabase sensor_readings
        - broadcast to connected dashboards
        - passed to alert detection

    This prevents the database and dashboard from being updated every
    second while preserving the latest accepted sensor state.
    """

    data = request.get_json(silent=True) or {}

    if "temperature" not in data or "humidity" not in data:
        return jsonify({
            "status": "error",
            "error": "Missing required fields: temperature, humidity"
        }), 400

    try:
        temp = float(data.get("temperature"))
        hum = float(data.get("humidity"))

        motion = bool(data.get(
            "motion_detected",
            False
        ))

        # sensor_readings.sound_level is INTEGER.
        sound = int(float(data.get(
            "sound_level",
            0
        )))

        wetness = bool(data.get(
            "wetness_detected",
            False
        ))

    except (TypeError, ValueError) as e:

        log.error(
            "Invalid sensor payload: %s | payload=%s",
            e,
            data
        )

        return jsonify({
            "status": "error",
            "error": "Invalid sensor data",
            "details": str(e)
        }), 400

    # --------------------------------------------------------
    # 5-SECOND SAMPLING
    # --------------------------------------------------------

    if not should_commit_sensor_reading():

        return jsonify({
            "status": "ok",
            "message": "Sensor reading received but skipped by 5-second sampler",
            "database_saved": False,
            "dashboard_updated": False,
            "next_update_seconds": SENSOR_UPDATE_INTERVAL
        }), 200

    # --------------------------------------------------------
    # SAVE ONE READING TO SUPABASE
    # --------------------------------------------------------

    try:

        saved_reading = save_sensor_reading_to_supabase(
            temp,
            hum,
            motion,
            sound,
            wetness
        )

    except Exception as e:

        log.error(
            "Supabase sensor reading save failed: %s",
            e
        )

        # Allow the next incoming reading to retry immediately rather
        # than waiting five seconds after a failed database write.
        global _last_sensor_update_time
        with _sensor_update_lock:
            _last_sensor_update_time = 0.0

        return jsonify({
            "status": "error",
            "error": "Sensor reading could not be saved to Supabase",
            "details": str(e)
        }), 503

    # --------------------------------------------------------
    # UPDATE DASHBOARD ONLY AFTER DB SUCCESS
    # --------------------------------------------------------

    current_data["temperature"] = temp
    current_data["humidity"] = hum
    current_data["motion"] = motion
    current_data["sound"] = sound
    current_data["wetness"] = wetness

    # Use a timezone-aware Lusaka timestamp for dashboard state.
    try:
        from zoneinfo import ZoneInfo

        current_data["last_update"] = datetime.now(
            ZoneInfo("Africa/Lusaka")
        ).isoformat()

    except Exception:
        current_data["last_update"] = datetime.now().isoformat()

    socketio.emit(
        "sensor_update",
        dict(current_data)
    )

    # Generate and broadcast a forecast after the database update.
    # Forecast errors must never prevent the sensor update from succeeding.
    try:
        forecast = get_environment_forecast()

        socketio.emit(
            "environment_forecast",
            forecast
        )

    except Exception as e:
        log.warning(
            "Forecast update skipped: %s",
            e
        )

    log.info(
        "5-SECOND UPDATE: DB saved id=%s | dashboard updated | "
        "temperature=%.1f humidity=%.1f motion=%s sound=%d wetness=%s",
        saved_reading.get("id"),
        temp,
        hum,
        motion,
        sound,
        wetness
    )

    # --------------------------------------------------------
    # PROCESS ALERTS ONLY FOR THE STORED 5-SECOND SAMPLE
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

    return jsonify({
        "status": "ok",
        "message": "Sensor reading saved and dashboard updated",
        "database_saved": True,
        "dashboard_updated": True,
        "sample_interval_seconds": SENSOR_UPDATE_INTERVAL,
        "abnormal": check_abnormal(
            temp,
            hum
        ),
        "reading": saved_reading
    }), 200


# ============================================================
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
