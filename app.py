#!/usr/bin/env python3

import os
import sys
import time
import queue
import threading
import logging
from datetime import datetime
from functools import wraps

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

from flask import Flask, render_template, request, jsonify, Response, redirect, make_response
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
# AUTHENTICATION / SESSION
# ============================================================

AUTH_COOKIE_NAME = "sb_access_token"
AUTH_COOKIE_MAX_AGE = 60 * 60 * 24 * 7  # 7 days


def supabase_auth_user(access_token):
    """Validate a Supabase access token and return the authenticated user."""
    if not access_token or not SUPABASE_URL or not SUPABASE_KEY:
        return None

    try:
        response = requests.get(
            f"{SUPABASE_URL.rstrip('/')}/auth/v1/user",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            timeout=10,
        )

        if response.status_code == 200:
            return response.json()

        log.warning(
            "AUTH: Supabase rejected token with HTTP %s",
            response.status_code,
        )
    except requests.RequestException as e:
        log.error("AUTH: token validation failed: %s", e)

    return None


def authenticate_request():
    """Validate the access token and refresh it automatically when needed."""
    access_token = request.cookies.get(AUTH_COOKIE_NAME)
    refresh_token = request.cookies.get("sb_refresh_token")

    user = supabase_auth_user(access_token)
    if user is not None:
        return user, None, None

    if not refresh_token or not SUPABASE_URL or not SUPABASE_KEY:
        return None, None, None

    try:
        response = requests.post(
            f"{SUPABASE_URL.rstrip('/')}/auth/v1/token?grant_type=refresh_token",
            headers={
                "apikey": SUPABASE_KEY,
                "Content-Type": "application/json",
            },
            json={"refresh_token": refresh_token},
            timeout=15,
        )
        if response.status_code == 200:
            data = response.json()
            new_access = data.get("access_token")
            new_refresh = data.get("refresh_token") or refresh_token
            user = data.get("user") or supabase_auth_user(new_access)
            if new_access and user:
                log.info("AUTH: access token refreshed for authenticated session")
                return user, new_access, new_refresh
    except (requests.RequestException, ValueError) as e:
        log.warning("AUTH: refresh failed: %s", e)

    return None, None, None


def current_user():
    """Return the authenticated user for this request, if any."""
    user, _, _ = authenticate_request()
    return user


def _set_auth_cookies(response_out, access_token=None, refresh_token=None):
    secure_cookie = request.is_secure or bool(os.getenv("RENDER"))
    if access_token:
        response_out.set_cookie(
            AUTH_COOKIE_NAME,
            access_token,
            max_age=AUTH_COOKIE_MAX_AGE,
            httponly=True,
            secure=secure_cookie,
            samesite="Lax",
            path="/",
        )
    if refresh_token:
        response_out.set_cookie(
            "sb_refresh_token",
            refresh_token,
            max_age=AUTH_COOKIE_MAX_AGE,
            httponly=True,
            secure=secure_cookie,
            samesite="Lax",
            path="/",
        )


def login_required(view):
    """Protect browser pages and user-facing API endpoints."""
    @wraps(view)
    def wrapped(*args, **kwargs):
        user, refreshed_access, refreshed_refresh = authenticate_request()
        if user is None:
            if request.path.startswith("/api/") or request.path == "/video_feed":
                return jsonify({
                    "status": "error",
                    "error": "Authentication required",
                    "redirect": "/login",
                }), 401
            return redirect("/login")

        request.auth_user = user
        request.auth_refreshed_access = refreshed_access
        request.auth_refreshed_refresh = refreshed_refresh

        result = view(*args, **kwargs)
        if refreshed_access:
            response_out = make_response(result)
            _set_auth_cookies(
                response_out,
                refreshed_access,
                refreshed_refresh,
            )
            return response_out
        return result

    return wrapped


# ============================================================
# LOGIN / LOGOUT
# ============================================================

@app.route("/login", methods=["GET"])
def login_page():
    if current_user() is not None:
        return redirect("/")
    return render_template("login.html")


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(silent=True) or {}
    email = str(data.get("email", "")).strip()
    password = str(data.get("password", ""))

    if not email or not password:
        return jsonify({
            "status": "error",
            "message": "Email and password are required.",
        }), 400

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("AUTH: SUPABASE_URL or SUPABASE_KEY is missing")
        return jsonify({
            "status": "error",
            "message": "Authentication is not configured on the server.",
        }), 503

    try:
        response = requests.post(
            f"{SUPABASE_URL.rstrip('/')}/auth/v1/token?grant_type=password",
            headers={
                "apikey": SUPABASE_KEY,
                "Content-Type": "application/json",
            },
            json={"email": email, "password": password},
            timeout=15,
        )
    except requests.RequestException as e:
        log.error("AUTH: Supabase login request failed: %s", e)
        return jsonify({
            "status": "error",
            "message": "Unable to contact the authentication service.",
        }), 503

    if response.status_code != 200:
        try:
            error_data = response.json()
        except ValueError:
            error_data = {}

        # Do not expose raw Supabase authentication details.
        log.warning(
            "AUTH: login rejected for %s with HTTP %s",
            email,
            response.status_code,
        )
        return jsonify({
            "status": "error",
            "message": "Invalid email or password.",
        }), 401

    try:
        auth_data = response.json()
        access_token = auth_data.get("access_token")
        refresh_token = auth_data.get("refresh_token")
        user = auth_data.get("user") or {}
    except ValueError:
        access_token = None
        refresh_token = None
        user = {}

    if not access_token:
        log.error("AUTH: Supabase login returned no access token")
        return jsonify({
            "status": "error",
            "message": "Login could not be completed.",
        }), 502

    response_out = make_response(jsonify({
        "status": "ok",
        "message": "Login successful",
        "user": {
            "id": user.get("id"),
            "email": user.get("email"),
        },
    }))

    # Keep both tokens in HttpOnly cookies so browser JavaScript never
    # needs to handle Supabase access or refresh tokens directly.
    _set_auth_cookies(response_out, access_token, refresh_token)

    return response_out, 200


@app.route("/api/logout", methods=["POST"])
def api_logout():
    access_token = request.cookies.get(AUTH_COOKIE_NAME)

    # Best-effort Supabase sign-out. Clearing the local cookies is what
    # guarantees that this browser can no longer access protected routes.
    if access_token and SUPABASE_URL and SUPABASE_KEY:
        try:
            requests.post(
                f"{SUPABASE_URL.rstrip('/')}/auth/v1/logout",
                headers={
                    "apikey": SUPABASE_KEY,
                    "Authorization": f"Bearer {access_token}",
                },
                timeout=10,
            )
        except requests.RequestException as e:
            log.warning("AUTH: remote logout failed: %s", e)

    response_out = make_response(jsonify({
        "status": "ok",
        "message": "Logged out successfully",
    }))
    response_out.delete_cookie(AUTH_COOKIE_NAME, path="/")
    response_out.delete_cookie("sb_refresh_token", path="/")
    return response_out, 200


@app.route("/api/me", methods=["GET"])
@login_required
def api_me():
    user = request.auth_user
    return jsonify({
        "status": "ok",
        "user": {
            "id": user.get("id"),
            "email": user.get("email"),
        },
    })


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
# VIDEO STREAMING (legacy JPEG / MJPEG path)
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
# H.264 / FRAGMENTED MP4 VIDEO STREAMING
# ============================================================
#
# This is a second, separate video path alongside the JPEG/MJPEG one
# above. The Pi encodes H.264 and muxes it into fragmented MP4 (fMP4)
# using ffmpeg, then POSTs:
#   - ONE "init segment" (the ftyp+moov boxes) once, when it starts
#   - a continuous stream of "fragments" (moof+mdat box pairs) after
#
# Unlike JPEG frames, fMP4 fragments must NOT be dropped/skipped for a
# given viewer - doing so corrupts that viewer's decode from that
# point on (unlike a still image, where showing a slightly stale frame
# is harmless). So fragment queues here are sized generously and we
# only ever drop for a specific subscriber that is falling behind
# (never for the others), accepting that a lagging viewer's video may
# glitch until their next page load/reconnect (which re-fetches the
# init segment and starts clean).
# ============================================================

_video_init_segment = None

_video_init_lock = threading.Lock()

_video_fragment_subscribers = []

_video_fragment_subscribers_lock = threading.Lock()


def _broadcast_video_fragment(fragment):

    with _video_fragment_subscribers_lock:

        for q in _video_fragment_subscribers:

            try:

                q.put_nowait(fragment)

            except queue.Full:

                # Deliberately do NOT drop-and-replace here like the
                # JPEG path does. Silently skipping this fragment for
                # this one slow subscriber is the least-bad option -
                # their stream may glitch, but other viewers are
                # unaffected, and this subscriber will self-correct on
                # their next reconnect (fresh init segment + fragments).
                pass


@app.route(
    "/api/upload_video_init",
    methods=["POST"]
)
def api_upload_video_init():
    """
    Receives the one-time fMP4 initialization segment (ftyp+moov boxes)
    from the Pi. Sent once when the Pi's video pipeline (re)starts, and
    cached so any browser connecting to /video_stream_mp4 - even ones
    joining well after the Pi started - can be sent it immediately
    before their live fragment stream begins.
    """

    global _video_init_segment

    data = request.get_data()

    if not data:
        return jsonify({
            "status": "error",
            "error": "Empty init segment"
        }), 400

    with _video_init_lock:
        _video_init_segment = data

    log.info(
        "VIDEO INIT: received fMP4 init segment (%d bytes)",
        len(data)
    )

    return jsonify({"status": "ok"})


@app.route(
    "/api/upload_video_fragment",
    methods=["POST"]
)
def api_upload_video_fragment():
    """
    Receives one fMP4 fragment (a complete moof+mdat box pair) from the
    Pi and relays it live to every connected /video_stream_mp4 viewer.
    """

    data = request.get_data()

    if not data:
        return jsonify({
            "status": "error",
            "error": "Empty fragment"
        }), 400

    _broadcast_video_fragment(data)

    return jsonify({"status": "ok"})


@app.route(
    "/video_stream_mp4"
)
@login_required
def video_stream_mp4():
    """
    Live H.264 video as a continuous fragmented-MP4 byte stream.

    Point a <video> element's src directly at this endpoint - modern
    Chrome/Firefox can play a growing/fragmented MP4 delivered over a
    chunked HTTP response as progressive playback, no MediaSource
    Extensions JavaScript required. (Safari support for this is more
    limited - see the frontend notes.)
    """

    def generate():

        q = queue.Queue(maxsize=60)

        with _video_fragment_subscribers_lock:
            _video_fragment_subscribers.append(q)

        try:

            with _video_init_lock:
                init = _video_init_segment

            if not init:
                # No video pipeline has connected yet.
                return

            yield init

            while True:

                fragment = q.get()

                yield fragment

        finally:

            with _video_fragment_subscribers_lock:
                if q in _video_fragment_subscribers:
                    _video_fragment_subscribers.remove(q)

    return Response(
        generate(),
        mimetype="video/mp4"
    )


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
#
# Alert "new event" detection compares the current reading against
# the PREVIOUS reading actually stored in Supabase — not an in-memory
# Python variable. This matters because this app can run under
# multiple Gunicorn worker processes (and Render's free tier can
# restart the dyno on inactivity). An in-memory global is scoped to a
# single worker process and resets on every restart, so two workers
# handling alternating requests — or a restart while the diaper is
# still wet — would each see a "False -> True" transition and fire a
# duplicate alert for a condition that never actually changed. Reading
# the previous state from Supabase makes this correct regardless of
# how many workers or restarts are involved.
#
# dry -> wet       = alert
# wet -> wet       = nothing
# wet -> dry       = reset
# dry -> wet       = alert again
#
# Same principle for temperature.
# ============================================================


def get_previous_reading_state():
    """
    Fetch the most recently stored sensor reading's wetness and
    abnormal-temperature flags from Supabase. Used as the baseline
    for detecting state transitions, instead of in-memory globals
    that don't survive worker restarts or exist across multiple
    Gunicorn worker processes.

    Returns (previous_wetness: bool, previous_temperature_abnormal: bool).
    Defaults to (False, False) when there is no prior reading or
    Supabase is not configured.
    """

    if supabase is None:
        return False, False

    try:
        response = (
            supabase
            .table("sensor_readings")
            .select("temperature,wetness_detected")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )

        rows = response.data or []

        if not rows:
            return False, False

        row = rows[0]

        # IMPORTANT:
        # is_abnormal represents temperature OR humidity.
        # Therefore it must NOT be used as the previous
        # temperature state.
        try:
            previous_temp = float(row.get("temperature"))
        except (TypeError, ValueError):
            previous_temp = None

        try:
            temp_min = float(os.getenv("TEMP_MIN", "20"))
        except (TypeError, ValueError):
            temp_min = 20.0

        try:
            temp_max = float(os.getenv("TEMP_MAX", "25"))
        except (TypeError, ValueError):
            temp_max = 25.0

        previous_temperature_abnormal = (
            previous_temp is not None
            and (
                previous_temp < temp_min
                or previous_temp > temp_max
            )
        )

        return (
            bool(row.get("wetness_detected", False)),
            previous_temperature_abnormal,
        )

    except Exception as e:
        log.error(
            "Failed to fetch previous reading state: %s",
            e
        )
        # Fail safe: treat as "no previous abnormal condition" so a
        # transient DB read error can't permanently suppress alerts,
        # at worst causing one duplicate rather than silent misses.
        return False, False


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

# True after the first temperature reading has been processed
# by this server process. This allows an abnormal temperature
# present at startup to trigger an alert once.
_temperature_alert_initialized = False


def check_alerts(
    temp,
    hum,
    wetness,
    sound,
    previous_wetness,
    previous_temperature_abnormal
):
    global _temperature_alert_initialized

    """
    Detect NEW alert events.

    previous_wetness / previous_temperature_abnormal are the state
    of the PREVIOUS stored reading, fetched from Supabase by the
    caller (see get_previous_reading_state). This makes the
    transition check correct across multiple Gunicorn workers and
    server restarts, instead of relying on an in-memory global that
    is only visible to a single process.

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
    # TEMPERATURE
    # ========================================================

    # Temperature alert rules:
    #
    # First reading after server startup:
    #     abnormal -> ALERT
    #
    # Normal -> abnormal:
    #     ALERT
    #
    # Abnormal -> abnormal:
    #     NO NEW ALERT
    #
    # Abnormal -> normal:
    #     RESET, so a later abnormal reading alerts again.
    new_temperature_event = (
        temperature_abnormal
        and
        (
            not _temperature_alert_initialized
            or
            not previous_temperature_abnormal
        )
    )

    _temperature_alert_initialized = True


    if temperature_abnormal:
        log.info(
            "TEMPERATURE CHECK: %.1f°C | SAFE RANGE: %.1f–%.1f°C | ABNORMAL",
            temp,
            temp_min,
            temp_max
        )
    else:
        log.info(
            "TEMPERATURE CHECK: %.1f°C | SAFE RANGE: %.1f–%.1f°C | NORMAL",
            temp,
            temp_min,
            temp_max
        )

    if new_temperature_event:

        log.warning(
            "TEMPERATURE ALERT TRIGGERED: %.1f°C "
            "is outside %.1f–%.1f°C",
            temp,
            temp_min,
            temp_max
        )

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

    # Do not block sensor processing while Bird is contacted.
    threading.Thread(
        target=send_alert_email_async,
        args=(list(alerts),),
        daemon=True
    ).start()


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
    wetness,
    previous_wetness,
    previous_temperature_abnormal
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
            sound,
            previous_wetness,
            previous_temperature_abnormal
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
@login_required
def index():

    return render_template(
        "index.html"
    )


# ============================================================
# LIVE PAGE
# ============================================================

@app.route("/live")
@login_required
def live():

    return render_template(
        "live.html"
    )


# ============================================================
# HISTORY PAGE
# ============================================================

@app.route("/history")
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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

# Forecasts use the most recent stored readings. The system stores one
# reading every 5 seconds (12 per minute). To predict 60 minutes ahead,
# the regression needs a window at least as long as the horizon itself
# — extrapolating a straight line far past the data it was fit on
# produces increasingly unreliable predictions. 720 samples covers the
# last 60 minutes of stored readings.
FORECAST_SAMPLE_COUNT = 720
FORECAST_HORIZONS_MINUTES = (60,)

# Use the same environmental limits as the alert system.
# These were previously read directly inside alert functions, but the
# forecasting code also needs them when deciding whether a prediction
# is outside the normal range.
TEMP_MIN = float(os.getenv("TEMP_MIN", "20"))
TEMP_MAX = float(os.getenv("TEMP_MAX", "25"))
HUM_MIN = float(os.getenv("HUM_MIN", "40"))
HUM_MAX = float(os.getenv("HUM_MAX", "60"))


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
        "generated_at": datetime.now().isoformat(),
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
    # READ PREVIOUS STATE BEFORE INSERTING THE NEW READING
    # --------------------------------------------------------
    # Must happen before the insert below, otherwise this would read
    # back the reading we are about to save instead of the prior one.

    previous_wetness, previous_temperature_abnormal = (
        get_previous_reading_state()
    )

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
            wetness,
            previous_wetness,
            previous_temperature_abnormal
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
# VIDEO FRAME UPLOAD (legacy JPEG path)
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
# LIVE VIDEO STREAM (legacy JPEG / MJPEG path)
# ============================================================

@app.route(
    "/video_feed"
)
@login_required
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
@login_required
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
