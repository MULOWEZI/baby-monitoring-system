#!/usr/bin/env python3

import io
import os
import time
import threading
from datetime import datetime

import board
import adafruit_dht
import requests

from gpiozero import DigitalInputDevice
from picamera2 import Picamera2

============================================================

ENVIRONMENT

============================================================
try:
from dotenv import load_dotenv
load_dotenv()
except ImportError:
pass

SERVER_URL = "https://baby-monitoring-system-7.onrender.com"

============================================================

VIDEO CONFIGURATION

============================================================

Reduced resolution for lower bandwidth and lower latency.

VIDEO_WIDTH = 288
VIDEO_HEIGHT = 216

Moderate FPS.

VIDEO_FPS = 10

Camera JPEG quality.

Since the camera performs the JPEG encoding directly,

we don't need PIL/OpenCV.

JPEG_QUALITY = 30

Maximum time allowed for an upload.

FRAME_TIMEOUT = 2.0

CAMERA_START_DELAY = 1.5

============================================================

SENSOR CONFIGURATION

============================================================

SENSOR_INTERVAL = 1.0
SENSOR_TIMEOUT = 5

WATER_PIN = 17
MOTION_PIN = 27
SOUND_PIN = 22

DHT_PIN = board.D4

WETNESS_ACTIVE_HIGH = False

============================================================

GPIO SETUP

============================================================

water_sensor = DigitalInputDevice(
WATER_PIN,
pull_up=False
)

motion_sensor = DigitalInputDevice(
MOTION_PIN,
pull_up=False,
bounce_time=0.2
)

sound_sensor = DigitalInputDevice(
SOUND_PIN,
pull_up=False
)

============================================================

DHT22

============================================================

dht = adafruit_dht.DHT22(
DHT_PIN,
use_pulseio=False
)

============================================================

CAMERA SETUP
============================================================

camera = Picamera2()

camera_config = camera.create_video_configuration(
main={
"size": (
VIDEO_WIDTH,
VIDEO_HEIGHT
),
"format": "RGB888"
},
buffer_count=2
)

camera.configure(camera_config)

camera.start()

time.sleep(CAMERA_START_DELAY)

============================================================

HTTP SESSIONS

============================================================

video_session = requests.Session()
sensor_session = requests.Session()

Keep HTTP connections alive.

video_session.headers.update({
"Connection": "keep-alive"
})

sensor_session.headers.update({
"Connection": "keep-alive"
})

============================================================

THREAD CONTROL

============================================================

stop_event = threading.Event()

============================================================

LATEST FRAME BUFFER

============================================================

latest_frame = None

frame_lock = threading.Lock()

new_frame_event = threading.Event()

============================================================

DHT CACHE

============================================================

last_temperature = 0
last_humidity = 0

============================================================

VIDEO CAPTURE

============================================================

def capture_loop():

global latest_frame

print(
    f"[VIDEO] Capture started: "
    f"{VIDEO_WIDTH}x{VIDEO_HEIGHT} @ {VIDEO_FPS} FPS"
)

frame_interval = 1.0 / VIDEO_FPS

while not stop_event.is_set():

    start = time.monotonic()

    try:

        # ------------------------------------------------
        # Capture JPEG directly from Picamera2.
        #
        # This is much better than:
        #
        # camera.capture_array()
        # -> PIL
        # -> RGB conversion
        # -> JPEG compression
        #
        # The camera/libcamera performs the JPEG encoding.
        # ------------------------------------------------

        buffer = io.BytesIO()

        camera.capture_file(
            buffer,
            format="jpeg"
        )

        frame_data = buffer.getvalue()

        if not frame_data:
            continue

        # ------------------------------------------------
        # Keep ONLY newest frame
        # ------------------------------------------------

        with frame_lock:
            latest_frame = frame_data

        new_frame_event.set()

    except Exception as e:

        print(
            f"[CAPTURE ERROR] {e}"
        )

        stop_event.wait(0.05)

    elapsed = time.monotonic() - start

    remaining = frame_interval - elapsed

    if remaining > 0:

        stop_event.wait(remaining)

============================================================

VIDEO UPLOAD
VIDEO UPLOAD

============================================================

def upload_loop():

print("[VIDEO] Upload loop started")

upload_count = 0

while not stop_event.is_set():

    # ----------------------------------------------------
    # Wait for new frame
    # ----------------------------------------------------

    if not new_frame_event.wait(timeout=1):

        continue

    # ----------------------------------------------------
    # Get newest frame
    # ----------------------------------------------------

    with frame_lock:

        frame = latest_frame

    # Clear AFTER obtaining the latest frame.
    #
    # This is important because capture may have produced
    # another frame while we were reading the previous one.
    new_frame_event.clear()

    if frame is None:

        continue

    try:

        start_upload = time.monotonic()

        response = video_session.post(

            SERVER_URL + "/api/upload_frame",

            data=frame,

            headers={
                "Content-Type": "image/jpeg",
                "Connection": "keep-alive"
            },

            timeout=FRAME_TIMEOUT
        )

        upload_time = (
            time.monotonic()
            - start_upload
        )

        upload_count += 1

        # ------------------------------------------------
        # Don't flood terminal
        # ------------------------------------------------

        if upload_count % 10 == 0:

            print(
                f"[VIDEO] "
                f"Frames: {upload_count} | "
                f"Size: {len(frame) / 1024:.1f} KB | "
                f"Upload: {upload_time:.2f}s | "
                f"HTTP: {response.status_code}"
            )

        # ------------------------------------------------
        # Server error
        # ------------------------------------------------

        if response.status_code != 200:

            print(
                f"[VIDEO ERROR] "
                f"HTTP {response.status_code}"
            )

    except requests.exceptions.Timeout:

        # Very important:
        #
        # DO NOT retry an old frame.
        #
        # We simply drop it and wait for the newest frame.
        print(
            "[VIDEO] Upload timeout - dropping frame"
        )

    except requests.exceptions.ConnectionError as e:

        print(
            f"[VIDEO] Connection error: {e}"
        )

        # Short pause so we don't hammer the server.
        stop_event.wait(0.05)

    except requests.exceptions.RequestException as e:

        print(
            f"[VIDEO] Request error: {e}"
        )

    except Exception as e:

        print(
            f"[VIDEO] Unexpected error: {e}"
        )

============================================================

DHT22 READING

============================================================

def read_dht():

global last_temperature
global last_humidity

try:

    temperature = dht.temperature
    humidity = dht.humidity

    if temperature is not None:

        last_temperature = round(
            temperature,
            1
        )

    if humidity is not None:

        last_humidity = round(
            humidity,
            1
        )

except Exception as e:

    # Don't destroy previous valid reading.
    print(
        f"[DHT22] {e}"
    )

return (
    last_temperature,
    last_humidity
)

============================================================

SENSOR LOOP

============================================================

def sensor_loop():

print("[SENSOR] Sensor loop started")

while not stop_event.is_set():

    loop_start = time.monotonic()

    try:

        # ------------------------------------------------
        # DHT22
        # ------------------------------------------------

        temperature, humidity = read_dht()


        # ------------------------------------------------
        # MOTION
        # ------------------------------------------------

        try:

            motion = bool(
                motion_sensor.value
            )

        except Exception as e:

            print(
                f"[MOTION ERROR] {e}"
            )

            motion = False


        # ------------------------------------------------
        # SOUND
        # ------------------------------------------------

        try:

            sound = bool(
                sound_sensor.value
            )

        except Exception as e:

            print(
                f"[SOUND ERROR] {e}"
            )

            sound = False


        # ------------------------------------------------
        # WETNESS
        # ------------------------------------------------

        try:

            wet = water_sensor.value

            if WETNESS_ACTIVE_HIGH:

                wetness = (
                    wet == 1
                )

            else:

                wetness = (
                    wet == 0
                )

        except Exception as e:

            print(
                f"[WETNESS ERROR] {e}"
            )

            wetness = False


        # ------------------------------------------------
        # PAYLOAD
        # ------------------------------------------------

        payload = {

            "temperature":
                temperature,

            "humidity":
                humidity,

            "motion_detected":
                motion,

            "sound_level":
                1 if sound else 0,

            "wetness_detected":
                wetness
        }


        # ------------------------------------------------
        # SEND SENSOR DATA
        # ------------------------------------------------

        try:

            response = sensor_session.post(

                SERVER_URL +
                "/api/ingest",

                json=payload,

                timeout=SENSOR_TIMEOUT
            )

            print(
                datetime.now().strftime("%H:%M:%S"),
                payload,
                "HTTP",
                response.status_code
            )

        except requests.exceptions.Timeout:

            print(
                "[SENSOR] Upload timeout"
            )

        except requests.exceptions.RequestException as e:

            print(
                f"[SENSOR] Network error: {e}"
            )


    except Exception as e:

        print(
            f"[SENSOR ERROR] {e}"
        )


    # ----------------------------------------------------
    # Maintain sensor interval
    # ----------------------------------------------------

    elapsed = (
        time.monotonic()
        - loop_start
    )

    remaining = (
        SENSOR_INTERVAL
        - elapsed
    )

    if remaining > 0:

        stop_event.wait(
            remaining
        )

============================================================

SHUTDOWN
============================================================

def shutdown():

print()
print("=" * 60)
print("Stopping Baby Monitoring System...")
print("=" * 60)

stop_event.set()

# Wake upload thread.
new_frame_event.set()

# --------------------------------------------------------
# Close HTTP sessions
# --------------------------------------------------------

try:
    video_session.close()
except Exception:
    pass

try:
    sensor_session.close()
except Exception:
    pass

# --------------------------------------------------------
# Camera
# --------------------------------------------------------

try:

    camera.stop()

    print(
        "[CAMERA] Stopped"
    )

except Exception as e:

    print(
        f"[CAMERA] {e}"
    )

# --------------------------------------------------------
# DHT
# --------------------------------------------------------

try:

    dht.exit()

    print(
        "[DHT22] Closed"
    )

except Exception:
    pass

# --------------------------------------------------------
# GPIO
# --------------------------------------------------------

for device in (
    water_sensor,
    motion_sensor,
    sound_sensor
):

    try:

        device.close()

    except Exception:
        pass

print(
    "[SYSTEM] Shutdown complete."
)

============================================================

MAIN

============================================================

if name == "main":

print("=" * 60)
print("BABY MONITORING SYSTEM")
print("LOW LATENCY RASPBERRY PI CLIENT")
print("=" * 60)

print(
    "Server:",
    SERVER_URL
)

print(
    "Resolution:",
    f"{VIDEO_WIDTH}x{VIDEO_HEIGHT}"
)

print(
    "FPS:",
    VIDEO_FPS
)

print(
    "JPEG quality:",
    JPEG_QUALITY
)

print("=" * 60)


# --------------------------------------------------------
# Create threads
# --------------------------------------------------------

capture_thread = threading.Thread(
    target=capture_loop,
    name="VideoCapture"
)

upload_thread = threading.Thread(
    target=upload_loop,
    name="VideoUpload"
)

sensor_thread = threading.Thread(
    target=sensor_loop,
    name="SensorLoop"
)


try:

    capture_thread.start()

    upload_thread.start()

    sensor_thread.start()

    print(
        "[SYSTEM] All systems running."
    )


    while not stop_event.is_set():

        time.sleep(1)


except KeyboardInterrupt:

    print(
        "\n[MAIN] Ctrl+C received."
    )


finally:

    shutdown()

    # ----------------------------------------------------
    # Wait for threads to exit
    # ----------------------------------------------------

    capture_thread.join(timeout=3)

    upload_thread.join(timeout=3)

    sensor_thread.join(timeout=3)

    print(
        "[MAIN] Program exited."
    )
