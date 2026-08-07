#!/usr/bin/env python3
"""
Raspberry Pi client — matches server_optimized.py.

- capture_loop: grabs frames at FRAME_FPS, always keeps only the LATEST one
- upload_loop:  posts the latest frame as soon as it's available, using a
                persistent requests.Session (keep-alive, no handshake per
                frame). If an upload is slow, it never queues old frames —
                it just picks up whatever the newest frame is once it's free.
- sensor_loop:  posts sensor readings once a second, also over a persistent
                Session, with self-correcting timing so a slow request
                doesn't push the schedule later and later.
"""
import io
import threading
import time
from datetime import datetime

import board
import adafruit_dht
import requests
from gpiozero import DigitalInputDevice
from picamera2 import Picamera2

SERVER_URL = "https://baby-monitoring-system.onrender.com"  
FRAME_FPS = 15
SENSOR_INTERVAL = 1.0
FRAME_TIMEOUT = 2          # short timeout so a bad request can't stall the pipeline
SENSOR_TIMEOUT = 3

WATER_PIN = 17
MOTION_PIN = 27
SOUND_PIN = 22
DHT_PIN = board.D4
WETNESS_ACTIVE_HIGH = False

water_sensor = DigitalInputDevice(WATER_PIN)
motion_sensor = DigitalInputDevice(MOTION_PIN, pull_up=False, bounce_time=0.2)
sound_sensor = DigitalInputDevice(SOUND_PIN)
dht = adafruit_dht.DHT22(DHT_PIN, use_pulseio=False)

camera = Picamera2()
camera.configure(camera.create_video_configuration(main={"size": (640, 480)}))
camera.start()

# Persistent HTTP sessions — reuse the TCP connection instead of a fresh
# handshake on every single request. This is the single biggest speedup
# for both the video and sensor paths.
frame_session = requests.Session()
sensor_session = requests.Session()

# Latest-frame buffer: capture thread writes, upload thread reads.
# If the network is briefly slow, the upload thread just sends whatever
# is newest when it's free — it never works through a backlog, so the
# video can't drift further and further behind real time.
latest_frame = None
frame_lock = threading.Lock()
new_frame_event = threading.Event()


def capture_loop():
    """Grabs frames as fast as FRAME_FPS allows and stores only the latest one."""
    global latest_frame
    delay = 1.0 / FRAME_FPS
    while True:
        start = time.time()
        try:
            buf = io.BytesIO()
            camera.capture_file(buf, format="jpeg")
            with frame_lock:
                latest_frame = buf.getvalue()
            new_frame_event.set()
        except Exception as e:
            print("[CAPTURE]", e)
        elapsed = time.time() - start
        time.sleep(max(0.0, delay - elapsed))


def upload_loop():
    """Sends whatever the newest frame is. Never queues old frames."""
    while True:
        new_frame_event.wait()          # block until a fresh frame exists
        new_frame_event.clear()
        with frame_lock:
            frame = latest_frame
        if frame is None:
            continue
        try:
            frame_session.post(
                SERVER_URL + "/api/upload_frame",
                data=frame,
                headers={"Content-Type": "image/jpeg"},
                timeout=FRAME_TIMEOUT,
            )
        except Exception as e:
            print("[UPLOAD]", e)
            # No sleep/backoff here — a slow or failed upload just means we
            # move on to whatever the next latest frame is.


def sensor_loop():
    while True:
        loop_start = time.time()
        try:
            temp = dht.temperature
            hum = dht.humidity
        except Exception:
            temp = None
            hum = None

        motion = bool(motion_sensor.value)
        sound = bool(sound_sensor.value)
        wet = water_sensor.value
        wetness = (wet == 1) if WETNESS_ACTIVE_HIGH else (wet == 0)

        payload = {
            "temperature": round(temp, 1) if temp is not None else 0,
            "humidity": round(hum, 1) if hum is not None else 0,
            "motion_detected": motion,
            "sound_level": 1 if sound else 0,
            "wetness_detected": wetness,
        }

        try:
            r = sensor_session.post(
                SERVER_URL + "/api/ingest",
                json=payload,
                timeout=SENSOR_TIMEOUT,
            )
            print(datetime.now().strftime("%H:%M:%S"), payload, "HTTP", r.status_code)
        except Exception as e:
            print("[SENSOR]", e)

        elapsed = time.time() - loop_start
        time.sleep(max(0.0, SENSOR_INTERVAL - elapsed))


if __name__ == "__main__":
    print("=" * 60)
    print("Raspberry Pi Client")
    print("Server:", SERVER_URL)
    print("=" * 60)

    threading.Thread(target=capture_loop, daemon=True).start()
    threading.Thread(target=upload_loop, daemon=True).start()
    threading.Thread(target=sensor_loop, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        camera.stop()
        dht.exit()
        print("Stopped.")