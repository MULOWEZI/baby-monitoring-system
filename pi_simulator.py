#!/usr/bin/env python3
"""
PC Webcam Simulator that mirrors the Raspberry Pi client architecture.

Differences from Raspberry Pi version:
- Uses OpenCV webcam instead of Picamera2
- Generates simulated sensor values instead of GPIO sensors
"""

import threading
import time
import random
from datetime import datetime

import cv2
import requests

SERVER_URL = "http://127.0.0.1:5000"

FRAME_FPS = 15
SENSOR_INTERVAL = 1.0
FRAME_TIMEOUT = 2
SENSOR_TIMEOUT = 3

frame_session = requests.Session()
sensor_session = requests.Session()

camera = cv2.VideoCapture(0)
camera.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
camera.set(cv2.CAP_PROP_FPS, FRAME_FPS)

latest_frame = None
frame_lock = threading.Lock()
new_frame_event = threading.Event()


def capture_loop():
    global latest_frame
    delay = 1.0 / FRAME_FPS

    while True:
        start = time.time()

        ok, frame = camera.read()

        if ok:
            cv2.putText(
                frame,
                datetime.now().strftime("%H:%M:%S"),
                (10, 30),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.8,
                (0, 255, 0),
                2,
            )

            ok, jpeg = cv2.imencode(
                ".jpg",
                frame,
                [cv2.IMWRITE_JPEG_QUALITY, 75]
            )

            if ok:
                with frame_lock:
                    latest_frame = jpeg.tobytes()

                new_frame_event.set()
        else:
            print("[CAPTURE] Webcam read failed")

        elapsed = time.time() - start
        time.sleep(max(0.0, delay - elapsed))


def upload_loop():
    while True:
        new_frame_event.wait()
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


def sensor_loop():
    while True:
        start = time.time()

        payload = {
            "temperature": round(random.uniform(20, 30), 1),
            "humidity": round(random.uniform(40, 70), 1),
            "motion_detected": random.random() < 0.35,
            "sound_level": 1 if random.random() < 0.25 else 0,
            "wetness_detected": random.random() < 0.10,
        }

        try:
            r = sensor_session.post(
                SERVER_URL + "/api/ingest",
                json=payload,
                timeout=SENSOR_TIMEOUT,
            )

            print(
                datetime.now().strftime("%H:%M:%S"),
                payload,
                "HTTP",
                r.status_code,
            )

        except Exception as e:
            print("[SENSOR]", e)

        elapsed = time.time() - start
        time.sleep(max(0.0, SENSOR_INTERVAL - elapsed))


if __name__ == "__main__":
    print("=" * 60)
    print("PC Webcam Simulator (Raspberry Pi Client Architecture)")
    print("Server:", SERVER_URL)
    print("=" * 60)

    threading.Thread(target=capture_loop, daemon=True).start()
    threading.Thread(target=upload_loop, daemon=True).start()
    threading.Thread(target=sensor_loop, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        camera.release()
        cv2.destroyAllWindows()
        print("Stopped.")