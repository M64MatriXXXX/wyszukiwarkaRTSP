import cv2
import base64
import threading
import time
from flask import Flask, render_template
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config["SECRET_KEY"] = "rtsp-scanner-secret"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

active_streams = {}
active_scans = {}
stream_lock = threading.Lock()


@app.route("/")
def index():
    return render_template("index.html")


@socketio.on("start_scan")
def handle_scan(data):
    ip = data.get("ip", "").strip()
    port = data.get("port", "554").strip() or "554"
    username = data.get("username", "admin").strip() or "admin"
    passwords_raw = data.get("passwords", "")
    sid = data.get("sid")

    passwords = [p.strip() for p in passwords_raw.replace(",", "\n").splitlines() if p.strip()]

    if not ip or not passwords:
        emit("error", {"message": "Podaj IP oraz co najmniej jedno hasło."})
        return

    emit("scan_started", {"total": len(passwords)})
    active_scans[sid] = {"cancelled": False}

    def scan():
        found = False
        for i, password in enumerate(passwords):
            if active_scans.get(sid, {}).get("cancelled"):
                break

            url = f"rtsp://{username}:{password}@{ip}:{port}/stream1"
            socketio.emit("trying", {"index": i + 1, "total": len(passwords), "url": f"rtsp://{username}:***@{ip}:{port}/stream1", "password": password}, to=sid)

            cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)
            cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 4000)
            cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 4000)

            connected = False
            if cap.isOpened():
                ret, frame = cap.read()
                if ret and frame is not None:
                    connected = True

            if active_scans.get(sid, {}).get("cancelled"):
                cap.release()
                break

            if connected:
                socketio.emit("found", {
                    "password": password,
                    "url": f"rtsp://{username}:{password}@{ip}:{port}/stream1",
                    "display_url": f"rtsp://{username}:***@{ip}:{port}/stream1",
                }, to=sid)
                found = True
                start_streaming(cap, sid, ip, password)
                break
            else:
                cap.release()

            time.sleep(0.1)

        was_cancelled = active_scans.pop(sid, {}).get("cancelled", False)
        if not found:
            socketio.emit("scan_done", {"found": False, "cancelled": was_cancelled}, to=sid)

    thread = threading.Thread(target=scan, daemon=True)
    thread.start()


@socketio.on("cancel_scan")
def handle_cancel_scan(data):
    sid = data.get("sid")
    if sid in active_scans:
        active_scans[sid]["cancelled"] = True
    emit("scan_cancelled", {})


@socketio.on("stop_stream")
def handle_stop(data):
    sid = data.get("sid")
    stop_stream(sid)
    emit("stream_stopped", {})


def start_streaming(cap, sid, ip, password):
    with stream_lock:
        if sid in active_streams:
            active_streams[sid]["running"] = False
            time.sleep(0.3)
        active_streams[sid] = {"running": True, "cap": cap}

    def stream_loop():
        while True:
            with stream_lock:
                info = active_streams.get(sid)
            if not info or not info["running"]:
                break

            ret, frame = info["cap"].read()
            if not ret or frame is None:
                socketio.emit("stream_error", {"message": "Utracono połączenie ze streamem."}, to=sid)
                break

            frame_resized = cv2.resize(frame, (854, 480))
            _, buffer = cv2.imencode(".jpg", frame_resized, [cv2.IMWRITE_JPEG_QUALITY, 70])
            b64 = base64.b64encode(buffer).decode("utf-8")
            socketio.emit("frame", {"data": b64}, to=sid)
            time.sleep(0.033)

        with stream_lock:
            info = active_streams.get(sid)
            if info:
                info["cap"].release()
                active_streams.pop(sid, None)

    thread = threading.Thread(target=stream_loop, daemon=True)
    thread.start()


def stop_stream(sid):
    with stream_lock:
        info = active_streams.get(sid)
        if info:
            info["running"] = False


@socketio.on("disconnect")
def handle_disconnect():
    from flask import request
    stop_stream(request.sid)


if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
