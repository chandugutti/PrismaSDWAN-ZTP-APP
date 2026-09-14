#!/usr/bin/env python3
"""ION Field Installer — Flask backend."""
import sys
import os
import uuid
import json
import threading
import datetime as _dt
from datetime import datetime, timezone
from flask import Flask, request, jsonify, render_template

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from prismasase_settings import PRISMASASE_CLIENT_ID, PRISMASASE_CLIENT_SECRET, PRISMASASE_TSG_ID
except ImportError:
    PRISMASASE_CLIENT_ID = None
    PRISMASASE_CLIENT_SECRET = None
    PRISMASASE_TSG_ID = None

# Env-var overrides (cloud hosting like Render)
PRISMASASE_CLIENT_ID = os.getenv("PRISMASASE_CLIENT_ID") or PRISMASASE_CLIENT_ID
PRISMASASE_CLIENT_SECRET = os.getenv("PRISMASASE_CLIENT_SECRET") or PRISMASASE_CLIENT_SECRET
PRISMASASE_TSG_ID = os.getenv("PRISMASASE_TSG_ID") or PRISMASASE_TSG_ID

try:
    import prisma_sase as sdk_module
except ImportError:
    import cloudgenix as sdk_module

app = Flask(__name__)

_sdk = None
_sdk_lock = threading.Lock()

_jobs = {}
_jobs_lock = threading.Lock()

_poller_thread = None
_stop_event = threading.Event()

POLL_INTERVAL = 10  # seconds — fast polling for field use
JOBS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jobs.json")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_jobs():
    with _jobs_lock:
        data = list(_jobs.values())
    try:
        with open(JOBS_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


def load_jobs():
    global _jobs
    if not os.path.exists(JOBS_FILE):
        return
    try:
        with open(JOBS_FILE) as f:
            data = json.load(f)
        with _jobs_lock:
            for j in data:
                _jobs[j["id"]] = j
    except Exception:
        pass


# ---------------------------------------------------------------------------
# SDK helpers
# ---------------------------------------------------------------------------

def get_sdk():
    global _sdk
    with _sdk_lock:
        if _sdk is None:
            _sdk = sdk_module.API()
            if not hasattr(_sdk, "jwt_expires_at"):
                _sdk.jwt_expires_at = _dt.datetime.now() + _dt.timedelta(hours=1)
            _sdk.set_debug(0)
            _sdk.interactive.login_secret(
                client_id=PRISMASASE_CLIENT_ID,
                client_secret=PRISMASASE_CLIENT_SECRET,
                tsg_id=PRISMASASE_TSG_ID,
            )
    return _sdk


def reset_sdk():
    global _sdk
    with _sdk_lock:
        _sdk = None


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def fetch_sites(sdk):
    resp = sdk.get.sites()
    return {s["id"]: s["name"] for s in resp.cgx_content.get("items", [])}


def fetch_all_machines(sdk):
    resp = sdk.get.machines()
    return resp.cgx_content.get("items", [])


def find_machine_by_serial(sdk, serial):
    serial = serial.strip().lower()
    for m in fetch_all_machines(sdk):
        if (m.get("serial_number") or "").strip().lower() == serial:
            return m
    return None


def fetch_element_shells(sdk, site_map):
    shells = []
    for site_id, site_name in site_map.items():
        resp = sdk.get.elementshells(site_id)
        for e in resp.cgx_content.get("items", []):
            shells.append({
                "id": e["id"],
                "name": e.get("name") or e["id"],
                "site_id": site_id,
                "site_name": site_name,
                "model": e.get("model_name", ""),
                "state": e.get("state", ""),
            })
    shells.sort(key=lambda x: (x["site_name"].lower(), x["name"].lower()))
    return shells


def check_machine_connected(sdk, machine_id):
    for m in fetch_all_machines(sdk):
        if m["id"] == machine_id:
            return bool(m.get("connected")), m.get("element_id")
    return False, None


def claim_machine_to_element(sdk, machine_id, element_id):
    resp = sdk.post.machines_allocate_to_shell(machine_id, {"element_shell_id": element_id})
    if not resp:
        return False, f"API error: {getattr(resp, 'cgx_content', str(resp))}"
    return True, "Device successfully assigned"


# ---------------------------------------------------------------------------
# Background poller
# ---------------------------------------------------------------------------

def _poll_once():
    with _jobs_lock:
        active = [dict(j) for j in _jobs.values()
                  if j["status"] in ("searching", "waiting_online", "assigning")]

    if not active:
        return

    try:
        sdk = get_sdk()
    except Exception:
        return

    now = datetime.now(timezone.utc).isoformat()

    for job in active:
        try:
            if job["status"] == "searching":
                machine = find_machine_by_serial(sdk, job["serial_number"])
                if machine:
                    _update_job(job["id"],
                                machine_id=machine["id"],
                                machine_name=machine.get("name") or machine.get("serial_number") or machine["id"],
                                machine_model=machine.get("model_name", ""),
                                status="waiting_online",
                                step=2,
                                message="Device registered. Waiting for it to connect to the network…",
                                last_checked=now)
                else:
                    _update_job(job["id"], message="Searching for device in the system…", last_checked=now)

            elif job["status"] == "waiting_online":
                connected, existing_element_id = check_machine_connected(sdk, job["machine_id"])
                if existing_element_id:
                    _update_job(job["id"], status="assigned", step=4,
                                message="Device was already assigned.",
                                assigned_at=now, last_checked=now)
                elif connected:
                    _update_job(job["id"], status="assigning", step=3,
                                message="Device is online! Assigning to site…",
                                last_checked=now)
                else:
                    _update_job(job["id"], message="Waiting for device to connect to the network…", last_checked=now)

            elif job["status"] == "assigning":
                success, msg = claim_machine_to_element(sdk, job["machine_id"], job["element_id"])
                if success:
                    _update_job(job["id"], status="assigned", step=4,
                                message="Installation complete! Device is now active.",
                                assigned_at=now, last_checked=now)
                else:
                    _update_job(job["id"], status="failed",
                                message=msg, last_checked=now)

        except Exception as e:
            _update_job(job["id"], message=f"Error: {e}", last_checked=now)

    save_jobs()


def _update_job(job_id, **kwargs):
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id].update(kwargs)


def poller_loop():
    while not _stop_event.is_set():
        try:
            _poll_once()
        except Exception:
            pass
        _stop_event.wait(POLL_INTERVAL)


def start_poller():
    global _poller_thread
    if _poller_thread is None or not _poller_thread.is_alive():
        _stop_event.clear()
        _poller_thread = threading.Thread(target=poller_loop, daemon=True, name="job-poller")
        _poller_thread.start()


# ---------------------------------------------------------------------------
# Field tech routes
# ---------------------------------------------------------------------------

@app.route("/")
def field_home():
    return render_template("field.html")


@app.route("/api/find-job", methods=["POST"])
def find_job():
    serial = (request.get_json(force=True).get("serial") or "").strip()
    if not serial:
        return jsonify({"error": "Serial number required"}), 400

    with _jobs_lock:
        for j in _jobs.values():
            if (j.get("serial_number") or "").lower() == serial.lower():
                if j["status"] == "assigned":
                    return jsonify({"error": "This device has already been installed.", "done": True}), 409
                if j["status"] not in ("failed", "cancelled"):
                    return jsonify({"job": dict(j)})

    return jsonify({"error": "No installation job found for this serial number. Contact your supervisor or set up manually below."}), 404


@app.route("/api/jobs/<job_id>/start", methods=["POST"])
def start_job(job_id):
    with _jobs_lock:
        if job_id not in _jobs:
            return jsonify({"error": "Job not found"}), 404
        job = _jobs[job_id]
        if job["status"] != "pending":
            return jsonify({"error": f"Job is already {job['status']}"}), 400
        job.update(
            status="searching",
            step=1,
            started_at=datetime.now(timezone.utc).isoformat(),
            message="Searching for device in the system…",
        )

    save_jobs()
    start_poller()

    with _jobs_lock:
        return jsonify({"ok": True, "job": dict(_jobs[job_id])})


@app.route("/api/jobs/<job_id>", methods=["GET"])
def get_job(job_id):
    with _jobs_lock:
        if job_id not in _jobs:
            return jsonify({"error": "Job not found"}), 404
        return jsonify({"job": dict(_jobs[job_id])})


# ---------------------------------------------------------------------------
# Admin routes
# ---------------------------------------------------------------------------

@app.route("/admin")
def admin_home():
    return render_template("admin.html")


@app.route("/api/jobs", methods=["GET"])
def list_jobs():
    with _jobs_lock:
        data = list(_jobs.values())
    data.sort(key=lambda j: j.get("created_at", ""), reverse=True)
    return jsonify({"jobs": data})


@app.route("/api/jobs", methods=["POST"])
def create_job():
    data = request.get_json(force=True)
    serial = (data.get("serial_number") or "").strip()
    element_id = data.get("element_id")
    element_name = data.get("element_name") or element_id
    site_id = data.get("site_id")
    site_name = data.get("site_name") or "Unknown"
    element_model = (data.get("element_model") or "").strip()

    if not all([serial, element_id, site_id]):
        return jsonify({"error": "serial_number, element_id, and site_id are required"}), 400

    with _jobs_lock:
        for j in _jobs.values():
            if (j.get("serial_number") or "").lower() == serial.lower() and \
               j["status"] not in ("assigned", "failed", "cancelled"):
                return jsonify({"error": f"An active job already exists for serial {serial}"}), 409

    job = {
        "id": str(uuid.uuid4()),
        "serial_number": serial,
        "machine_id": None,
        "machine_name": None,
        "machine_model": None,
        "element_id": element_id,
        "element_name": element_name,
        "element_model": element_model,
        "site_id": site_id,
        "site_name": site_name,
        "status": "pending",
        "step": 0,
        "message": "Ready for field installation",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "started_at": None,
        "assigned_at": None,
        "last_checked": None,
    }

    with _jobs_lock:
        _jobs[job["id"]] = job
    save_jobs()
    return jsonify({"ok": True, "job": job})


@app.route("/api/jobs/<job_id>", methods=["DELETE"])
def delete_job(job_id):
    with _jobs_lock:
        if job_id not in _jobs:
            return jsonify({"error": "Job not found"}), 404
        del _jobs[job_id]
    save_jobs()
    return jsonify({"ok": True})


@app.route("/api/shells")
def get_shells():
    try:
        sdk = get_sdk()
        site_map = fetch_sites(sdk)
        shells = fetch_element_shells(sdk, site_map)
        return jsonify({"shells": shells})
    except Exception as e:
        reset_sdk()
        return jsonify({"error": str(e)}), 500


@app.route("/api/machines")
def get_machines():
    try:
        sdk = get_sdk()
        machines = []
        for m in fetch_all_machines(sdk):
            state = (m.get("machine_state") or "").lower()
            if state == "claimed" or m.get("claimed") or m.get("element_id"):
                continue
            machines.append({
                "id": m["id"],
                "serial": m.get("serial_number", ""),
                "model": m.get("model_name", ""),
                "name": m.get("name") or m.get("serial_number") or m["id"],
                "connected": bool(m.get("connected")),
            })
        machines.sort(key=lambda x: x["name"].lower())
        return jsonify({"machines": machines})
    except Exception as e:
        reset_sdk()
        return jsonify({"error": str(e)}), 500


@app.route("/api/auth-status")
def auth_status():
    try:
        sdk = get_sdk()
        sdk.get.sites()
        tenant = getattr(sdk, "tenant_name", None) or getattr(sdk, "tenant_id", None) or "Connected"
        return jsonify({"ok": True, "tenant": tenant})
    except Exception as e:
        reset_sdk()
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/settings", methods=["GET"])
def get_settings():
    return jsonify({
        "client_id": PRISMASASE_CLIENT_ID or "",
        "client_secret": PRISMASASE_CLIENT_SECRET or "",
        "tsg_id": PRISMASASE_TSG_ID or "",
    })


@app.route("/api/settings", methods=["POST"])
def save_settings():
    global PRISMASASE_CLIENT_ID, PRISMASASE_CLIENT_SECRET, PRISMASASE_TSG_ID
    data = request.get_json(force=True)
    client_id = (data.get("client_id") or "").strip()
    client_secret = (data.get("client_secret") or "").strip()
    tsg_id = (data.get("tsg_id") or "").strip()

    if not all([client_id, client_secret, tsg_id]):
        return jsonify({"error": "All three fields are required"}), 400

    settings_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prismasase_settings.py")
    content = (
        "######################################################\n"
        "# Service Account\n"
        "######################################################\n"
        f'PRISMASASE_CLIENT_ID="{client_id}"\n'
        f'PRISMASASE_CLIENT_SECRET="{client_secret}"\n'
        f'PRISMASASE_TSG_ID="{tsg_id}"\n'
    )
    try:
        with open(settings_path, "w") as f:
            f.write(content)
    except Exception as e:
        return jsonify({"error": f"Could not write settings: {e}"}), 500

    PRISMASASE_CLIENT_ID = client_id
    PRISMASASE_CLIENT_SECRET = client_secret
    PRISMASASE_TSG_ID = tsg_id
    reset_sdk()
    return jsonify({"ok": True})


if __name__ == "__main__":
    load_jobs()
    start_poller()
    cert = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cert.crt")
    key = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cert.key")
    ssl_ctx = (cert, key) if os.path.exists(cert) else None
    app.run(host="0.0.0.0", port=5002, debug=True, ssl_context=ssl_ctx)
