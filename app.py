#!/usr/bin/env python3
"""ION Field Installer — Flask backend."""
import sys
import os
import re
import uuid
import json
import math
import logging
import threading
import datetime as _dt
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from flask import Flask, request, jsonify, render_template

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/tmp/ion-installer-app.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
_log = logging.getLogger("ion-installer")

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


class _PrefixMiddleware:
    """Strip /presentation/{user}/{slug} prefix injected by sasesensai platform."""
    _PATTERN = re.compile(r'^(/presentation/[^/]+/[^/]+)(/.*)$')

    def __init__(self, wsgi_app):
        self.wsgi_app = wsgi_app

    def __call__(self, environ, start_response):
        path = environ.get('PATH_INFO', '')
        m = self._PATTERN.match(path)
        if m:
            environ['SCRIPT_NAME'] = environ.get('SCRIPT_NAME', '') + m.group(1)
            environ['PATH_INFO'] = m.group(2)
        return self.wsgi_app(environ, start_response)


app.wsgi_app = _PrefixMiddleware(app.wsgi_app)

_sdk = None
_sdk_lock = threading.Lock()

_jobs = {}
_jobs_lock = threading.Lock()

_poller_thread = None
_stop_event = threading.Event()

POLL_INTERVAL = 10   # seconds — fast polling for field use
UPGRADE_TIMEOUT = 600  # seconds — max time to wait for software upgrade
FABRIC_TIMEOUT = 90    # seconds — max time to wait for SD-Fabric tunnels before declaring done
JOBS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jobs.json")
LOCATION_SETTINGS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "location_settings.json")
DEFAULT_LOCATION_SETTINGS = {
    "radius_km": 50,
    "use_test_location": False,
    "test_lat": None,
    "test_lon": None,
}

_geocode_cache = {}  # address_str → (lat, lon) or None


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


def load_location_settings():
    if not os.path.exists(LOCATION_SETTINGS_FILE):
        return dict(DEFAULT_LOCATION_SETTINGS)
    try:
        with open(LOCATION_SETTINGS_FILE) as f:
            saved = json.load(f)
        merged = dict(DEFAULT_LOCATION_SETTINGS)
        merged.update(saved)
        return merged
    except Exception:
        return dict(DEFAULT_LOCATION_SETTINGS)


def save_location_settings_to_file(settings):
    try:
        with open(LOCATION_SETTINGS_FILE, "w") as f:
            json.dump(settings, f, indent=2)
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

def _safe_items(resp, *extra_keys):
    """Extract a list from an SDK response regardless of envelope format."""
    raw = getattr(resp, "cgx_content", None)
    if raw is None:
        raw = resp
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("items",) + extra_keys + ("data", "results"):
            val = raw.get(key)
            if isinstance(val, list):
                return val
    return []


def fetch_sites(sdk):
    resp = sdk.get.sites()
    items = _safe_items(resp)
    return {s["id"]: s.get("name") or s.get("display_name") or s["id"]
            for s in items if isinstance(s, dict) and s.get("id")}


def fetch_all_machines(sdk):
    resp = sdk.get.machines()
    return _safe_items(resp)


def _normalize_serial(s):
    return re.sub(r'[^a-zA-Z0-9]', '', s).lower()


def find_machine_by_serial(sdk, serial):
    norm = _normalize_serial(serial)
    for m in fetch_all_machines(sdk):
        if _normalize_serial(m.get("serial_number") or "") == norm:
            return m
        if _normalize_serial(m.get("hw_id") or "") == norm:
            return m
    return None


def fetch_element_shells(sdk, site_map):
    # Build set of shell IDs that already have a physical machine allocated
    claimed_shell_ids = set()
    try:
        for m in fetch_all_machines(sdk):
            sid = m.get("element_shell_id")
            if sid:
                claimed_shell_ids.add(sid)
    except Exception:
        pass

    shells = []
    for site_id, site_name in site_map.items():
        resp = sdk.get.elementshells(site_id)
        for e in _safe_items(resp):
            if not isinstance(e, dict) or not e.get("id"):
                continue
            if e["id"] in claimed_shell_ids:
                continue  # physical device already allocated here
            shells.append({
                "id": e["id"],
                "name": e.get("name") or e["id"],
                "site_id": site_id,
                "site_name": site_name,
                "model": e.get("model_name", ""),
                "state": e.get("state", ""),
                "element_id": e.get("element_id"),
            })
    shells.sort(key=lambda x: (x["site_name"].lower(), x["name"].lower()))
    return shells


def get_elementshell(sdk, site_id, shell_id):
    # Single-item GET returns 404 for some shells; list and filter instead
    resp = sdk.get.elementshells(site_id)
    ok = getattr(resp, "cgx_status", False)
    if not ok:
        _log.warning(f"get_elementshell list for site {site_id}: status={getattr(resp,'status_code',None)} ok={ok}")
        return None
    for sh in _safe_items(resp):
        if sh.get("id") == shell_id:
            _log.info(f"get_elementshell found: id={shell_id} model={sh.get('model_name')!r} state={sh.get('state')!r}")
            return sh
    _log.warning(f"get_elementshell: shell {shell_id} not found in site {site_id} listing")
    return None


def check_machine_connected(sdk, machine_id):
    for m in fetch_all_machines(sdk):
        if m["id"] == machine_id:
            m_state = (m.get("machine_state") or "").lower()
            # element_shell_id set or machine_state=claimed means device is already allocated
            already_allocated = m.get("element_shell_id") or m_state == "claimed"
            return bool(m.get("connected")), already_allocated or None
    return False, None


def get_machine_by_id(sdk, machine_id):
    for m in fetch_all_machines(sdk):
        if m.get("id") == machine_id:
            return m
    return None


def claim_machine_to_element(sdk, machine_id, element_id):
    resp = sdk.post.machines_allocate_to_shell(machine_id, {"element_shell_id": element_id})
    ok = getattr(resp, "cgx_status", False)
    status_code = getattr(resp, "status_code", None)
    content = getattr(resp, "cgx_content", {})
    _log.info(f"machines_allocate_to_shell: status={status_code} ok={ok} content={content}")
    if not ok:
        return False, f"Claim failed (HTTP {status_code}): {content}"

    # Verify the allocation actually stuck — API sometimes returns 200 without binding
    import time
    time.sleep(2)
    machine = get_machine_by_id(sdk, machine_id)
    if machine:
        shell_id_on_machine = machine.get("element_shell_id")
        m_state = (machine.get("machine_state") or "").lower()
        _log.info(f"post-allocate check: machine element_shell_id={shell_id_on_machine!r} machine_state={m_state!r}")
        if not shell_id_on_machine and m_state not in ("allocated", "claimed"):
            return False, "Claim API returned 200 but binding did not persist — possible model mismatch between device and shell"
    return True, "Claim request sent"


def check_machine_provisioned(sdk, machine_id):
    """Return (provisioned: bool, state_description: str).

    Ground truth is the machine record:
    - element_shell_id set, element_id None → allocated to shell, provisioning in progress
    - element_id set + connected → fully provisioned
    """
    machine = get_machine_by_id(sdk, machine_id)
    if not machine:
        return False, "machine not found"

    m_state = (machine.get("machine_state") or "").lower()
    connected = bool(machine.get("connected"))
    element_id = machine.get("element_id")
    element_shell_id = machine.get("element_shell_id")

    _log.info(f"machine {machine_id}: machine_state={m_state!r} connected={connected} "
              f"element_id={element_id!r} element_shell_id={element_shell_id!r}")

    # Primary completion signal: machine_state transitions to 'claimed' after device provisions
    if m_state == "claimed":
        return True, "claimed"

    # Secondary: element_id populated also means fully provisioned
    if element_id:
        return True, "online"

    # Still in progress — allocation sent, device is configuring
    if element_shell_id:
        return False, "allocated to shell — device is downloading configuration"

    return False, f"state={m_state or 'unknown'} — waiting for controller to process allocation"


def get_software_upgrade_status(sdk, site_id, element_id):
    """Returns (is_done: bool, message: str)."""
    fn = getattr(sdk.get, "software_status", None)
    if not fn:
        return True, "Software status API not available — assuming current"
    try:
        resp = fn(site_id, element_id)
        ok = getattr(resp, "cgx_status", False)
        if not ok:
            return True, "Software status unavailable — assuming current"
        items = _safe_items(resp)
        if not items:
            return True, "No software upgrade in progress"
        for item in items:
            state = (item.get("upgrade_state") or item.get("state") or "").lower()
            if state in ("upgrading", "downloading", "in_progress", "pending"):
                pct = item.get("percentage") or item.get("progress") or ""
                pct_str = f" ({pct}%)" if pct else ""
                return False, f"Downloading and installing software update{pct_str}…"
        return True, "Software is current"
    except Exception as e:
        _log.warning(f"software_status error: {e}")
        return True, "Software status check skipped"


def _topology_links_query(sdk, site_id):
    """Returns list of link dicts from topology/links/query API."""
    body = {
        "or": {
            "source_site_id": {"in": [site_id]},
            "target_site_id": {"in": [site_id]}
        },
        "type": {"in": ["public-anynet", "private-anynet", "auto-sase", "internet-stub"]}
    }
    # Try known SDK method names first
    for method_name in ("topology_query", "topologylinks_query"):
        fn = getattr(sdk.post, method_name, None)
        if fn:
            try:
                resp = fn(body)
                items = _safe_items(resp)
                if items is not None:
                    return items
            except Exception:
                continue
    # Fall back to direct HTTP via SDK session
    try:
        controller = getattr(sdk, "controller", None) or getattr(sdk, "_controller", None) or ""
        if not controller:
            return []
        url = f"{controller}/sdwan/v2.0/api/topology/links/query"
        sess = getattr(sdk, "_session", None) or getattr(sdk, "session", None)
        if not sess:
            return []
        r = sess.post(url, json=body, timeout=30)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                return data.get("items", [])
            return data if isinstance(data, list) else []
    except Exception as e:
        _log.warning(f"topology_links_query HTTP fallback failed: {e}")
    return []


def get_network_status(sdk, site_id, element_id):
    """Return structured network status with three sub-checks.

    Returns dict:
      {
        "sdwan_tunnels": {"status": "up"|"init"|"none", "up": int, "total": int},
        "prisma_access":  {"configured": bool, "status": "up"|"init"|"none"},
        "lan":            {"status": "up"|"none", "up": int, "total": int},
        "all_up": bool,
        "message": str,
      }
    """
    result = {
        "sdwan_tunnels": {"status": "none", "up": 0, "total": 0},
        "prisma_access":  {"configured": False, "status": "none"},
        "lan":            {"status": "none", "up": 0, "total": 0},
        "all_up": False,
        "message": "",
    }

    # --- SD-WAN & Prisma Access via topology links ---
    links = _topology_links_query(sdk, site_id)
    anynet_links   = [l for l in links if l.get("type") in ("public-anynet", "private-anynet")]
    auto_sase_links = [l for l in links if l.get("type") == "auto-sase"]

    if anynet_links:
        up = [l for l in anynet_links if l.get("status") == "up"]
        result["sdwan_tunnels"] = {
            "status": "up" if up else "init",
            "up": len(up),
            "total": len(anynet_links),
        }
    # else: remains "none" — no SD-WAN tunnels configured; not a failure

    if auto_sase_links:
        pa_up = [l for l in auto_sase_links if l.get("status") == "up"]
        result["prisma_access"] = {
            "configured": True,
            "status": "up" if pa_up else "init",
        }

    # --- LAN connectivity via element interfaces ---
    try:
        iface_resp = sdk.get.elementinterfaces(site_id, element_id)
        ifaces = _safe_items(iface_resp)
        lan_ifaces = [i for i in ifaces if (i.get("if_type") or "").upper() == "LAN"]
        if lan_ifaces:
            # "operational_state" or "admin_state" may indicate up/down
            up_lan = [i for i in lan_ifaces if
                      (i.get("operational_state") or i.get("admin_state") or "").lower() in ("up", "enabled", "active")]
            result["lan"] = {
                "status": "up" if up_lan else "init",
                "up": len(up_lan),
                "total": len(lan_ifaces),
            }
    except Exception as e:
        _log.warning(f"get_network_status: elementinterfaces failed: {e}")

    # Determine all_up: SD-WAN must be up (or none configured); Prisma Access must be up
    # if configured; LAN is informational (don't block on it)
    sdwan_ok = result["sdwan_tunnels"]["status"] in ("up", "none")
    pa_ok    = not result["prisma_access"]["configured"] or result["prisma_access"]["status"] == "up"
    result["all_up"] = sdwan_ok and pa_ok

    # Build summary message
    parts = []
    st = result["sdwan_tunnels"]
    if st["status"] == "up":
        parts.append(f"SD-WAN: {st['up']}/{st['total']} tunnel(s) up")
    elif st["status"] == "init":
        parts.append(f"SD-WAN: {st['up']}/{st['total']} tunnel(s) up (initializing)")
    else:
        parts.append("SD-WAN: no tunnels configured")

    pa = result["prisma_access"]
    if pa["configured"]:
        parts.append(f"Prisma Access: {'up' if pa['status'] == 'up' else 'initializing'}")
    else:
        parts.append("Prisma Access: not configured")

    lan = result["lan"]
    if lan["status"] == "up":
        parts.append(f"LAN: {lan['up']}/{lan['total']} port(s) up")
    elif lan["status"] == "init":
        parts.append(f"LAN: {lan['up']}/{lan['total']} port(s) up")
    else:
        parts.append("LAN: no LAN ports found")

    result["message"] = " · ".join(parts)
    return result


def get_port_config(sdk, site_id, element_id):
    """Return WAN and LAN port info for a device.

    Returns {"wan_ports": [...], "lan_ports": [...]}
    Each WAN port: {"name": str, "circuit": str, "description": str}
    Each LAN port: {"name": str, "description": str}
    """
    # Build waninterface_id → circuit name map
    wan_name_map = {}
    try:
        wi_resp = sdk.get.waninterfaces(site_id)
        for wi in _safe_items(wi_resp):
            wid = wi.get("id") or wi.get("waninterface_id")
            name = wi.get("name") or wi.get("label") or wid
            if wid:
                wan_name_map[wid] = name
    except Exception as e:
        _log.warning(f"get_port_config: waninterfaces failed: {e}")

    wan_ports = []
    lan_ports = []
    try:
        iface_resp = sdk.get.elementinterfaces(site_id, element_id)
        for iface in _safe_items(iface_resp):
            if_type = (iface.get("if_type") or "").upper()
            if if_type == "MANAGEMENT":
                continue
            port_name = iface.get("name") or iface.get("if_name") or "?"
            description = iface.get("description") or ""
            if if_type == "WAN":
                wan_cfg = iface.get("wan_config") or {}
                wid = wan_cfg.get("waninterface_id")
                circuit = wan_name_map.get(wid, "") if wid else ""
                wan_ports.append({"name": port_name, "circuit": circuit, "description": description})
            else:
                lan_ports.append({"name": port_name, "description": description})
    except Exception as e:
        _log.warning(f"get_port_config: elementinterfaces failed: {e}")

    return {"wan_ports": wan_ports, "lan_ports": lan_ports}


# ---------------------------------------------------------------------------
# Location helpers
# ---------------------------------------------------------------------------

def haversine(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two lat/lon points."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))




def geocode_address(address_str):
    """Return (lat, lon) for an address string via Nominatim, cached."""
    if address_str in _geocode_cache:
        return _geocode_cache[address_str]
    try:
        params = urllib.parse.urlencode({"q": address_str, "format": "json", "limit": 1})
        url = f"https://nominatim.openstreetmap.org/search?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "ION-Field-Installer/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            results = json.loads(r.read().decode())
        if results:
            result = (float(results[0]["lat"]), float(results[0]["lon"]))
        else:
            result = None
    except Exception as e:
        _log.warning(f"geocode_address({address_str!r}): {e}")
        result = None
    _geocode_cache[address_str] = result
    return result


def get_site_coords(site):
    """Return (lat, lon) for a site dict, or None if unavailable."""
    loc = site.get("location") or {}
    if isinstance(loc, dict):
        try:
            lat = float(loc.get("latitude") or loc.get("lat") or 0)
            lon = float(loc.get("longitude") or loc.get("lon") or 0)
            if lat != 0 and lon != 0:
                return lat, lon
        except (TypeError, ValueError):
            pass

    # Fall back to geocoding the address
    addr = site.get("address") or {}
    if isinstance(addr, dict):
        parts = [addr.get("city"), addr.get("state"), addr.get("post_code"), addr.get("country")]
        addr_str = ", ".join(p for p in parts if p)
        if addr_str:
            return geocode_address(addr_str)

    # Try address as plain string
    if isinstance(site.get("address"), str) and site["address"].strip():
        return geocode_address(site["address"])

    return None


def fetch_sites_with_location(sdk):
    """Return a list of full site dicts."""
    resp = sdk.get.sites()
    return [s for s in _safe_items(resp) if isinstance(s, dict) and s.get("id")]


def get_available_shells_for_site(sdk, site_id):
    """Return list of element shell dicts for a site that have no physical machine allocated.

    In Prisma SASE, every shell has element_id set (it's the logical element record the
    shell maps to) — that field does NOT indicate a physical device is present.
    A shell is unavailable only when a machine's element_shell_id points to it.
    """
    resp = sdk.get.elementshells(site_id)
    shells = []
    for sh in _safe_items(resp):
        if not isinstance(sh, dict) or not sh.get("id"):
            continue
        shells.append(sh)

    if not shells:
        return shells

    # Build set of shell IDs that already have a machine allocated to them
    claimed_shell_ids = set()
    try:
        for m in fetch_all_machines(sdk):
            sid = m.get("element_shell_id")
            if sid:
                claimed_shell_ids.add(sid)
    except Exception:
        pass  # if we can't fetch machines, err on the side of showing shells

    return [sh for sh in shells if sh["id"] not in claimed_shell_ids]


def find_nearby_sites(sdk, lat, lon, radius_km):
    """Return (nearby_list, all_with_distance_list).

    nearby_list: sites within radius_km that have at least one available shell,
                 each dict has: id, name, address_str, distance_km, available_shells (count), shell_id (first available)
    all_with_distance_list: all geolocatable sites sorted by distance (no shell info)
    """
    sites = fetch_sites_with_location(sdk)
    with_distance = []
    for s in sites:
        coords = get_site_coords(s)
        if not coords:
            continue
        slat, slon = coords
        dist = haversine(lat, lon, slat, slon)
        addr = s.get("address") or {}
        if isinstance(addr, dict):
            addr_str = ", ".join(p for p in [addr.get("city"), addr.get("state"), addr.get("country")] if p)
        else:
            addr_str = str(addr) if addr else ""
        with_distance.append({
            "id": s["id"],
            "name": s.get("name") or s.get("display_name") or s["id"],
            "address_str": addr_str,
            "distance_km": round(dist, 1),
            "available_shells": None,
            "shell_id": None,
            "shell_name": None,
        })
    with_distance.sort(key=lambda x: x["distance_km"])

    nearby = []
    for entry in with_distance:
        if entry["distance_km"] > radius_km:
            continue
        shells = get_available_shells_for_site(sdk, entry["id"])
        if not shells:
            continue
        first = shells[0]
        entry = dict(entry)
        entry["available_shells"] = len(shells)
        entry["shell_id"] = first["id"]
        entry["shell_name"] = first.get("name") or first["id"]
        nearby.append(entry)

    return nearby, with_distance


# ---------------------------------------------------------------------------
# Background poller
# ---------------------------------------------------------------------------

def _poll_once():
    with _jobs_lock:
        active = [dict(j) for j in _jobs.values()
                  if j["status"] in ("searching", "waiting_online", "assigning",
                                     "provisioning", "upgrading", "fabric_check")]

    if not active:
        return

    try:
        sdk = get_sdk()
    except Exception:
        return

    now = datetime.now(timezone.utc).isoformat()

    for job in active:
        jid = job["id"][:8]
        try:
            if job["status"] == "searching":
                machine = find_machine_by_serial(sdk, job["serial_number"])
                if machine:
                    machine_state = (machine.get("machine_state") or "").lower()
                    unclaimed = not machine.get("element_id") and machine_state != "claimed"
                    _log.info(f"[{jid}] searching→waiting_online machine={machine['id']} state={machine_state}")
                    _update_job(job["id"],
                                machine_id=machine["id"],
                                machine_name=machine.get("name") or machine.get("serial_number") or machine["id"],
                                machine_model=machine.get("model_name", ""),
                                machine_unclaimed=unclaimed,
                                status="waiting_online",
                                step=2,
                                message="Device found — waiting for it to come online…",
                                last_checked=now)
                else:
                    _log.info(f"[{jid}] searching: no match yet for serial {job['serial_number']}")
                    _update_job(job["id"], message="Searching for device in the controller…", last_checked=now)

            elif job["status"] == "waiting_online":
                connected, existing_element_id = check_machine_connected(sdk, job["machine_id"])
                _log.info(f"[{jid}] waiting_online: connected={connected} existing_element_id={existing_element_id}")
                if existing_element_id:
                    _update_job(job["id"], status="provisioning", step=4,
                                message="Device already claimed — verifying provisioning…",
                                last_checked=now)
                elif connected:
                    _log.info(f"[{jid}] waiting_online→assigning")
                    _update_job(job["id"], status="assigning", step=3,
                                message="Device is online! Claiming and assigning to site…",
                                last_checked=now)
                else:
                    _update_job(job["id"], message="Device found but not yet online — waiting…", last_checked=now)

            elif job["status"] == "assigning":
                _log.info(f"[{jid}] assigning: machine={job['machine_id']} element={job['element_id']}")

                # Model compatibility check — prevents silent hang when models don't match
                machine = get_machine_by_id(sdk, job["machine_id"])
                shell = get_elementshell(sdk, job["site_id"], job["element_id"])
                if machine and shell:
                    machine_model = (machine.get("model_name") or "").lower().strip()
                    shell_model = (shell.get("model_name") or "").lower().strip()
                    _log.info(f"[{jid}] model check: machine={machine_model!r} shell={shell_model!r}")
                    if machine_model and shell_model and machine_model != shell_model:
                        error_msg = (
                            f"Model mismatch: device is '{machine.get('model_name')}' "
                            f"but the selected shell expects '{shell.get('model_name')}'. "
                            f"Please use a shell with model '{machine.get('model_name')}' "
                            f"or create one at this site."
                        )
                        _log.warning(f"[{jid}] {error_msg}")
                        _update_job(job["id"], status="failed", message=error_msg, last_checked=now)
                        continue

                success, msg = claim_machine_to_element(sdk, job["machine_id"], job["element_id"])
                _log.info(f"[{jid}] claim result: success={success} msg={msg}")
                if success:
                    _update_job(job["id"], status="provisioning", step=4,
                                message="Device claimed! Waiting for site provisioning to complete…",
                                last_checked=now)
                else:
                    _update_job(job["id"], status="failed",
                                message=msg, last_checked=now)

            elif job["status"] == "provisioning":
                machine_id = job.get("machine_id")
                _log.info(f"[{jid}] provisioning: checking machine {machine_id}")
                if not machine_id:
                    _update_job(job["id"], status="failed",
                                message="Machine ID missing — cannot verify provisioning.", last_checked=now)
                    continue

                machine = get_machine_by_id(sdk, machine_id)
                if machine:
                    m_state = (machine.get("machine_state") or "").lower()
                    shell_id_on_machine = machine.get("element_shell_id")

                    if m_state == "allocated" and not shell_id_on_machine:
                        # element_shell_id is null despite allocation being issued —
                        # binding failed. The controller would set element_shell_id within
                        # seconds if the claim was valid, so any poll showing null means failure.
                        # Try to get a specific reason; fall back to a generic error if shell gone.
                        error_msg = None
                        shell = get_elementshell(sdk, job.get("site_id"), job.get("element_id"))
                        if shell:
                            machine_model = (machine.get("model_name") or "").lower().strip()
                            shell_model = (shell.get("model_name") or "").lower().strip()
                            _log.info(f"[{jid}] hung-allocation check: machine={machine_model!r} shell={shell_model!r}")
                            if machine_model and shell_model and machine_model != shell_model:
                                error_msg = (
                                    f"Claim failed — model mismatch: device is "
                                    f"'{machine.get('model_name')}' but the selected shell expects "
                                    f"'{shell.get('model_name')}'. Delete this job and create a new one "
                                    f"using a shell with model '{machine.get('model_name')}'."
                                )
                        if not error_msg:
                            error_msg = (
                                f"Claim failed — device is in 'allocated' state but was never "
                                f"bound to the selected shell. This is usually caused by a model "
                                f"mismatch between the device ({machine.get('model_name', 'unknown')}) "
                                f"and the shell. Delete this job and create a new one using a shell "
                                f"whose model matches the device."
                            )
                        _log.warning(f"[{jid}] {error_msg}")
                        _update_job(job["id"], status="failed", message=error_msg, last_checked=now)
                        continue

                provisioned, state = check_machine_provisioned(sdk, machine_id)
                _log.info(f"[{jid}] provisioning check: provisioned={provisioned} state={state!r}")
                if provisioned:
                    # Prisma API often omits element_id from the machine record even when claimed.
                    # Fall back to job["element_id"] (the shell element ID stored at job creation).
                    machine_elem_id = (machine.get("element_id") if machine else None) or job.get("element_id")
                    _log.info(f"[{jid}] provisioning→upgrading (machine_element_id={machine_elem_id!r})")
                    _update_job(job["id"], status="upgrading", step=5,
                                message="Device provisioned — checking software version…",
                                upgrading_at=now, machine_element_id=machine_elem_id,
                                last_checked=now)
                elif state == "unclaimed":
                    _update_job(job["id"],
                                message="Waiting for controller to confirm device assignment…",
                                last_checked=now)
                else:
                    _update_job(job["id"],
                                message=f"Provisioning in progress ({state})…",
                                last_checked=now)

            elif job["status"] == "upgrading":
                elapsed_upgrading = 0
                if job.get("upgrading_at"):
                    try:
                        started = datetime.fromisoformat(job["upgrading_at"].replace("Z", "+00:00"))
                        elapsed_upgrading = (datetime.now(timezone.utc) - started).total_seconds()
                    except Exception:
                        pass

                machine_elem_id = job.get("machine_element_id")
                if not machine_elem_id:
                    machine = get_machine_by_id(sdk, job["machine_id"])
                    if machine:
                        # Prisma API may not expose element_id on the machine record even when
                        # fully claimed. Fall back to job["element_id"] (the shell element ID
                        # stored at job creation) so the software check can proceed.
                        machine_elem_id = machine.get("element_id") or job.get("element_id")
                        if machine_elem_id:
                            _update_job(job["id"], machine_element_id=machine_elem_id, last_checked=now)

                if not machine_elem_id:
                    if elapsed_upgrading > UPGRADE_TIMEOUT:
                        _log.info(f"[{jid}] upgrading→fabric_check (no element_id, timed out)")
                        _update_job(job["id"], status="fabric_check", step=6,
                                    fabric_check_at=now,
                                    message="Checking SD-Fabric connectivity…",
                                    last_checked=now)
                    else:
                        _update_job(job["id"],
                                    message="Waiting for element to be fully registered…",
                                    last_checked=now)
                    continue

                upgrade_done, upgrade_msg = get_software_upgrade_status(sdk, job["site_id"], machine_elem_id)
                _log.info(f"[{jid}] upgrading: done={upgrade_done} msg={upgrade_msg!r} elapsed={elapsed_upgrading:.0f}s")

                if upgrade_done:
                    _log.info(f"[{jid}] upgrading→fabric_check")
                    _update_job(job["id"], status="fabric_check", step=6,
                                fabric_check_at=now,
                                message="Software up to date — checking SD-Fabric connectivity…",
                                last_checked=now)
                elif elapsed_upgrading > UPGRADE_TIMEOUT:
                    _log.info(f"[{jid}] upgrading→fabric_check (timed out after {elapsed_upgrading:.0f}s)")
                    _update_job(job["id"], status="fabric_check", step=6,
                                fabric_check_at=now,
                                message="Software upgrade timed out — checking SD-Fabric connectivity…",
                                last_checked=now)
                else:
                    _update_job(job["id"], message=upgrade_msg, last_checked=now)

            elif job["status"] == "fabric_check":
                elapsed_fabric = 0
                if job.get("fabric_check_at"):
                    try:
                        started = datetime.fromisoformat(job["fabric_check_at"].replace("Z", "+00:00"))
                        elapsed_fabric = (datetime.now(timezone.utc) - started).total_seconds()
                    except Exception:
                        pass

                element_id_for_check = job.get("element_id")
                net_status = get_network_status(sdk, job["site_id"], element_id_for_check)
                _log.info(f"[{jid}] fabric_check: all_up={net_status['all_up']} "
                          f"elapsed={elapsed_fabric:.0f}s msg={net_status['message']!r}")

                if net_status["all_up"]:
                    _log.info(f"[{jid}] fabric_check→assigned")
                    _update_job(job["id"], status="assigned", step=6,
                                message=f"Installation complete! {net_status['message']}",
                                network_status=net_status,
                                assigned_at=now, last_checked=now)
                elif elapsed_fabric > FABRIC_TIMEOUT:
                    _log.info(f"[{jid}] fabric_check→assigned (timed out after {elapsed_fabric:.0f}s)")
                    _update_job(job["id"], status="assigned", step=6,
                                message="Installation complete! Some tunnels are still initializing and will come up automatically — device is online and assigned.",
                                network_status=net_status,
                                assigned_at=now, last_checked=now)
                else:
                    elapsed_str = f"{int(elapsed_fabric)}s" if elapsed_fabric < 60 else f"{int(elapsed_fabric/60)}m {int(elapsed_fabric%60)}s"
                    _update_job(job["id"], message=f"{net_status['message']} ({elapsed_str} elapsed)",
                                network_status=net_status, last_checked=now)

        except Exception as e:
            _log.exception(f"[{jid}] poll error: {e}")
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


@app.route("/api/jobs/<job_id>/ports", methods=["GET"])
def get_job_ports(job_id):
    with _jobs_lock:
        if job_id not in _jobs:
            return jsonify({"error": "Job not found"}), 404
        job = dict(_jobs[job_id])
    try:
        sdk = get_sdk()
        ports = get_port_config(sdk, job["site_id"], job["element_id"])
        return jsonify({"ok": True, "ports": ports})
    except Exception as e:
        reset_sdk()
        return jsonify({"ok": False, "error": str(e), "ports": {"wan_ports": [], "lan_ports": []}}), 200


@app.route("/api/jobs/<job_id>/recheck-network", methods=["POST"])
def recheck_network(job_id):
    with _jobs_lock:
        if job_id not in _jobs:
            return jsonify({"error": "Job not found"}), 404
        job = _jobs[job_id]
        if job["status"] not in ("assigned", "failed"):
            return jsonify({"error": f"Job is in '{job['status']}' — can only recheck from assigned or failed"}), 400
        now = datetime.now(timezone.utc).isoformat()
        job.update(
            status="fabric_check",
            step=6,
            fabric_check_at=now,
            message="Re-checking network connectivity…",
            network_status=None,
            last_checked=now,
        )
        result = dict(job)
    save_jobs()
    start_poller()
    return jsonify({"ok": True, "job": result})


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

    if not serial or not site_id:
        return jsonify({"error": "serial_number and site_id are required"}), 400

    # Auto-pick a shell if none provided
    if not element_id:
        try:
            sdk = get_sdk()
            shells = get_available_shells_for_site(sdk, site_id)
            if not shells:
                return jsonify({"error": "No available element shells at this site. Contact your network admin to create one."}), 422
            first = shells[0]
            element_id = first["id"]
            element_name = first.get("name") or first["id"]
            element_model = first.get("model_name", "")
        except Exception as e:
            reset_sdk()
            return jsonify({"error": f"Could not fetch shells for site: {e}"}), 500

    with _jobs_lock:
        for j in _jobs.values():
            if (j.get("serial_number") or "").lower() == serial.lower() and \
               j["status"] not in ("assigned", "failed", "cancelled"):
                return jsonify({
                    "error": f"An active job already exists for serial {serial}",
                    "conflict_job": dict(j),
                }), 409

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
        "upgrading_at": None,
        "fabric_check_at": None,
        "machine_element_id": None,
        "network_status": None,
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


@app.route("/api/debug")
def debug_shells():
    try:
        sdk = get_sdk()

        # SDK identity
        sdk_info = {}
        for attr in ("tenant_id", "tenant_name", "controller", "version", "client_login"):
            v = getattr(sdk, attr, None)
            if v is not None:
                sdk_info[attr] = str(v)

        # Probe sites
        sites_resp = sdk.get.sites()
        sites_raw = sites_resp.cgx_content
        sites_items = _safe_items(sites_resp)

        # Probe machines — baseline auth check
        machines_resp = sdk.get.machines()
        machines_items = _safe_items(machines_resp)

        # Probe tenants — discover hierarchy if available
        tenant_items = []
        try:
            tenants_resp = sdk.get.tenants()
            tenant_items = _safe_items(tenants_resp)
        except Exception:
            pass

        # Build plain-English diagnosis
        if sites_items:
            diagnosis = f"OK — {len(sites_items)} site(s) accessible."
        elif machines_items and not sites_items:
            tenant_hint = ""
            if tenant_items:
                names = ", ".join(t.get("name", t.get("id", "?")) for t in tenant_items[:5])
                tenant_hint = f" Child tenants visible: {names}."
            diagnosis = (
                "Sites are EMPTY but machines ARE accessible. "
                "Your TSG ID is likely pointing to an MSP/parent tenant that has no direct sites. "
                "Switch to the TSG ID of the child tenant where your sites are configured."
                + tenant_hint
            )
        elif not machines_items and not sites_items:
            diagnosis = (
                "Both sites and machines are empty. "
                "Either the service account lacks read permissions, or this tenant has no resources."
            )
        else:
            diagnosis = "Unexpected state — see raw data below."

        return jsonify({
            "diagnosis": diagnosis,
            "sdk": sdk_info,
            "sites": {
                "status_code": getattr(sites_resp, "status_code", None),
                "raw_type": type(sites_raw).__name__,
                "raw_keys": list(sites_raw.keys()) if isinstance(sites_raw, dict) else None,
                "item_count": len(sites_items),
                "first_item": sites_items[0] if sites_items else None,
                "full_raw": sites_raw,
            },
            "machines": {
                "status_code": getattr(machines_resp, "status_code", None),
                "item_count": len(machines_items),
                "samples": [{"id": m.get("id"), "serial": m.get("serial_number")}
                            for m in machines_items[:5]],
            },
            "tenants": {
                "item_count": len(tenant_items),
                "items": [{"id": t.get("id"), "name": t.get("name")} for t in tenant_items[:10]],
            },
        })
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


@app.route("/api/my-ip")
def my_ip():
    import urllib.request
    try:
        ip = urllib.request.urlopen("https://api.ipify.org", timeout=5).read().decode()
        return jsonify({"public_ip": ip})
    except Exception as e:
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


@app.route("/api/locate", methods=["POST"])
def locate_device():
    """Verify device is in controller, then find sites near the provided GPS coordinates."""
    body = request.get_json(force=True)
    serial = (body.get("serial") or "").strip()
    if not serial:
        return jsonify({"error": "serial is required"}), 400

    loc_settings = load_location_settings()

    # Determine lat/lon — test override wins; otherwise use coordinates from client (phone GPS)
    if loc_settings.get("use_test_location") and loc_settings.get("test_lat") is not None and loc_settings.get("test_lon") is not None:
        lat = float(loc_settings["test_lat"])
        lon = float(loc_settings["test_lon"])
        geo_source = "test_location"
        location_label = "Test Location"
    elif body.get("lat") is not None and body.get("lon") is not None:
        try:
            lat = float(body["lat"])
            lon = float(body["lon"])
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid lat/lon values."}), 400
        geo_source = "phone_gps"
        location_label = body.get("location_label", f"{lat:.4f}, {lon:.4f}")
    else:
        return jsonify({
            "error": "Location access is required. Please allow location permission on your device, or enable Test Location in Admin Settings.",
            "hint": "enable_location",
        }), 422

    # Verify device exists in controller
    try:
        sdk = get_sdk()
    except Exception as e:
        reset_sdk()
        return jsonify({"error": f"Cannot connect to controller: {e}"}), 500

    machine = find_machine_by_serial(sdk, serial)
    if not machine:
        return jsonify({"error": "Device not found in controller. Make sure the device is powered on and has reached the Prisma SASE controller."}), 404

    radius_km = float(loc_settings.get("radius_km") or 50)

    try:
        nearby, with_distance = find_nearby_sites(sdk, lat, lon, radius_km)
    except Exception as e:
        _log.exception(f"find_nearby_sites error: {e}")
        return jsonify({"error": f"Error fetching sites: {e}"}), 500

    return jsonify({
        "lat": lat,
        "lon": lon,
        "location_label": location_label,
        "geo_source": geo_source,
        "radius_km": radius_km,
        "machine_id": machine.get("id"),
        "machine_model": machine.get("model_name", ""),
        "nearby": nearby,
        "all_with_distance": with_distance[:20],
    })


@app.route("/api/location-settings", methods=["GET"])
def get_location_settings():
    return jsonify(load_location_settings())


@app.route("/api/location-settings", methods=["POST"])
def save_location_settings():
    data = request.get_json(force=True)
    settings = dict(DEFAULT_LOCATION_SETTINGS)
    if "radius_km" in data:
        try:
            settings["radius_km"] = max(1, float(data["radius_km"]))
        except (TypeError, ValueError):
            pass
    settings["use_test_location"] = bool(data.get("use_test_location"))
    for key in ("test_lat", "test_lon"):
        val = data.get(key)
        try:
            settings[key] = float(val) if val not in (None, "") else None
        except (TypeError, ValueError):
            settings[key] = None
    settings["test_ip"] = (data.get("test_ip") or "").strip() or None
    save_location_settings_to_file(settings)
    return jsonify({"ok": True, "settings": settings})


if __name__ == "__main__":
    load_jobs()
    start_poller()
    cert = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cert.crt")
    key = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cert.key")
    ssl_ctx = (cert, key) if os.path.exists(cert) else None
    app.run(host="0.0.0.0", port=5002, debug=True, ssl_context=ssl_ctx)
