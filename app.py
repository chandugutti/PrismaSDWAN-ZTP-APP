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
from concurrent.futures import ThreadPoolExecutor, wait as _futures_wait
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
SEARCHING_TIMEOUT = 1800        # 30 min — fail if serial never found in controller
WAITING_ONLINE_TIMEOUT = 1800   # 30 min — fail if device never comes online after found
PROVISIONING_TIMEOUT = 900      # 15 min — fail if provisioning never completes
UPGRADE_TIMEOUT = 600   # seconds — max time to wait for software upgrade
VERSION_CHECK_TIMEOUT = 300  # seconds — max time to confirm running version after upgrade
FABRIC_TIMEOUT = 90    # seconds — max time to wait for SDWAN Fabric tunnels before declaring done
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
    if not isinstance(raw, dict):
        try:
            raw = dict(raw)
        except Exception:
            return []
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
    """Returns (is_done: bool, message: str, from_ver: str, to_ver: str).

    Tries in order:
      1. GET /sdwan/v2.1/api/elements/{element_id}/software/status  (per-element)
      2. POST /sdwan/v2.1/api/software/status/query                 (tenant-wide, filtered)
      3. sdk.get.software_status(site_id, element_id)               (SDK fallback)
    """
    sess = getattr(sdk, "session", None)
    ctrl = (getattr(sdk, "controller", None) or "").rstrip("/")
    from_ver = to_ver = ""

    def _parse_items(items):
        """Return (is_done, message, from_ver, to_ver) from a list of status items."""
        nonlocal from_ver, to_ver
        for item in items:
            state = (item.get("upgrade_state") or item.get("state") or "").lower()
            fv = item.get("from_image_version") or item.get("from_version") or ""
            tv = item.get("to_image_version") or item.get("to_version") or item.get("image_version") or ""
            if fv: from_ver = fv
            if tv: to_ver = tv
            pct = item.get("percentage") or item.get("progress") or ""
            pct_str = f" ({pct}%)" if pct else ""
            ver_str = f"{from_ver} → {to_ver} — " if from_ver and to_ver else ""
            if state in ("upgrading", "downloading", "in_progress"):
                return False, f"{ver_str}Upgrade in progress{pct_str}…", from_ver, to_ver
            if state == "rebooting":
                return False, f"{ver_str}Device rebooting after upgrade…", from_ver, to_ver
            if state in ("pending", "scheduled"):
                # "pending"/"scheduled" means the controller has a scheduled upgrade record but
                # the device hasn't started downloading yet — effectively no active upgrade.
                # If an upgrade truly starts, the state moves to "downloading" immediately.
                done_msg = f"Running {to_ver}" if to_ver else "Software is current"
                return True, done_msg, from_ver, to_ver
            if state in ("complete", "success", "succeeded", "current"):
                done_msg = f"Running {to_ver}" if to_ver else "Software is current"
                return True, done_msg, from_ver, to_ver
        # Items present but no active upgrade state → done
        done_msg = f"Running {to_ver}" if to_ver else "Software is current"
        return True, done_msg, from_ver, to_ver

    # 1. Per-element REST endpoint (most authoritative)
    if sess and ctrl and element_id:
        try:
            url = f"{ctrl}/sdwan/v2.1/api/elements/{element_id}/software/status"
            r = sess.get(url, timeout=10)
            if r.status_code == 200:
                body = r.json()
                items = body.get("items", []) if isinstance(body, dict) else []
                if not items and isinstance(body, dict) and body:
                    items = [body]  # single-object response
                if items:
                    return _parse_items(items)
                return True, "No software upgrade in progress", from_ver, to_ver
        except Exception as e:
            _log.warning(f"software/status element endpoint failed: {e}")

    # 2. Tenant-wide query filtered to this element
    if sess and ctrl and element_id:
        try:
            url = f"{ctrl}/sdwan/v2.1/api/software/status/query"
            r = sess.post(url, json={"query": {"element_id": {"in": [element_id]}}}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                items = data.get("items", []) if isinstance(data, dict) else []
                if items:
                    return _parse_items(items)
                return True, "No software upgrade in progress", from_ver, to_ver
        except Exception as e:
            _log.warning(f"software/status query fallback failed: {e}")

    return True, "Software status unavailable — assuming current", "", ""


def get_current_software_version(sdk, element_id):
    """Returns (version: str, message: str) — confirms the version currently running on the element.

    Calls POST /sdwan/v2.1/api/software/current_status/query.
    """
    sess = getattr(sdk, "session", None)
    ctrl = (getattr(sdk, "controller", None) or "").rstrip("/")
    if not (sess and ctrl and element_id):
        return "", "Version check unavailable"
    try:
        url = f"{ctrl}/sdwan/v2.1/api/software/current_status/query"
        r = sess.post(url, json={"query": {"element_id": {"in": [element_id]}}}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            items = data.get("items", []) if isinstance(data, dict) else []
            for item in items:
                ver = (item.get("current_version") or item.get("version")
                       or item.get("image_version") or "")
                if ver:
                    return ver, f"Running {ver}"
    except Exception as e:
        _log.warning(f"current_status/query failed: {e}")
    return "", "Running version unavailable"


def get_element_operational_status(sdk, site_id, element_id):
    """Returns (is_online: bool, state_str: str) — calls elements/{id}/status.

    Tries v2.6 → v2.1 → v2.0 direct REST; falls back to machine connected check.
    """
    sess = getattr(sdk, "session", None)
    ctrl = (getattr(sdk, "controller", None) or "").rstrip("/")
    if sess and ctrl and element_id:
        for ver in ("v2.6", "v2.1", "v2.0"):
            try:
                url = f"{ctrl}/sdwan/{ver}/api/elements/{element_id}/status"
                r = sess.get(url, timeout=8)
                if r.status_code == 200:
                    body = r.json()
                    if isinstance(body, dict):
                        state = (body.get("elem_state") or body.get("state") or "").lower()
                        cloud = (body.get("cloud_state") or "").lower()
                        # "bound" = administrative pairing only, not operational connectivity.
                        # A device can be elem_state="bound" while Offline in the controller.
                        # Use cloud_state as the primary indicator of actual connectivity.
                        is_online = cloud in ("connected", "up") or state in ("online", "active", "up")
                        label = state or cloud or "unknown"
                        return is_online, label.title()
            except Exception as e:
                _log.warning(f"elements/{element_id}/status {ver} failed: {e}")
    return False, "Status unavailable"


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
        r = sess.post(url, json=body, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                return data.get("items", [])
            return data if isinstance(data, list) else []
    except Exception as e:
        _log.warning(f"topology_links_query HTTP fallback failed: {e}")
    return []


def get_network_status(sdk, site_id, element_id):
    """Return simplified network status: per-interface up/down + fabric rows.

    Returns dict:
      {
        "interfaces": [{"name": str, "role": str, "up": bool, "circuit": str}],
        "fabric": [
          {"name": "Data Center",   "connected": bool, "up": int, "count": int},
          {"name": "Prisma Access", "connected": bool, "up": int, "count": int},
        ],
        "all_up": bool,
        "message": str,
      }
    """
    sess = getattr(sdk, "session", None)
    ctrl = (getattr(sdk, "controller", None) or "").rstrip("/")

    def _rest(method, path, body=None):
        if not (sess and ctrl):
            return None
        try:
            if method == "GET":
                r = sess.get(f"{ctrl}{path}", timeout=8)
            else:
                r = sess.post(f"{ctrl}{path}", json=body or {}, timeout=8)
            if r.status_code == 200:
                return r.json()
            _log.debug(f"get_network_status: {method} {path} → {r.status_code}")
        except Exception as e:
            _log.warning(f"get_network_status: {method} {path} failed: {e}")
        return None

    interfaces = []

    # WAN interface label map — used to name circuits on interface chips
    wanlabel_map = {}
    try:
        for lbl in (_safe_items(sdk.get.waninterfacelabels()) or []):
            if lbl.get("id"):
                wanlabel_map[lbl["id"]] = lbl.get("name") or ""
    except Exception:
        pass

    wannetwork_map = {}
    try:
        for net in (_safe_items(sdk.get.wannetworks()) or []):
            if net.get("id"):
                wannetwork_map[net["id"]] = net.get("name") or ""
    except Exception:
        pass

    waniface_map = {}
    try:
        d = _rest("GET", f"/sdwan/v2.1/api/sites/{site_id}/waninterfaces")
        if d:
            for wi in d.get("items", []):
                if wi.get("id"):
                    waniface_map[wi["id"]] = wi
    except Exception:
        pass

    def _circuit_from_waniface(wid):
        wi = waniface_map.get(wid, {})
        name = wi.get("name") or ""
        if not name and wi.get("network_id"):
            name = wannetwork_map.get(wi["network_id"], "")
        if not name and wi.get("label_id"):
            name = wanlabel_map.get(wi["label_id"], "")
        return name

    # ------------------------------------------------------------------
    # 1. VPN links — SD-WAN fabric tunnels
    # ------------------------------------------------------------------
    vpn_up = vpn_total = 0
    vpn_links_raw = []

    # Try v2.0 vpnlinks/query
    vpn_data = _rest("POST", "/sdwan/v2.0/api/vpnlinks/query",
                     {"query": {"site_id": {"in": [site_id]}}})
    if vpn_data:
        vpn_links_raw = vpn_data.get("items", [])

    # Fallback: topology links query (already proven to work)
    if not vpn_links_raw:
        topo = _topology_links_query(sdk, site_id)
        for link in topo:
            if link.get("type") in ("public-anynet", "private-anynet"):
                vpn_links_raw.append({
                    "id": link.get("id"),
                    "source_site_id": link.get("source_site_id"),
                    "dest_site_id": link.get("target_site_id"),
                    "state": (link.get("status") or "").lower(),
                    "_topo": True,
                })

    def _fetch_vpn_link_status(link):
        link_id = link.get("id")
        actual = "down"
        if link_id and not link.get("_topo"):
            sd = _rest("GET", f"/sdwan/v2.2/api/vpnlinks/{link_id}/status")
            if sd:
                st = (sd.get("state") or "").lower()
                actual = "up" if st == "up" else "init" if st in ("init", "initializing", "pending") else "down"
        if actual == "down":
            raw = (link.get("state") or link.get("status") or "").lower()
            actual = "up" if raw == "up" else "init" if raw in ("init", "initializing") else "down"
        return actual

    # Resolve peer site IDs to names for display using SDK
    site_name_map = {}
    try:
        for s in (_safe_items(sdk.get.sites()) or []):
            if s.get("id"):
                site_name_map[s["id"]] = s.get("name") or s["id"]
    except Exception:
        pass

    vpn_total = len(vpn_links_raw)
    vpn_up_peers = []
    if vpn_links_raw:
        with ThreadPoolExecutor(max_workers=8) as _pool:
            futs = {_pool.submit(_fetch_vpn_link_status, lnk): lnk for lnk in vpn_links_raw}
            _futures_wait(futs, timeout=12)
            for fut, lnk in futs.items():
                try:
                    if fut.result() == "up":
                        vpn_up += 1
                        pid = lnk.get("dest_site_id") or lnk.get("target_site_id")
                        if pid and pid != site_id:
                            vpn_up_peers.append(site_name_map.get(pid, pid))
                except Exception:
                    pass
    # Deduplicate while preserving order
    seen = set()
    vpn_peers_unique = [p for p in vpn_up_peers if not (p in seen or seen.add(p))]

    # ------------------------------------------------------------------
    # 2. Prisma Access connections
    # ------------------------------------------------------------------
    pa_up = pa_total = 0
    pa_conns = []

    d = _rest("GET", f"/sdwan/v2.0/api/sites/{site_id}/prismasase_connections")
    if d:
        pa_conns = d.get("items", [])

    if not pa_conns:
        # Fallback: topology auto-sase links
        topo = _topology_links_query(sdk, site_id)
        for link in topo:
            if link.get("type") == "auto-sase":
                pa_conns.append({
                    "id": link.get("id"),
                    "name": "Prisma Access",
                    "_status": (link.get("status") or "").lower(),
                    "_topo": True,
                })

    def _fetch_pa_conn_status(conn):
        conn_id = conn.get("id")
        actual = "down"
        if conn_id and not conn.get("_topo"):
            sd = _rest("GET", f"/sdwan/v2.0/api/sites/{site_id}/prismasase_connections/{conn_id}/status")
            if sd:
                st = (sd.get("state") or "").lower()
                actual = ("up" if st in ("up", "active", "established")
                          else "init" if ("init" in st or "pend" in st)
                          else "down")
        if actual == "down":
            raw = conn.get("_status") or (conn.get("status") or "")
            actual = "up" if raw == "up" else "init" if raw in ("init", "initializing") else "down"
        return actual

    pa_total = len(pa_conns)
    if pa_conns:
        with ThreadPoolExecutor(max_workers=8) as _pool:
            futs = {_pool.submit(_fetch_pa_conn_status, c): c for c in pa_conns}
            _futures_wait(futs, timeout=12)
            for fut in futs:
                try:
                    if fut.result() == "up":
                        pa_up += 1
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # 3. Element interfaces — port config + per-interface physical status
    # ------------------------------------------------------------------
    wan_up = wan_total = 0
    lan_up = lan_total = 0
    ha_up  = ha_total  = 0

    iface_configs = []
    d = _rest("GET", f"/sdwan/v4.21/api/sites/{site_id}/elements/{element_id}/interfaces")
    if d:
        iface_configs = d.get("items", [])
    if not iface_configs:
        try:
            iface_resp = sdk.get.elementinterfaces(site_id, element_id)
            iface_configs = _safe_items(iface_resp) or []
        except Exception as e:
            _log.warning(f"get_network_status: elementinterfaces fallback failed: {e}")

    def _fetch_iface_result(iface):
        iface_id = iface.get("id")
        used_for = (iface.get("used_for") or iface.get("if_type") or "").lower()
        admin_up = (iface.get("admin_state") or "").lower() in ("up", "enabled", "active")

        if used_for in ("wan", "publicwan", "privatewan") or iface.get("site_wan_interface_ids"):
            role = "WAN"
        elif used_for == "lan":
            role = "LAN"
        elif used_for == "ha":
            role = "HA"
        else:
            return None

        actual = "down"
        if iface_id:
            sd = _rest("GET",
                       f"/sdwan/v3.9/api/sites/{site_id}/elements/{element_id}"
                       f"/interfaces/{iface_id}/status")
            if sd:
                st = (sd.get("operational_state") or sd.get("link_state")
                      or sd.get("state") or "").lower()
                actual = "up" if st in ("up", "enabled", "active", "connected", "established") else "down"
        if actual == "down":
            st = (iface.get("operational_state") or "").lower()
            actual = "up" if st in ("up", "active") else ("up" if admin_up else "down")

        circuit = ""
        if role == "WAN":
            for wid in (iface.get("site_wan_interface_ids") or []):
                circuit = _circuit_from_waniface(wid)
                if circuit:
                    break

        port_name = iface.get("name") or iface.get("display_name") or "?"
        return {"name": port_name, "role": role, "up": actual == "up", "circuit": circuit}

    if iface_configs:
        with ThreadPoolExecutor(max_workers=8) as _pool:
            futs = [_pool.submit(_fetch_iface_result, ifc) for ifc in iface_configs]
            _futures_wait(futs, timeout=15)
            for fut in futs:
                try:
                    rec = fut.result()
                except Exception:
                    rec = None
                if not rec:
                    continue
                interfaces.append(rec)
                role, is_up = rec["role"], rec["up"]
                if role == "WAN":
                    wan_total += 1
                    if is_up:
                        wan_up += 1
                elif role == "LAN":
                    lan_total += 1
                    if is_up:
                        lan_up += 1
                elif role == "HA":
                    ha_total += 1
                    if is_up:
                        ha_up += 1

    # ------------------------------------------------------------------
    # Fabric rows + all_up gate
    # ------------------------------------------------------------------
    fabric = [
        {
            "name":      "Data Center",
            "connected": vpn_up > 0,
            "up":        vpn_up,
            "count":     vpn_total,
            "peers":     vpn_peers_unique,
        },
        {
            "name":      "Prisma Access",
            "connected": pa_up > 0,
            "up":        pa_up,
            "count":     pa_total,
        },
    ]

    vpn_ok = vpn_total == 0 or vpn_up > 0
    pa_ok  = pa_total  == 0 or pa_up  > 0
    wan_ok = wan_total == 0 or wan_up > 0
    all_up = vpn_ok and pa_ok and wan_ok

    parts = []
    if vpn_total:
        parts.append(f"Data Center: {vpn_up}/{vpn_total} tunnel(s) up")
    if pa_total:
        parts.append(f"Prisma Access: {pa_up}/{pa_total} up")
    if wan_total:
        parts.append(f"WAN: {wan_up}/{wan_total} port(s) up")
    message = " · ".join(parts) if parts else "Network check complete"

    return {
        "interfaces": interfaces,
        "fabric":     fabric,
        "all_up":     all_up,
        "message":    message,
    }


def get_port_config(sdk, site_id, element_id):
    """Return port info sourced from the element shell.

    Returns {
      "wan_ports":     [{"name": str, "circuit": str, "description": str, "wan_type": str}],
      "lan_ports":     [{"name": str, "description": str}],
      "ha_ports":      [{"name": str, "description": str}],
      "bypass_pairs":  [{"name": str, "peer": str, "description": str}],
    }

    Circuit name resolution priority per WAN port:
      1. waninterface.name (admin-set label like "ISP1")
      2. wannetwork.name  (carrier name like "AT&T", "internet-comcast")
      3. waninterfacelabel.name (connection type like "Ethernet Internet")
    """
    sess = getattr(sdk, "session", None)
    ctrl = (getattr(sdk, "controller", None) or "").rstrip("/")

    # 1. Fetch shell interfaces via direct v2.2 REST (SDK uses v2.0 which returns 403)
    items = []
    if sess and ctrl:
        try:
            url = f"{ctrl}/sdwan/v2.2/api/sites/{site_id}/elementshells/{element_id}/interfaces"
            r = sess.get(url, timeout=10)
            if r.status_code == 200:
                body = r.json()
                items = body.get("items", []) if isinstance(body, dict) else []
                _log.info(f"get_port_config: v2.2 shell interfaces — {len(items)} items")
            else:
                _log.warning(f"get_port_config: v2.2 returned {r.status_code}")
        except Exception as e:
            _log.warning(f"get_port_config: direct v2.2 call failed: {e}")

    if not items:
        try:
            shell_resp = sdk.get.elementshells_interfaces(site_id, element_id)
            items = _safe_items(shell_resp)
        except Exception as e:
            _log.warning(f"get_port_config: elementshells_interfaces failed: {e}")

    if not items:
        return {"wan_ports": [], "lan_ports": [], "ha_ports": [], "bypass_pairs": []}

    # 2. Pre-fetch all WAN networks and labels in bulk (avoids N+1 per-ID calls)
    wannetwork_map = {}   # network_id → name (e.g. "AT&T", "internet-comcast")
    try:
        for net in _safe_items(sdk.get.wannetworks()):
            nid = net.get("id")
            if nid:
                wannetwork_map[nid] = net.get("name") or ""
    except Exception as e:
        _log.warning(f"get_port_config: wannetworks list failed: {e}")

    wanlabel_map = {}     # label_id → name (e.g. "Ethernet Internet", "GAP Cellular")
    try:
        for lbl in _safe_items(sdk.get.waninterfacelabels()):
            lid = lbl.get("id")
            if lid:
                wanlabel_map[lid] = lbl.get("name") or ""
    except Exception as e:
        _log.warning(f"get_port_config: waninterfacelabels list failed: {e}")

    # 3. Build waninterface_id → {circuit_name, wan_type} map via SDK (internally v2.10)
    #    Name resolution: waninterface.name → wannetwork.name → waninterfacelabel.name
    wan_name_map = {}
    try:
        for wi in _safe_items(sdk.get.waninterfaces(site_id)):
            wid = wi.get("id")
            if not wid:
                continue
            name = wi.get("name") or ""
            if not name:
                name = wannetwork_map.get(wi.get("network_id") or "", "")
            if not name:
                name = wanlabel_map.get(wi.get("label_id") or "", "")
            raw_type = wi.get("type") or ""
            wan_name_map[wid] = {
                "name": name,
                "type": raw_type,
            }
    except Exception as e:
        _log.warning(f"get_port_config: sdk.get.waninterfaces failed: {e}")

    # 4. Categorize ports — used_for is the authoritative signal
    def _fmt_wan_type(raw):
        return {"publicwan": "Public WAN", "privatewan": "Private WAN"}.get(raw.lower(), raw.replace("_", " ").title())

    # Build interface-ID → port-name map so bypass peer IDs can be resolved to human names.
    # bypass_pair.wan / bypass_pair.lan are internal DB IDs, not port labels.
    id_to_portname = {}
    for iface in items:
        iid = iface.get("id")
        if iid:
            id_to_portname[iid] = iface.get("name") or iface.get("if_name") or ""

    wan_ports, lan_ports, ha_ports, bypass_pairs = [], [], [], []
    seen_bypass = set()
    for iface in items:
        port_name = iface.get("name") or iface.get("if_name") or "?"
        description = iface.get("description") or ""
        used_for = (iface.get("used_for") or "").lower()

        if port_name.lower().startswith("controller"):
            continue  # management port — always skip
        if used_for == "none":
            continue  # unconfigured port — skip

        wan_ids = iface.get("site_wan_interface_ids") or []
        bypass = iface.get("bypass_pair")

        # Check bypass FIRST — bypass ports may also have wan_ids as an artifact,
        # but the bypass_pair dict is the authoritative signal they are inline bypass ports.
        if bypass and isinstance(bypass, dict):
            current_id = iface.get("id") or ""
            wan_id = bypass.get("wan") or ""
            lan_id = bypass.get("lan") or ""
            # Resolve peer ID to a port name (skip if same as current or unresolvable)
            peer_name = ""
            for cand_id in (wan_id, lan_id):
                if cand_id and cand_id != current_id:
                    cand_name = id_to_portname.get(cand_id, "")
                    if cand_name and cand_name != port_name:
                        peer_name = cand_name
                        break
            _log.info(f"get_port_config: bypass port={port_name!r} resolved_peer={peer_name!r}")

            # Normalize to actual port numbers for deduplication.
            # API may return "34" (combined) and/or separate "3"/"4" interfaces for the same pair.
            def _expand(nm):
                """Expand a combined port name like '34' into its individual ports ['3','4']."""
                if nm and nm.isdigit() and len(nm) > 1:
                    mid = len(nm) // 2 if len(nm) > 2 else 1
                    return [nm[:mid], nm[mid:]] if len(nm) == 2 else [nm[:mid], nm[mid:]]
                return [nm] if nm else []

            actual = sorted(set(_expand(port_name) + (_expand(peer_name) if peer_name else [])))
            key = tuple(actual)
            if key not in seen_bypass:
                seen_bypass.add(key)
                bypass_pairs.append({"name": port_name, "peer": peer_name, "description": description})
        elif wan_ids:
            circuits, wan_type = [], ""
            for wid in wan_ids:
                info = wan_name_map.get(wid, {})
                label = info.get("name", "")
                if label and not str(label).strip().isdigit():
                    circuits.append(label)
                wan_type = wan_type or info.get("type", "")
            circuit = ", ".join(circuits) if circuits else ""
            type_label = _fmt_wan_type(wan_type) if wan_type else used_for.replace("_", " ").title()
            wan_ports.append({"name": port_name, "circuit": circuit, "description": description, "wan_type": type_label})
        elif used_for == "ha":
            ha_ports.append({"name": port_name, "description": description})
        elif used_for == "lan":
            lan_ports.append({"name": port_name, "description": description})

    _log.info(
        f"get_port_config: {len(wan_ports)} WAN, {len(lan_ports)} LAN, "
        f"{len(ha_ports)} HA, {len(bypass_pairs)} bypass pairs"
    )
    return {"wan_ports": wan_ports, "lan_ports": lan_ports, "ha_ports": ha_ports, "bypass_pairs": bypass_pairs}


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
        entry["shell_element_id"] = first.get("element_id")  # actual element entity ID (may be None pre-provision)
        nearby.append(entry)

    return nearby, with_distance


# ---------------------------------------------------------------------------
# Background poller
# ---------------------------------------------------------------------------

def _poll_once():
    with _jobs_lock:
        active = [dict(j) for j in _jobs.values()
                  if j["status"] in ("searching", "waiting_online", "assigning",
                                     "provisioning", "upgrading", "version_check",
                                     "fabric_check")]

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
                # Timeout: fail if device never appears in the controller
                started_at = job.get("started_at") or now
                try:
                    elapsed_searching = (datetime.now(timezone.utc) -
                                         datetime.fromisoformat(started_at.replace("Z", "+00:00"))).total_seconds()
                except Exception:
                    elapsed_searching = 0
                if elapsed_searching > SEARCHING_TIMEOUT:
                    _log.warning(f"[{jid}] searching timed out after {elapsed_searching:.0f}s")
                    _update_job(job["id"], status="failed",
                                message=f"Device with serial '{job['serial_number']}' was not found in the controller after 30 minutes. Verify the serial number and that the device has internet access.",
                                last_checked=now)
                    continue

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
                                waiting_online_at=now,
                                message="Device found — waiting for it to come online…",
                                last_checked=now)
                else:
                    _log.info(f"[{jid}] searching: no match yet for serial {job['serial_number']}")
                    _update_job(job["id"], message="Searching for device in the controller…", last_checked=now)

            elif job["status"] == "waiting_online":
                waiting_online_at = job.get("waiting_online_at") or now
                try:
                    elapsed_waiting = (datetime.now(timezone.utc) -
                                       datetime.fromisoformat(waiting_online_at.replace("Z", "+00:00"))).total_seconds()
                except Exception:
                    elapsed_waiting = 0
                if elapsed_waiting > WAITING_ONLINE_TIMEOUT:
                    _log.warning(f"[{jid}] waiting_online timed out after {elapsed_waiting:.0f}s")
                    _update_job(job["id"], status="failed",
                                message="Device was found but did not come online within 30 minutes. Check the device power, WAN connectivity, and that it can reach the Prisma SD-WAN controller.",
                                last_checked=now)
                    continue

                connected, existing_element_id = check_machine_connected(sdk, job["machine_id"])
                _log.info(f"[{jid}] waiting_online: connected={connected} existing_element_id={existing_element_id}")
                if existing_element_id:
                    _update_job(job["id"], status="provisioning", step=4,
                                provisioning_at=now,
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

                # element_shell_id is the shell's own ID required by machines_allocate_to_shell;
                # element_id is the actual element entity ID used for monitoring (may differ).
                shell_id_to_claim = job.get("element_shell_id") or job.get("element_id")
                success, msg = claim_machine_to_element(sdk, job["machine_id"], shell_id_to_claim)
                _log.info(f"[{jid}] claim result: success={success} msg={msg}")
                if success:
                    _update_job(job["id"], status="provisioning", step=4,
                                provisioning_at=now,
                                message="Device claimed! Waiting for site provisioning to complete…",
                                last_checked=now)
                else:
                    _update_job(job["id"], status="failed",
                                message=msg, last_checked=now)

            elif job["status"] == "provisioning":
                provisioning_at = job.get("provisioning_at") or now
                try:
                    elapsed_provisioning = (datetime.now(timezone.utc) -
                                            datetime.fromisoformat(provisioning_at.replace("Z", "+00:00"))).total_seconds()
                except Exception:
                    elapsed_provisioning = 0
                if elapsed_provisioning > PROVISIONING_TIMEOUT:
                    _log.warning(f"[{jid}] provisioning timed out after {elapsed_provisioning:.0f}s")
                    _update_job(job["id"], status="failed",
                                message="Device did not complete provisioning within 15 minutes. Check the device status in the Prisma SD-WAN controller and try again.",
                                last_checked=now)
                    continue

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
                    # Prefer element_id from the machine record (set by controller after provisioning).
                    # Fall back to job["element_id"] which stores the shell's element entity ID
                    # (populated at job creation from elementshell.element_id — distinct from the
                    # shell's own ID stored in element_shell_id).
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
                _log.info(f"[{jid}] POLL: entering upgrading block")
                elapsed_upgrading = 0
                if job.get("upgrading_at"):
                    try:
                        started = datetime.fromisoformat(job["upgrading_at"].replace("Z", "+00:00"))
                        elapsed_upgrading = (datetime.now(timezone.utc) - started).total_seconds()
                    except Exception:
                        pass

                machine_elem_id = job.get("machine_element_id")
                machine = None
                if not machine_elem_id:
                    machine = get_machine_by_id(sdk, job["machine_id"])
                    if machine:
                        # Prefer element_id from the machine record (set by controller after provisioning).
                        # Fall back to job["element_id"] which is the shell's element entity ID
                        # (elementshell.element_id, not the shell's own ID — those are now separate).
                        machine_elem_id = machine.get("element_id") or job.get("element_id")
                        if machine_elem_id:
                            _update_job(job["id"], machine_element_id=machine_elem_id, last_checked=now)

                if not machine_elem_id:
                    if elapsed_upgrading > UPGRADE_TIMEOUT:
                        _log.info(f"[{jid}] upgrading→version_check (no element_id, timed out)")
                        _update_job(job["id"], status="version_check", step=5,
                                    version_check_at=now,
                                    message="Verifying device status…",
                                    last_checked=now)
                    else:
                        _update_job(job["id"],
                                    message="Waiting for element to be fully registered…",
                                    last_checked=now)
                    continue

                elem_online, _elem_state = get_element_operational_status(sdk, job["site_id"], machine_elem_id)
                # Fallback: if the element-status API fails (e.g. element not yet visible), check
                # machine connectivity directly — avoids getting stuck when the API returns 404.
                if not elem_online and _elem_state.lower() in ("status unavailable", "unknown"):
                    if machine is None:
                        machine = get_machine_by_id(sdk, job["machine_id"])
                    if machine and machine.get("connected"):
                        elem_online = True
                        _elem_state = "Connected"
                        _log.info(f"[{jid}] element-status unavailable — machine is connected, treating as online")
                upgrade_done, upgrade_msg, from_ver, to_ver = get_software_upgrade_status(
                    sdk, job["site_id"], machine_elem_id)
                _log.info(f"[{jid}] upgrading: done={upgrade_done} online={elem_online} "
                          f"msg={upgrade_msg!r} from={from_ver!r} to={to_ver!r} "
                          f"elapsed={elapsed_upgrading:.0f}s")

                updates = {"last_checked": now}
                if from_ver and not job.get("from_version"):
                    updates["from_version"] = from_ver
                if to_ver and not job.get("to_version"):
                    updates["to_version"] = to_ver

                if upgrade_done and elem_online:
                    # Software done AND device is back online — safe to advance.
                    # Do NOT advance when device is offline: the "done" signal may come
                    # from a 502 fallback ("assuming current"), not a real API response.
                    # Wait for the device to reconnect before moving on.
                    _log.info(f"[{jid}] upgrading→version_check (done + online)")
                    updates.update(status="version_check", step=5,
                                   version_check_at=now,
                                   upgrade_phase=3,
                                   message="Software upgrade complete — verifying running version…")
                elif elapsed_upgrading > UPGRADE_TIMEOUT:
                    _log.info(f"[{jid}] upgrading→version_check (timed out after {elapsed_upgrading:.0f}s)")
                    updates.update(status="version_check", step=5,
                                   version_check_at=now,
                                   upgrade_phase=3,
                                   message="Software upgrade timed out — verifying version and device status…")
                elif elem_online:
                    # Device is already online but software/status still shows an incomplete
                    # state (commonly "pending" — scheduled but not yet downloading). An online
                    # device that isn't actively downloading or rebooting is likely already at the
                    # correct version; advance after a short grace period so the controller catches up.
                    actively = ("downloading" in upgrade_msg.lower() or
                                "rebooting"   in upgrade_msg.lower() or
                                "in progress" in upgrade_msg.lower())
                    if not actively and elapsed_upgrading > 10:
                        _log.info(f"[{jid}] upgrading→version_check (device online, no active "
                                  f"upgrade detected after {elapsed_upgrading:.0f}s)")
                        updates.update(status="version_check", step=5,
                                       version_check_at=now,
                                       upgrade_phase=3,
                                       message="Device online — verifying software version…")
                    elif actively:
                        msg = upgrade_msg or "Downloading and installing software update…"
                        phase = 2 if "rebooting" in msg.lower() else 1
                        updates["message"] = msg
                        updates["upgrade_phase"] = phase
                    else:
                        # Online but no active upgrade signal in the initial window — neutral state
                        updates["message"] = "Device online — checking software status…"
                        updates["upgrade_phase"] = 1
                else:
                    # Device is offline. The controller may cache stale upgrade states
                    # (e.g. "downloading") after a device disconnects — don't show
                    # "Installing software update" when the device is actually offline.
                    if "rebooting" in upgrade_msg.lower():
                        updates["message"] = upgrade_msg
                        updates["upgrade_phase"] = 2
                    else:
                        updates["message"] = f"Device offline — waiting to reconnect ({_elem_state})…"
                        updates["upgrade_phase"] = 3

                _update_job(job["id"], **updates)

            elif job["status"] == "version_check":
                elapsed_vc = 0
                if job.get("version_check_at"):
                    try:
                        started = datetime.fromisoformat(job["version_check_at"].replace("Z", "+00:00"))
                        elapsed_vc = (datetime.now(timezone.utc) - started).total_seconds()
                    except Exception:
                        pass

                # machine_element_id is the authoritative element entity ID set during provisioning.
                # element_id is the shell's entity ID (elementshell.element_id) set at job creation —
                # a valid fallback since both refer to the same element entity (not the shell's own ID).
                machine_elem_id = job.get("machine_element_id") or job.get("element_id")
                version, ver_msg = get_current_software_version(sdk, machine_elem_id)
                is_online, online_state = get_element_operational_status(sdk, job["site_id"], machine_elem_id)
                # If element-status API fails but machine is connected, treat as online
                if not is_online and online_state.lower() in ("status unavailable", "unknown"):
                    chk_machine = get_machine_by_id(sdk, job["machine_id"])
                    if chk_machine and chk_machine.get("connected"):
                        is_online = True
                        online_state = "Connected"
                        _log.info(f"[{jid}] version_check: element-status unavailable — machine connected")
                _log.info(f"[{jid}] version_check: version={version!r} online={is_online} "
                          f"state={online_state!r} elapsed={elapsed_vc:.0f}s")

                updates = {"last_checked": now}
                if version and not job.get("running_version"):
                    updates["running_version"] = version

                expected = job.get("to_version") or ""
                no_upgrade_needed = not expected
                api_unavailable = online_state in ("Status unavailable", "status unavailable")
                version_ok = (not expected) or (version and version == expected) or elapsed_vc > VERSION_CHECK_TIMEOUT

                # When no upgrade was triggered and the element-status API is failing,
                # don't wait the full VERSION_CHECK_TIMEOUT — 30s is enough to confirm
                # the device didn't actually reboot for an upgrade.
                fast_exit = no_upgrade_needed and api_unavailable and elapsed_vc > 30

                if version_ok and (is_online or fast_exit):
                    _log.info(f"[{jid}] version_check→fabric_check"
                              + (" (fast-exit: no upgrade needed)" if fast_exit else ""))
                    ver_display = f"Running {version}" if version else "Software verified"
                    updates.update(status="fabric_check", step=6,
                                   upgrade_phase=5,
                                   fabric_check_at=now,
                                   message=f"{ver_display} — checking SDWAN Fabric connectivity…")
                elif elapsed_vc > VERSION_CHECK_TIMEOUT:
                    _log.info(f"[{jid}] version_check→fabric_check (timed out)")
                    updates.update(status="fabric_check", step=6,
                                   upgrade_phase=5,
                                   fabric_check_at=now,
                                   message="Device online — checking SDWAN Fabric connectivity…")
                else:
                    if not is_online:
                        if expected:
                            updates["message"] = f"Device rebooting after upgrade ({online_state})… {ver_msg}"
                        else:
                            updates["message"] = f"Device offline — waiting to reconnect ({online_state})…"
                        updates["upgrade_phase"] = 3
                    elif version and expected and version != expected:
                        updates["message"] = f"Waiting for expected version {expected} (currently {version})…"
                        updates["upgrade_phase"] = 4
                    else:
                        updates["message"] = f"{ver_msg} — Device coming back online…"
                        updates["upgrade_phase"] = 4

                _update_job(job["id"], **updates)

            elif job["status"] == "fabric_check":
                elapsed_fabric = 0
                if job.get("fabric_check_at"):
                    try:
                        started = datetime.fromisoformat(job["fabric_check_at"].replace("Z", "+00:00"))
                        elapsed_fabric = (datetime.now(timezone.utc) - started).total_seconds()
                    except Exception:
                        pass

                # Use the actual element entity ID for network/fabric APIs, not the shell's own ID.
                element_id_for_check = job.get("machine_element_id") or job.get("element_id")
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
    import time as _time
    build_ts = str(int(_time.time()))
    resp = app.make_response(render_template("field.html", build_ts=build_ts))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


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
        # get_port_config queries elementshells/{id}/interfaces, which requires the shell's own ID.
        shell_id_for_ports = job.get("element_shell_id") or job.get("element_id")
        ports = get_port_config(sdk, job["site_id"], shell_id_for_ports)
        model_name = job.get("machine_model") or job.get("element_model") or ""
        return jsonify({"ok": True, "ports": ports, "model_name": model_name})
    except Exception as e:
        reset_sdk()
        return jsonify({"ok": False, "error": str(e), "ports": {"wan_ports": [], "lan_ports": [], "ha_ports": [], "bypass_pairs": []}}), 200


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
    # element_shell_id = the elementshell's own ID (used for claim + shell interface APIs)
    # element_id       = the actual element entity ID (used for all monitoring/status APIs)
    element_shell_id = data.get("element_shell_id")
    element_id = data.get("element_id")
    element_name = data.get("element_name") or element_id
    site_id = data.get("site_id")
    site_name = data.get("site_name") or "Unknown"
    element_model = (data.get("element_model") or "").strip()

    if not serial or not site_id:
        return jsonify({"error": "serial_number and site_id are required"}), 400

    # Auto-pick a shell if none provided
    if not element_shell_id:
        try:
            sdk = get_sdk()
            shells = get_available_shells_for_site(sdk, site_id)
            if not shells:
                return jsonify({"error": "No available element shells at this site. Contact your network admin to create one."}), 422
            first = shells[0]
            element_shell_id = first["id"]           # shell's own ID — for claim/shell-interface APIs
            element_id = first.get("element_id")     # actual element entity ID — for monitoring APIs (may be None pre-provision)
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
        "element_shell_id": element_shell_id,  # shell's own ID — used for claim + shell interface APIs
        "element_id": element_id,              # actual element entity ID — used for monitoring/status APIs
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
