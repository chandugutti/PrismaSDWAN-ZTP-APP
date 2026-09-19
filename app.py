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
VERSION_CHECK_TIMEOUT = 600  # seconds — max time to confirm running version after upgrade
FABRIC_TUNNEL_TIMEOUT = 90   # seconds — max time in partial-tunnel state after device is online
FABRIC_REBOOT_WAIT = 600     # seconds — safety ceiling: max time waiting for device to come back online after reboot

# Normalized upgrade state values returned by get_software_upgrade_status.
# NONE    = API 200, parsed correctly, zero records for this element (confirmed no active upgrade).
# UNKNOWN = API timeout / non-200 / exception / malformed / missing element_id.
# All other values come from the upgrade_state field returned by the API.
UPG_NONE        = "NONE"
UPG_PENDING     = "PENDING"
UPG_DOWNLOADING = "DOWNLOADING"
UPG_INSTALLING  = "INSTALLING"
UPG_REBOOTING   = "REBOOTING"
UPG_COMPLETE    = "COMPLETE"
UPG_FAILED      = "FAILED"
UPG_UNKNOWN     = "UNKNOWN"

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

    Completion requires BOTH:
      - machine_state == 'claimed'   (controller sent the config)
      - em_element_id is populated   (device called back; session established)
    These are effectively atomic but the dual check guards the brief window
    where claimed may appear before em_element_id is written.

    On success, state_description is 'claimed:<em_element_id>' so the caller
    can extract the real element ID without a second machine lookup.
    """
    machine = get_machine_by_id(sdk, machine_id)
    if not machine:
        return False, "machine not found"

    m_state = (machine.get("machine_state") or "").lower()
    em_element_id = machine.get("em_element_id") or ""
    element_shell_id = machine.get("element_shell_id")

    _log.info(f"machine {machine_id}: machine_state={m_state!r} "
              f"em_element_id={em_element_id!r} element_shell_id={element_shell_id!r}")

    if m_state == "claimed" and em_element_id:
        return True, f"claimed:{em_element_id}"

    if m_state == "allocated":
        if element_shell_id:
            return False, "allocated — device is installing certificate and downloading configuration"
        return False, "allocated — waiting for controller to bind device to shell"

    return False, f"state={m_state or 'unknown'} — waiting..."


def get_software_upgrade_status(sdk, site_id, element_id):
    """Returns a normalized upgrade-state dict.

    Keys:
      state        — one of the UPG_* module constants
      progress     — int percentage or None
      from_version — str or ""
      to_version   — str or ""
      api_success  — True if HTTP 200 + clean parse; False otherwise

    NONE  : API 200, parsed correctly, zero records → confirmed no active upgrade.
    UNKNOWN : API timeout / non-200 / exception / malformed / missing element_id.

    Tries in order:
      1. GET /sdwan/v2.1/api/elements/{element_id}/software/status  (per-element)
      2. POST /sdwan/v2.1/api/software/status/query                 (tenant-wide, filtered)
    """
    def _unknown():
        return {"state": UPG_UNKNOWN, "progress": None,
                "from_version": "", "to_version": "", "api_success": False}

    def _none():
        return {"state": UPG_NONE, "progress": None,
                "from_version": "", "to_version": "", "api_success": True}

    if not element_id:
        return _unknown()

    sess = getattr(sdk, "session", None)
    ctrl = (getattr(sdk, "controller", None) or "").rstrip("/")
    if not (sess and ctrl):
        return _unknown()

    def _parse_items(items, filter_eid=None):
        """Select the best upgrade record: filter by element_id, prefer active states, then newest timestamp."""
        _STATE_PRIO = {
            "downloading": (4, UPG_DOWNLOADING),
            "upgrading":   (4, UPG_INSTALLING),
            "in_progress": (4, UPG_INSTALLING),
            "rebooting":   (4, UPG_REBOOTING),
            "pending":     (3, UPG_PENDING),
            "scheduled":   (3, UPG_PENDING),
            "complete":    (2, UPG_COMPLETE),
            "success":     (2, UPG_COMPLETE),
            "succeeded":   (2, UPG_COMPLETE),
            "current":     (2, UPG_COMPLETE),
            "failed":      (1, UPG_FAILED),
            "error":       (1, UPG_FAILED),
        }
        best_prio, best_ts, best_rec = -1, -1.0, None
        for item in items:
            if filter_eid:
                item_eid = item.get("element_id") or item.get("elementId") or ""
                if item_eid and item_eid != filter_eid:
                    continue
            raw = (item.get("upgrade_state") or item.get("state") or "").lower()
            prio, state = _STATE_PRIO.get(raw, (0, UPG_UNKNOWN))
            fv = item.get("from_image_version") or item.get("from_version") or ""
            tv = (item.get("to_image_version") or item.get("to_version")
                  or item.get("image_version") or "")
            pct_raw = item.get("percentage") or item.get("progress")
            progress = None
            if pct_raw is not None:
                try:
                    progress = int(pct_raw)
                except (ValueError, TypeError):
                    pass
            ts_str = (item.get("updated_on") or item.get("timestamp")
                      or item.get("_updated_at_utc") or "")
            try:
                ts = (datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
                      if ts_str else 0.0)
            except Exception:
                ts = 0.0
            if ts > best_ts or (ts == best_ts and prio > best_prio):
                best_prio, best_ts = prio, ts
                best_rec = {"state": state, "progress": progress,
                            "from_version": fv, "to_version": tv, "api_success": True}
        return best_rec

    # 1. Per-element REST endpoint (most authoritative)
    try:
        url = f"{ctrl}/sdwan/v2.1/api/elements/{element_id}/software/status"
        r = sess.get(url, timeout=10)
        if r.status_code == 200:
            body = r.json()
            items = body.get("items", []) if isinstance(body, dict) else []
            if not items and isinstance(body, dict) and body:
                items = [body]
            if items:
                result = _parse_items(items, filter_eid=element_id)
                if result is not None:
                    return result
            # HTTP 200 + empty, or no element-id-matched record → fall through to tenant-wide query
        # Non-200 on per-element → fall through to query endpoint
    except Exception as e:
        _log.warning(f"software/status element endpoint failed: {e}")

    # 2. Tenant-wide query filtered to this element
    try:
        url = f"{ctrl}/sdwan/v2.1/api/software/status/query"
        r = sess.post(url, json={"query": {"element_id": {"in": [element_id]}}}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            items = data.get("items", []) if isinstance(data, dict) else []
            if items:
                result = _parse_items(items, filter_eid=element_id)
                if result is not None:
                    return result
            else:
                return _none()
    except Exception as e:
        _log.warning(f"software/status query fallback failed: {e}")

    return _unknown()


def _parse_version(ver_str):
    """Parse 'X.Y.Z-bN' into a tuple for comparison: (X, Y, Z, N).
    Non-numeric segments are treated as 0 so comparisons degrade gracefully."""
    import re
    ver_str = (ver_str or "").strip().lower().lstrip("v")
    # Split on '-b' to separate base version from build number
    if "-b" in ver_str:
        base, build = ver_str.rsplit("-b", 1)
    else:
        base, build = ver_str, "0"
    parts = re.split(r"[.\-]", base)
    nums = []
    for p in parts:
        try:
            nums.append(int(p))
        except ValueError:
            nums.append(0)
    # Pad to at least 3 segments
    while len(nums) < 3:
        nums.append(0)
    try:
        build_num = int(build)
    except ValueError:
        build_num = 0
    return tuple(nums[:3]) + (build_num,)


def _version_gte(current, target):
    """Return True if current version >= target version (same or newer is acceptable)."""
    return _parse_version(current) >= _parse_version(target)


def _upgrade_state_message(upg):
    """Return a human-readable status string from a normalized upgrade dict."""
    state = upg.get("state", UPG_UNKNOWN)
    fv = upg.get("from_version", "")
    tv = upg.get("to_version", "")
    pct = upg.get("progress")

    ver_str = f"{fv} → {tv} — " if fv and tv else ""
    pct_str = f" ({pct}%)" if pct is not None else ""

    if state == UPG_DOWNLOADING:
        return f"{ver_str}Downloading software update{pct_str}…"
    if state == UPG_INSTALLING:
        return f"{ver_str}Installing software update{pct_str}…"
    if state == UPG_REBOOTING:
        return f"{ver_str}Device rebooting after upgrade…"
    if state == UPG_PENDING:
        return "Upgrade pending — waiting to start…"
    if state == UPG_COMPLETE:
        return f"Running {tv}" if tv else "Software is current"
    if state == UPG_FAILED:
        return f"{ver_str}Software upgrade failed"
    if state == UPG_NONE:
        return "No active upgrade record — device is current"
    return "Software status unavailable"


def get_current_software_version(sdk, element_id):
    """Returns (version: str, message: str) — confirms the version currently running on the element.

    Primary: POST /sdwan/v2.1/api/software/current_status/query
    Fallback: GET /sdwan/v2.0/api/elements/{id} → software_version field
    """
    if not element_id:
        return "", "Version check unavailable"

    # Primary: current_status query
    try:
        resp = sdk.get.elements_query({"query": {"element_id": {"in": [element_id]}}})
    except Exception:
        resp = None

    sess = getattr(sdk, "session", None)
    ctrl = (getattr(sdk, "controller", None) or "").rstrip("/")
    if sess and ctrl:
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

    # Fallback: read software_version directly from the element record
    try:
        resp_e = sdk.get.elements(element_id)
        elem = getattr(resp_e, "cgx_content", {})
        ver = (elem.get("software_version") or "")
        if ver and not ver.startswith("SHELL#"):
            return ver, f"Running {ver}"
    except Exception as e:
        _log.warning(f"elements fallback for version failed: {e}")

    return "", "Running version unavailable"


def get_software_desired_version(sdk, element_id):
    """Returns (target_version: str, message: str) — desired version from elements/{id}/software/state."""
    sess = getattr(sdk, "session", None)
    ctrl = (getattr(sdk, "controller", None) or "").rstrip("/")
    if not (sess and ctrl and element_id):
        return "", "Desired version unavailable"
    try:
        url = f"{ctrl}/sdwan/v2.0/api/elements/{element_id}/software/state"
        r = sess.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                ver = (data.get("image_version") or data.get("version") or
                       data.get("target_version") or "")
                if ver:
                    return ver, f"Target version: {ver}"
    except Exception as e:
        _log.warning(f"software/state endpoint failed: {e}")
    return "", "Desired version unavailable"


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
    sess = getattr(sdk, "session", None) or getattr(sdk, "_session", None)
    ctrl = (getattr(sdk, "controller", None) or "").rstrip("/")

    _api_ok = [False]  # tracks whether any REST call returned HTTP 200

    def _rest(method, path, body=None):
        if not (sess and ctrl):
            return None
        try:
            if method == "GET":
                r = sess.get(f"{ctrl}{path}", timeout=8)
            else:
                r = sess.post(f"{ctrl}{path}", json=body or {}, timeout=8)
            if r.status_code == 200:
                _api_ok[0] = True
                return r.json()
            _log.warning(f"get_network_status: {method} {path} → {r.status_code}")
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

    _log.info(f"get_network_status: site={site_id} element={element_id} vpn_links={len(vpn_links_raw)}")

    # Fallback: topology links query when vpnlinks/query returned nothing
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

    _UP_STATES = {"up", "active", "established", "connected", "online"}

    def _fetch_vpn_link_status(link):
        link_id = link.get("id")
        actual = "down"
        if link_id and not link.get("_topo"):
            for _ver in ("v2.2", "v2.1", "v2.0"):
                sd = _rest("GET", f"/sdwan/{_ver}/api/vpnlinks/{link_id}/status")
                if sd:
                    st = (sd.get("state") or sd.get("vpnlink_state") or "").lower()
                    actual = "up" if st in _UP_STATES else "init" if st in ("init", "initializing", "pending") else "down"
                    break
        if actual == "down":
            raw = (link.get("state") or link.get("status") or link.get("vpnlink_state") or "").lower()
            actual = "up" if raw in _UP_STATES else "init" if raw in ("init", "initializing") else "down"
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

    # If REST query returned links but none show UP, topology may have better status data.
    # Only use this fallback when no specific element_id is scoped — topology is site-wide
    # and would incorrectly show other devices' tunnels as belonging to the new device.
    if vpn_total > 0 and vpn_up == 0:
        topo = _topology_links_query(sdk, site_id)
        topo_up = 0
        topo_peers = []
        for link in topo:
            if link.get("type") in ("public-anynet", "private-anynet"):
                raw = (link.get("status") or link.get("state") or "").lower()
                if raw in _UP_STATES:
                    topo_up += 1
                    pid = link.get("target_site_id") or link.get("source_site_id")
                    if pid and pid != site_id:
                        topo_peers.append(site_name_map.get(pid, pid))
        if topo_up > 0:
            _log.info(f"get_network_status: REST showed 0/{vpn_total} up; topology shows {topo_up} up — using topology")
            vpn_up = topo_up
            vpn_up_peers = topo_peers

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
    _iface_paths = [
        f"/sdwan/v4.21/api/sites/{site_id}/elements/{element_id}/interfaces",
        f"/sdwan/v2.1/api/sites/{site_id}/elements/{element_id}/interfaces",
        f"/sdwan/v4.1/api/sites/{site_id}/elements/{element_id}/interfaces",
        f"/sdwan/v2.0/api/sites/{site_id}/elements/{element_id}/interfaces",
    ]
    for _ipath in _iface_paths:
        d = _rest("GET", _ipath)
        if d:
            iface_configs = d.get("items", [])
            if iface_configs:
                _log.info(f"get_network_status: {len(iface_configs)} interfaces via {_ipath}")
                break
    if not iface_configs:
        try:
            iface_resp = sdk.get.interfaces(site_id, element_id)
            iface_configs = _safe_items(iface_resp) or []
            if iface_configs:
                _log.info(f"get_network_status: {len(iface_configs)} interfaces via sdk.get.interfaces")
        except Exception as e:
            _log.warning(f"get_network_status: sdk.get.interfaces fallback failed: {e}")

    def _fetch_iface_result(iface):
        iface_id = iface.get("id")
        used_for = (iface.get("used_for") or iface.get("if_type") or "").lower()
        admin_up = (iface.get("admin_state") or "").lower() in ("up", "enabled", "active")

        if used_for in ("wan", "publicwan", "privatewan", "public", "private") or iface.get("site_wan_interface_ids"):
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
        "raw_ok":     _api_ok[0],
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

    # 4. Categorize ports
    def _fmt_wan_type(raw):
        return {"publicwan": "Public WAN", "privatewan": "Private WAN"}.get(raw.lower(), raw.replace("_", " ").title())

    # Build ID → interface map for bypass pair resolution.
    # bypass_pair.wan / bypass_pair.lan are interface IDs, not port labels.
    id_to_iface = {}
    for iface in items:
        iid = iface.get("id")
        if iid:
            id_to_iface[iid] = iface

    # Pass 1 — collect all bypass pair port IDs and build bypass_pairs list.
    # Each bypass pair has a WAN side (P3, ISP connects here) and a LAN side (P4,
    # cross-connect cable to the peer ION's regular WAN port).  The circuit name
    # belongs to the WAN side.
    bypass_port_ids = set()
    seen_bypass_keys = set()
    bypass_pairs = []

    for iface in items:
        bypass = iface.get("bypass_pair")
        if not bypass or not isinstance(bypass, dict):
            continue
        wan_id = bypass.get("wan") or ""
        lan_id = bypass.get("lan") or ""
        for bid in (wan_id, lan_id):
            if bid:
                bypass_port_ids.add(bid)

        pair_key = tuple(sorted(filter(None, [wan_id, lan_id])))
        if not pair_key or pair_key in seen_bypass_keys:
            continue
        seen_bypass_keys.add(pair_key)

        wan_iface = id_to_iface.get(wan_id) or {}
        lan_iface = id_to_iface.get(lan_id) or {}
        wan_port = wan_iface.get("name") or wan_iface.get("if_name") or "?"
        lan_port = lan_iface.get("name") or lan_iface.get("if_name") or "?"

        # Circuit name comes from the WAN-side port's wan_interface_ids
        circuit, wan_type = "", ""
        for wid in (wan_iface.get("site_wan_interface_ids") or []):
            info = wan_name_map.get(wid, {})
            lbl = info.get("name", "")
            if lbl and not str(lbl).strip().isdigit():
                circuit = lbl
            wan_type = wan_type or info.get("type", "")

        type_label = _fmt_wan_type(wan_type) if wan_type else ""
        description = wan_iface.get("description") or lan_iface.get("description") or ""
        _log.info(f"get_port_config: bypass wan_port={wan_port!r} lan_port={lan_port!r} circuit={circuit!r}")
        bypass_pairs.append({
            "wan_port": wan_port,   # P3 — ISP cable connects here
            "lan_port": lan_port,   # P4 — cross-connect to peer ION WAN port
            "circuit": circuit,
            "wan_type": type_label,
            "description": description,
        })

    # Pass 2 — classify all non-bypass ports
    wan_ports, lan_ports, ha_ports = [], [], []

    for iface in items:
        port_name = iface.get("name") or iface.get("if_name") or "?"
        iid = iface.get("id") or ""

        if port_name.lower().startswith("controller"):
            continue
        if iid in bypass_port_ids:
            continue  # already captured as part of a bypass pair

        used_for = (iface.get("used_for") or "").lower()
        if used_for == "none":
            continue

        description = iface.get("description") or ""
        wan_ids = iface.get("site_wan_interface_ids") or []

        if wan_ids:
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
        elif used_for in ("wan", "publicwan", "privatewan"):
            # WAN port without a circuit assigned yet — include it so it shows up in port validation
            type_label = _fmt_wan_type(used_for) if used_for != "wan" else "WAN"
            wan_ports.append({"name": port_name, "circuit": "", "description": description, "wan_type": type_label})
        elif used_for == "ha":
            ha_ports.append({"name": port_name, "description": description})
        elif used_for == "lan":
            lan_ports.append({"name": port_name, "description": description})

    _log.info(
        f"get_port_config: {len(wan_ports)} WAN, {len(lan_ports)} LAN, "
        f"{len(ha_ports)} HA, {len(bypass_pairs)} bypass pairs"
    )
    return {"wan_ports": wan_ports, "lan_ports": lan_ports, "ha_ports": ha_ports, "bypass_pairs": bypass_pairs}


def validate_ports(port_config, network_status):
    """Join port config with live operational state to produce per-port verdicts."""
    TIPS = {
        "WAN":         "Check the physical cable to the ISP/router. Verify the ISP circuit is active and the SFP/connector is fully seated.",
        "WAN-Bypass":  "Check ISP cable on the WAN side of this bypass pair. Verify the circuit is active and SFP is fully seated.",
        "LAN-Bypass":  "Check cross-connect cable between this bypass LAN port and the peer ION device's WAN port.",
        "LAN":         "Check cable to the downstream switch. Verify the VLAN is configured correctly on the switch port.",
        "HA":          "HA heartbeat link down — check the direct cable between both ION devices on the HA port.",
    }
    results = []

    if not network_status:
        return results

    interfaces = network_status.get("interfaces") or []

    # After provisioning the shell API goes stale. When port_config is empty but
    # get_network_status already returned categorised interfaces (name/role/up/circuit),
    # use those directly instead of trying to join against an empty port_config.
    has_port_config = port_config and any(
        port_config.get(k) for k in ("wan_ports", "lan_ports", "ha_ports", "bypass_pairs")
    )
    if not has_port_config:
        for iface in interfaces:
            name = iface.get("name", "")
            role = iface.get("role", "")
            is_up = iface.get("up", False)
            circuit = iface.get("circuit", "")
            tip = "" if is_up else TIPS.get(role, "Check the physical connection.")
            results.append({"name": name, "role": role, "circuit": circuit, "up": is_up,
                            "result": "pass" if is_up else "fail", "tip": tip})
        return results

    # port_config path: join shell-derived port layout with element interface status
    status_map = {i.get("name", ""): i.get("up", False) for i in interfaces if i.get("name")}

    def _add(name, role, circuit=""):
        up = status_map.get(name)
        if up is None:
            result, tip = "unknown", ""
        elif up:
            result, tip = "pass", ""
        else:
            result = "fail"
            tip = TIPS.get(role, "Check the physical connection.")
        results.append({"name": name, "role": role, "circuit": circuit, "up": up,
                        "result": result, "tip": tip})

    for p in (port_config.get("wan_ports") or []):
        _add(p["name"], "WAN", circuit=p.get("circuit", ""))
    for bp in (port_config.get("bypass_pairs") or []):
        _add(bp["wan_port"], "WAN-Bypass", circuit=bp.get("circuit", ""))
        _add(bp["lan_port"], "LAN-Bypass")
    for p in (port_config.get("ha_ports") or []):
        _add(p["name"], "HA")
    for p in (port_config.get("lan_ports") or []):
        _add(p["name"], "LAN")
    return results


def get_ha_roles(sdk, site_id):
    """Return {element_id: 'active'|'backup'} for all elements at a site."""
    roles = {}
    try:
        resp = sdk.get.elements(site_id)
        for elem in _safe_items(resp):
            eid = elem.get("id")
            if not eid:
                continue
            cr = (elem.get("cluster_role") or "").lower()
            sn = elem.get("serial_number", "")
            if "primary" in cr or "active" in cr:
                roles[eid] = {"role": "active", "serial": sn}
            elif "secondary" in cr or "backup" in cr or "standby" in cr:
                roles[eid] = {"role": "backup", "serial": sn}
            else:
                roles[eid] = {"role": "unknown", "serial": sn}
    except Exception as e:
        _log.warning(f"get_ha_roles: {e}")
    return roles


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
                    _audit(job["id"], "Search timed out → failed",
                           api="GET /sdwan/v2.0/api/machines/query", result="err",
                           detail=f"serial={job['serial_number']} elapsed={elapsed_searching:.0f}s")
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
                    _audit(job["id"], "Device found → waiting online",
                           api="GET /sdwan/v2.0/api/machines/query", result="ok",
                           detail=f"machine_id={machine['id']} state={machine_state}")
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
                    _audit(job["id"], "Waiting online timed out → failed",
                           api="GET /sdwan/v2.0/api/machines/{id}", result="err",
                           detail=f"machine_id={job.get('machine_id')} elapsed={elapsed_waiting:.0f}s")
                    continue

                connected, existing_element_id = check_machine_connected(sdk, job["machine_id"])
                _log.info(f"[{jid}] waiting_online: connected={connected} existing_element_id={existing_element_id}")
                if existing_element_id:
                    _update_job(job["id"], status="provisioning", step=4,
                                provisioning_at=now,
                                message="Device already claimed — verifying provisioning…",
                                last_checked=now)
                    _audit(job["id"], "Device already claimed → provisioning",
                           api="GET /sdwan/v2.0/api/machines/{id}", result="ok",
                           detail=f"existing_element_id={existing_element_id}")
                elif connected:
                    _log.info(f"[{jid}] waiting_online→assigning")
                    _update_job(job["id"], status="assigning", step=3,
                                message="Device is online! Claiming and assigning to site…",
                                last_checked=now)
                    _audit(job["id"], "Device came online → assigning",
                           api="GET /sdwan/v2.0/api/machines/{id}", result="ok",
                           detail=f"machine_id={job.get('machine_id')} connected=True")
                else:
                    _update_job(job["id"], message="Device found but not yet online — waiting…", last_checked=now)

            elif job["status"] == "assigning":
                _log.info(f"[{jid}] assigning: machine={job['machine_id']} element={job['element_id']}")

                # Model compatibility check — prevents silent hang when models don't match
                machine = get_machine_by_id(sdk, job["machine_id"])
                shell = get_elementshell(sdk, job["site_id"], job.get("element_shell_id") or job["element_id"])
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
                        _audit(job["id"], "Model mismatch → failed",
                               api="GET /sdwan/v2.0/api/elementshells/{id}", result="err",
                               detail=f"machine={machine_model} shell={shell_model}")
                        continue

                # element_shell_id is the shell's own ID required by machines_allocate_to_shell;
                # element_id is the actual element entity ID used for monitoring (may differ).
                shell_id_to_claim = job.get("element_shell_id") or job.get("element_id")
                success, msg = claim_machine_to_element(sdk, job["machine_id"], shell_id_to_claim)
                _log.info(f"[{jid}] claim result: success={success} msg={msg}")
                if success:
                    shell_target_ver = (shell.get("software_version") or "") if shell else ""
                    _log.info(f"[{jid}] shell target_version={shell_target_ver!r}")
                    _update_job(job["id"], status="provisioning", step=4,
                                provisioning_at=now,
                                target_version=shell_target_ver,
                                message="Device claimed! Waiting for site provisioning to complete…",
                                last_checked=now)
                    _audit(job["id"], "Device claimed → provisioning",
                           api="POST /sdwan/v2.0/api/machines/{id}/allocate_to_shell", result="ok",
                           detail=f"machine_id={job['machine_id']} shell_id={shell_id_to_claim}")
                else:
                    _update_job(job["id"], status="failed",
                                message=msg, last_checked=now)
                    _audit(job["id"], "Claim failed → failed",
                           api="POST /sdwan/v2.0/api/machines/{id}/allocate_to_shell", result="err",
                           detail=msg)

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
                    _audit(job["id"], "Provisioning timed out → failed",
                           api="GET /sdwan/v2.0/api/machines/{id}", result="err",
                           detail=f"elapsed={elapsed_provisioning:.0f}s")
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
                        _audit(job["id"], "Hung allocation → failed",
                               api="GET /sdwan/v2.0/api/machines/{id}", result="err",
                               detail=error_msg[:200])
                        continue

                provisioned, state = check_machine_provisioned(sdk, machine_id)
                _log.info(f"[{jid}] provisioning check: provisioned={provisioned} state={state!r}")
                if provisioned:
                    # state is 'claimed:<em_element_id>' — extract the real element ID.
                    # Fall back to the shell's pre-assigned element_id if parsing fails.
                    em_id_from_state = state.split(":", 1)[1] if ":" in state else ""
                    machine_elem_id = em_id_from_state or job.get("element_id")
                    target_ver_init = job.get("target_version") or ""
                    _log.info(f"[{jid}] provisioning→upgrading (machine_element_id={machine_elem_id!r}) target_version={target_ver_init!r}")
                    _update_job(job["id"], status="upgrading", step=5,
                                message="Device provisioned — checking software version…",
                                upgrading_at=now, machine_element_id=machine_elem_id,
                                target_version=target_ver_init,
                                last_checked=now)
                    _audit(job["id"], "Provisioning complete → upgrading",
                           api="GET /sdwan/v2.0/api/machines/{id}", result="ok",
                           detail=f"element_id={machine_elem_id}")
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
                now_dt = datetime.now(timezone.utc)
                elapsed_upgrading = 0
                if job.get("upgrading_at"):
                    try:
                        started = datetime.fromisoformat(job["upgrading_at"].replace("Z", "+00:00"))
                        elapsed_upgrading = (now_dt - started).total_seconds()
                    except Exception:
                        pass

                machine_elem_id = job.get("machine_element_id")
                if not machine_elem_id:
                    machine = get_machine_by_id(sdk, job["machine_id"])
                    if machine:
                        machine_elem_id = machine.get("element_id")
                        if machine_elem_id:
                            _update_job(job["id"], machine_element_id=machine_elem_id, last_checked=now)
                if not machine_elem_id:
                    _update_job(job["id"],
                                message="Waiting for device element ID to be registered — do not unplug the device…",
                                last_checked=now)
                    continue

                updates = {"last_checked": now}

                target_ver = job.get("target_version") or ""

                # upgrade_phase sub-states:
                #   0 = wait for device to be steadily online (3 consecutive polls = 30s) before doing anything
                #   1 = version check / upgrade in progress (device is online)
                #   2 = device went offline/rebooting — ONLY poll online, never call upgrade APIs (stale cache)
                #   3 = version matched, 30s stability timer before advancing to fabric_check
                upgrade_phase = job.get("upgrade_phase") or 0
                online_consecutive = job.get("online_consecutive") or 0
                ONLINE_STEADY_COUNT = 3  # 3 × 10s = 30s steady

                # Online check — used in all phases
                is_online, online_state = get_element_operational_status(sdk, job["site_id"], machine_elem_id)
                if not is_online and online_state.lower() in ("status unavailable", "unknown"):
                    chk_machine = get_machine_by_id(sdk, job["machine_id"])
                    if chk_machine and chk_machine.get("connected"):
                        is_online = True
                        online_state = "Connected"

                _log.info(f"[{jid}] upgrading: phase={upgrade_phase} online={is_online} "
                          f"consecutive={online_consecutive} elapsed={elapsed_upgrading:.0f}s")

                if upgrade_phase == 0:
                    # Wait for 3 consecutive online polls before checking version
                    if is_online:
                        online_consecutive += 1
                        updates["online_consecutive"] = online_consecutive
                        if online_consecutive >= ONLINE_STEADY_COUNT:
                            _log.info(f"[{jid}] upgrading: device steady online → checking version")
                            updates["upgrade_phase"] = 1
                            updates["online_consecutive"] = 0
                            updates["message"] = "Device online — checking software version…"
                        else:
                            updates["message"] = f"Device online — confirming stability… ({online_consecutive}/{ONLINE_STEADY_COUNT})"
                    else:
                        updates["online_consecutive"] = 0
                        updates["message"] = "Waiting for device to come online…"

                elif upgrade_phase == 1:
                    # Device is online — compare version, show upgrade progress
                    if is_online:
                        current_ver, _ = get_current_software_version(sdk, machine_elem_id)
                        version_match = bool(
                            current_ver and target_ver
                            and _version_gte(current_ver.strip(), target_ver.strip())
                        )
                        _log.info(f"[{jid}] upgrading: current={current_ver!r} target={target_ver!r} match={version_match}")

                        if version_match:
                            updates["upgrade_phase"] = 3
                            updates["version_matched_at"] = now_dt.isoformat()
                            updates["running_version"] = current_ver
                            updates["message"] = f"Version confirmed ({current_ver}) — verifying stability…"
                            _log.info(f"[{jid}] upgrading: version matched → starting 30s stability timer")
                        else:
                            # Upgrade API for display messages only — device is online so data is fresh
                            upg = get_software_upgrade_status(sdk, job["site_id"], machine_elem_id)
                            upg_state = upg["state"]
                            upg_msg = _upgrade_state_message(upg)
                            if upg.get("from_version") and not job.get("from_version"):
                                updates["from_version"] = upg["from_version"]

                            _log.info(f"[{jid}] upgrading: version mismatch upg_state={upg_state!r} elapsed={elapsed_upgrading:.0f}s")

                            if upg_state == UPG_FAILED:
                                updates.update(status="upgrade_failed",
                                               message=f"Software upgrade failed — manual intervention required. {upg_msg}")
                                _audit(job["id"], "Upgrade FAILED → upgrade_failed",
                                       api="POST /sdwan/v2.1/api/software/status/query",
                                       result="err", detail=upg_msg[:200])
                            elif elapsed_upgrading > UPGRADE_TIMEOUT:
                                updates.update(status="upgrade_unverified",
                                               message=(
                                                   f"Software upgrade has not completed after "
                                                   f"{int(elapsed_upgrading // 60)} minutes. "
                                                   "Use Admin Override to force-advance if you are certain the upgrade succeeded."
                                               ))
                                _audit(job["id"], "Upgrade timeout → upgrade_unverified",
                                       api="POST /sdwan/v2.1/api/software/status/query",
                                       result="err", detail=f"elapsed={elapsed_upgrading:.0f}s")
                            elif upg_state in (UPG_DOWNLOADING, UPG_PENDING):
                                updates["message"] = upg_msg or "Downloading software upgrade…"
                            elif upg_state == UPG_INSTALLING:
                                updates["message"] = upg_msg or "Installing software upgrade…"
                            elif upg_state == UPG_REBOOTING:
                                updates["message"] = upg_msg or "Device rebooting after upgrade…"
                            elif current_ver and target_ver:
                                updates["message"] = f"Waiting for version {target_ver} (currently {current_ver})…"
                            else:
                                updates["message"] = "Checking software version…"
                    else:
                        # Device went offline — move to reconnect-wait phase
                        # Do NOT call upgrade APIs here: they return stale cached data
                        _log.info(f"[{jid}] upgrading: device went offline → phase 2 (reconnect wait)")
                        updates["upgrade_phase"] = 2
                        updates["online_consecutive"] = 0
                        updates["reconnect_wait_at"] = now_dt.isoformat()
                        updates["message"] = "Device offline — waiting to reconnect after upgrade…"

                elif upgrade_phase == 2:
                    # Device is offline/rebooting — ONLY check online status, no upgrade APIs
                    elapsed_reconnect = 0
                    if job.get("reconnect_wait_at"):
                        try:
                            rw_at = datetime.fromisoformat(job["reconnect_wait_at"].replace("Z", "+00:00"))
                            elapsed_reconnect = (now_dt - rw_at).total_seconds()
                        except Exception:
                            pass

                    RECONNECT_TIMEOUT = 600  # 10 minutes

                    if is_online:
                        _log.info(f"[{jid}] upgrading: device reconnected after {elapsed_reconnect:.0f}s → phase 0")
                        updates["upgrade_phase"] = 0
                        updates["online_consecutive"] = 0
                        updates["message"] = "Device reconnected — confirming stability…"
                    elif elapsed_reconnect > RECONNECT_TIMEOUT:
                        updates.update(status="upgrade_unverified",
                                       message=(
                                           f"Device did not reconnect after {int(elapsed_reconnect // 60)} minutes. "
                                           "Use Admin Override to force-advance if you are certain the upgrade succeeded."
                                       ))
                        _audit(job["id"], "Reconnect timeout → upgrade_unverified",
                               api="GET /sdwan/v2.0/api/elements/{eid}/status",
                               result="err", detail=f"elapsed_reconnect={elapsed_reconnect:.0f}s")
                    else:
                        r_str = f"{int(elapsed_reconnect)}s" if elapsed_reconnect < 60 else f"{int(elapsed_reconnect//60)}m {int(elapsed_reconnect%60)}s"
                        updates["message"] = f"Device offline — waiting to reconnect… ({r_str})"

                elif upgrade_phase == 3:
                    # Version matched — 30s stability timer
                    current_ver = job.get("running_version", "")
                    if is_online:
                        try:
                            matched_at = datetime.fromisoformat(job["version_matched_at"].replace("Z", "+00:00"))
                            elapsed_stable = (now_dt - matched_at).total_seconds()
                        except Exception:
                            elapsed_stable = 0

                        if elapsed_stable >= 30:
                            _log.info(f"[{jid}] upgrading→fabric_check (version stable {elapsed_stable:.0f}s)")
                            updates.update(status="fabric_check", step=6,
                                           upgrade_phase=5,
                                           fabric_check_at=now_dt.isoformat(),
                                           message=f"Software {current_ver} confirmed — checking SDWAN Fabric…")
                            _audit(job["id"], "Version confirmed → fabric check",
                                   api="POST /sdwan/v2.1/api/software/current_status/query",
                                   result="ok",
                                   detail=f"version={current_ver} stable={elapsed_stable:.0f}s")
                        else:
                            updates["message"] = (
                                f"Version confirmed ({current_ver}) — "
                                f"verifying stability… ({int(30 - elapsed_stable)}s remaining)"
                            )
                    else:
                        # Went offline during stability timer — restart reconnect wait
                        _log.info(f"[{jid}] upgrading: device offline during stability check → phase 2")
                        updates["upgrade_phase"] = 2
                        updates["online_consecutive"] = 0
                        updates["version_matched_at"] = None
                        updates["reconnect_wait_at"] = now_dt.isoformat()
                        updates["message"] = "Device offline — waiting to reconnect…"

                _update_job(job["id"], **updates)

            elif job["status"] == "version_check":
                # Version checking is now handled entirely within the upgrading state.
                # Any job that arrived here from a prior code version advances immediately.
                _log.info(f"[{jid}] version_check: legacy state — transitioning to fabric_check")
                _update_job(job["id"], status="fabric_check", step=6,
                            upgrade_phase=5,
                            fabric_check_at=datetime.now(timezone.utc).isoformat(),
                            message="Checking SDWAN Fabric connectivity…",
                            last_checked=now)

            elif job["status"] == "fabric_check":
                elapsed_fabric = 0
                if job.get("fabric_check_at"):
                    try:
                        started = datetime.fromisoformat(job["fabric_check_at"].replace("Z", "+00:00"))
                        elapsed_fabric = (datetime.now(timezone.utc) - started).total_seconds()
                    except Exception:
                        pass

                element_id_for_check = job.get("machine_element_id")
                if not element_id_for_check:
                    _update_job(job["id"], message="Waiting for device element ID to be registered…", last_checked=now)
                    continue
                elapsed_str = f"{int(elapsed_fabric)}s" if elapsed_fabric < 60 else f"{int(elapsed_fabric//60)}m {int(elapsed_fabric%60)}s"

                net_status = get_network_status(sdk, job["site_id"], element_id_for_check)
                raw_ok = net_status.get("raw_ok", False)
                total_links = net_status["fabric"][0]["count"] + net_status["fabric"][1]["count"]

                _log.info(f"[{jid}] fabric_check: raw_ok={raw_ok} total_links={total_links} "
                          f"all_up={net_status['all_up']} elapsed={elapsed_str}")

                if raw_ok and total_links > 0:
                    _log.info(f"[{jid}] fabric_check→assigned (API returned tunnel data)")
                    _update_job(job["id"], status="assigned", step=6,
                                message=f"Installation complete! {net_status['message']}",
                                network_status=net_status, assigned_at=now, last_checked=now)
                    _audit(job["id"], "Fabric check passed → assigned",
                           api="GET /sdwan/v2.0/api/vpnlinks/query", result="ok",
                           detail=f"elapsed={elapsed_str} total_links={total_links} {net_status.get('message','')[:100]}")

                elif elapsed_fabric > 90:
                    _log.info(f"[{jid}] fabric_check→assigned (90s timeout at {elapsed_str})")
                    _update_job(job["id"], status="assigned", step=6,
                                message="Installation complete! Device is connected — tunnels will initialize automatically.",
                                network_status=net_status, assigned_at=now, last_checked=now)
                    _audit(job["id"], "Fabric check timeout → assigned",
                           api="GET /sdwan/v2.0/api/vpnlinks/query", result="err",
                           detail=f"elapsed={elapsed_str} raw_ok={raw_ok} total_links={total_links}")

                else:
                    is_online, online_state = get_element_operational_status(
                        sdk, job["site_id"], element_id_for_check)
                    if not is_online and online_state.lower() in ("status unavailable", "unknown"):
                        chk_machine = get_machine_by_id(sdk, job["machine_id"])
                        if chk_machine and chk_machine.get("connected"):
                            is_online = True
                    if not raw_ok:
                        msg = f"Network API not yet available — device is {'online' if is_online else 'offline'} ({elapsed_str})…"
                    else:
                        msg = f"Waiting for tunnel data — no links returned yet ({elapsed_str})…"
                    _log.info(f"[{jid}] fabric_check: retrying — {msg}")
                    _update_job(job["id"], message=msg, last_checked=now)

        except Exception as e:
            _log.exception(f"[{jid}] poll error: {e}")
            _update_job(job["id"], message=f"Error: {e}", last_checked=now)

    save_jobs()


def _update_job(job_id, **kwargs):
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id].update(kwargs)


def _audit(job_id, action, actor="System", api="", result="ok", detail=""):
    """Append a structured audit entry to the job's audit_log list."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "actor": actor,
        "action": action,
        "api": api,
        "result": result,
        "detail": detail,
    }
    with _jobs_lock:
        if job_id in _jobs:
            if "audit_log" not in _jobs[job_id]:
                _jobs[job_id]["audit_log"] = []
            _jobs[job_id]["audit_log"].append(entry)


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
    _audit(job_id, "Installation started", actor="Field Tech",
           api=f"POST /api/jobs/{job_id}/start",
           result="ok",
           detail=f"serial={job.get('serial_number')} site={job.get('site_name')}")
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
        job = dict(_jobs[job_id])
        if job["status"] not in ("assigned", "failed"):
            return jsonify({"error": f"Job is in '{job['status']}' — can only recheck from assigned or failed"}), 400

    try:
        sdk = get_sdk()
        site_id = job["site_id"]
        shell_id = job.get("element_shell_id") or job.get("element_id")
        elem_id = job.get("machine_element_id")

        # Resolve element ID the same way validate-ports does
        if not elem_id:
            serial = (job.get("serial_number") or "").lower()
            try:
                for elem in (_safe_items(sdk.get.elements(site_id)) or []):
                    eid = elem.get("id")
                    if not eid:
                        continue
                    if not elem_id:
                        elem_id = eid
                    if serial and (elem.get("serial_number") or "").lower() == serial:
                        elem_id = eid
                        break
            except Exception as e:
                _log.warning(f"recheck_network: elements lookup failed: {e}")
            if not elem_id:
                elem_id = job.get("element_id") or shell_id

        net_status = get_network_status(sdk, site_id, elem_id)
        port_config = get_port_config(sdk, site_id, shell_id)
        validation = validate_ports(port_config, net_status)
        all_pass = all(v["result"] == "pass" for v in validation) if validation else False
        has_fail = any(v["result"] == "fail" for v in validation)

        now = datetime.now(timezone.utc).isoformat()
        _update_job(job["id"], network_status=net_status, last_checked=now)
        save_jobs()

        return jsonify({
            "ok": True,
            "network_status": net_status,
            "port_validation": {"ok": True, "validation": validation, "all_pass": all_pass, "has_fail": has_fail},
        })
    except Exception as e:
        reset_sdk()
        return jsonify({"ok": False, "error": str(e)}), 200


@app.route("/api/jobs/<job_id>/validate-ports", methods=["GET"])
def validate_job_ports(job_id):
    with _jobs_lock:
        if job_id not in _jobs:
            return jsonify({"error": "Job not found"}), 404
        job = dict(_jobs[job_id])
    try:
        sdk = get_sdk()
        site_id = job["site_id"]
        shell_id = job.get("element_shell_id") or job.get("element_id")

        # After provisioning, the shell API goes stale. Look up the actual provisioned
        # element(s) for the site — same approach used by fabric_check and get_ha_roles.
        # Match by serial when possible; fall back to first element found.
        elem_id = None
        serial = (job.get("serial_number") or "").lower()
        try:
            for elem in (_safe_items(sdk.get.elements(site_id)) or []):
                eid = elem.get("id")
                if not eid:
                    continue
                if not elem_id:
                    elem_id = eid  # first element as fallback
                if serial and (elem.get("serial_number") or "").lower() == serial:
                    elem_id = eid
                    break
            if elem_id:
                _log.info(f"validate_job_ports: resolved elem_id={elem_id!r} via sdk.get.elements (serial={serial!r})")
        except Exception as e:
            _log.warning(f"validate_job_ports: elements lookup failed: {e}")

        if not elem_id:
            elem_id = job.get("machine_element_id") or job.get("element_id") or shell_id

        _log.info(f"validate_job_ports: shell_id={shell_id!r} elem_id={elem_id!r}")
        port_config = get_port_config(sdk, site_id, shell_id)
        network_status = get_network_status(sdk, site_id, elem_id)
        _log.info(f"validate_job_ports: port_config={port_config}")
        _log.info(f"validate_job_ports: network_status interfaces={network_status.get('interfaces') if network_status else None}")
        validation = validate_ports(port_config, network_status)
        all_pass = all(v["result"] == "pass" for v in validation) if validation else False
        has_fail = any(v["result"] == "fail" for v in validation)
        return jsonify({"ok": True, "validation": validation, "all_pass": all_pass, "has_fail": has_fail})
    except Exception as e:
        reset_sdk()
        return jsonify({"ok": False, "error": str(e), "validation": []}), 200


@app.route("/api/sites/<site_id>/shells", methods=["GET"])
def get_site_shells(site_id):
    """Return unclaimed element shells for a site."""
    try:
        sdk = get_sdk()
        shells = get_available_shells_for_site(sdk, site_id)
        return jsonify({"ok": True, "shells": shells, "ha_site": len(shells) >= 2})
    except Exception as e:
        reset_sdk()
        return jsonify({"ok": False, "error": str(e), "shells": []}), 200


@app.route("/api/sites/<site_id>/shells-with-ports", methods=["GET"])
def get_site_shells_with_ports(site_id):
    """Return unclaimed shells plus their port configs — used to build the HA cabling checklist."""
    try:
        sdk = get_sdk()
        shells = get_available_shells_for_site(sdk, site_id)
        result = []
        for sh in shells:
            shell_id = sh.get("id")
            sn = sh.get("serial_number", "")
            try:
                ports = get_port_config(sdk, site_id, shell_id)
            except Exception:
                ports = {"wan_ports": [], "lan_ports": [], "ha_ports": [], "bypass_pairs": []}
            result.append({"shell_id": shell_id, "serial_number": sn, "ports": ports})
        return jsonify({"ok": True, "shells": result, "ha_site": len(result) >= 2})
    except Exception as e:
        reset_sdk()
        return jsonify({"ok": False, "error": str(e), "shells": []}), 200


@app.route("/api/jobs/<job_id>/ha-status", methods=["GET"])
def get_job_ha_status(job_id):
    """Return HA role (active/backup) for each element at the job's site."""
    with _jobs_lock:
        if job_id not in _jobs:
            return jsonify({"error": "Job not found"}), 404
        job = dict(_jobs[job_id])
    try:
        sdk = get_sdk()
        roles = get_ha_roles(sdk, job["site_id"])
        return jsonify({"ok": True, "roles": roles})
    except Exception as e:
        reset_sdk()
        return jsonify({"ok": False, "error": str(e), "roles": {}}), 200


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
        "saw_active_upgrade": False,
        "network_status": None,
        "last_checked": None,
        "audit_log": [],
    }

    with _jobs_lock:
        _jobs[job["id"]] = job
    _audit(job["id"], "Job created", actor="Admin",
           api="POST /api/jobs",
           result="ok",
           detail=f"serial={serial} site={site_name} shell={element_shell_id}")
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
    app.run(host="0.0.0.0", port=5002, debug=False, ssl_context=ssl_ctx)
