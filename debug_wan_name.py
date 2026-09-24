#!/usr/bin/env python3
"""Find where the circuit name lives for WAN interface ID 1789520523208014396."""
import sys, json
sys.path.insert(0, '.')
import app as _app

sdk  = _app.get_sdk()
sess = getattr(sdk, 'session', None) or getattr(sdk, '_session', None)
ctrl = (getattr(sdk, 'controller', None) or "").rstrip('/')

site_id  = "1789512862243017496"
shell_id = "1789575191578009296"
wan_id   = "1789520523208014396"

def try_get(path, label):
    url = ctrl + path
    try:
        r = sess.get(url)
        try:
            body = r.json()
        except Exception:
            body = {}
        status = r.status_code
        if status == 200:
            print(f"✅ {label}")
            print(json.dumps(body, indent=2, default=str)[:2000])
        else:
            items = body.get("items", []) if isinstance(body, dict) else []
            print(f"   [{status}] {label}  items={len(items)}")
    except Exception as e:
        print(f"   [ERR] {label}: {e}")
    print()

print(f"Controller: {ctrl}")
print(f"WAN interface ID to find: {wan_id}\n")
print("=" * 60)

# Site-level waninterfaces — multiple versions
for ver in ("v2.0", "v2.1", "v2.2", "v3.0", "v4.0"):
    try_get(f"/sdwan/{ver}/api/sites/{site_id}/waninterfaces", f"site waninterfaces list {ver}")
    try_get(f"/sdwan/{ver}/api/sites/{site_id}/waninterfaces/{wan_id}", f"site waninterfaces/{wan_id} {ver}")

# Element-shell-level waninterfaces
for ver in ("v2.0", "v2.1", "v2.2"):
    try_get(f"/sdwan/{ver}/api/sites/{site_id}/elementshells/{shell_id}/waninterfaces", f"elementshell waninterfaces {ver}")

# Global (no site scope)
for ver in ("v2.0", "v2.1"):
    try_get(f"/sdwan/{ver}/api/waninterfaces", f"global waninterfaces {ver}")
    try_get(f"/sdwan/{ver}/api/waninterfaces/{wan_id}", f"global waninterfaces/{wan_id} {ver}")

# SDK call
print("=== SDK sdk.get.waninterfaces(site_id) ===")
try:
    r = sdk.get.waninterfaces(site_id)
    print(f"status={getattr(r,'status_code','?')}")
    try:
        d = dict(r)
        print(json.dumps(d, indent=2, default=str)[:1000])
    except Exception:
        print(repr(r)[:500])
except Exception as e:
    print(f"ERR: {e}")
