#!/usr/bin/env python3
"""Read the raw shell object and try interface data via direct REST."""
import sys, json, traceback
sys.path.insert(0, '.')
import app as _app

sdk = _app.get_sdk()

site_id  = "1789512862243017496"
shell_id = "1789575191578009296"

sess  = getattr(sdk, 'session', None) or getattr(sdk, '_session', None)
ctrl  = getattr(sdk, 'controller', None) or getattr(sdk, '_parent_controller', None) or ""
ctrl  = ctrl.rstrip('/')

print(f"Controller: {ctrl}\n")

def rest(path, label=None):
    url = ctrl + path
    r = sess.get(url)
    try:
        body = r.json()
    except Exception:
        body = {}
    items = body.get("items", []) if isinstance(body, dict) else []
    lbl = label or path
    print(f"[{r.status_code}] {lbl}  items={len(items)}")
    if items:
        print(json.dumps(items[0], indent=2, default=str))
    elif isinstance(body, dict) and body:
        print(json.dumps(body, indent=2, default=str)[:1000])
    return body, items

print("=== Sites ===")
rest(f"/sdwan/v2.0/api/sites", "sites")
rest(f"/sdwan/v2.0/api/sites/{site_id}", "site single")

print("\n=== Shells ===")
rest(f"/sdwan/v2.0/api/sites/{site_id}/elementshells", "elementshells list")
body, _ = rest(f"/sdwan/v2.0/api/sites/{site_id}/elementshells/{shell_id}", "shell single")

print("\n=== Shell interfaces — all API versions ===")
for ver in ("v2.0", "v2.1", "v2.2", "v3.0", "v3.1", "v4.0"):
    rest(f"/sdwan/{ver}/api/sites/{site_id}/elementshells/{shell_id}/interfaces", ver)

print("\n=== WAN interfaces ===")
for ver in ("v2.0", "v2.1", "v3.0"):
    rest(f"/sdwan/{ver}/api/sites/{site_id}/waninterfaces", ver)

print("\n=== SDK get.sites() ===")
r2 = sdk.get.sites()
print(f"SDK status={getattr(r2,'status_code','?')}")
try:
    d = dict(r2)
    print(json.dumps(d, indent=2, default=str)[:500])
except Exception:
    try:
        print(r2.json())
    except Exception:
        print(repr(r2))

print("\n=== SDK get.elementshells(site, shell) ===")
r3 = sdk.get.elementshells(site_id, shell_id)
print(f"SDK status={getattr(r3,'status_code','?')}")
try:
    d = dict(r3)
    print(json.dumps(d, indent=2, default=str)[:1000])
except Exception:
    try:
        print(json.dumps(r3.json(), indent=2, default=str)[:1000])
    except Exception:
        print(repr(r3)[:500])
