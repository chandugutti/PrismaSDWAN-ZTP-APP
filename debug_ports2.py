#!/usr/bin/env python3
"""Print ALL interface records for the shell + full waninterfaces response."""
import sys, json
sys.path.insert(0, '.')
import app as _app

sdk = _app.get_sdk()

site_id  = "1789512862243017496"
shell_id = "1789575191578009296"

sess = getattr(sdk, 'session', None) or getattr(sdk, '_session', None)
ctrl = (getattr(sdk, 'controller', None) or getattr(sdk, '_parent_controller', None) or "").rstrip('/')

print(f"Controller: {ctrl}\n")

def get(path):
    r = sess.get(ctrl + path)
    try:
        body = r.json()
    except Exception:
        body = {}
    return r.status_code, body

# ── 1. All shell interfaces ──────────────────────────────────────────────────
print("=" * 60)
print("SHELL INTERFACES — v2.2 (all records)")
print("=" * 60)
status, body = get(f"/sdwan/v2.2/api/sites/{site_id}/elementshells/{shell_id}/interfaces")
items = body.get("items", []) if isinstance(body, dict) else []
print(f"HTTP {status} — {len(items)} interfaces\n")
for iface in items:
    name = iface.get("name", "?")
    used_for = iface.get("used_for")
    wan_ids  = iface.get("site_wan_interface_ids")
    lan_nets = iface.get("attached_lan_networks")
    bypass   = iface.get("bypass_pair")
    print(f"  Port {name!r:12s}  used_for={used_for!r:10s}  "
          f"wan_ids={wan_ids}  lan_nets={lan_nets}  bypass={bypass}")
print()

# ── 2. Full waninterfaces response ────────────────────────────────────────────
print("=" * 60)
print("SITE WAN INTERFACES — v2.0 (full records)")
print("=" * 60)
status, body = get(f"/sdwan/v2.0/api/sites/{site_id}/waninterfaces")
wi_items = body.get("items", []) if isinstance(body, dict) else []
print(f"HTTP {status} — {len(wi_items)} WAN interfaces\n")
for wi in wi_items:
    print(json.dumps(wi, indent=2, default=str))
    print("---")
