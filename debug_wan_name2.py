#!/usr/bin/env python3
"""Inspect the SDK waninterfaces 200 response to find circuit names."""
import sys, json
sys.path.insert(0, '.')
import app as _app

sdk  = _app.get_sdk()
site_id = "1789512862243017496"

print("=== sdk.get.waninterfaces(site_id) ===")
r = sdk.get.waninterfaces(site_id)
print(f"type(r)      = {type(r)}")
print(f"status_code  = {getattr(r, 'status_code', '?')}")

# Try every known attribute that holds the body
for attr in ("cgx_content", "_content", "text", "content"):
    val = getattr(r, attr, "NOT_FOUND")
    if val != "NOT_FOUND":
        print(f"r.{attr} = {repr(val)[:500]}")

# Try .json()
try:
    body = r.json()
    print(f"\nr.json() type = {type(body)}")
    print(json.dumps(body, indent=2, default=str)[:3000])
except Exception as e:
    print(f"r.json() failed: {e}")

# Try iterating (SDK responses often support __iter__)
try:
    d = dict(r)
    print(f"\ndict(r) = {json.dumps(d, indent=2, default=str)[:3000]}")
except Exception as e:
    print(f"dict(r) failed: {e}")

# Try __getitem__
try:
    items = r["items"]
    print(f"\nr['items'] = {json.dumps(items, indent=2, default=str)[:2000]}")
except Exception as e:
    print(f"r['items'] failed: {e}")

print("\n=== Check what URL was actually called ===")
# The SDK session intercepts requests — check request history
try:
    req = getattr(r, 'request', None)
    if req:
        print(f"URL: {req.url}")
except Exception as e:
    print(f"No request attr: {e}")
