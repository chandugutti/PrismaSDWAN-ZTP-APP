#!/usr/bin/env python3
"""Verify wannetworks and waninterfacelabels SDK calls and print their responses."""
import sys, json
sys.path.insert(0, '.')
import app as _app

sdk = _app.get_sdk()
site_id    = "1789512862243017496"
network_id = "1781213679065024096"
label_id   = "1744752312920008796"

def dump(label, r):
    body = getattr(r, 'cgx_content', None)
    if body is None:
        try: body = r.json()
        except: body = {}
    status = getattr(r, 'status_code', '?')
    print(f"\n{'='*55}")
    print(f"{label}  [{status}]")
    print(json.dumps(body, indent=2, default=str)[:2000])

# 1. Verify waninterfaces SDK call — confirm name field
dump("sdk.get.waninterfaces(site_id)", sdk.get.waninterfaces(site_id))

# 2. WAN network by ID (should return human-readable network name)
dump(f"sdk.get.wannetworks({network_id})", sdk.get.wannetworks(network_id))

# 3. WAN interface label by ID (should return label like "Public Internet")
try:
    dump(f"sdk.get.waninterfacelabels({label_id})", sdk.get.waninterfacelabels(label_id))
except Exception as e:
    print(f"\nwaninterfacelabels failed: {e}")

# 4. List all WAN networks (to see all options)
dump("sdk.get.wannetworks() — all", sdk.get.wannetworks())

# 5. List all WAN interface labels
try:
    dump("sdk.get.waninterfacelabels() — all", sdk.get.waninterfacelabels())
except Exception as e:
    print(f"\nwaninterfacelabels() list failed: {e}")
