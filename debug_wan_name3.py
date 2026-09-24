#!/usr/bin/env python3
"""Find circuit name via network_id and label_id lookups."""
import sys, json
sys.path.insert(0, '.')
import app as _app

sdk     = _app.get_sdk()
sess    = getattr(sdk, 'session', None)
ctrl    = (getattr(sdk, 'controller', None) or "").rstrip('/')
site_id = "1789512862243017496"

network_id = "1781213679065024096"
label_id   = "1744752312920008796"

def show(label, r_or_body):
    """Print response content."""
    if hasattr(r_or_body, 'cgx_content'):
        body = r_or_body.cgx_content
    elif hasattr(r_or_body, 'json'):
        try: body = r_or_body.json()
        except: body = {}
    else:
        body = r_or_body
    status = getattr(r_or_body, 'status_code', '?')
    items = body.get('items', []) if isinstance(body, dict) else []
    print(f"\n{'='*55}")
    print(f"{label}  [{status}]  items={len(items)}")
    print(json.dumps(body, indent=2, default=str)[:3000])

# --- WAN networks (tenant-level — has the human-readable circuit name) ---
show("sdk.get.wannetworks()", sdk.get.wannetworks())

# SDK by network_id
try:
    show(f"sdk.get.wannetworks(network_id)", sdk.get.wannetworks(network_id))
except Exception as e:
    print(f"wannetworks(id) failed: {e}")

# --- WAN labels ---
show("sdk.get.wanlabels()", sdk.get.wanlabels())

# SDK by label_id
try:
    show(f"sdk.get.wanlabels(label_id)", sdk.get.wanlabels(label_id))
except Exception as e:
    print(f"wanlabels(id) failed: {e}")

# --- Site WAN networks (site-scoped variant if it exists) ---
try:
    show(f"sdk.get.site_wannetworks(site_id)", sdk.get.site_wannetworks(site_id))
except Exception as e:
    print(f"site_wannetworks failed: {e}")

# --- Direct REST for the network_id using v2.10 ---
print(f"\n{'='*55}")
print("Direct REST at v2.10 for wannetworks")
for path in (
    f"/sdwan/v2.0/api/wannetworks/{network_id}",
    f"/sdwan/v2.10/api/wannetworks/{network_id}",
    f"/sdwan/v2.0/api/wannetworks",
    f"/sdwan/v2.10/api/wannetworks",
):
    try:
        r = sess.get(ctrl + path)
        body = r.json() if r.status_code == 200 else {}
        items = body.get('items', []) if isinstance(body, dict) else []
        if r.status_code == 200:
            print(f"✅ [{r.status_code}] {path}  items={len(items)}")
            print(json.dumps(body, indent=2, default=str)[:1500])
        else:
            print(f"   [{r.status_code}] {path}")
    except Exception as e:
        print(f"   [ERR] {path}: {e}")
