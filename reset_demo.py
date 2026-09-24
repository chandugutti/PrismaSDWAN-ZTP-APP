#!/usr/bin/env python3
"""One-shot demo reset: unclaims the machine from the controller so it returns to Unclaimed state."""
import sys, os, re, datetime as _dt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from prismasase_settings import PRISMASASE_CLIENT_ID, PRISMASASE_CLIENT_SECRET, PRISMASASE_TSG_ID
except ImportError:
    print("ERROR: prismasase_settings.py not found"); sys.exit(1)

try:
    import prisma_sase as sdk_module
except ImportError:
    import cloudgenix as sdk_module

SERIAL = "024701-000682-4264"

def norm(s):
    return re.sub(r'[^a-zA-Z0-9]', '', s or '').lower()

def main():
    print("Connecting to Prisma SASE…")
    sdk = sdk_module.API()
    if not hasattr(sdk, 'jwt_expires_at'):
        sdk.jwt_expires_at = _dt.datetime.now() + _dt.timedelta(hours=1)
    sdk.set_debug(0)
    sdk.interactive.login_secret(
        client_id=PRISMASASE_CLIENT_ID,
        client_secret=PRISMASASE_CLIENT_SECRET,
        tsg_id=PRISMASASE_TSG_ID,
    )
    print("Connected.")

    # --- Find machine ---
    print(f"Looking for machine with serial {SERIAL}…")
    machines_resp = sdk.get.machines()
    machines = getattr(machines_resp, 'cgx_content', {})
    if isinstance(machines, dict):
        machines = machines.get('items', [])
    machine = None
    for m in machines:
        if norm(m.get('serial_number')) == norm(SERIAL) or norm(m.get('hw_id')) == norm(SERIAL):
            machine = m
            break
    if not machine:
        print("ERROR: Machine not found."); sys.exit(1)

    machine_id = machine['id']
    m_state = machine.get('machine_state', '')
    element_id = machine.get('element_id')
    element_shell_id = machine.get('element_shell_id')
    print(f"Found: id={machine_id} state={m_state!r} element_id={element_id!r} element_shell_id={element_shell_id!r}")

    if m_state.lower() not in ('claimed', 'claim_pending', 'allocated'):
        print(f"Machine is already in state {m_state!r} — nothing to reset.")
        sys.exit(0)

    # --- Find element at Branch-Site-1 (known site from the job) ---
    KNOWN_SITE_ID = "1742323768149024796"
    KNOWN_SHELL_ID = "1789415642253009196"  # ztp-test shell

    # After full claim, element fields on machine may be cleared.
    # Search elements at the known site directly.
    target_element_id = element_id or element_shell_id
    site_id = None

    print(f"Searching elements at Branch-Site-1 (site {KNOWN_SITE_ID})…")
    elems_resp = sdk.get.elements(KNOWN_SITE_ID)
    elems = getattr(elems_resp, 'cgx_content', {})
    if isinstance(elems, dict):
        elems = elems.get('items', [])
    print(f"  Found {len(elems)} element(s):")
    for el in elems:
        eid = el.get('id', '')
        ename = el.get('name', '')
        estate = el.get('state', '')
        eserial = el.get('serial_number', '')
        print(f"    id={eid} name={ename!r} state={estate!r} serial={eserial!r}")
        if norm(eserial) == norm(SERIAL) or eid == KNOWN_SHELL_ID:
            target_element_id = eid
            site_id = KNOWN_SITE_ID
            print(f"  → Matched: {eid}")

    if not target_element_id or not site_id:
        print("\nElement not found at Branch-Site-1. Scanning all sites…")
        sites_resp = sdk.get.sites()
        sites = getattr(sites_resp, 'cgx_content', {})
        if isinstance(sites, dict):
            sites = sites.get('items', [])
        for s in sites:
            if not isinstance(s, dict) or not s.get('id'):
                continue
            er = sdk.get.elements(s['id'])
            es = getattr(er, 'cgx_content', {})
            if isinstance(es, dict):
                es = es.get('items', [])
            for el in es:
                if norm(el.get('serial_number')) == norm(SERIAL):
                    target_element_id = el['id']
                    site_id = s['id']
                    print(f"  Found at site {s.get('name', s['id'])}: element_id={target_element_id}")
                    break
            if site_id:
                break

    if not target_element_id or not site_id:
        print("\nERROR: Could not find element for this machine in any site.")
        print("Delete it manually: Prisma GUI → Devices → ION Devices → Branch-Site-1 → ztp-test → Delete")
        sys.exit(1)

    # --- Delete element to unclaim machine ---
    print(f"\nDeleting element {target_element_id} from site {site_id}…")
    del_resp = sdk.delete.elements(site_id, target_element_id)
    ok = getattr(del_resp, 'cgx_status', False)
    code = getattr(del_resp, 'status_code', None)
    content = getattr(del_resp, 'cgx_content', '')
    print(f"Delete response: status={code} ok={ok}")

    if ok:
        print("\n✓ Done — machine should return to Unclaimed in the controller within ~30 seconds.")
    else:
        print(f"\nDelete failed (HTTP {code}): {content}")
        print("Delete it manually: Prisma GUI → Devices → ION Devices → Branch-Site-1 → ztp-test → Delete")

if __name__ == '__main__':
    main()
