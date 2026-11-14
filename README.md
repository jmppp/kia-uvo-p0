# Kia UVO P0

An unofficial, modified fork of Kia UVO 3.22.0 with a private, version-locked
hyundai_kia_connect_api 4.45.0 SDK. This is not an official Hyundai/Kia release
or an upstream release. The integration domain remains `kia_uvo`.

Repository and documentation: https://github.com/jmppp/kia-uvo-p0
Support and bug reports: https://github.com/jmppp/kia-uvo-p0/issues
Owner and codeowner: **@jmppp**.

## Qualified version

- Fixed release reference: **v3.22.0-pgp0.9**.
- Integration: **3.22.0+pgp0.9**.
- Private SDK: **4.45.0+pgp0.9**; no global SDK replacement.
- Qualified target: exactly **Home Assistant Core 2026.11.0 / Python 3.14**.
  Other versions fail closed until separately qualified.
- Distribution identity: **pgp0.9-marketplace-proposal.1**. Python runtime code
  is unchanged from the reviewed pgp0.9 integration.

## Changes from upstream

This fork adds an opt-in guarded climate send boundary, bounded EU authentication
recovery, coordinator-owned authentication work, strict token validation,
authentication resource cleanup and explicit administrator recovery. Reconnect
preserves cache retry limits and ordinary account reauthentication decisions.
The original climate payload is retained. Steering/seat heating capabilities
are not added or claimed by this release.

Guarded services require a compatible separately managed caller contract.
Ordinary upstream services still exist; a caller must explicitly use the guarded
services to receive their protection. No household dashboard, helpers, schedule,
credentials or vehicle configuration is included here.

## Marketplace installation and ownership

Core 2026.11 Marketplace uses `hacs.json` for repository metadata. Add this
repository's actual URL through **Add from link** and choose the fixed release.
The layout is `custom_components/kia_uvo`, `content_in_root: false`,
`zip_release: false`: Marketplace downloads repository source, not a release asset.

**Never install over a directory still owned by the official Kia repository.**
Because both repositories use domain `kia_uvo`, migration needs one owner,
a complete backup, a supervised cold-stop/handover/cold-start procedure and a
matching caller contract. Removing an integration config entry deletes account
configuration; that is not an ownership transfer. Do not edit `.storage` by hand.
Publication of this source is not permission to change a running installation.

`custom_components/kia_uvo/RELEASE.json` records every installed integration
file's SHA256. Release assets provide the deterministic integration-only ZIP and
its complete publication ledger. Verify the fixed tag's commit and file hashes;
do not use a moving branch as an installation pin. Keep historical tags fixed.

## Explicit authentication recovery

Protocol failures pause after three automatic attempts. After the recorded
backoff, an administrator can invoke **Kia UVO: Reconnect** (`kia_uvo.reconnect`)
from Developer Tools → Actions for the affected config entry. It reuses existing
tokens/credentials and cached data, performs one controlled attempt and respects
Retry-After. Diagnostics expose the pause category and earliest allowed attempt.
A genuine credential/OTP/consent failure uses the normal reauthentication flow.

Reconnect never reloads Core, starts/stops climate or invokes Force Refresh.
Ordinary polling after recovery keeps the configured upstream update policy;
reconnect does not make all future polling cache-only. No automatic reload
watchdog is included. Requests already handed to the network cannot be recalled
atomically, and a running synchronous SDK worker must drain before replacement.

## License and attribution

MIT. Original copyright and source notices are retained. See `LICENSE` and
`UPSTREAM.md`, including the private SDK's retained MIT license. Upstream
maintainers are not responsible for this fork. No real account or vehicle
credentials are included.

## Bounded vehicle status wake (EU legacy)

Native Force Refresh and guarded status wake share a persistent account-entry
reservation and at least 1800 seconds between attempts. Unknown outcomes consume
the reservation; unfinished records on startup block automatic force polling.
A later explicit request must pass the original budget. No automatic replay.

This release supports status-only wake for EU legacy vehicles. It does **not**
refresh GPS or driving data and does not claim CCS2 or other-region wake support.
A manual caller requiring fresh GPS can still refuse climate start. The guarded
force service requires an explicit compatible caller contract, not a schedule
included in this repository. No heating duration or original climate payload
is changed. Kia availability, 12 V battery impact and response times are not
guaranteed by the local budget.
