# Upstream attribution and local changes

- Kia UVO integration: https://github.com/Hyundai-Kia-Connect/kia_uvo/tree/v3.22.0
- API library: https://pypi.org/project/hyundai-kia-connect-api/4.45.0/
- API source: https://github.com/Hyundai-Kia-Connect/hyundai_kia_connect_api
- Upstream copyright: Copyright (c) 2021, Fuat Akgun and contributing authors.
- Both included components retain their complete MIT license text. Existing
  source copyright/author notices remain intact. No upstream maintainer is
  represented as responsible for this fork.

Local P0 changes: guarded command admission; bounded typed EU auth recovery;
coordinator-owned auth lifecycle; token validation; deterministic auth resource
cleanup; explicit administrator reconnect; cache backoff and reauth preservation.
The climate payload and vehicle/GPS policy are not modified by the cache fix.

The build input ledger locks all upstream files and patch bytes by SHA256.
The publication ledger locks the source staging archive, metadata templates,
builder and every output member. The SDK is vendored under
`custom_components/kia_uvo/_vendor/hyundai_kia_connect_api`; no global SDK is patched.

No additional license restrictions or invented copyright assignments are added.
Fork owner and codeowner: @jmppp. Public source and support:
https://github.com/jmppp/kia-uvo-p0 and its Issues page.
This is an independently maintained, modified fork, not an upstream release.
