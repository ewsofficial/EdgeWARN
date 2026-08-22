# CTAM protocol compatibility

The internal API uses explicit integer string versions in both module manifests
and the `X-CTAM-API-Version` request header. The current initial protocol is
v1, mounted at `/internal/ctam/v1`; it has no earlier supported version.

When v2 is introduced, the host must support v2 and v1 for one documented
compatibility window, preserve v1's OpenAPI/schema artifact, and add contract
tests for both versions. A module declaring or requesting an unsupported
version is rejected before execution or receives HTTP 426, respectively.
