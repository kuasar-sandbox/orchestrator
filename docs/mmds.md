# Extensible MMDS Endpoints

Status: initial design.
Scope: standalone nodes.

## 1. Goals

- **Declare endpoints:** the Create request declares multiple bounded GET endpoints at sandbox Create.
- **Controlled guest access:** a source-bound MMDS token permits only exact paths owned by that sandbox.
- **Store lifecycle:** a value provider configures, updates, or revokes a store value independently from sandbox restart.
- **Relay access:** a relay endpoint uses an encrypted, independently rotatable and revocable relay auth value to GET a fixed validated HTTPS upstream; the platform does not persist upstream data.
- **Deployment parity:** internal and external modes expose the same endpoint and error semantics.
- **Secret confinement:** relay auth and store value plaintext do not enter persisted sandbox metadata, shared routes, logs, metrics, or dumps.

Relay data changes at the upstream; store values and relay auth values change through the admin API. Relay URL and auth header name are immutable.

## 2. Constraints and limitations

Store data or relay auth can arrive after guest start during sandbox startup or resume. A uniform node-policy wait of 2-5 seconds covers the common race, but applications must keep polling. This wait is independent from the shared 30-second route park.

## 3. Endpoint declaration

The design distinguishes four roles without prescribing which product component owns them: the Create caller supplies immutable endpoint definitions; a value provider manages mutable store values and relay auth through the authenticated admin UDS; the node validates, persists, synchronizes, and serves endpoints; and the guest only reads paths declared for its sandbox. The namespace and header are transport mechanisms, not proof of identity; deployments must authenticate and authorize the Create caller according to their API trust boundary.

The decoded `kuasar-sandbox.mmds` namespace payload below illustrates two configurable endpoints: relay-backed `credentials` and store-backed `user-data`. Neither name nor path is built in:

~~~yaml
schema_version: 1
endpoints:
  - name: credentials
    path: /latest/meta-data/credentials
    backend:
      type: relay
      url: https://identity.example.com/credentials
      auth:
        header_name: X-Upstream-Assertion
  - name: user-data
    path: /latest/user-data
    backend:
      type: store
~~~

This namespace is accepted only by sandbox Create; template register/build endpoints reject it. On the Create wire, the e2b metadata map carries this payload as a JSON string under `kuasar-sandbox.mmds`. `X-Kuasar-Sandbox-MMDS` is the alternate injection surface and carries the same JSON object directly as its header value. If both are present, the header wins. The selected JSON must decode and validate or Create fails before launch. API ingress removes the namespace from generic guest metadata; validated endpoint configuration is persisted only in the dedicated MMDS table.

Node policy owns resource limits:

This is the conductor's node policy — declaration/persistence policy plus the conductor's own internal-mode relay-serving runtime:

~~~yaml
mmds:
  endpoints:
    enabled: false
    max_endpoints_per_sandbox: 16
    max_metadata_bytes: 65536
    max_path_bytes: 512
    reserved_path_prefixes:
      - /latest/api/
      - /internal/
    max_store_value_bytes: 16384
    max_relay_url_bytes: 2048
    max_relay_auth_bytes: 16384
    # relay-serving runtime (shared shape with the external proxy's own copy below)
    value_wait_timeout: 3s
    relay_request_timeout: 2s
    max_relay_response_bytes: 65536
    max_relay_decompressed_bytes: 262144
    max_relay_inflight_per_sandbox: 4
    max_relay_requests_per_second: 2
    max_relay_dns_answers: 16
~~~

The conductor is always the sole owner of declaration/persistence policy (name/path rules, endpoint counts, store/relay-url/auth size caps), regardless of `proxy.mode`. In external mode, the proxy master's own config carries a second, independently maintained copy of only the relay-serving runtime subset, plus its own worker-RPC settings — declaration/persistence policy has no meaning there, since the proxy master never validates a declaration itself; it trusts and mirrors whatever the conductor already validated and decided to sync:

~~~yaml
mmds:
  enabled: true
  listen: 127.0.0.1:19254
  endpoints:
    enabled: false
    value_wait_timeout: 3s
    relay_request_timeout: 2s
    max_relay_response_bytes: 65536
    max_relay_decompressed_bytes: 262144
    max_relay_inflight_per_sandbox: 4
    max_relay_requests_per_second: 2
    max_relay_dns_answers: 16
    max_worker_inflight: 128
    max_worker_rpc_frame_bytes: 524288
~~~

The proxy master's in-memory endpoint table capacity (bounding how many endpoints' decrypted secret plaintext it holds in process memory at once) is not operator-configurable — an internal, fixed safety ceiling, since the conductor is the sole source of truth for how many endpoints legitimately exist.

Sizes are integer bytes. Create input cannot raise these limits. When `enabled` is false, Create requests containing the MMDS namespace fail 400 and Admin MMDS routes are not registered; the configuration is never silently ignored. Built-in MMDS token/metadata behavior remains enabled by its existing setting. Existing endpoint ciphertext remains persisted but is neither loaded nor served until re-enabled. At startup, enabled mode validates every persisted definition's name/path against current limits and reserved prefixes, and every sandbox's declared endpoint count against `max_endpoints_per_sandbox`; any conflict or undecryptable present value fails conductor startup with a non-secret diagnostic rather than truncating, grandfathering, or silently dropping data. `relay_request_timeout` is the total deadline; implementation phase limits must fit inside it.

Name is the stable control-plane identifier, is guest-invisible, case-sensitive lowercase, and matches `[a-z][a-z0-9_-]{0,62}`. Name and path are immutable and unique per sandbox. A path may be any validated absolute path, but it must not equal or fall under a built-in route or an operator-configured reserved prefix. Reserved-prefix matching is segment-aware. Conflicts fail Create before launch. Path segments use bounded lowercase ASCII `[a-z0-9._-]`; percent escapes, empty segments, dot segments, backslashes, query, fragments, trailing slash, wildcards, and host patterns are rejected before ServeMux canonicalization. Guest access is exact GET only: no body, query, method list, or automatic redirect.

`backend` and `backend.type` are mandatory and never inferred. `type: relay` requires `url` and a non-null `auth` containing exactly `header_name`, and permits no other fields; `type: store` contains only `type`. Unknown backend types or fields, null backends, and fields belonging to another backend type fail Create before launch. Relay URL and auth header name are immutable and declared in the Create request. The auth value is not accepted in Create metadata and is configured, rotated, or revoked only by a value provider through the admin API; upstream data changes at the upstream. Node policy bounds endpoint count and metadata/secret sizes. Guest input never selects name, URL, auth header, or handler. `schema_version` is mandatory; unknown values fail Create and do not imply a product-version roadmap. Fixed implementation limits also bound header-name and Content-Type lengths; Content-Type must parse as a media type, and relay auth values reject NUL, CR, and LF; effective limits are always the minimum of requested values and node policy.

## 4. Persistence

~~~text
sandbox_mmds_endpoints(
  sandbox_id, name, path, backend_type,
  public_config_json, secret_ciphertext,
  content_type, expires_unix,
  revision, value_present,
  created_unix, updated_unix,
  PRIMARY KEY(sandbox_id,name),
  UNIQUE(sandbox_id,path),
  FOREIGN KEY(sandbox_id) REFERENCES sandboxes(id) ON DELETE CASCADE
)
~~~

public_config_json holds validated non-secrets, including the relay URL and auth header name. secret_ciphertext holds a relay auth value or store value, encrypted with internal/secretbox. AEAD associated data binds record type, sandbox ID, name, backend type, and revision. Relay auth plaintext enters only through its authenticated admin PUT request and is never accepted in Create metadata. Secrets never enter persisted sandbox JSON, routes, proxyshm, export/import, logs, metrics, or dumps.

Create validates and extracts the MMDS namespace before any runtime side effect. A new store transaction primitive inserts the sandbox row and all revision-0 endpoint rows atomically; failure commits neither. SQLite foreign keys are enabled on every connection, and sandbox deletion plus endpoint cascade is one transaction. Ordinary later sandbox upserts never replace immutable endpoint definitions.

## 5. Store backend

~~~text
PUT    /internal/admin/sandboxes/{id}/mmds/{name}
DELETE /internal/admin/sandboxes/{id}/mmds/{name}
~~~

These routes live only on the conductor config-socket admin plane, never the public API listener. Every accepted Unix connection is checked with SO_PEERCRED; socket mode is 0600, and production enables the existing admin_pidfile PID allowlist (same-UID socket-only fallback is for development). PUT accepts a bounded opaque body, optional Content-Type (default `application/octet-stream`), and optional `X-Kuasar-MMDS-Expires-Unix` (decimal Unix seconds; omitted or 0 means no expiry). Each PUT replaces the complete value: omitted Content-Type resets to `application/octet-stream`, and omitted expiration resets to 0. Content-Type, ciphertext, expiration, and the internal revision are persisted atomically with last-write-wins semantics. An absent or wrong-backend endpoint returns 404. DELETE clears ciphertext, Content-Type, expiration, and `value_present`, then increments revision. No admin API returns store plaintext or exposes a separate status route. Successful PUT/DELETE returns 204. Malformed input returns 400. The mutation response follows the durable SQLite commit and does not wait for external sync acknowledgement; disconnected external mode therefore returns 503 to guests until resync.

For Guest GET, `expires_unix > 0 && now >= expires_unix` is treated as absent and returns 404. Expiration alone does not change revision. Proxy master enforces it from its local clock while its synced table is available.

Only a never-configured endpoint (`revision=0`, `value_present=false`) waits for the uniform node-policy value timeout (default 3 seconds, hard range 2-5 seconds). The Create request cannot override it. PUT and DELETE wake waiters. A PUT during the wait makes the waiting Guest GET return 200; timeout returns 404. Deleted (`revision>0`, `value_present=false`) or expired values return 404 immediately;
sync failure returns 503. Late data is seen by a later poll. Fetch-once clients are not guaranteed.

## 6. Relay backend and mandatory SSRF policy

```text
PUT    /internal/admin/sandboxes/{id}/mmds/{name}/auth
DELETE /internal/admin/sandboxes/{id}/mmds/{name}/auth
```

These write-only endpoints use the same config-socket admin authentication as store. PUT accepts a bounded opaque auth value up to `max_relay_auth_bytes`. PUT replaces the complete auth value and DELETE clears it; each operation atomically increments an internal revision used only to order endpoint sync updates. Writes use last-write-wins semantics. No API returns auth plaintext or exposes a separate status route. A never-configured auth (`revision=0`, `value_present=false`) waits for the same uniform node-policy value timeout used by store; PUT during the wait proceeds with the relay request, while timeout returns 404. Revoked auth (`revision>0`, `value_present=false`) returns 404 immediately. No missing-auth state contacts the upstream; auth PUT and DELETE wake waiters. URL and auth header name remain fixed for the sandbox lifetime.

~~~text
guest -> exact route + source-bound token -> policy/rate limit
      -> resolve, validate, and pin IP -> HTTPS + auth header -> bounded response
~~~

The guest never sees URL or auth. The relay handler constructs a new GET to the fixed configured URL. Guest query, body, and headers are not forwarded; an optional platform-controlled `Accept` may be added. The configured auth header is injected only after URL, DNS, peer, header-name, and header-value validation. The client advertises only identity and gzip encoding; compressed and decompressed byte counts are bounded independently.

The declared URL and auth header name, and the value-provider-injected auth value, are all untrusted input. Every request must:

1. allow HTTPS hostnames only; reject IP literals, userinfo, query, fragments, malformed hosts, and unsupported ports;
2. validate every DNS answer, reject the whole resolution if any answer is disallowed, and reject IPv4/IPv6 loopback, unspecified, multicast, link-local,
   private, carrier-grade NAT, documentation, benchmark, and other non-global addresses, always
   including 169.254.0.0/16;
3. connect directly to one validated IP, use original host for SNI/certificate/Host, never re-resolve,
   and verify the connected peer equals the pinned IP before sending the auth;
4. disable environment proxies and reject every redirect;
5. bound DNS answers, timeouts, request/response/decompressed bytes, concurrency, and per-endpoint rate;
6. validate `auth.header_name` with the HTTP field-name grammar and reject Host, Content-Length, hop-by-hop, routing, forwarding, or proxy headers;
7. never log auth, bodies, or sensitive URL material.

Operator policy may further restrict domains, ports, public CIDRs, or CAs, never weaken these checks. Redirect following is outside this design.

## 7. Authentication and responses

`PUT /latest/api/token` requires exactly one `X-metadata-token-ttl-seconds` header containing a decimal integer from 1 through 21600. Missing, duplicate, malformed, zero, negative, or out-of-range values return 400 without issuing a token. Success returns 200 with a plain-text opaque token and echoes the accepted value in `X-metadata-token-ttl-seconds`. The token internal encoding is not part of the Guest API contract; it must be integrity-protected, expire after the requested TTL, and bind the sandbox ID, source IP, current sandbox incarnation (the launch/resume run ID), and audience `mmds`. Every protected GET re-resolves the current source IP and verifies all fields against current sandbox/incarnation state. The internal source and existing route sync therefore expose the current run ID as the MMDS incarnation; pause/resume changes it and invalidates old tokens. The implementation may use a stateless signed token; tokens are reusable rather than single-use. Reuse from the same source, sandbox, and incarnation within TTL is accepted; cross-source, cross-sandbox, pre-resume/snapshot, and expired-token replay fail.

The Create request may declare endpoints. An outer raw-path guard rejects malformed/encoded paths before ServeMux canonicalization. ServeMux keeps the built-in token and metadata routes plus one GET fallback; after source and token authentication, that fallback performs an exact `(sandbox_id, path)` lookup in the immutable endpoint registry. The guest path never becomes a backend name, URL, or handler identifier. The existing Sandbox GET representation exposes only redacted endpoint status (`name`, `path`, `backend_type`, `configured`, `revision`, and `expired`); relay auth and store plaintext are never returned. A value provider is an authenticated service on the local admin UDS and may PUT or DELETE store values and PUT or DELETE relay auth values. Guest may only GET the MMDS paths declared for its sandbox. Path, name, backend type, relay URL, and auth header name are immutable. Relay auth value rotation or revocation takes effect without restarting the sandbox.

~~~text
200 success
204 admin mutation success
400 malformed request
401 invalid token
404 unknown endpoint, absent/deleted store value, or absent/revoked relay auth
405 non-GET method
429 relay rate limit
502 unusable upstream response
503 sync/handler unavailable
504 relay timeout
~~~

All guest responses use `Cache-Control: no-store`, `X-Content-Type-Options: nosniff`, and a bounded body. Store returns its persisted Content-Type plus `X-Kuasar-MMDS-Revision`. Relay passes through the upstream status and bounded body for final 2xx/4xx/5xx responses, preserves only a validated Content-Type, and strips all other upstream headers; 1xx, 3xx, malformed, unsupported-encoding, or limit-violating responses become 502. Relay does not invent a revision for upstream content.

## 8. External process topology

~~~text
conductor: encrypted generic MMDS authority
       | generic endpoint sync
       v
proxy master: bounded endpoint table + compiled store/relay handlers
       | inherited socketpair per worker
       v
MMDS worker -> guest
~~~

Internal mode runs the same registry and handlers in conductor. External mode adds an independent MMDS message family to the proxy master's existing authenticated config-socket plugin stream; workers never dial conductor. The proxy registration explicitly advertises the MMDS-endpoint capability; absence or version mismatch has no downgrade path and keeps configurable endpoints unavailable with 503. On connect, the master marks its table unavailable, clears old plaintext, stages one bounded full generation, and atomically makes it available only at the matching bookmark. A disconnect or protocol error clears staging and live plaintext and returns 503 until a new full generation completes.

~~~text
MMDSSyncBegin {generation}
MMDSEndpointUpsert {generation,sandbox_id,name,path,backend_type,revision,value_present,content_type,expires_unix,public_config,secret_plaintext}
MMDSEndpointDelete {generation,sandbox_id,name,revision}
MMDSSyncBookmark {generation}
~~~

Authority subscribes to live changes before scanning one consistent database snapshot, so mutations racing the scan are replayed after its bookmark. Full-sync bookmark drops endpoints not seen in that generation. Live Upsert/Delete follows the bookmark; lower revisions are ignored, equal revisions are idempotent only when payload hashes match, and an equal-revision mismatch is a protocol error that forces resync. `secret_plaintext` exists only on the authenticated local stream and in bounded process buffers. Admin mutation success does not wait for this stream.

Shared mmap remains for high-rate non-secret routes; MMDS values and relay auth never enter it. For every worker start, master creates `AF_UNIX SOCK_STREAM | SOCK_CLOEXEC` socketpair endpoints before entering the worker network namespace, retains one endpoint, and passes exactly the other via ExtraFiles. Both sides use a length-prefixed bounded frame codec, one serialized writer, and one reader pump:

~~~text
EndpointRequest  {request_id,sandbox_id,method,path,deadline_unix_ms}
EndpointCancel   {request_id}
EndpointResponse {request_id,status,content_type,headers,body,error_code}
~~~

The worker authenticates source and token first, then sends only GET plus the exact guest path; master resolves `(sandbox_id,path)` and never accepts a backend name or URL from the worker. Request IDs multiplex concurrent calls. Per-worker and per-sandbox inflight limits, frame/body limits, deadlines, cancellation, duplicate-ID rejection, and slow-writer backpressure are mandatory. Worker socket close cancels its inflight calls; worker restart creates a fresh pair.

## 9. Plaintext boundary

Store plaintext is limited to conductor request/sync buffers, proxy-master bounded memory, current
worker responses, and guest memory. Relay auth is limited to conductor request/sync buffers
and the relay handler while building the validated request. Upstream response is transient and not
persisted. Handlers use bounded buffers, no body logging, disabled dumps, least privilege,
NoNewPrivileges, minimal capabilities, restricted filesystems, and best-effort clearing.

## 10. Failure semantics

| Failure | Required behavior |
|---|---|
| duplicate/invalid endpoint | fail Create before launch |
| sandbox/endpoint transaction failure | commit neither; do not publish MMDS state |
| store encryption/persistence failure | fail the mutation; old revision remains |
| relay auth update encryption/persistence failure | fail the update; old revision remains |
| relay auth never configured | bounded value wait, then 404 without contacting upstream |
| relay auth revoked | immediate 404 without contacting upstream |
| store data arrives after wait | current request may be 404; later poll sees it |
| store value expires | Guest gets 404; the redacted Sandbox GET status marks it expired; revision is unchanged |
| delete races response | best-effort cancel; data already sent cannot be retracted |
| blocked/mixed DNS answer | reject without auth |
| peer differs from pinned IP | abort before auth |
| relay timeout/oversize/rate | bounded 504/502/429 |
| sync disconnect/protocol error | clear plaintext tables; return 503 until a valid full generation/bookmark |
| worker RPC close/timeout/backpressure | cancel bounded inflight work; return 503/504 as classified |
| master restart | clear plaintext and full resync |
| sandbox delete | remove every endpoint and value |

## 11. Observability

~~~text
mmds_requests_total{backend_type,result}
mmds_value_wait_seconds
mmds_relay_requests_total{result}
mmds_relay_latency_seconds
mmds_relay_blocked_total{reason}
mmds_sync_connected
mmds_cache_entries{backend_type}
mmds_admin_mutations_total{backend_type,op,result}
mmds_worker_rpc_inflight
mmds_worker_rpc_errors_total{reason}
~~~

Only bounded enums are labels. Endpoint names, URLs, hosts, relay auth values, store values, bodies, tokens, and sandbox
IDs are never labels. Admin audit records the authenticated value provider, sandbox, endpoint name, operation, revision, and result only.

## 12. Implementation sequence

1. Add `kuasar-sandbox.mmds`/Header ingress, strict schema types, raw-path validation, redaction, and policy limits.
2. Add SQLite schema, foreign-key enforcement, sandbox+endpoint Create transaction, atomic last-write-wins mutations, cascade delete, and migration tests.
3. Add config-socket Admin PUT/DELETE, SO_PEERCRED/PID gating, expiration headers, audit, and status projection.
4. Add the in-memory endpoint authority, revisioned notifications, initial-value waiters, and internal-mode store handler.
5. Replace the MMDS token with an opaque, expiring, source/incarnation-bound format and enforce Firecracker-compatible TTL request semantics, add exact `(sandbox_id,path)` dispatch while preserving built-in routes.
6. Add the relay client with mandatory SSRF/DNS pinning/TLS/header/redirect/rate/size/response-filter defenses and cancellation on auth revision changes.
7. Extend the existing proxy plugin stream with capability negotiation, MMDS full-generation sync, live revisions, disconnect clearing, and protocol tests.
8. Add the proxy-master bounded endpoint table and shared compiled store/relay handlers.
9. Add per-worker inherited socketpairs, framed multiplexed RPC, cancellation, deadlines, backpressure, worker restart, and namespace tests.
10. Add metrics, secret-log scanning, process hardening, fault injection, compatibility tests for existing envd MMDS calls, and rollout configuration.

## 13. Acceptance tests

- metadata-vs-Header precedence, Create-only rejection on build APIs, disabled-feature rejection with built-in compatibility, JSON errors, schema version, tagged-union, built-in and operator-reserved path rejection, name/path/node-total bounds, duplicates, redaction, and exact GET routing;
- token TTL header required/duplicate/format/1..21600 bounds/response echo, source/sandbox/incarnation binding, tamper, expiry, built-in compatibility, and cross-source/cross-sandbox/pre-resume replay;
- Admin PUT/DELETE last-write-wins behavior, Content-Type, expiration and clock skew, encryption, Create transaction rollback, cascade delete, delete races, initial wait versus immediate deleted state, polling, and park independence;
- Create-time URL/auth-header declaration, value-provider-only write-only auth API, bounded bootstrap wait, rotation/revocation without sandbox restart, and upstream behavior;
- HTTPS-only plus IPv4/IPv6 private/link-local/metadata/mixed/rebinding/redirect/peer rejection;
- auth-header filtering, encrypted persistence, plaintext boundaries, relay-auth absence from guest responses, store-value presence only in its intended guest response, and absence of both secrets from logs, metrics, and errors;
- relay timeout, rate, concurrency, size, decompression, cancellation, and backpressure;
- sync capability negotiation, generation staging/swap, equal-revision mismatch, disconnect plaintext clearing, reconnect/full/bookmark/stale revision handling;
- socketpair FD ownership, restart, namespace separation, request-ID multiplexing, framing bounds, cancellation, timeout, duplicate IDs, slow-consumer isolation, and internal/external parity;
- sandbox deletion cleanup.
