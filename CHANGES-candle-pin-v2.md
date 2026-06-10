# Branch Summary: `candle-pin-v2` — AAP-73765 Candlepin Full Lifecycle

## What this branch does

Adds full Candlepin consumer identity certificate support to metrics-utility, enabling AAP Controller instances to authenticate analytics uploads to `cert.console.redhat.com` using **mTLS** instead of service-account OAuth2 credentials.

The implementation is **standalone** — metrics-utility handles every stage of the lifecycle (registration, check-in, proactive renewal, and upload) without requiring the AWX Analytics Collector to be running, and without needing any database connection in the default configuration.

---

## New modules

### `metrics_utility/library/candlepin/client.py` — `CandlepinClient`

Thin REST client for the Candlepin subscription service API. All post-registration calls use the consumer identity certificate (mTLS), matching the pattern used by `subscription-manager`.

| Method | Endpoint | Auth | Purpose |
|---|---|---|---|
| `register_consumer` | `POST /consumers?owner={org}` | Basic (username/password) | Register this AAP instance; returns `(cert_pem, key_pem, consumer_uuid)` |
| `checkin` | `PUT /consumers/{uuid}` | mTLS | Reset inactivity timer; best-effort, never raises |
| `regenerate_cert` | `POST /consumers/{uuid}` | mTLS | Force certificate renewal; raises `RuntimeError` on failure |

TLS server verification is enabled by default. Pass `candlepin_ca` for a custom CA bundle (e.g. `/etc/rhsm/ca/redhat-uep.pem`) or `verify_tls=False` only in test environments. Proxy support normalises the supplied URL for both `https` and `http` keys.

Temp PEM files written for mTLS calls use mode `0o600` and are unconditionally deleted on context-manager exit, even on exception.

---

### `metrics_utility/library/candlepin/lifecycle.py`

Certificate inspection and lifecycle orchestration.

| Function | Purpose |
|---|---|
| `parse_cert(pem_text)` | Parse a PEM cert; return serial, CN, issuer, expiry, days remaining |
| `is_cert_valid(cert_pem)` | Guard check — parseable, not-before <= now, not yet expired; uses `cert.not_valid_before/after_utc` directly (no ISO-string roundtrip) |
| `needs_renewal(pem_text, days_before_expiry)` | `True` if cert expires within the threshold or is already expired |
| `run_candlepin_lifecycle(...)` | Per-gather lifecycle: check-in → proactive renewal if within threshold; returns `(cert_pem, key_pem)` (original or renewed) |
| `get_candlepin_url()` | Reads `METRICS_UTILITY_CANDLEPIN_URL` |
| `get_renewal_days()` | Reads `METRICS_UTILITY_CANDLEPIN_RENEWAL_DAYS` (default: 30) |
| `get_candlepin_ca()` | Reads `METRICS_UTILITY_CANDLEPIN_CA` |

**Bug fixed:** the previous `is_cert_valid` converted datetimes to ISO strings and parsed them back with `datetime.fromisoformat`, which fails on Python ≤ 3.10 when the output contains a UTC offset suffix. The rewrite uses `cert.not_valid_before_utc` / `cert.not_valid_after_utc` directly.

---

### `metrics_utility/library/candlepin/store.py` — storage abstraction *(new in final commit)*

Provides a backend-agnostic interface for persisting the Candlepin consumer cert, key, and UUID.

```
CandlepinStore (ABC)
├── load()                                → (cert_pem, key_pem, consumer_uuid)
├── save_registration(cert, key, uuid)    → bool
└── save_cert(cert, key)                  → bool  (renewal update only)
```

#### `LocalCandlepinStore` (default)

Reads/writes `cert.pem`, `key.pem`, `uuid.txt` from a configurable directory.

- Default dir: `/etc/metrics-utility/candlepin/` (override with `METRICS_UTILITY_CANDLEPIN_CERT_DIR`)
- Files created with mode `0o600`; directory created with mode `0o700`
- Writes are **atomic** — content is written to a `.tmp` sibling then renamed into place
- `load()` returns `(None, None, None)` if files are absent; never raises
- Works with no database connection — the default for standalone deployments

#### `DBCandlepinStore` (opt-in)

Reads/writes via the AWX `conf_setting` PostgreSQL table. Key names match what AWX PR [ansible/awx#16388](https://github.com/ansible/awx/pull/16388) (merged) writes:

| Field | Key name |
|---|---|
| Certificate PEM | `CANDLEPIN_CERT_PEM` |
| Private key PEM | `CANDLEPIN_KEY_PEM` |
| Consumer UUID | `CANDLEPIN_CONSUMER_UUID` |

All DB operations are best-effort: errors are logged and never propagate.

#### `get_candlepin_store()` factory

```bash
METRICS_UTILITY_CANDLEPIN_STORAGE=local   # default — no DB required
METRICS_UTILITY_CANDLEPIN_STORAGE=db      # use AWX conf_setting table
```

---

## Changed modules

### `metrics_utility/management/validation.py`

Refactored the Candlepin section of `handle_crc_ship_target()` to use the storage abstraction and standalone-friendly credential resolution.

**Credential resolution** — new `_resolve_registration_credentials()`:

| Priority | Source |
|---|---|
| 1st | `METRICS_UTILITY_RH_USERNAME` / `METRICS_UTILITY_RH_PASSWORD` / `METRICS_UTILITY_CANDLEPIN_ORG` env vars |
| 2nd (storage=db only) | `SUBSCRIPTIONS_USERNAME` / `SUBSCRIPTIONS_PASSWORD` / `LICENSE.account_number` from AWX `conf_setting` |

**`handle_crc_ship_target()` flow:**

1. Load cert/key/UUID from configured store
2. If no cert and `METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED` → attempt auto-registration via `_register_candlepin_consumer(store)`
3. If cert loaded, log a warning when fewer than 30 days remain
4. If `METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED` → run `_run_candlepin_lifecycle(cert, key, uuid, store)` (check-in + renewal)
5. Inject `candlepin_cert_pem` / `candlepin_key_pem` into `billing_provider_params` for upload

All DB-specific helpers (`_fetch_candlepin_lifecycle_from_db`, `_upsert_conf_settings`, `_save_candlepin_cert_to_db`, `_save_candlepin_registration_to_db`) are removed from `validation.py` and absorbed into `DBCandlepinStore`.

---

### `metrics_utility/management/commands/candlepin_manage.py`

Django management command for manual Candlepin operations. Updated to use `get_candlepin_store()` and `_resolve_registration_credentials()`.

**Subcommands:**

```
uv run ./manage.py candlepin_manage register [--username X] [--password X] [--org X]
                                             [--candlepin-url URL] [--candlepin-ca PATH]
                                             [--proxy URL] [--force] [--dry-run]

uv run ./manage.py candlepin_manage renew    [--candlepin-url URL] [--candlepin-ca PATH]
                                             [--proxy URL] [--force] [--dry-run]
```

Credential resolution for `register`: CLI flags → `METRICS_UTILITY_RH_*` env vars → AWX `conf_setting` (when `storage=db`).

---

### `metrics_utility/automation_controller_billing/package/package_crc.py`

**mTLS auth mode** — `shipping_auth_mode()` now returns `SHIPPING_AUTH_CERTIFICATES` when a valid Candlepin cert is available (checked via `is_cert_valid`), caching the result. Falls back to `SHIPPING_AUTH_SERVICE_ACCOUNT` on any failure.

**`ship()` override** — writes cert/key to secure temp files for the duration of the POST, then unconditionally cleans up. On `SSLError`:
- If service-account credentials are configured → falls back to OAuth2 and retries
- If not → raises `FailedToUploadPayload` with an actionable error message

**`_get_cert_ingress_url()`** — dynamically prepends `cert.` to the configured ingress hostname (e.g. `console.redhat.com` → `cert.console.redhat.com`), matching the approach used in AWX PR #16388. This is required by the AAP-73765 AC, which specifies `cert.console.redhat.com/api/ingress/v1/upload` as the mTLS endpoint.

**PEM material stripped from payload** — `collector._gather_config()` removes `candlepin_cert_pem` / `candlepin_key_pem` from `billing_provider_params` before writing it to `config.json` in the tarball, so private key material is never uploaded to the ingress endpoint.

---

## New environment variables

| Variable | Default | Purpose |
|---|---|---|
| `METRICS_UTILITY_CANDLEPIN_STORAGE` | `local` | Storage backend: `local` (filesystem) or `db` (AWX conf_setting) |
| `METRICS_UTILITY_CANDLEPIN_CERT_DIR` | `/etc/metrics-utility/candlepin/` | Directory for local cert/key/UUID files |
| `METRICS_UTILITY_RH_USERNAME` | — | Red Hat subscription username for standalone registration |
| `METRICS_UTILITY_RH_PASSWORD` | — | Red Hat subscription password for standalone registration |
| `METRICS_UTILITY_CANDLEPIN_ORG` | — | Candlepin owner/org key for standalone registration |

**Existing env vars (unchanged):**

| Variable | Default | Purpose |
|---|---|---|
| `METRICS_UTILITY_CANDLEPIN_URL` | `https://subscription.rhsm.redhat.com/subscription` | Candlepin base URL |
| `METRICS_UTILITY_CANDLEPIN_CA` | *(system CA store)* | Path to Candlepin CA cert for TLS verification |
| `METRICS_UTILITY_CANDLEPIN_RENEWAL_DAYS` | `30` | Days before expiry to trigger proactive renewal |
| `METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED` | *(unset)* | Enable auto-registration on gather runs |
| `METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED` | *(unset)* | Enable check-in and renewal on gather runs |
| `METRICS_UTILITY_CRC_INGRESS_URL` | `https://console.redhat.com/api/ingress/v1/upload` | Base ingress URL (cert. subdomain derived automatically) |

---

## Local storage layout

```
/etc/metrics-utility/candlepin/   (mode 0700)
├── cert.pem                      (mode 0600) — Candlepin consumer identity certificate
├── key.pem                       (mode 0600) — Consumer identity private key
└── uuid.txt                      (mode 0600) — Candlepin consumer UUID
```

---

## Auth flow during upload

```
gather run
  └── handle_crc_ship_target()
        ├── store.load()                     ← local files or conf_setting
        ├── [opt] register_consumer()        ← if REGISTRATION_ENABLED and no cert
        ├── [warn] near-expiry log           ← if days_remaining < 30
        ├── [opt] run_candlepin_lifecycle()  ← if LIFECYCLE_ENABLED: check-in + renewal
        └── inject cert/key into billing_provider_params

  PackageCRC.ship()
        ├── shipping_auth_mode() == CERTIFICATES
        │     └── write cert/key to secure temp files (0600)
        │           └── POST to cert.console.redhat.com (mTLS)
        │                 └── on SSLError → fallback to service-account OAuth2
        └── shipping_auth_mode() == SERVICE_ACCOUNT
              └── POST to SSO → bearer token → POST to console.redhat.com
```

---

## Test coverage

| Test file | What is covered |
|---|---|
| `test/library/test_candlepin_store.py` *(new)* | `LocalCandlepinStore`: load, atomic writes, 0600/0700 permissions, env var override. `DBCandlepinStore`: load, save_registration, save_cert, key name correctness. `get_candlepin_store()` factory. |
| `test/library/test_candlepin_client.py` | `CandlepinClient`: construction, TLS/proxy config, `_temp_cert_files`, `checkin`, `regenerate_cert`, `register_consumer` |
| `test/library/test_candlepin_lifecycle.py` | `parse_cert`, `is_cert_valid` (fixed roundtrip), `needs_renewal`, `run_candlepin_lifecycle`, `_run_candlepin_lifecycle` (store arg), `handle_crc_ship_target` lifecycle wiring |
| `test/validation/test_candlepin_validation.py` | `_fetch_registration_credentials_from_db`, `_resolve_registration_credentials` (env vs DB priority), `_register_candlepin_consumer` (store arg), `_run_candlepin_lifecycle` (store arg), `handle_crc_ship_target` (cert injection, registration/lifecycle flags, near-expiry warning) |
| `test/management/commands/test_candlepin_manage.py` | `candlepin_manage register` and `renew`: store mock, env-var credential resolution, dry-run, --force, API failure handling |
| `test/gather/test_package_crc.py` | `PackageCRC`: auth-mode selection, mTLS ship path, SSLError fallback, temp-file cleanup, `_get_cert_ingress_url` transformation |

---

## Dependencies

`cryptography==48.0.0` added to `pyproject.toml` for X.509 certificate parsing (`x509.load_pem_x509_certificate`, `cert.not_valid_after_utc`).

---

## AI Assistance

This change was developed with assistance from Claude Code (Claude Sonnet 4.6).
All generated code was reviewed, tested, and validated before merging.
