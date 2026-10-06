# Organization-aware analytics collectors

This is an implementation guide for a future metrics-service endpoint such as
`GET /api/v1/analytics/organization/<organization_id>/<group.collector>/`. It catalogs the raw
metrics-utility payloads that can be filtered per organization and distinguishes direct filtering
from aggregates and cross-collector joins. The five `*_org` collectors below are implemented in the
metrics-utility change for AAP-95739; they still need a separate metrics-service registry change
before they can be collected or exposed.

## Payload and identity contract

The analytics API persists the collector's raw `gather()` result in `AnalyticsPayload.payload`.
DataFrame collectors serialize as a list of record dictionaries; `DictOutput` collectors serialize
as dictionaries. The payload shapes are collector-specific—there is no universal org field.

The existing `GET /api/v1/analytics/<collector>/` filters **collection envelopes** by overlap of the
stored `since`/`until` bounds, then returns those envelopes and their raw payloads. An organization
route should retain that time-window behavior, filter inside each payload, preserve the envelope
metadata, and apply organization filtering before calculating pagination/counts. Do not change or
rewrite the stored payload.

Controller organization IDs are local integer `main_organization.id` values. Names are strings and
may change; the collectors below do not provide an organization UUID. `organization_remote_id` is
also the local integer ID, not a separate UUID. Rows with a null org stay unassigned and must not be
given a default organization.

`AnalyticsPayload` also stores `cluster_id` (Controller `INSTALL_UUID`) when available. The same local
organization ID can identify different organizations on different Controller installations. Keep
the `cluster_id` boundary when selecting or combining payloads; if an endpoint can see several
installations, its semantics must account for that rather than treating the integer ID as globally
unique.

For record-list payloads, the organization selector is either `organization_id` or
`organization_remote_id`. For `org_counts`, it is the dictionary key; JSON serialization makes that
key a string. New `*_org` collectors emit rows with `organization_id` and `organization_name`; one
organization can have multiple rows when the aggregate has additional dimensions such as job type,
status, inventory kind, or SCM type.

## Self-contained organization dimensions

These payloads carry sufficient org identity to filter without joining another collector payload.
“Registry status” refers to the metrics-service `apps/analytics/registry.py` at the time this guide
was written; registry entries control whether data is persisted and public.

| Collector | Org field and payload grain | Registry status |
|---|---|---|
| `controller.unified_jobs_dashboard` | `organization_id`, `organization_name`; one row per job | Enabled, hourly |
| `controller.job_host_summary_service` | `organization_remote_id`, `organization_name`; one row per job/host summary | Enabled, hourly |
| `controller.execution_environments` | `organization_id` only; one row per execution environment | Enabled, snapshot |
| `controller.main_host` | `organization_remote_id`, `organization_name`; one row per enabled host, attributed through its inventory | Enabled when `SERVICE_FUNCTIONS_AVAILABLE` is true, snapshot |
| `controller.main_host_daily` | `organization_remote_id`, `organization_name`; one row per enabled host changed in the window, attributed through its inventory | Enabled when `SERVICE_FUNCTIONS_AVAILABLE` is true, daily |
| `controller.org_counts` | Dictionary keyed by org ID; value has `name`, `users`, and `teams` | Enabled, snapshot |
| `controller.main_indirectmanagednodeaudit` | `organization_remote_id`, `organization_name`; one row per indirect-node audit | Registered but disabled pending the ANSTRAT-2160 path |

For `controller.org_counts`, select only the requested key (typically its string form) and retain the
existing dictionary shape. For the row payloads, keep all matching records; don't reduce them to one
record just because the URL names one organization.

Other existing metrics-utility outputs contain an org field but are not currently part of the
analytics API allowlist: `controller.unified_jobs` and `controller.job_host_summary` are excluded
base/legacy variants. `dashboard.dashboard_jobs` also contains `organization_id`, but belongs to a
separate dashboard-sync pipeline. Their fields are useful references, but they are not stored by the
current analytics registry.

## New per-organization aggregate collectors (AAP-95739)

These are additive collectors, not server-side filters over their similarly named global versions.
They produce compact payload rows already grouped by organization and preserve null org attribution.
All five are in the metrics-utility library branch for AAP-95739; none is registered in
metrics-service yet.

| Collector | Output grain and measures | Collection mode |
|---|---|---|
| `controller.unified_jobs_org` | `organization_id`, `organization_name`, job type, status, job count, failure count, total and average elapsed time | Windowed on job `finished` (hourly) |
| `controller.projects_by_scm_type_org` | Org and normalized SCM type (`manual` for empty SCM type), project count; org comes from the project's unified-job-template parent | Snapshot |
| `controller.cred_type_counts_org` | Credential-owning org, credential type ID/name/managed flag, credential count | Snapshot |
| `controller.credentials_service_org` | Org of the job that used a managed credential type, plus credential type; distinct type rows in the finished-job window | Windowed on job `finished` (hourly) |
| `controller.inventory_counts_org` | Org and inventory kind (`normal`/`smart`), inventory count, host count, source count; hosts and sources are pre-aggregated per inventory to avoid join fan-out | Snapshot |

Do not conflate credential ownership (`cred_type_counts_org`) with credential use
(`credentials_service_org`): the latter is attributed to the job's organization. `inventory_counts_org`
matches the existing inventory collector's normal/smart scope; it is not a detailed per-inventory
source listing. The existing global collectors (`projects_by_scm_type`, `cred_type_counts`,
`credentials_service`, and `inventory_counts`) remain global payloads.

## Join-assisted payloads and limits

These collectors do not contain an organization field themselves. A filter needs a compatible
organization-bearing payload and a well-defined join key; do not treat them as directly filterable.

| Collector(s) | Available key(s) | Caveat |
|---|---|---|
| `controller.main_jobevent_service`, `controller.events_table` | `job_id`; service events also expose `job_remote_id` | Could join to a job payload on `id`, but the available `unified_jobs_dashboard` payload intentionally excludes workflow and sync jobs. `main_jobevent_service` is also capped by job and row limits, so its rows represent only collected events. Establish coverage for the desired event set before promising complete org results. `events_table` windows on event modification time, not job finish time. |
| `controller.main_jobevent` | `job_remote_id` | Legacy collector excluded from the analytics registry; same job-payload join caveat. |
| `controller.workflow_job_node_table` | `workflow_job_id` (also node job/template IDs) | Current `unified_jobs_dashboard` is a `main_job` view and does not provide a complete parent workflow-job lookup. |
| `controller.workflow_job_template_node_table` | `workflow_job_template_id` | `controller.unified_job_template_table` does not include organization in its raw payload, so the current analytics payloads do not provide a complete org lookup. |
| `controller.inventory_counts` | Inventory IDs are dictionary keys | No org is included. Joining through `main_host` misses inventories without enabled hosts; use `inventory_counts_org` for complete org-level totals. |

`controller.main_hostmetric` is hostname/history-oriented and cannot be safely assigned to an org
from the host's current inventory. `controller.unified_job_template_table` similarly omits the org
field even though AWX stores it on the parent template model. Do not infer either association from a
different, incomplete snapshot.

The base `controller.unified_jobs` collector already exists in metrics-utility and carries
`id` + organization ID/name for all unified-job types, but metrics-service currently excludes it in
favor of `controller.unified_jobs_dashboard`. A service-only registry/scheduling change could use
the base payload as a join source for events or workflow records without changing metrics-utility;
it would persist a second, full job dataset, so account for the extra payload volume. Joining to the
dashboard variant is smaller in scope but can omit workflow-child and sync jobs. If the current
persisted payloads cannot cover the required rows, keep that collector out of the org endpoint until
the service-side source/coverage is explicit.

## Global collectors

Keep platform-wide or operational data global; do not manufacture an organization dimension for it.
This includes `controller.config`, `controller.config_django`, `controller.controller_version_service`,
`controller.feature_flags_service`, `controller.host_metric_summary_monthly_table`,
`controller.instance_info`, `controller.query_info`, `controller.table_metadata`,
`service.task_executions_service`, and `others.total_workers_vcpu`. `controller.counts` has a scalar
total for organizations, not a per-org breakdown, and its other totals must not be attributed to
each org. `controller.main_hostmetric` is not global platform state, but its historical records also
lack reliable organization attribution.

## Metrics-service follow-up

To expose the five new aggregate collectors, the separate metrics-service change must add entries to
`apps/analytics/registry.py` using their exact public names and function names as `collector_type`:

- `controller.unified_jobs_org` — windowed/hourly
- `controller.credentials_service_org` — windowed/hourly
- `controller.projects_by_scm_type_org` — snapshot
- `controller.cred_type_counts_org` — snapshot
- `controller.inventory_counts_org` — snapshot

The service registry is the whitelist for collection persistence and the existing analytics API;
unregistered collectors are not persisted or exposed. Add the corresponding scheduling/collector
registration and service tests there as needed. This repository change supplies the collector
payload contract only.

For the proposed organization route, keep the existing System Admin/System Auditor authorization;
the org selector is a BI/reporting convenience, not a new org-scoped RBAC boundary. Preserve
collection-window overlap filtering and the collection envelope fields. Because `payload` is JSON
with collector-specific shapes, the route must filter inside each payload (or use an explicit
join-assisted implementation for the caveated collectors above), not assume `AnalyticsPayload` has
a relational `organization_id` column.

## Code references

- metrics-utility collector exports and implementations: `metrics_utility/library/collectors/controller/`
- metrics-service whitelist and modes: `apps/analytics/registry.py`
- raw payload storage and JSON conversion: `apps/analytics/models.py`, `apps/analytics/persist.py`
- existing row API, URL patterns, and response envelope: `apps/analytics/v1/views.py`,
  `apps/analytics/v1/urls.py`, `apps/analytics/v1/serializers.py`
