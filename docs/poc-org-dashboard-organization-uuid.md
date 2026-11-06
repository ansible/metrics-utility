# POC — Organization UUID on Dashboard Collection Rows

Branch: `poc/org-dashboard-collection` (rebased onto `devel` @ `ed47af6`)
Draft PR: (to be opened)
Supersedes: closed PR #596 / branch `feat/organization-dashboard-rbac`

## Purpose

metrics-service's organization-scoped dashboard scopes every query with
`JobData.organization_ansible_id` — the organization's stable DAB
`dab_resource_registry_resource.ansible_id`. That column is populated from two
metrics-utility collection paths:

1. the historical dashboard collector (`dashboard.dashboard_jobs`), and
2. the hourly `controller.unified_jobs_dashboard` collector.

Without the UUID on those rows, `JobData.organization_ansible_id` is null,
`scope_jobdata_queryset()` filters to nothing, and organization members/admins
see an empty dashboard and have no organization context for saved reports.

This branch adds the UUID to both collectors. It contains the refreshed
versions of the original PR #596 work (`e9e7bf0`, `fcf014f`) rebased onto
current `devel`; the original historical commit (`e29f21b`) was **not** reused
because `devel` has since gained `organization_name` and an `o`-aliased
`main_organization` join (PR #603), which would have caused a duplicate column
and a conflicting join.

## Changes

### Historical dashboard collector

`metrics_utility/library/collectors/dashboard/queries.py` (`_JOBS_BASE_SQL`):

- Select `main_organization.name AS organization_name` and
  `dab_resource.ansible_id AS organization_ansible_id`.
- Replace the single `LEFT JOIN main_organization o` with:

```sql
LEFT JOIN main_organization
    ON main_organization.id = uj.organization_id
LEFT JOIN django_content_type organization_content_type
    ON organization_content_type.app_label = 'main'
    AND organization_content_type.model = 'organization'
LEFT JOIN dab_resource_registry_resource dab_resource
    ON dab_resource.content_type_id = organization_content_type.id
    AND dab_resource.object_id = main_organization.id::text
```

`get_jobs_batch_query()` reuses `_JOBS_BASE_SQL`, so both the full and batched
paths are covered.

`metrics_utility/library/collectors/dashboard/collectors.py`:

- `AWXJobType`: add `organization_ansible_id: str | None`.
- `dashboard_jobs()` row unpacking: add the field.
- Result dict: add
  `'organization_ansible_id': str(data['organization_ansible_id']) if data['organization_ansible_id'] else None`.
- Docstring output contract lists the new key.

### Hourly collector

`metrics_utility/library/collectors/controller/unified_jobs_dashboard.py`:

- Add `dab_resource.ansible_id AS organization_ansible_id` after
  `main_organization.name AS organization_name`.
- Add the same `django_content_type AS organization_content_type` and
  `dab_resource_registry_resource AS dab_resource` joins.

### Tests

- `test/library/dashboard/test_queries_dashboard.py`: assert the new select
  alias and DAB joins in both the full and batch query tests.
- `test/library/dashboard/test_collectors_dashboard.py`: add the column to the
  three column lists and update the mocked rows/expected dicts.
- `test/library/test_collectors_unified_jobs_dashboard.py`: new
  `test_includes_shared_organization_ansible_id`.

## Contract and semantics

- The field is named exactly `organization_ansible_id`; metrics-service already
  expects that name in its dashboard column/string-field maps.
- The historical path converts the UUID to `str`; the hourly path relies on
  metrics-service's existing psycopg UUID-to-string handling.
- The joins are `LEFT JOIN`s: unregistered organizations and jobs with no
  organization yield `NULL` rather than dropping the row. metrics-service's
  organization scope will hide those rows for org-scoped users, which is the
  intended fail-closed behavior.
- `dab_resource_registry_resource` has a unique
  `(content_type_id, object_id)` index, so the join cannot fan out rows.

## Risks / ordering

1. The join hard-references `dab_resource_registry_resource`. On an AWX without
   the DAB resource registry (or before its migration) the collector fails with
   `relation does not exist`; there is no fallback or feature detection.
2. Controller DAB resource sync must register the organization before
   collection, or the UUID is null and the job is invisible to org-scoped
   dashboard users.
3. Deploying metrics-service first is safe (null UUIDs simply fall out of org
   scope). Existing `JobData` rows need metrics-service's
   `backfill_dashboard_organization_ids` management command after this ships.
4. No metrics-utility migrations are required; metrics-utility is a read-only
   client. `tools/docker/latest.sql` already contains the DAB tables.

## Validation

```bash
pytest metrics_utility/test/library/dashboard/test_queries_dashboard.py \
       metrics_utility/test/library/dashboard/test_collectors_dashboard.py \
       metrics_utility/test/library/test_collectors_unified_jobs_dashboard.py
```

## Related changes

- metrics-service PR #525 — server-side save-report permission gate.
- metrics-service PR #441 — Option B organization-scoped dashboard (consumes
  `organization_ansible_id`).
- ansible-ui PR #3861 — UI save-report gate.
