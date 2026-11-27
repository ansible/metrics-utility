---
name: privacy-review
description: Review a diff for violations of privacy-contracts.yaml. Use when changes touch collectors, anonymized rollups, Segment storage, billing archives, exports, or configuration that could carry credentials.
---

# Privacy review

Use this skill to review a change before committing or merging. The goal is to
catch contract violations early — not to re-implement the privacy guard in
prose.

## Inputs

- `privacy-contracts.yaml` — the four data-flow contracts and their allowed
  fields.
- The diff under review.
- `metrics_utility/test/privacy/` — synthetic canary tests that exercise each
  contract.
- `docs/data-lineage.md` — field-level provenance for anonymized output.

## Procedure

1. **Read the contract first.** Open `privacy-contracts.yaml` and note which
   contract(s) the diff touches.
2. **Run the deterministic checks.**

   ```bash
   uv run python -m tools.privacy_guard check-diff --base origin/devel
   uv run pytest metrics_utility/test/privacy -v
   ```

3. **Classify the diff** using the contract map below.
4. **Inspect for violations** using the checklists.
5. **Report** findings grouped by severity: `error` (must fix), `warning`
   (should fix / needs owner review), `info` (note only). Include file and line
   references and the contract name.

## Contract map and checklists

### Segment Telemetry

Files: `metrics_utility/anonymized_rollups/**`,
`metrics_utility/library/storage/segment.py`.

- [ ] Output is an explicit allowlist, not a passthrough of collector rows.
- [ ] No hostnames, organization names, usernames, emails, or raw controller
      IDs reach `flatten_json_report()`.
- [ ] New keys were added to `privacy-contracts.yaml` and covered by a
      synthetic test.
- [ ] Content allowlists (`collections.json`, module/role allowlists) still
      drop unknown entries.

### Billing Archives

Files: `metrics_utility/automation_controller_billing/**`,
billing-related collectors and serializers.

- [ ] Billing fields are limited to what the contract permits.
- [ ] No credentials, tokens, passwords, private keys, or connection strings in
      archives, logs, exceptions, or `config.json`.
- [ ] Redaction happens before serialization, not after logging.

### AWX / Controller Collectors

Files: `metrics_utility/library/collectors/controller/**`,
`metrics_utility/schemas/**`, `metrics_utility/management/validation.py`.

- [ ] SQL privacy predicates such as `managed = true` are unchanged or the
      change is explicitly justified and reflected in the contract.
- [ ] Column projections are explicit; no `SELECT *` added to privacy-sensitive
      queries.
- [ ] Schema changes are reflected in the JSON schema and contract.

### Local Reports

Files: report builders and XLSX/ODS writers.

- [ ] Reports remain customer-local; no upload or telemetry path is added.
- [ ] Report content does not leak data across customers or tenants.

## Stop conditions

Escalate to the code owners instead of approving when:

- a contract file changes without a matching `privacy-contracts.yaml` update;
- a privacy predicate or allowlist is relaxed;
- a new field cannot be traced to an allowed source in `docs/data-lineage.md`;
- credentials or payload samples appear in logs, tests, or fixtures.

## Non-goals

- Do not rewrite code during review unless the user asks.
- Do not approve a contract change based only on this skill; a human owner
  listed in `.github/CODEOWNERS` must review it.
