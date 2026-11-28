# AGENTS.md

Guidance for AI coding agents (Claude Code, OpenCode, Codex, Cursor) working in
`metrics-utility`. Read this before changing collectors, anonymization, storage,
billing, or report code.

`privacy-contracts.yaml` at the repository root is the source of truth for the
data-flow contracts. When this file and the YAML disagree, the YAML wins.

## The four data-flow contracts

| Contract | Where it lives | Rule |
| --- | --- | --- |
| **Segment Telemetry** (`segment_telemetry`) | `metrics_utility/anonymized_rollups/`, `metrics_utility/library/storage/segment.py` | Strictly anonymized **allowlist** only. Aggregate counts and approved public content names. No hostnames, organization names, user identifiers, credentials, or raw controller IDs. |
| **Billing Archives** (`billing_archive`) | `metrics_utility/automation_controller_billing/`, `metrics_utility/library/collectors/` | Customer billing fields are allowed (install UUID, counts, job/template metadata needed for billing). **Zero credentials or secrets** in payloads, archives, logs, or `config.json`. |
| **AWX / Controller Collectors** (`awx_analytics`) | `metrics_utility/library/collectors/controller/`, `metrics_utility/schemas/` | Schema-tracked. SQL filters such as `managed = true` and explicit column projections are privacy boundaries, not performance hints. |
| **Local Reports** (`internal_local_reports`) | report/XLSX/ODS output | Customer-local XLSX/ODS only. Reports are never uploaded as telemetry. |

Contract changes require an update to `privacy-contracts.yaml` and review from
the owners in `.github/CODEOWNERS`.

## Privacy invariants

1. **Never widen `flatten_json_report()` with raw identifiers.** It is the final
   projection for anonymized output. Adding a field there is a privacy change.
2. **Keep allowlists fail-closed.** Unknown collections, modules, roles, or
   field names must be dropped, not passed through. Do not add catch-all
   `**kwargs`, `SELECT *`, or dynamic column passthrough to anonymized paths.
3. **Never log payloads, secrets, or credentials.** No tokens, passwords,
   connection strings, private keys, request bodies, or full collector rows in
   logs, exceptions, test fixtures, or AI prompts.
4. **Never embed credentials in `config.json`.** Runtime configuration must not
   contain API keys, Segment write keys, cloud credentials, or database
   passwords.
5. **Preserve SQL privacy predicates.** Filters like `managed = true`,
   `condition = 'boolean'`, and `value = 'True'` define what may leave the
   controller. Do not relax them to "collect more data".
6. **New output fields need a contract update.** Add the field to
   `privacy-contracts.yaml` and the synthetic tests before shipping it.

## Verification before finishing

```bash
# Deterministic privacy check against staged changes
uv run python -m tools.privacy_guard check-diff --staged

# Full privacy test suite (synthetic canaries)
uv run pytest metrics_utility/test/privacy -v

# Optional non-blocking AI review of a branch diff
uv run python -m tools.privacy_guard ai-review --base origin/devel --dry-run
```

If a check reports a violation, fix the code or update the contract — do not
silence the check.

## Agent harness integration

The repository ships thin adapters that all call the same CLI. Do not duplicate
privacy rules in adapter configs:

- `.claude/settings.json` — Claude Code `PreToolUse` / `PostToolUse` hooks.
- `.codex/hooks.json` — Codex `PreToolUse` / `PostToolUse` hooks.
- `.cursor/hooks.json` — Cursor `beforeShellExecution` / `afterFileEdit` hooks.
- `.opencode/plugins/privacy-guard.ts` — OpenCode permission and tool hooks.

A hook exit code of `2` means "block this action". Any other non-zero code is a
tooling failure and must fail open so it never bricks the agent.

## Useful skills and docs

- `.agents/skills/privacy-review/SKILL.md` — portable privacy review workflow.
- `docs/privacy-safeguards.md` — team guide, setup, and troubleshooting.
- `docs/data-lineage.md` — field-level anonymization lineage.
