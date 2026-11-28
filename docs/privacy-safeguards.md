# Privacy safeguards

This guide explains how `metrics-utility` keeps customer data inside the
boundaries described by `privacy-contracts.yaml`, and how to use the checks that
enforce those boundaries.

The model is deliberately simple:

- **One shared CLI** (`tools/privacy_guard`) owns every privacy rule.
- **Thin adapters** connect that CLI to Claude Code, OpenCode, Codex, and Cursor.
- **`pre-commit`** and **GitHub Actions** run the same deterministic checks.
- **Synthetic canaries** in `metrics_utility/test/privacy/` prove the rules still
  hold.

Adapters never contain copies of the rules. If behaviour needs to change, change
`privacy-contracts.yaml` and the CLI — not the harness configs.

## The four data contracts

`privacy-contracts.yaml` defines four flows. Each has a different audience and a
different allowed payload.

| Contract ID | Destination | Allowed | Never allowed |
| --- | --- | --- | --- |
| `segment_telemetry` | Segment (`storage/segment.py`) | Aggregate counts and allowlisted public content names | Hostnames, organization names, usernames, emails, credentials, raw controller IDs |
| `billing_archive` | Customer billing pipeline | Billing fields required for invoicing (install UUID, counts, job/template metadata) | Tokens, passwords, connection strings, private keys, API keys |
| `awx_analytics` | Controller → local database | Schema-tracked columns selected by explicit queries | Unfiltered rows; relaxing privacy predicates such as `managed = true` |
| `internal_local_reports` | Customer's own XLSX/ODS files | Anything the customer already owns | Uploads, telemetry, or cross-customer data |

### Segment Telemetry

Anonymized rollups are built in `metrics_utility/anonymized_rollups/` and sent by
`metrics_utility/library/storage/segment.py`. `flatten_json_report()` is the
final projection. Treat every field it emits as public.

- Content names are allowlisted (for example via `collections.json`). Unknown
  collections, modules, and roles are dropped.
- Hostnames and organization names are reduced to counts or set sizes before
  the final payload.
- Adding a new key is a contract change.

### Billing Archives

Billing needs customer-identifying fields that Segment must never see. That is
expected. The hard rule for this flow is that **no credential or secret may
appear anywhere**: not in archives, not in logs, not in exception messages, and
not in `config.json`.

### AWX / Controller Collectors

Collectors under `metrics_utility/library/collectors/controller/` feed both
billing and anonymized paths. SQL predicates are the privacy boundary:

- `managed = true` limits rows to managed content.
- `condition = 'boolean'` and `value = 'True'` limit feature flags.
- Explicit column lists prevent accidental passthrough.

Changing one of these predicates changes what data can leave the controller, so
the change needs a contract update and code-owner review.

### Local Reports

Reports are generated for the customer on the customer's machine. They may
contain customer data. They must never be uploaded as telemetry or mixed across
customers.

## Architecture

```text
                     ┌────────────────────────────────┐
                     │ privacy-contracts.yaml         │
                     │ (single source of truth)       │
                     └───────────────┬────────────────┘
                                     │ rules loaded by
                                     ▼
  Claude Code ─┐          ┌──────────────────────────┐          ┌─ pre-commit
  OpenCode ────┼─────────▶│ privacy-guard CLI        │◀─────────┤  (check-diff)
  Codex ───────┤  thin    │ tools/privacy_guard      │          └─ GitHub Actions
  Cursor ──────┘ adapters └───────┬──────────┬───────┘
                                  │          │
                     check-diff / hook   ai-review (advisory)
                                  │          │
                     ┌────────────▼──┐   ┌───▼─────────────────┐
                     │ GitHub Actions│   │ LLM endpoint        │
                     │ blocking job  │   │ internal or external│
                     └────────────┬──┘   └─────────────────────┘
                                  │
                     ┌────────────▼──────────────────┐
                     │ metrics_utility/test/privacy/ │
                     │ synthetic canary tests        │
                     └───────────────────────────────┘
```

Two layers enforce the contracts:

1. **Deterministic** — `check-diff`, harness hooks, pre-commit, and the
   `privacy-deterministic` CI job. These block on `error` findings and are the
   only checks that can stop a merge.
2. **Advisory** — `ai-review` looks for semantic issues that pattern matching
   misses (for example, a field that is technically allowed but semantically
   identifying). It never blocks a merge.

## Running the checks locally

From the repository root:

```bash
# Check the staged diff (what pre-commit runs)
uv run python -m tools.privacy_guard check-diff --staged

# Check a branch against devel
uv run python -m tools.privacy_guard check-diff --base origin/devel

# Check specific files
uv run python -m tools.privacy_guard check-files \
  metrics_utility/library/storage/segment.py \
  metrics_utility/anonymized_rollups/anonymized_rollups.py

# Run the synthetic privacy test suite
uv run pytest metrics_utility/test/privacy -v

# Preview an AI review without sending anything to a provider
uv run python -m tools.privacy_guard ai-review --base origin/devel --dry-run
```

Useful options:

| Option | Effect |
| --- | --- |
| `--format text\|json\|markdown` | Output format. `json` is useful in scripts. |
| `--fail-on error\|warning` | Exit non-zero on warnings too (stricter local runs). |
| `--staged` | Inspect staged changes instead of a branch diff. |
| `--dry-run` | Build the AI prompt but do not call the model. |
| `--allow-skip-unconfigured` | Exit 0 when no AI provider is configured. |

To enable the pre-commit hook in a clone:

```bash
uv run pre-commit install
```

The hook runs `check-diff --staged` and blocks the commit on an `error`.

## Harness setup

All four adapters call the same CLI with the same exit-code contract:

- `0` — clean (or advisory-only output).
- `2` — blocked; the harness must deny the action and show the message.
- Any other non-zero code — tooling failure; fail open so the agent is not
  bricked.

### Claude Code

Config: `.claude/settings.json`.

- `PreToolUse` with matcher `Bash` runs the `pre-tool` hook.
- `PostToolUse` with matcher `Edit|Write|MultiEdit` runs the `post-tool` hook.

Claude Code asks you to trust project hooks the first time they run. Review the
commands, then approve them. If you do not trust the hooks, they do not execute
and you only have the `pre-commit` and CI checks.

### Codex

Config: `.codex/hooks.json`.

- `PreToolUse` with matcher `Bash` runs the `pre-tool` hook.
- `PostToolUse` with matcher `apply_patch|Edit|Write` runs the `post-tool` hook.

Codex loads project hooks only when the `.codex/` layer is trusted. Run `/hooks`
in the Codex CLI to inspect, trust, or disable individual hooks. Codex records
trust against each hook's hash, so an edited hook needs review again.

### Cursor

Config: `.cursor/hooks.json` (`version: 1`).

- `beforeShellExecution` runs the `pre-tool` hook.
- `afterFileEdit` runs the `post-tool` hook.

Cursor only runs project hooks in a trusted workspace. Both hooks set
`"failClosed": false`: a missing `uv` binary or a broken hook lets the action
proceed rather than blocking all work.

### OpenCode

Config: `.opencode/plugins/privacy-guard.ts`.

The plugin registers two OpenCode V2 hooks:

- `ctx.permission.hook("evaluate", ...)` inspects shell commands and denies the
  action (`event.effect = "deny"`) when the CLI exits `2`.
- `ctx.tool.hook("execute.after", ...)` runs the `post-tool` check for
  `edit`/`write` tools and attaches any warning to the result metadata and the
  server log.

The plugin is fail-open: if `uv` or the CLI is unavailable, it logs a warning and
leaves the action alone.

### If hooks are too slow

The hooks only run the privacy CLI; they do not scan the whole repository. If a
particular harness feels slow, disable that harness's hook and rely on
`pre-commit` plus CI. Do not remove the CI jobs.

## GitHub Actions

### `privacy-guard.yml`

Runs on every `pull_request`.

**`privacy-deterministic` (blocking)** — checks out the full history, installs
`uv` and Python, then runs:

```bash
uv run python -m tools.privacy_guard check-diff --base "origin/<base>"
uv run pytest metrics_utility/test/privacy -v
```

**`ai-privacy-review` (advisory)** — runs only when an API key is configured and
`continue-on-error: true`. It writes a Markdown report and creates or updates a
single sticky PR comment identified by `<!-- privacy-guard-ai-review -->`.

GitHub does not allow `secrets.*` in a job-level `if`, so the job is gated on
same-repo PRs and a step-level environment check. Fork PRs never receive
repository secrets; they are covered by the deterministic job.

### `schema-review.yml`

Requests review from Emerging Services and Analytics HCC when a PR touches:

- `metrics_utility/schemas/**`
- `metrics_utility/anonymized_rollups/**`
- `metrics_utility/library/storage/segment.py`
- `metrics_utility/library/collectors/controller/credentials_service.py`
- `metrics_utility/automation_controller_billing/collector.py`
- `privacy-contracts.yaml`

`.github/CODEOWNERS` assigns the same owners to the broader privacy-critical
paths.

## Configuring GitHub Secrets and Variables

Set these in **Settings → Secrets and variables → Actions**.

| Name | Type | Required | Purpose |
| --- | --- | --- | --- |
| `PRIVACY_AI_API_KEY` | Secret | For AI review | API key for the LLM endpoint. Empty disables the advisory job. |
| `PRIVACY_AI_API_URL` | Variable | For internal endpoints | Base URL of an OpenAI-compatible or internal gateway. |
| `PRIVACY_AI_PROVIDER` | Variable | Recommended | Provider identifier understood by the CLI: `openai_compatible` (also accepts `openai-compatible` or `openai`) for an OpenAI-compatible gateway or hosted API, or `anthropic`. |
| `PRIVACY_AI_MODEL` | Variable | Recommended | Model name to use for the review. |
| `PRIVACY_AI_ENABLED` | Variable | Optional | Set to `true` to let fork PRs run the advisory job when they have their own endpoint. |

### Internal LLM endpoint

Use an OpenAI-compatible internal gateway so code diffs never leave the
company network:

```text
PRIVACY_AI_PROVIDER = openai_compatible
PRIVACY_AI_API_URL  = https://llm-gateway.internal.example/v1
PRIVACY_AI_MODEL    = privacy-reviewer
PRIVACY_AI_API_KEY  = <stored as a secret>
```

### External LLM endpoint

For a hosted provider, leave `PRIVACY_AI_API_URL` unset (or point it at the
provider's documented base URL) and set the provider and model:

```text
PRIVACY_AI_PROVIDER = anthropic
PRIVACY_AI_MODEL    = <approved model>
PRIVACY_AI_API_KEY  = <stored as a secret>
```

Review your organisation's data-handling policy before sending diffs to an
external provider. The deterministic checks never send code anywhere.

Test the configuration locally before relying on CI:

```bash
PRIVACY_AI_PROVIDER=openai-compatible \
PRIVACY_AI_API_URL=https://llm-gateway.internal.example/v1 \
PRIVACY_AI_MODEL=privacy-reviewer \
PRIVACY_AI_API_KEY=... \
uv run python -m tools.privacy_guard ai-review --base origin/devel --dry-run
```

`--dry-run` builds the prompt and prints it without contacting the provider.

## Synthetic data testing and team demo

The canary suite in `metrics_utility/test/privacy/` uses fake data only. It
injects synthetic identifiers and credentials into collector-shaped fixtures and
asserts they never reach an output payload.

Run it:

```bash
uv run pytest metrics_utility/test/privacy -v
```

### Suggested demo (10 minutes)

1. **Show the happy path.** Run the canary suite and point out that the fixtures
   contain deliberately fake hostnames, organization names, and tokens.
2. **Show anonymization.** Open `docs/data-lineage.md` and trace one field from a
   controller column to its final aggregate.
3. **Show a block.** Create a scratch file under a contract path with a
   secret-like assignment. The `printf` below assembles the value at runtime so
   this guide does not contain a secret-shaped literal that the checker would
   flag:

   ```bash
   printf 'S3_SECRET_%s = "SYNTHETIC_%s"\n' KEY SECRET_DO_NOT_USE \
     > metrics_utility/anonymized_rollups/.privacy_probe.py
   ```

   Run the checker directly, then stage it and check the staged diff:

   ```bash
   uv run python -m tools.privacy_guard check-files \
     metrics_utility/anonymized_rollups/.privacy_probe.py

   git add metrics_utility/anonymized_rollups/.privacy_probe.py
   uv run python -m tools.privacy_guard check-diff --staged
   ```

   The check fails with `SEC-001` and names the contract. Unstage and delete the
   scratch file afterwards (`git restore --staged <file> && rm <file>`).
4. **Show the hook.** With the scratch file staged, feed a fake `git commit`
   payload to the same CLI the harnesses call:

   ```bash
   printf '%s' '{"tool_name":"Bash","tool_input":{"command":"git commit -m demo"}}' \
     | uv run python -m tools.privacy_guard hook --harness claude --event pre-tool
   echo "exit: $?"
   ```

   The hook inspects the staged diff, prints the finding, and exits `2`. In a
   real session the harness denies the command instead of running it.
5. **Show the advisory layer.** Run `ai-review --dry-run` and show the prompt,
   then explain that CI posts the result as a sticky comment but never blocks.
6. **Show governance.** Open a PR touching `privacy-contracts.yaml` and show the
   requested reviewers and the CODEOWNERS entries.

## Changing a contract

1. Update `privacy-contracts.yaml` first.
2. Update the code and `docs/data-lineage.md`.
3. Add or update a synthetic canary in `metrics_utility/test/privacy/`.
4. Run the deterministic checks and the full privacy test suite.
5. Request review from the owners listed in `.github/CODEOWNERS`.

Never loosen a contract or an allowlist to make a check pass. Fix the data flow
or get an explicit, reviewed contract change.

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `ModuleNotFoundError: tools.privacy_guard` | Running outside the repo root or dependencies not synced | `cd` to the repo root and run `uv sync` |
| Hook never runs | Project hooks are not trusted | Claude Code: approve project hooks. Codex: `/hooks` and trust. Cursor: trust the workspace. |
| Hook blocks everything | CLI exits `2` for an unrelated reason | Run the hook command manually with JSON on stdin to see the message |
| AI job skipped on a fork PR | Secrets are not passed to fork workflows | Use `PRIVACY_AI_ENABLED=true` with a fork-safe endpoint, or rely on the deterministic job |
| Sticky comment duplicated | Marker text changed | Keep `<!-- privacy-guard-ai-review -->` in the comment body |
| `uv.lock` changed after `uv sync` | Dependency drift | Commit the updated lock file (CI checks this) |
