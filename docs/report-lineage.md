# Report dependency trees

These diagrams trace the **existing CLI reports**, not a replacement reporting
implementation. They separate the data needed by each report's core/default
output from smaller trees for its configurable sheets. See
[collector data lineage](data-lineage.md) for the parallel anonymized,
dashboard, and raw analytics paths; the service's
[report recipes](https://github.com/ansible/metrics-service/blob/devel/docs/report-recipes.md)
explain reconstruction through the analytics API.

Start with [defaults and required output](#required-versus-default-versus-configurable),
then the [CCSP](#ccsp-core-and-default-output),
[CCSPv2](#ccspv2-core-and-default-output), or
[renewal](#renewal-guidance-core-and-default-output) tree. The
[sheet trees](#smaller-sheet-dependency-trees) and
[loading gates](#current-loading-gates) cover custom configurations.

## Collection to workbook

```mermaid
flowchart TD
    DB[(Controller DB)] --> Gather[CLI gather collectors]
    Gather --> Archives[Directory / S3 tarballs\nCSV datasets + config + collection status]
    Archives --> Extract[ExtractorDirectory / ExtractorS3\nCCSP and CCSPv2]
    DB --> Lifecycle[main_hostmetric collector\nExtractorControllerDB for renewal]
    Extract --> Frames[DataframeFactory\nreport-specific dataframe engines]
    Lifecycle --> Frames
    Frames --> Dedup[DedupFactory\nselected deduplicator]
    Dedup --> Builder[ReportFactory\ncore and configured sheets]
    Params[CLI options and report environment\nperiod, sheet flags, company, SKU, price] --> Builder
    Builder --> Saver[ReportSaver\nDirectory / S3 XLSX]
```

CCSP/CCSPv2 build from collected files, not fresh Controller queries. Renewal
guidance uses `METRICS_UTILITY_SHIP_TARGET=controller_db` to read Controller
directly; its report saver writes to the configured directory. The factory
does not select the extractor merely from the report name.

### Dataset names used in the trees

| Input | Engine / meaning |
| --- | --- |
| `job_host_summary.csv` + `config.json` | `DataframeJobhostSummaryUsage`: reachable direct-host usage, hostname normalization, installation/job/org/template context. |
| `main_indirectmanagednodeaudit.csv` + `config.json` | The same engine's indirect branch: target identity, audit task counts, facts, and content attribution. |
| `main_host.csv` / `main_host_daily.csv` + `config.json` | `DataframeInventoryScope`: accumulated inventory memberships, facts, last automation, and compound serials. |
| `main_jobevent.csv` + `config.json` | `DataframeContentUsage`: module/collection/role usage per host, installation, and job. |
| `data_collection_status.csv` | `DataframeCollectionStatus`: CLI gather status from packages, not a Controller collector. |
| `main_hostmetric` collector result | `DBDataframeHostMetric`: lifecycle metrics joined to current inventory-host facts/variables, then filtered by last automation. |

`config.install_uuid` supplies installation identity to the usage, scope, and
content engines. Company, SKU, price, and address fields instead come from report
environment/`extra_params`; they are not measurements from `config.json`.

## Required versus default versus configurable

The shared default for `METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS` is:

```text
ccsp_summary,managed_nodes,usage_by_organizations,usage_by_collections,usage_by_roles,usage_by_modules
```

Only the flags implemented by the selected report have an effect on its sheets:

| Flag / output | CCSP | CCSPv2 | Renewal guidance |
| --- | --- | --- | --- |
| Usage Reporting / `ccsp_summary` | Always built; no builder flag check | On by default; configurable | Always built; no builder flag check |
| `managed_nodes` | On by default | On by default | On by default; also enables Deleted Managed nodes and, with `--ephemeral`, ephemeral detail sheets |
| `usage_by_organizations` | No sheet | On by default | No sheet |
| `usage_by_collections` | On by default | On by default | No sheet |
| `usage_by_roles` | On by default | On by default | No sheet |
| `usage_by_modules` | On by default | On by default | No sheet |
| `indirectly_managed_nodes` | Opt-in | Opt-in | No sheet |
| `inventory_scope` | Opt-in | Opt-in | No sheet |
| `jobs` | No sheet | Opt-in | No sheet |
| `managed_nodes_by_organizations` | No sheet | Opt-in; adds per-org tabs and modifies Managed nodes when that sheet is enabled | No sheet |
| `infrastructure_summary` | Warning; skipped | Opt-in | No sheet |
| `data_collection_status` | No sheet | Opt-in | No sheet |

"Always built" describes the builder when a workbook is generated: the command
can still skip an existing report or return without building if every dataframe
is empty. **Default does not mean mandatory**: several default sheets are
configurable, and configurable sheets can share inputs with core output.

Solid arrows below show the normal data path. Dotted arrows describe a
conditional dependency, not an extra collector that always runs.

### CCSP: core and default output

```mermaid
flowchart TD
    Summary[job_host_summary.csv\nhost/connection, counters, created, job/org/template] --> Usage[DataframeJobhostSummaryUsage\nreachable-task filter; normalize hostname; group usage]
    Config[config.json\ninstall_uuid] --> Usage
    Usage --> Dedup[DedupCCSP\ndefault: no additional hardware merge]
    Scope[main_host / main_host_daily\ncompound serials from inventory scope] -. ccsp-experimental only .-> Dedup
    Dedup --> Direct[DIRECT usage]
    Direct --> Quantity[Distinct normalized hosts\nper organization name]
    Quantity --> Core[Usage Reporting\nalways built]
    Commercial[Company / SKU / price parameters] --> Core
    Direct --> Nodes[Managed nodes\nconfigurable; on by default]
    Events[main_jobevent.csv + config] --> Content[DataframeContentUsage\ndedup + summary-based hostname mapping]
    Dedup --> Content
    Content --> Collections[Usage by collections\nconfigurable; on by default]
    Content --> Roles[Usage by roles\nconfigurable; on by default]
    Content --> Modules[Usage by modules\nconfigurable; on by default]
```

The summary's measured quantity depends on direct summaries and installation
context, not event content or indirect nodes. Price/configuration completes
the commercial presentation. Inventory facts affect that quantity only when
the selected `ccsp-experimental` deduplicator produces a hostname mapping.
Indirect audit data can be loaded by the shared engine under the default
flags, but the CCSP default sheets do not add indirect usage to their outputs.

### CCSPv2: core and default output

```mermaid
flowchart TD
    Summary[job_host_summary.csv + config] --> Usage[DataframeJobhostSummaryUsage\ndirect and available indirect usage]
    Audit[main_indirectmanagednodeaudit.csv + config\nwhen present and enabled by loader] --> Usage
    Usage --> Dedup[DedupCCSP\ndefault: no additional hardware merge]
    Scope[main_host / main_host_daily\ninventory-scope serials] -. ccsp-experimental only .-> Dedup
    Dedup --> Filter[Optional organization-name filter\non usage dataframe]
    Filter --> Direct[DIRECT usage]
    Direct --> Quantity[Distinct normalized hosts\noverall direct quantity]
    Quantity --> Core[Usage Reporting\nccsp_summary; on by default]
    Commercial[Company / SKU / price / address parameters] --> Core
    Direct --> Nodes[Managed nodes\non by default]
    Filter --> Orgs[Usage by organizations\non by default]
    Events[main_jobevent.csv + config] --> Content[DataframeContentUsage\ndedup + summary-based hostname mapping]
    Dedup --> Content
    Content --> Collections[Usage by collections\non by default]
    Content --> Roles[Usage by roles\non by default]
    Content --> Modules[Usage by modules\non by default]
```

Unlike CCSP's per-org billing rows, CCSPv2's summary uses the global distinct
direct-host quantity after its optional org filter. The filter applies to the
usage dataframe, not the inventory-scope or content dataframe. In Usage by
organizations, job and task totals use all available usage rows, including
indirect rows; separate indirect host-count columns require
`indirectly_managed_nodes`. Indirect nodes never become part of the billing
summary merely because they appear in another sheet.

### Renewal guidance: core and default output

```mermaid
flowchart TD
    Metrics[main_hostmetric\nfirst/last automation, counters, deleted state] --> Collector[main_hostmetric collector\nLEFT JOIN on hostname]
    Hosts[main_host\nconnection variables, product serial, machine ID] --> Collector
    Collector --> Frame[DBDataframeHostMetric\nkeep last_automation at or after report since]
    Frame --> Dedup[Selected renewal deduplicator\ndefault: renewal, three expansion passes]
    Dedup --> Lifecycle[Deduped lifecycle records\nactive/deleted state and days_automated]
    Lifecycle --> Quantities[Distinct active and deleted\nrepresentative hostnames]
    Quantities --> Core[Usage Reporting\nalways built]
    Lifecycle --> Active[Managed nodes\nmanaged_nodes; on by default]
    Lifecycle --> Deleted[Deleted Managed nodes\nsame flag; on by default]
    Ephemeral[--ephemeral threshold\nexecution date and report interval] -. when supplied .-> Classify[Ephemeral classification\nand sliding-window peak]
    Lifecycle -.-> Classify
    Classify -. changes summary quantities .-> Core
```

All renewal sheets share this one dataset: there is no event, indirect audit,
tarball, or additional per-sheet collector. The collector's SQL includes
current host matches needed for identity/dedup. The default `renewal` strategy
expands hostname, connection-host, product-serial, and machine-ID matches;
`renewal-hostname` and `renewal-experimental` are alternative strategies, not
additional report inputs.

The CLI collects with the report's `since` and **no upper bound**, then applies
the last-automation filter above. `until` controls the displayed report period
and ephemeral windows, not that lifecycle query. Ephemeral classification also
depends on execution date; see the detail tree below.

## Smaller sheet dependency trees

The trees below describe output dependencies. They are not promises that
disabling a sheet eliminates all loading of its datasets; see
[current loading gates](#current-loading-gates) before choosing a minimal
collection or custom flag set.

### `managed_nodes` — CCSP and CCSPv2

Default-enabled but configurable in both reports.

```mermaid
flowchart LR
    Input[job_host_summary.csv + config] --> Engine[Usage engine\nreachable filter + hostname normalization]
    Engine --> Selected[Selected CCSP dedup\nDIRECT rows]
    Facts[Inventory scope / canonical facts] -. ccsp-experimental mapping and enrichment .-> Selected
    Selected --> Group[Group by host_name\norg count, row count, task sum, first/last automation]
    Group --> Sheet[Managed nodes]
```

At this final grouping, Job runs counts grouped usage rows, not a fresh
distinct-job query. First/last automation are the min/max summary `created`
timestamps carried by the usage engine. Experimental mode can add canonical
facts, facts, and pre-dedup-name tracking. CCSPv2's
`managed_nodes_by_organizations` mode additionally changes this sheet as shown
in its own tree below.

### `indirectly_managed_nodes` — CCSP and CCSPv2

Opt-in; does not alter either report's direct-only billing quantity.

```mermaid
flowchart LR
    Audit[main_indirectmanagednodeaudit.csv + config\ntarget, count, created, job/org, facts, events] --> Engine[Usage engine\ntask_runs from audit count]
    Engine --> Selected[Selected CCSP dedup\nINDIRECT rows]
    Selected --> Group[Group by target host_name\norg/row counts, task sum, dates, merged facts/events]
    Group --> Sheet[Indirectly Managed nodes]
```

Indirect audit rows do not use the direct reachable-task filter. Their
`events` array supplies content attribution, while `facts` and
`canonical_facts` describe the target. Absent audit data does not establish
that no indirect automation occurred.

### `inventory_scope` — CCSP and CCSPv2

Opt-in; this is inventory membership, not the billable usage set.

```mermaid
flowchart LR
    Hosts[main_host.csv / main_host_daily.csv + config\nhost, inventory/org, facts, last automation] --> Normalize[Normalize hostname\nansible_host_variable or original name]
    Normalize --> Group[DataframeInventoryScope\ngroup by host_name + install_uuid]
    Group --> Sets[Union organizations, inventories and facts\nmaximum last_automation]
    Sets --> Dedup[Selected CCSP dedup\noptional serial mapping]
    Dedup --> Sheet[Inventory Scope]
```

The same scope dataframe supplies compound serials to `ccsp-experimental`
even when the Inventory Scope sheet itself is not selected. Historical
observations and a fresh snapshot need not produce identical scope/facts.

### `jobs` — CCSPv2

Opt-in. Despite its title, this sheet does not require a separate unified-jobs
collector: the summary/audit datasets already contain its job context.

```mermaid
flowchart LR
    Input[Summary/audit usage + config\njob ID, template, org, host, task_runs, job_created] --> Usage[Usage engine + selected dedup\noptional CCSPv2 org filter]
    Usage --> Group[Group by organization_name + job_template_name]
    Group --> Values[Distinct job ID + install_uuid pairs\nhost counts, task sum, min/max job_created]
    Values --> Sheet[Jobs]
```

It consumes available direct **and** indirect usage; host counts here are not
filtered to direct nodes.

### `usage_by_organizations` — CCSPv2

Default-enabled but configurable. CCSP has no corresponding sheet.

```mermaid
flowchart LR
    Direct[job_host_summary.csv + config] --> Usage[Usage engine + selected dedup\noptional org filter]
    Indirect[Available indirect audit usage] --> Usage
    Usage --> Group[Group by organization_name\ndistinct job/installation pairs and task sum]
    Group --> Counts[Direct unique hosts and usage-row counts]
    Counts --> Sheet[Usage by organizations]
    Flag[indirectly_managed_nodes selected] -. also show indirect host counts .-> Sheet
```

Job/task totals use the combined dataframe independently of whether the
indirect host-count columns are shown. This sheet does not consume event
module/role data, even though its loader flag also enables `main_jobevent.csv`.

### `managed_nodes_by_organizations` — CCSPv2

Opt-in; one flag has two presentation effects.

```mermaid
flowchart TD
    Usage[Summary/audit usage\nafter selected dedup and optional org filter] --> Orgs[Distinct organization names]
    Orgs --> Filter[Filter usage for each organization\ngroup by host_name]
    Filter --> Tabs[One managed-node tab per organization\ncan include direct and indirect usage]
    Usage --> Direct[DIRECT usage only]
    Direct --> Pivot[Group by host_name and pivot org names\nmaximum last automation per organization]
    Pivot --> Main[Managed nodes with org-detail columns\nonly if managed_nodes is also selected]
```

Per-org tabs do not require `managed_nodes` to be selected, but the org-detail
variant of the main Managed nodes sheet does. The separate Indirectly Managed
nodes sheet keeps its normal layout.

### Shared content preparation

The next three sheets are separate flags, each default-enabled in CCSP and
CCSPv2. All use the following prepared content dataframe:

```mermaid
flowchart LR
    Events[main_jobevent.csv\ntask/resolved action, role/resolved role, host, job, duration] --> Content[DataframeContentUsage + config.install_uuid\nfilter, resolve names, count rows and sum duration]
    Content --> Dedup[Selected CCSP dedup\noptional serial mapping]
    Usage[Summary/audit usage\noriginal and normalized host, install_uuid, job ID] --> Map[Report hostname mapping]
    Dedup --> Map
    Map --> Prepared[Prepared content dataframe\nmodule, collection, role, host/job identity, counts, duration]
```

The content engine groups by host/module/collection/role/installation/job.
The report then maps original hostnames using installation/job identity.
These are the CLI `main_jobevent` variant's counts, not interchangeable with
the capped `main_jobevent_service` anonymized feed.

### `usage_by_collections` — CCSP and CCSPv2

```mermaid
flowchart LR
    Content[Prepared content\ncollection from resolved module FQCN prefix] --> Group[Group by collection_name\nunique hosts, unique host/job/installation pairs\ntask sum and duration sum]
    Audit[Indirect usage\nevents FQCNs, target, job, count] -. indirectly_managed_nodes selected .-> Expand[Expand distinct collection prefixes\nattribute audit count; duration zero]
    Expand -.-> Group
    Group --> Sheet[Usage by collections]
```

An indirect audit row's entire task count is attributed to **each** of its
distinct collections; it is not a per-collection event count. Only this
content sheet adds indirect collection attribution. The report's content
sheet block is still gated by the content dataframe being non-`None`.

### `usage_by_roles` — CCSP and CCSPv2

```mermaid
flowchart LR
    Content[Prepared content\nresolved_role or role; normalized role name] --> Group[Group by role_name\nunique hosts, unique host/job/installation pairs\ntask sum and duration sum]
    Group --> Sheet[Usage by roles]
```

No extra role collector and no indirect-audit expansion are used.

### `usage_by_modules` — CCSP and CCSPv2

```mermaid
flowchart LR
    Content[Prepared content\nresolved_action or task_action as module_name] --> Group[Group by module_name\nunique hosts, unique host/job/installation pairs\ntask sum and duration sum]
    Group --> Sheet[Usage by modules]
```

No extra module collector and no indirect-audit expansion are used.

### `infrastructure_summary` — CCSPv2

Opt-in; CCSP logs a warning and skips this sheet.

```mermaid
flowchart LR
    Audit[main_indirectmanagednodeaudit.csv + config] --> Usage[Usage engine + selected dedup\nINDIRECT rows; optional org filter]
    Usage --> Facts[facts.infra_type\nfacts.infra_bucket\nfacts.device_type]
    Facts --> Group[Group by infrastructure / bucket / device\ndistinct targets and usage-row counts]
    Group --> Sheet[Infrastructure Summary]
```

Missing fact values display as `Unknown`; this is not derived from direct
inventory-host hardware facts. This flag enables audit CSV loading directly;
no managed-node sheet is required.

### `data_collection_status` — CCSPv2

Opt-in; no Controller collector or analytics feed substitutes for this input.

```mermaid
flowchart LR
    Packages[All selected tarballs\ndata_collection_status.csv] --> Engine[DataframeCollectionStatus\nconcatenate rows; parse timestamps]
    Engine --> Gaps[Successful collection bounds\nreport start/end; gaps over two seconds]
    Engine --> Status[Collection timestamp, file, status, elapsed\ninterval since previous collection]
    Gaps --> Sheet[Data collection status\nmissing-window and collection-status tables]
    Status --> Sheet
```

Unlike the dataset-specific engines, this engine visits all tarballs because
status metadata is present in packages named after other collectors. It does
not deduplicate or reconstruct failed gather attempts from job records.

### Renewal `managed_nodes` and `--ephemeral` details

`managed_nodes` is default-enabled. Without `--ephemeral`, it enables the two
detail sheets below; disabling it leaves the core Usage Reporting sheet.

```mermaid
flowchart LR
    Input[Deduped lifecycle records\nfirst/last automation, counters, deleted\nidentity unions and record counts] --> Active[Non-deleted representatives]
    Active --> Managed[Managed nodes]
    Input --> Deleted[Deleted representatives]
    Deleted --> DeletedSheet[Deleted Managed nodes]
```

With `--ephemeral=Ndays` (or another supported duration), the same input is
classified by `days_automated <= N` and an execution-date-based first-automation
threshold. Peak usage also needs the requested report interval:

```mermaid
flowchart TD
    Input[Deduped lifecycle records\nfirst/last automation and deleted state] --> Classify[days_automated + first-automation age threshold]
    Now[--ephemeral N and execution date] --> Classify
    Classify --> Active[Non-deleted, non-ephemeral]
    Active --> Managed[Managed nodes\nmanaged_nodes flag]
    Classify --> Ephemeral[Non-deleted, ephemeral]
    Ephemeral --> Detail[Managed nodes ephemeral\nmanaged_nodes flag]
    Classify --> History[Ephemeral lifecycle records\ninclude deleted representatives]
    Period[Report since/until + N] --> Windows[Slide N-day windows by one day]
    History --> Windows
    Windows --> Peak[Maximum overlapping distinct representatives\nUsage Reporting quantity]
    Windows --> Usage[Managed nodes ephemeral usage\nper-window counts; managed_nodes flag]
    Input --> Deleted[Deleted Managed nodes\nmanaged_nodes flag]
```

The peak calculation runs for Usage Reporting even if detail sheets are
disabled. `Deleted Managed nodes` continues to include all deleted groups,
not only ephemeral ones. No additional collector is needed for any of these
sheets.

## Current loading gates

Sheet dependencies above and file-loading behavior are deliberately separate.
`DataframeFactory` always creates all CCSP engines (plus collection-status for
CCSPv2), and `DedupCCSP.run()` calls every engine's `build_dataframe()`, even
without experimental dedup. The extractor then applies **both** the engine's
requested collection names and `CSV_SHEETS`:

| File | At least one of these flags enables CSV loading |
| --- | --- |
| `job_host_summary.csv` | `ccsp_summary`, `indirectly_managed_nodes`, `inventory_scope`, `jobs`, `managed_nodes`, `managed_nodes_by_organizations`, `usage_by_organizations` |
| `main_host.csv`, `main_host_daily.csv` | `inventory_scope`, `jobs`, `managed_nodes`, `managed_nodes_by_organizations`, `usage_by_collections`, `usage_by_modules`, `usage_by_roles` |
| `main_indirectmanagednodeaudit.csv` | `indirectly_managed_nodes`, `infrastructure_summary`, `managed_nodes`, `usage_by_organizations` |
| `main_jobevent.csv` | `usage_by_collections`, `usage_by_modules`, `usage_by_organizations`, `usage_by_roles` |
| `data_collection_status.csv` | `data_collection_status` |

`config.json` is requested independently as context. With the stock defaults,
both CCSP variants can load summary, host-scope, audit, and event files even
though Inventory Scope and Indirectly Managed nodes are not rendered.
Creating/invoking an engine does not mean every CSV is enabled, and disabling
one sheet does not imply its input is unused by all other paths.

`jobs` enables its summary CSV input directly, and `infrastructure_summary`
enables its audit CSV input directly. Either sheet can be selected without an
unrelated sheet serving as a workaround to load its required data.

## Source references

- [Command defaults and orchestration](../metrics_utility/management/commands/build_report.py)
- [Extractor factory](../metrics_utility/automation_controller_billing/extract/factory.py) and [CSV sheet gates](../metrics_utility/automation_controller_billing/extract/base.py)
- [Dataframe factory](../metrics_utility/automation_controller_billing/dataframe_engine/factory.py) and [engines](../metrics_utility/automation_controller_billing/dataframe_engine/)
- [Deduplicators](../metrics_utility/automation_controller_billing/dedup/)
- [CCSP builder](../metrics_utility/automation_controller_billing/report/report_ccsp.py), [CCSPv2 builder](../metrics_utility/automation_controller_billing/report/report_ccsp_v2.py), [renewal builder](../metrics_utility/automation_controller_billing/report/report_renewal_guidance.py)
- [Shared sheet aggregation and content/hostname mapping](../metrics_utility/automation_controller_billing/report/base.py)
