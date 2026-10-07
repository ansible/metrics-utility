# repos_update

Clones and updates all AAP-related repositories into a single parent directory.

## Usage

```bash
./update_all.sh
```

The script auto-detects its location inside the `metrics-utility` repo and
operates one directory above it (the common parent where all sibling repos
live). For example, if `metrics-utility` is checked out at `~/aap/metrics-utility`,
repos will be cloned/updated in `~/aap/`.

## What it does

1. **Clone** — any repo from the list that isn't already present is cloned.
   Private or inaccessible repos (e.g. GitLab behind VPN) are skipped with a
   warning.
2. **Update** — every git repo found in the parent directory is fetched and
   fast-forward pulled on its default branch. If you're on a feature branch the
   script temporarily switches to the default branch, pulls, and switches back.
   Uncommitted changes are auto-stashed and restored.

## Prerequisites

- SSH keys configured for GitHub (and GitLab + Red Hat VPN for internal repos).
- Git ≥ 2.22 (for `branch --show-current`).

## Managed repos

| Area | Repos |
|---|---|
| Gateway | aap-gateway, aap-gateway-operator |
| Metrics Service | metrics-service, aap-metrics-service, automation-metrics-service-container, system-certifi, automation-metrics-operator, automation-metrics-operator-container, aap-containerized-installer, emerging-services-test-suite, platform-services-test-suite |
| Metrics Utility | metrics-utility |
| AWX | awx |
| EDA | eda-server, automation-eda-controller-operator-source, eda-partner-testing |
| AAP UI | aap-ui |
| AAP Dev Environment | aap-dev |
| CI Infrastructure | aap-jenkins-shared-library, aapqa-provisioner |
| Automation Dashboard | automation-reports |
| Related / External | django-ansible-base, handbook |

To add a new repo, append an entry to the `REPOS` array in `update_all.sh`
using the format `"clone_url|local_dirname"`.
