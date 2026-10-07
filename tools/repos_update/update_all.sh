#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Walk up from tools/repos_update/ to the metrics-utility root, then one more
# level to reach the parent directory where all sibling repos live.
METRICS_UTILITY_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null)" \
  || { echo "Cannot find metrics-utility repo root"; exit 1; }
AAP_DIR="$(dirname "$METRICS_UTILITY_ROOT")"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
RESET='\033[0m'

ok()   { echo -e "  ${GREEN}✓${RESET} $*"; }
warn() { echo -e "  ${YELLOW}!${RESET} $*"; }
err()  { echo -e "  ${RED}✗${RESET} $*"; }

# ---------------------------------------------------------------------------
# Repos to keep cloned — any entry not yet present will be cloned.
# Format: "clone_url|local_dirname"
# GitLab entries require Red Hat SSO / VPN access.
# ---------------------------------------------------------------------------
REPOS=(
  # Gateway
  "git@github.com:ansible/aap-gateway.git|aap-gateway"
  "git@github.com:ansible-automation-platform/aap-gateway-operator.git|aap-gateway-operator"
  # Metrics Service
  "git@github.com:ansible/metrics-service.git|metrics-service"
  "git@github.com:ansible-automation-platform/metrics-service.git|aap-metrics-service"
  "git@github.com:ansible-automation-platform/automation-metrics-service-container.git|automation-metrics-service-container"
  "git@github.com:ansible/system-certifi.git|system-certifi"
  "git@github.com:ansible-automation-platform/automation-metrics-operator.git|automation-metrics-operator"
  "git@github.com:ansible-automation-platform/automation-metrics-operator-container.git|automation-metrics-operator-container"
  "git@gitlab.cee.redhat.com:ansible/aap-containerized-installer.git|aap-containerized-installer"
  "git@gitlab.cee.redhat.com:ansible/testing/emerging-services-test-suite.git|emerging-services-test-suite"
  "git@github.com:ansible-automation-platform/platform-services-test-suite.git|platform-services-test-suite"
  # Metrics Utility
  "git@github.com:ansible/metrics-utility.git|metrics-utility"
  # AWX
  "git@github.com:ansible/awx.git|awx"
  # EDA
  "git@github.com:ansible/eda-server.git|eda-server"
  "git@github.com:ansible-automation-platform/automation-eda-controller-operator-source.git|automation-eda-controller-operator-source"
  "git@github.com:ansible/eda-partner-testing.git|eda-partner-testing"
  # AAP UI
  "git@github.com:ansible-automation-platform/aap-ui.git|aap-ui"
  # AAP Dev Environment
  "git@github.com:ansible/aap-dev.git|aap-dev"
  # CI Infrastructure (GitLab — needs RH VPN/SSO)
  "git@gitlab.cee.redhat.com:aap-ci/aap-jenkins-shared-library.git|aap-jenkins-shared-library"
  "git@gitlab.cee.redhat.com:aap-ci/aapqa-provisioner.git|aapqa-provisioner"
  # Automation Dashboard
  "git@github.com:ansible/automation-reports.git|automation-reports"
  # Related / External
  "git@github.com:ansible/django-ansible-base.git|django-ansible-base"
  "git@github.com:ansible/handbook.git|handbook"
)

ensure_cloned() {
  local url="$1"
  local dirname="$2"
  local dest="$AAP_DIR/$dirname"

  [[ -d "$dest/.git" ]] && return 0

  echo -e "\n${CYAN}==> Cloning ${dirname}${RESET}"
  if git clone "$url" "$dest" -q 2>/dev/null; then
    ok "Cloned from $url"
  else
    warn "Could not clone $url (private or inaccessible — skipping)"
  fi
}

get_default_branch() {
  local dir="$1"
  # Try origin/HEAD first
  local branch
  branch=$(git -C "$dir" symbolic-ref refs/remotes/origin/HEAD 2>/dev/null | sed 's|refs/remotes/origin/||')
  if [[ -n "$branch" ]]; then
    echo "$branch"; return
  fi
  # Fall back to checking common branch names in priority order
  for b in devel ansible-automation-platform-devel main master; do
    if git -C "$dir" show-ref --verify --quiet "refs/remotes/origin/$b"; then
      echo "$b"; return
    fi
  done
}

update_repo() {
  local dir="$1"
  local repo
  repo=$(basename "$dir")

  echo -e "\n${CYAN}==> ${repo}${RESET}"

  local default_branch
  default_branch=$(get_default_branch "$dir")

  if [[ -z "$default_branch" ]]; then
    err "Could not determine default branch, skipping."
    return
  fi

  local current_branch
  current_branch=$(git -C "$dir" branch --show-current 2>/dev/null || echo "")

  # Stash uncommitted changes so we can switch branches safely
  local stashed=false
  if ! git -C "$dir" diff --quiet || ! git -C "$dir" diff --cached --quiet; then
    warn "Stashing local changes..."
    git -C "$dir" stash push -m "update_all.sh auto-stash" --include-untracked -q
    stashed=true
  fi

  # Fetch latest from origin
  git -C "$dir" fetch origin --prune -q
  ok "Fetched origin (default branch: ${default_branch})"

  if [[ "$current_branch" == "$default_branch" ]]; then
    # Already on default branch — just pull
    if git -C "$dir" pull --ff-only -q; then
      ok "Pulled (fast-forward)"
    else
      warn "Fast-forward failed — skipping pull (rebase manually if needed)"
    fi
  else
    # Update default branch ref without switching, then briefly switch to pull
    git -C "$dir" checkout -q "$default_branch"
    if git -C "$dir" pull --ff-only -q; then
      ok "Updated ${default_branch} (was on: ${current_branch:-detached HEAD})"
    else
      warn "Fast-forward failed on ${default_branch} — skipping pull"
    fi
    git -C "$dir" checkout -q "${current_branch:-$default_branch}"
  fi

  # Restore stash if we created one
  if $stashed; then
    git -C "$dir" stash pop -q
    ok "Restored stashed changes"
  fi
}

echo "Ensuring all repos are cloned in: $AAP_DIR"

for entry in "${REPOS[@]}"; do
  ensure_cloned "${entry%%|*}" "${entry##*|}"
done

echo -e "\nUpdating all repos in: $AAP_DIR"

for dir in "$AAP_DIR"/*/; do
  if [[ -d "$dir/.git" ]]; then
    update_repo "$dir" || err "Unexpected error processing $(basename "$dir")"
  fi
done

echo -e "\n${GREEN}Done.${RESET}"
