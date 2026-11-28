"""Deterministic, offline privacy contract checks for diffs and files."""

from __future__ import annotations

import re
import subprocess

from dataclasses import dataclass, field
from pathlib import Path

from tools.privacy_guard.contracts import Contract, matched_contracts, normalize_path


ERROR = 'error'
WARNING = 'warning'

_TEST_PREFIX = 'metrics_utility/test/'

_SEGMENT_CONTRACT = 'segment_telemetry'
_BILLING_CONTRACT = 'billing_archive'
_AWX_CONTRACT = 'awx_analytics'
_LOG_CONTRACTS = (_SEGMENT_CONTRACT, _BILLING_CONTRACT, _AWX_CONTRACT)

_SEG001_FORBIDDEN_KEYS = (
    'host_ids',
    'host_names',
    'host_name',
    'host_remote_id',
    'canonical_facts',
    'ansible_host',
    'organizations',
    'organization_name',
    'job_template_name',
    'inventory_name',
    'project_name',
    'modules_used_per_playbook_total',
    'account_number',
    'subscription_id',
    'install_uuid',
    'launched_by_username',
)
_SEG001_KEY_ALTERNATION = '|'.join(re.escape(key) for key in sorted(_SEG001_FORBIDDEN_KEYS, key=len, reverse=True))
_SEG001_RE = re.compile(rf"""(?:['"](?P<dict_key>{_SEG001_KEY_ALTERNATION})['"]\s*:|\[\s*['"](?P<item_key>{_SEG001_KEY_ALTERNATION})['"]\s*\]\s*=)""")

_MANAGED_TRUE_RE = re.compile(r'\bmanaged\s*=\s*true\b', re.IGNORECASE)
_FILLNA_CUSTOM_RE = re.compile(r"""fillna\(\s*['"]Custom['"]\s*\)""")
_CUSTOM_SOURCE_RE = re.compile(r"collection_source.*['\"]Custom['\"]")
_HELPER_NAMES = ('_remove_custom_items', '_remove_unknown_installed_collections')

_SEG004_STABLE_IDS = ('install_uuid', 'system_uuid', 'account_number', 'subscription_id')
_SEG004_USER_ID_RE = re.compile(r"""['"]userId['"]\s*[:=]""")

_BILL_SECRET_KEYS = (
    'bucket_secret_key',
    'bucket_access_key',
    'candlepin_key_pem',
    'candlepin_cert_pem',
    'secret_key',
    'access_key',
    'password',
    'token',
    'private_key',
    'key_pem',
    'cert_pem',
)
_BILL_KEY_ALTERNATION = '|'.join(re.escape(key) for key in sorted(_BILL_SECRET_KEYS, key=len, reverse=True))
_BILL_SECRET_RE = re.compile(
    rf"""(?:['"](?P<dict_key>{_BILL_KEY_ALTERNATION})['"]\s*:|\[\s*['"](?P<item_key>{_BILL_KEY_ALTERNATION})['"]\s*\]\s*=|(?<![\w])(?P<bare_key>{_BILL_KEY_ALTERNATION})(?![\w])\s*=)"""
)
_BILL_QUOTED_KEY_RE = re.compile(rf"""['"](?P<key>{_BILL_KEY_ALTERNATION})['"]""")

_LOG_CALL_RE = re.compile(r'\b(?:logger|LOG|log|logging)\.(?:debug|info|warning|warn|error|exception|critical)\s*\(')
_LOG_SENSITIVE_TOKENS = (
    'segment_data',
    'anonymized_data',
    'billing_provider_params',
    'bucket_secret_key',
    'bucket_access_key',
    'candlepin_key_pem',
    'candlepin_cert_pem',
    'write_key',
    'event_data',
    'request_body',
    'password',
    'secret',
)

_PEM_RE = re.compile(r'-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----')
_HARDCODED_SECRET_RE = re.compile(
    r"""(?<![\w])(?P<var>(?:[A-Za-z_][A-Za-z0-9_]*)?(?:password|secret|token|api_key|private_key|access_key)[A-Za-z0-9_]*)['"]?\s*(?::=|:|=)\s*(?P<quote>['"])(?P<value>[^'"]{6,})(?P=quote)""",
    re.IGNORECASE,
)
_SECRET_PLACEHOLDERS = ('changeme', 'example', 'your-', 'xxxx', '****', 'redacted', 'placeholder', 'dummy', 'not-a-real')
_SECRET_TYPE_NAMES = {'string', 'boolean', 'integer', 'number', 'object', 'array', 'null', 'str', 'int', 'bool'}

_AWX_SENSITIVE_COLUMNS_RE = re.compile(r'\b(launched_by_username|launched_by_email|created_by_username|email|password)\b', re.IGNORECASE)


@dataclass(frozen=True)
class Finding:
    """A single privacy contract finding."""

    rule_id: str
    severity: str
    contract: str
    file: str
    line: int | None
    message: str
    recommendation: str

    def to_dict(self) -> dict[str, object]:
        return {
            'rule_id': self.rule_id,
            'severity': self.severity,
            'contract': self.contract,
            'file': self.file,
            'line': self.line,
            'message': self.message,
            'recommendation': self.recommendation,
        }


@dataclass(frozen=True)
class DiffLine:
    """One added or removed line in a unified diff."""

    kind: str
    line: int
    text: str


@dataclass
class FileDiff:
    """Added and removed lines for one file in a unified diff."""

    path: str
    added: list[DiffLine] = field(default_factory=list)
    removed: list[DiffLine] = field(default_factory=list)


def _clean_diff_path(raw_path: str) -> str | None:
    value = raw_path.strip()
    if value == '/dev/null':
        return None
    if '\t' in value:
        value = value.split('\t', 1)[0]
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        value = value[1:-1]
    if value.startswith(('a/', 'b/')):
        value = value[2:]
    return normalize_path(value)


_HUNK_RE = re.compile(r'^@@ -(?P<old>\d+)(?:,\d+)? \+(?P<new>\d+)(?:,\d+)? @@')


def parse_unified_diff(diff_text: str) -> dict[str, FileDiff]:
    """Parse a unified diff into per-file added and removed lines.

    Line numbers refer to the old file for removed lines and the new file for
    added lines.
    """
    files: dict[str, FileDiff] = {}
    current_path: str | None = None
    old_path: str | None = None
    in_hunk = False
    old_line = 0
    new_line = 0

    for raw_line in diff_text.splitlines():
        if in_hunk:
            if raw_line.startswith('+'):
                if current_path is not None:
                    files[current_path].added.append(DiffLine('added', new_line, raw_line[1:]))
                new_line += 1
                continue
            if raw_line.startswith('-'):
                if current_path is not None:
                    files[current_path].removed.append(DiffLine('removed', old_line, raw_line[1:]))
                old_line += 1
                continue
            if raw_line.startswith(' '):
                old_line += 1
                new_line += 1
                continue
            if raw_line.startswith('\\'):
                continue
            in_hunk = False

        if raw_line.startswith('--- '):
            old_path = _clean_diff_path(raw_line[4:])
            continue
        if raw_line.startswith('+++ '):
            new_path = _clean_diff_path(raw_line[4:])
            current_path = new_path or old_path
            if current_path is not None:
                files.setdefault(current_path, FileDiff(current_path))
            continue
        if raw_line.startswith('diff --git '):
            in_hunk = False
            continue
        match = _HUNK_RE.match(raw_line)
        if match is not None:
            in_hunk = True
            old_line = int(match.group('old'))
            new_line = int(match.group('new'))
            continue

    return files


def _is_comment(text: str) -> bool:
    stripped = text.strip()
    return stripped.startswith(('#', '//', '--', '/*', '*'))


def _is_negative_guard(text: str) -> bool:
    lower = text.lower()
    if 'not in' in lower and 'assert' in lower:
        return True
    return '.pop(' in text or bool(re.search(r'\bdel\s', text))


def _added_text(file_diff: FileDiff) -> str:
    return '\n'.join(line.text for line in file_diff.added)


def _removed_text(file_diff: FileDiff) -> str:
    return '\n'.join(line.text for line in file_diff.removed)


def _finding(rule_id: str, severity: str, contract: str, path: str, line: int | None, message: str, recommendation: str) -> Finding:
    return Finding(rule_id, severity, contract, path, line, message, recommendation)


def _check_seg001(file_diff: FileDiff) -> list[Finding]:
    findings = []
    for line in file_diff.added:
        if _is_comment(line.text) or _is_negative_guard(line.text):
            continue
        match = _SEG001_RE.search(line.text)
        if match is None:
            continue
        key = match.group('dict_key') or match.group('item_key')
        findings.append(
            _finding(
                'SEG-001',
                ERROR,
                _SEGMENT_CONTRACT,
                file_diff.path,
                line.line,
                f"Forbidden customer identifier field '{key}' added to anonymized telemetry.",
                'Remove the field or route it through an approved aggregate/allowlist before it reaches Segment.',
            )
        )
    return findings


def _check_seg002_paths(file_diff: FileDiff) -> list[Finding]:
    """Check anonymization filter and allowlist weakening."""
    findings = []
    added = _added_text(file_diff)
    path = normalize_path(file_diff.path)
    basename = Path(path).name

    if basename == 'credentials_service.py':
        for line in file_diff.removed:
            if _MANAGED_TRUE_RE.search(line.text) and not _MANAGED_TRUE_RE.search(added):
                findings.append(
                    _finding(
                        'SEG-002',
                        ERROR,
                        _SEGMENT_CONTRACT,
                        path,
                        line.line,
                        'Removed the managed = true credential-type privacy predicate.',
                        'Restore managed = true; custom credential types must never reach Segment telemetry.',
                    )
                )

    if path.startswith('metrics_utility/anonymized_rollups/'):
        findings.extend(_check_anonymization_filters(file_diff, added, path, basename))

    return findings


def _check_anonymization_filters(file_diff: FileDiff, added: str, path: str, basename: str) -> list[Finding]:
    findings = []

    for line in file_diff.removed:
        if _FILLNA_CUSTOM_RE.search(line.text) and not _FILLNA_CUSTOM_RE.search(added):
            findings.append(
                _finding(
                    'SEG-002',
                    ERROR,
                    _SEGMENT_CONTRACT,
                    path,
                    line.line,
                    "Changed the fail-closed fillna('Custom') anonymization fallback.",
                    "Keep fillna('Custom') so unknown collection sources are dropped from anonymized rollups.",
                )
            )

        if 'collection_source' in line.text and re.search(r"""['"]Custom['"]""", line.text):
            added_has_guard = any(_CUSTOM_SOURCE_RE.search(added_line.text) for added_line in file_diff.added)
            if not added_has_guard:
                findings.append(
                    _finding(
                        'SEG-002',
                        ERROR,
                        _SEGMENT_CONTRACT,
                        path,
                        line.line,
                        "Removed or weakened the collection_source == 'Custom' exclusion.",
                        'Restore the Custom exclusion so private collection content cannot be emitted.',
                    )
                )

        for helper_name in _HELPER_NAMES:
            if helper_name in line.text and helper_name not in added:
                findings.append(
                    _finding(
                        'SEG-002',
                        ERROR,
                        _SEGMENT_CONTRACT,
                        path,
                        line.line,
                        f'Removed the {helper_name} allowlist enforcement.',
                        'Restore the fail-closed helper and keep unknown/custom entries excluded.',
                    )
                )
                break

        if basename == 'helpers.py' and 'return {}' in line.text and 'return {}' not in added:
            findings.append(
                _finding(
                    'SEG-002',
                    ERROR,
                    _SEGMENT_CONTRACT,
                    path,
                    line.line,
                    'Weakened the load_known_collections fail-closed empty-dict fallback.',
                    'Restore the empty-dict fallback so a missing or malformed allowlist emits nothing unknown.',
                )
            )

    return findings


def _check_seg003(file_diff: FileDiff) -> list[Finding]:
    path = normalize_path(file_diff.path)
    if not path.endswith(('anonymized_rollups/collections.json', 'anonymized_rollups/types.py')):
        return []
    if not (file_diff.added or file_diff.removed):
        return []
    first_line = file_diff.added[0] if file_diff.added else file_diff.removed[0]
    return [
        _finding(
            'SEG-003',
            WARNING,
            _SEGMENT_CONTRACT,
            path,
            first_line.line,
            'Privacy-sensitive allowlist or payload schema file changed.',
            'Request explicit privacy/schema owner review and add a synthetic canary test for the new content.',
        )
    ]


def _check_seg004(file_diff: FileDiff) -> list[Finding]:
    findings = []
    path = normalize_path(file_diff.path)
    if Path(path).name != 'segment.py':
        return findings

    for line in file_diff.added:
        if _is_comment(line.text):
            continue
        text = line.text
        stable_id = next((identifier for identifier in _SEG004_STABLE_IDS if identifier in text), None)
        has_user_id = _SEG004_USER_ID_RE.search(text) is not None
        has_hash = 'hashlib' in text

        if has_user_id and 'anonymous' not in text.lower():
            findings.append(
                _finding(
                    'SEG-004',
                    ERROR,
                    _SEGMENT_CONTRACT,
                    path,
                    line.line,
                    'Potential stable customer identifier passed into the Segment userId envelope.',
                    'Use the ephemeral random anonymousId only; never derive Segment identity from install_uuid, account_number, or user_id.',
                )
            )
        elif stable_id is not None and (has_hash or has_user_id or 'event' in text.lower() or 'segment_meta' in text):
            findings.append(
                _finding(
                    'SEG-004',
                    ERROR,
                    _SEGMENT_CONTRACT,
                    path,
                    line.line,
                    f"Stable customer identifier '{stable_id}' flows into the Segment event/envelope.",
                    'Keep stable identifiers out of Segment. Hash-based pseudonymisation is still a stable identifier.',
                )
            )
    return findings


def _check_bill001(file_diff: FileDiff) -> list[Finding]:
    findings = []
    path = normalize_path(file_diff.path)
    removed_sensitive: dict[str, DiffLine] = {}
    added_sensitive: set[str] = set()

    for line in file_diff.removed:
        if '_SENSITIVE_BILLING_KEYS' not in line.text:
            continue
        for match in _BILL_QUOTED_KEY_RE.finditer(line.text):
            removed_sensitive.setdefault(match.group('key'), line)
    for line in file_diff.added:
        if '_SENSITIVE_BILLING_KEYS' in line.text:
            added_sensitive.update(match.group('key') for match in _BILL_QUOTED_KEY_RE.finditer(line.text))

    for key, line in removed_sensitive.items():
        if key not in added_sensitive:
            findings.append(
                _finding(
                    'BILL-001',
                    ERROR,
                    _BILLING_CONTRACT,
                    path,
                    line.line,
                    f"Removed '{key}' from the _SENSITIVE_BILLING_KEYS redaction denylist.",
                    'Restore the key; credentials must never be serialized into billing archives.',
                )
            )

    for line in file_diff.added:
        findings.extend(_check_added_billing_secret(path, line))
    return findings


def _check_added_billing_secret(path: str, line: DiffLine) -> list[Finding]:
    text = line.text
    if _is_comment(text) or '_SENSITIVE' in text or 'redact' in text.lower():
        return []

    match = _BILL_SECRET_RE.search(text)
    if match is None:
        return []
    key = match.group('dict_key') or match.group('item_key') or match.group('bare_key')
    if 'not in' in text or '.pop(' in text:
        return []
    return [
        _finding(
            'BILL-001',
            ERROR,
            _BILLING_CONTRACT,
            path,
            line.line,
            f"Secret/credential key '{key}' added to billing payload or config without redaction.",
            'Keep credentials out of billing_provider_params and config.json; redact before serialization.',
        )
    ]


def _check_log001(file_diff: FileDiff, contract: str) -> list[Finding]:
    findings = []
    path = normalize_path(file_diff.path)
    payload_is_sensitive = Path(path).name == 'segment.py'

    for line in file_diff.added:
        if _is_comment(line.text) or _LOG_CALL_RE.search(line.text) is None:
            continue
        if 'redact' in line.text.lower() or '[redacted]' in line.text.lower():
            continue
        tokens = list(_LOG_SENSITIVE_TOKENS)
        if payload_is_sensitive:
            tokens.append('payload')
        token = next((candidate for candidate in tokens if candidate in line.text.lower()), None)
        if token is None:
            continue
        findings.append(
            _finding(
                'LOG-001',
                ERROR,
                contract,
                path,
                line.line,
                f"Sensitive value '{token}' added to a logger call.",
                'Log only counts, identifiers of the operation, or redacted summaries; never log payloads or secrets.',
            )
        )
    return findings


def _check_sec001(file_diff: FileDiff, contract: str) -> list[Finding]:
    findings = []
    path = normalize_path(file_diff.path)
    if path.startswith(_TEST_PREFIX):
        return findings

    for line in file_diff.added:
        if _is_comment(line.text):
            continue
        if _PEM_RE.search(line.text):
            findings.append(
                _finding(
                    'SEC-001',
                    ERROR,
                    contract,
                    path,
                    line.line,
                    'Private key material added to the repository.',
                    'Never commit private keys. Load them from a secret store at runtime and rotate the exposed key.',
                )
            )
            continue
        if _hardcoded_secret(line.text):
            findings.append(
                _finding(
                    'SEC-001',
                    ERROR,
                    contract,
                    path,
                    line.line,
                    'Hardcoded secret or credential assignment added.',
                    'Read the value from an environment variable or secret manager; do not commit it.',
                )
            )
    return findings


def _hardcoded_secret(text: str) -> bool:
    if any(marker in text for marker in ('os.getenv', 'os.environ', 'settings.', 'secret_key = None')):
        return False
    match = _HARDCODED_SECRET_RE.search(text)
    if match is None:
        return False
    value = match.group('value').lower()
    if value.startswith(('${', '{{', '%(')):
        return False
    if value in _SECRET_TYPE_NAMES:
        return False
    return not any(placeholder in value for placeholder in _SECRET_PLACEHOLDERS)


def _looks_like_sql(text: str) -> bool:
    upper = text.upper()
    return any(marker in upper for marker in ('SELECT', ' FROM ', ' AS ', 'JOIN '))


def _looks_like_column_projection(text: str) -> bool:
    stripped = text.strip()
    return stripped.endswith(',') or bool(re.search(r'\b[A-Za-z_][A-Za-z0-9_]*\.(?:email|password)\b', text, re.IGNORECASE))


def _check_awx001(file_diff: FileDiff) -> list[Finding]:
    findings = []
    path = normalize_path(file_diff.path)
    basename = Path(path).name
    added = _added_text(file_diff)
    removed = _removed_text(file_diff)
    if not path.startswith(('metrics_utility/base/', 'metrics_utility/library/collectors/controller/')):
        return findings

    broadened_lines = [line for line in file_diff.added if 'pg_stat_user_tables' in line.text] if basename == 'table_metadata.py' else []
    dropped_allowlist = 'c.relname' in removed and 'c.relname' not in added
    if broadened_lines or dropped_allowlist:
        first = broadened_lines[0] if broadened_lines else None
        line_number = first.line if first is not None else (file_diff.removed[0].line if file_diff.removed else None)
        findings.append(
            _finding(
                'AWX-001',
                WARNING,
                _AWX_CONTRACT,
                path,
                line_number,
                'Hardcoded table allowlist generalized or removed from a controller collector.',
                'Keep explicit table allowlists; broadening to pg_stat_user_tables can collect unrelated or sensitive rows.',
            )
        )

    for line in file_diff.added:
        if _is_comment(line.text):
            continue
        match = _AWX_SENSITIVE_COLUMNS_RE.search(line.text)
        if match is None:
            continue
        column = match.group(1)
        if column.lower() in {'email', 'password'} and not (_looks_like_sql(line.text) or _looks_like_column_projection(line.text)):
            continue
        findings.append(
            _finding(
                'AWX-001',
                WARNING,
                _AWX_CONTRACT,
                path,
                line.line,
                f"Sensitive user column '{column}' added to a controller collector.",
                'Confirm the column is required and covered by downstream exclusions; prefer an aggregate or omit it.',
            )
        )
    return findings


def _check_file_diff(file_diff: FileDiff, contract_names: list[str]) -> list[Finding]:
    findings: list[Finding] = []
    path = normalize_path(file_diff.path)
    has_segment = _SEGMENT_CONTRACT in contract_names or path.startswith('metrics_utility/anonymized_rollups/')
    has_billing = _BILLING_CONTRACT in contract_names
    has_awx = _AWX_CONTRACT in contract_names
    if not (has_segment or has_billing or has_awx or contract_names):
        primary_contract = 'all'
    else:
        primary_contract = contract_names[0] if contract_names else 'all'

    if has_segment:
        if path.startswith('metrics_utility/anonymized_rollups/') or Path(path).name == 'segment.py':
            findings.extend(_check_seg001(file_diff))
        findings.extend(_check_seg002_paths(file_diff))
        findings.extend(_check_seg003(file_diff))
        findings.extend(_check_seg004(file_diff))

    if has_billing:
        findings.extend(_check_bill001(file_diff))

    log_contract = next((name for name in _LOG_CONTRACTS if name in contract_names), None)
    if log_contract is not None:
        findings.extend(_check_log001(file_diff, log_contract))

    if has_awx:
        findings.extend(_check_awx001(file_diff))

    findings.extend(_check_sec001(file_diff, primary_contract))
    return findings


def _dedupe(findings: list[Finding]) -> list[Finding]:
    seen: set[tuple[object, ...]] = set()
    unique: list[Finding] = []
    for finding in findings:
        key = (finding.rule_id, finding.file, finding.line, finding.message)
        if key in seen:
            continue
        seen.add(key)
        unique.append(finding)
    return unique


def check_unified_diff(diff_text: str, contracts: dict[str, Contract]) -> list[Finding]:
    """Check a unified diff against every matched data contract."""
    findings: list[Finding] = []
    for path, file_diff in parse_unified_diff(diff_text).items():
        findings.extend(_check_file_diff(file_diff, matched_contracts(path, contracts)))
    return _dedupe(findings)


def _synthetic_diff(path: str, content: str) -> str:
    lines = content.splitlines()
    header = [f'diff --git a/{path} b/{path}', 'new file mode 100644', '--- /dev/null', f'+++ b/{path}', f'@@ -0,0 +1,{len(lines)} @@']
    return '\n'.join([*header, *(f'+{line}' for line in lines)]) + '\n'


def _file_diff_for(path: str, repo_root: Path) -> str | None:
    try:
        tracked = subprocess.run(
            ['git', '-C', str(repo_root), 'ls-files', '--error-unmatch', '--', path],
            capture_output=True,
            text=True,
            check=False,
        )
        if tracked.returncode == 0:
            diff = subprocess.run(
                ['git', '-C', str(repo_root), 'diff', 'HEAD', '--', path],
                capture_output=True,
                text=True,
                check=False,
            )
            if diff.returncode == 0:
                return diff.stdout
    except (OSError, subprocess.SubprocessError):
        pass

    absolute_path = repo_root / path
    try:
        return _synthetic_diff(path, absolute_path.read_text(encoding='utf-8'))
    except OSError:
        return None


def check_files(paths: list[str | Path], repo_root: str | Path, contracts: dict[str, Contract]) -> list[Finding]:
    """Check changed lines in files, falling back to full contents for new files.

    Tracked files are checked through ``git diff HEAD -- <file>`` so an editor
    hook does not re-report pre-existing code. Untracked files and repositories
    without git fall back to a synthetic all-added diff of the current contents.
    """
    root = Path(repo_root).resolve()
    findings: list[Finding] = []

    for raw_path in paths:
        candidate = Path(raw_path)
        if not candidate.is_absolute():
            candidate = root / candidate
        try:
            resolved = candidate.resolve()
            relative = resolved.relative_to(root).as_posix()
        except (OSError, ValueError):
            relative = normalize_path(raw_path)

        diff_text = _file_diff_for(relative, root)
        if diff_text is None:
            continue
        for path, file_diff in parse_unified_diff(diff_text).items():
            findings.extend(_check_file_diff(file_diff, matched_contracts(path, contracts)))

    return _dedupe(findings)


def blocking_findings(findings: list[Finding], fail_on: str = ERROR) -> list[Finding]:
    """Return findings severe enough to fail a command."""
    if fail_on == WARNING:
        return [finding for finding in findings if finding.severity in {ERROR, WARNING}]
    return [finding for finding in findings if finding.severity == ERROR]


__all__ = [
    'ERROR',
    'WARNING',
    'DiffLine',
    'FileDiff',
    'Finding',
    'blocking_findings',
    'check_files',
    'check_unified_diff',
    'parse_unified_diff',
]
