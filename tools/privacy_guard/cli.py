"""Shared privacy guard CLI used by every agent harness."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys

from pathlib import Path
from typing import Any

import requests

from tools.privacy_guard import ai_reviewer
from tools.privacy_guard.checker import Finding, blocking_findings, check_files, check_unified_diff
from tools.privacy_guard.contracts import Contract, classify_files, load_contracts


PRE_TOOL_TRIGGER_RE = re.compile(r'\b(?:git\s+(?:commit|push)|gh\s+pr\s+create)\b', re.IGNORECASE)
_ECHO_ARG_RE = re.compile(r'\becho\b[^;&|]*', re.IGNORECASE)
_GIT_COMMIT_RE = re.compile(r'\bgit\s+commit\b', re.IGNORECASE)
_GIT_PUSH_RE = re.compile(r'\bgit\s+push\b', re.IGNORECASE)
_GH_PR_CREATE_RE = re.compile(r'\bgh\s+pr\s+create\b', re.IGNORECASE)
HOOK_HARNESSES = ('claude', 'codex', 'cursor', 'opencode')


def _looks_like_pre_tool_trigger(command: str) -> bool:
    """Return whether a shell command runs a gated git/gh operation.

    Quoted or unquoted echo arguments are ignored so messages such as
    ``echo "git push"`` do not trigger the hook, while ``echo hi && git push``
    still does.
    """
    without_echo_arguments = _ECHO_ARG_RE.sub('echo ', command)
    return PRE_TOOL_TRIGGER_RE.search(without_echo_arguments) is not None


def _repo_root(start: str | Path | None = None) -> Path:
    cwd = Path(start) if start is not None else Path.cwd()
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--show-toplevel'],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return cwd.resolve()
    if result.returncode == 0 and result.stdout.strip():
        return Path(result.stdout.strip()).resolve()
    return cwd.resolve()


def _run_git(root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(['git', '-C', str(root), *args], capture_output=True, text=True, check=False)


def _git_diff_text(root: Path, *, base: str | None = None, staged: bool = False) -> str:
    args = ['diff']
    if staged:
        args.append('--cached')
    if base:
        args.append(f'{base}...HEAD')
    elif not staged:
        args.append('HEAD')

    result = _run_git(root, args)
    if result.returncode != 0:
        fallback = ['diff']
        if staged:
            fallback.append('--cached')
        elif base:
            fallback.append(base)
        result = _run_git(root, fallback)
        if result.returncode != 0:
            detail = result.stderr.strip() or f'exit code {result.returncode}'
            msg = f'git diff failed: {detail}'
            raise RuntimeError(msg)
    return result.stdout


def _git_changed_files(root: Path, *, base: str | None = None) -> list[str]:
    args = ['diff', '--name-only']
    if base:
        args.append(f'{base}...HEAD')
    else:
        args.append('HEAD')
    result = _run_git(root, args)
    if result.returncode != 0:
        fallback = ['diff', '--name-only']
        if base:
            fallback.append(base)
        result = _run_git(root, fallback)

    changed: list[str] = []
    for line in result.stdout.splitlines():
        path = line.strip()
        if path and path not in changed:
            changed.append(path)
    return changed


def _format_findings(findings: list[Finding], output_format: str) -> str:
    if output_format == 'json':
        return json.dumps([finding.to_dict() for finding in findings], indent=2)
    if output_format == 'markdown':
        if not findings:
            return '## Privacy Guard\n\nNo privacy contract violations found.\n'
        lines = [
            '## Privacy Guard',
            '',
            '| Severity | Rule | Contract | File | Message |',
            '| --- | --- | --- | --- | --- |',
        ]
        for finding in findings:
            location = finding.file if finding.line is None else f'{finding.file}:{finding.line}'
            message = finding.message.replace('|', '\\|')
            lines.append(f'| {finding.severity} | {finding.rule_id} | {finding.contract} | {location} | {message} |')
        return '\n'.join(lines) + '\n'

    if not findings:
        return 'No privacy contract violations found.'
    lines = []
    for finding in findings:
        location = finding.file if finding.line is None else f'{finding.file}:{finding.line}'
        lines.append(f'{location}: {finding.severity} [{finding.rule_id}] ({finding.contract}) {finding.message}')
        lines.append(f'  recommendation: {finding.recommendation}')
    return '\n'.join(lines)


def _exit_code(findings: list[Finding], fail_on: str) -> int:
    return 1 if blocking_findings(findings, fail_on) else 0


def _load_contracts(contracts_path: str | None) -> dict[str, Contract]:
    return load_contracts(contracts_path)


def _loads_payload(raw: str) -> dict[str, Any]:
    if not raw.strip():
        return {}
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _extract_command(payload: dict[str, Any], harness: str) -> str:
    tool_input = _mapping(payload.get('tool_input'))
    nested_input = _mapping(payload.get('input'))
    if harness in {'claude', 'codex'}:
        candidates = (tool_input.get('command'), payload.get('command'), nested_input.get('command'))
    elif harness == 'cursor':
        candidates = (payload.get('command'), tool_input.get('command'), nested_input.get('command'))
    else:
        candidates = (nested_input.get('command'), payload.get('command'), tool_input.get('command'))
    return next((candidate for candidate in candidates if isinstance(candidate, str) and candidate.strip()), '')


def _paths_from_value(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        paths: list[str] = []
        for item in value:
            if isinstance(item, str):
                paths.append(item)
            elif isinstance(item, dict):
                candidate = item.get('file_path') or item.get('path')
                if isinstance(candidate, str):
                    paths.append(candidate)
        return paths
    return []


def _extract_paths(payload: dict[str, Any]) -> list[str]:
    tool_input = _mapping(payload.get('tool_input'))
    nested_input = _mapping(payload.get('input'))
    sources = (tool_input, payload, nested_input)
    paths: list[str] = []
    for source in sources:
        for key in ('file_path', 'path', 'files', 'file_paths'):
            for candidate in _paths_from_value(source.get(key)):
                if candidate not in paths:
                    paths.append(candidate)
    return paths


def _hook_reason(findings: list[Finding]) -> str:
    lines = ['Privacy guard blocked this change because it violates a data contract:']
    for finding in findings[:5]:
        location = finding.file if finding.line is None else f'{finding.file}:{finding.line}'
        lines.append(f'- {location}: [{finding.rule_id}] {finding.message}')
        lines.append(f'  Fix: {finding.recommendation}')
    if len(findings) > 5:
        lines.append(f'- ...and {len(findings) - 5} more finding(s).')
    lines.append('Fix the finding or update privacy-contracts.yaml with owner review.')
    return '\n'.join(lines)


def _deny(harness: str, findings: list[Finding]) -> int:
    reason = _hook_reason(findings)
    if harness == 'opencode':
        print(json.dumps({'decision': 'deny', 'reason': reason}))
    else:
        print(reason, file=sys.stderr)
    return 2


def _hook_pre_tool(harness: str, payload: dict[str, Any], contracts: dict[str, Contract]) -> int:
    command = _extract_command(payload, harness)
    if not _looks_like_pre_tool_trigger(command):
        return 0

    root = _repo_root()
    staged_only = bool(_GIT_COMMIT_RE.search(command)) and not _GIT_PUSH_RE.search(command) and not _GH_PR_CREATE_RE.search(command)
    diff_text = _git_diff_text(root, staged=staged_only)
    findings = check_unified_diff(diff_text, contracts)
    blocking = blocking_findings(findings)
    if not blocking:
        return 0
    return _deny(harness, blocking)


def _hook_post_tool(harness: str, payload: dict[str, Any], contracts: dict[str, Contract]) -> int:
    paths = _extract_paths(payload)
    if not paths:
        return 0

    root = _repo_root()
    classification = classify_files(paths, contracts, repo_root=root)
    matched = [path for path, names in classification.items() if names]
    if not matched:
        return 0

    findings = check_files(matched, root, contracts)
    blocking = blocking_findings(findings)
    if not blocking:
        return 0
    return _deny(harness, blocking)


def _cmd_check_diff(args: argparse.Namespace) -> int:
    contracts = _load_contracts(args.contracts)
    root = _repo_root()
    try:
        diff_text = _git_diff_text(root, base=args.base, staged=args.staged)
    except RuntimeError as error:
        print(f'Privacy guard failed: {error}', file=sys.stderr)
        return 2
    findings = check_unified_diff(diff_text, contracts)
    print(_format_findings(findings, args.format))
    return _exit_code(findings, args.fail_on)


def _cmd_check_files(args: argparse.Namespace) -> int:
    contracts = _load_contracts(args.contracts)
    root = _repo_root()
    findings = check_files(args.files, root, contracts)
    print(_format_findings(findings, args.format))
    return _exit_code(findings, args.fail_on)


def _cmd_hook(args: argparse.Namespace) -> int:
    contracts = _load_contracts(args.contracts)
    payload = _loads_payload(sys.stdin.read())
    if not payload:
        return 0
    if args.event == 'pre-tool':
        return _hook_pre_tool(args.harness, payload, contracts)
    return _hook_post_tool(args.harness, payload, contracts)


def _matched_contract_subset(changed: list[str], contracts: dict[str, Contract], root: Path) -> dict[str, Contract]:
    classification = classify_files(changed, contracts, repo_root=root)
    matched_names: set[str] = set()
    for names in classification.values():
        matched_names.update(names)
    return {name: contracts[name] for name in contracts if name in matched_names}


def _cmd_ai_review(args: argparse.Namespace) -> int:
    contracts = _load_contracts(args.contracts)
    root = _repo_root()
    try:
        diff_text = _git_diff_text(root, base=args.base)
        changed = _git_changed_files(root, base=args.base)
    except RuntimeError as error:
        print(f'Privacy guard failed: {error}', file=sys.stderr)
        return 2
    subset = _matched_contract_subset(changed, contracts, root)

    if not subset and not args.force:
        print('No privacy-contract paths modified; skipping AI review')
        return 0

    prompt_contracts = subset or contracts
    config = ai_reviewer.load_config(require_key=False)
    if args.dry_run:
        _bounded_diff, truncated = ai_reviewer.truncate_diff(diff_text)
        metadata = {
            'provider': config.provider,
            'model': config.model,
            'api_url': config.api_url,
            'contracts': list(prompt_contracts),
            'changed_files': changed,
            'matched_contract_files': sorted(path for path, names in classify_files(changed, contracts, repo_root=root).items() if names),
            'diff_characters': len(diff_text),
            'diff_truncated': truncated,
            'system_prompt': ai_reviewer.SYSTEM_PROMPT,
            'user_prompt': ai_reviewer.build_user_prompt(diff_text, prompt_contracts),
        }
        print(json.dumps(metadata, indent=2))
        return 0

    if not config.api_key:
        if args.allow_skip_unconfigured:
            print('PRIVACY_AI_API_KEY is not set; skipping AI review')
            return 0
        print('PRIVACY_AI_API_KEY is required for ai-review (or pass --allow-skip-unconfigured)', file=sys.stderr)
        return 2

    try:
        result = ai_reviewer.review_diff(diff_text, prompt_contracts, config=config)
    except (ValueError, KeyError, requests.RequestException) as error:
        print(f'AI review failed: {error}', file=sys.stderr)
        return 2

    if args.format == 'json':
        output = ai_reviewer.format_json(result)
    else:
        output = ai_reviewer.format_markdown_comment(result, prompt_contracts)

    if args.output:
        Path(args.output).write_text(output, encoding='utf-8')
        print(f'Wrote AI review to {args.output}')
    else:
        print(output)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='privacy-guard', description='Deterministic and AI privacy contract checks for metrics-utility.')
    subparsers = parser.add_subparsers(dest='command', required=True)

    check_diff = subparsers.add_parser('check-diff', help='Check a git diff against privacy contracts.')
    check_diff.add_argument('--base', help='Base git ref to compare against (uses base...HEAD).')
    check_diff.add_argument('--staged', action='store_true', help='Check only staged changes.')
    check_diff.add_argument('--contracts', help='Path to privacy-contracts.yaml.')
    check_diff.add_argument('--format', choices=('text', 'json', 'markdown'), default='text')
    check_diff.add_argument('--fail-on', choices=('error', 'warning'), default='error')
    check_diff.set_defaults(func=_cmd_check_diff)

    check_files_parser = subparsers.add_parser('check-files', help='Check changed lines in one or more files.')
    check_files_parser.add_argument('files', nargs='+', help='Files to check.')
    check_files_parser.add_argument('--contracts', help='Path to privacy-contracts.yaml.')
    check_files_parser.add_argument('--format', choices=('text', 'json', 'markdown'), default='text')
    check_files_parser.add_argument('--fail-on', choices=('error', 'warning'), default='error')
    check_files_parser.set_defaults(func=_cmd_check_files)

    hook = subparsers.add_parser('hook', help='Agent harness pre-tool/post-tool adapter.')
    hook.add_argument('--harness', choices=HOOK_HARNESSES, required=True)
    hook.add_argument('--event', choices=('pre-tool', 'post-tool'), default='pre-tool')
    hook.add_argument('--contracts', help='Path to privacy-contracts.yaml.')
    hook.set_defaults(func=_cmd_hook)

    ai_review = subparsers.add_parser('ai-review', help='Optional AI semantic review of a pull request diff.')
    ai_review.add_argument('--base', help='Base git ref to compare against (uses base...HEAD).')
    ai_review.add_argument('--contracts', help='Path to privacy-contracts.yaml.')
    ai_review.add_argument('--dry-run', action='store_true', help='Print the assembled prompt and metadata without calling a provider.')
    ai_review.add_argument('--force', action='store_true', help='Run even when no privacy-contract paths changed.')
    ai_review.add_argument('--allow-skip-unconfigured', action='store_true', help='Exit 0 when PRIVACY_AI_API_KEY is unset.')
    ai_review.add_argument('--format', choices=('markdown', 'json'), default='markdown')
    ai_review.add_argument('--output', help='Write the review to a file instead of stdout.')
    ai_review.set_defaults(func=_cmd_ai_review)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the privacy guard CLI and return a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


__all__ = ['build_parser', 'main']
