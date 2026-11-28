"""Load privacy data contracts and match repository-relative paths."""

from __future__ import annotations

import re

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONTRACTS_PATH = Path(__file__).resolve().parents[2] / 'privacy-contracts.yaml'


@dataclass(frozen=True)
class Contract:
    """A single data-flow contract from ``privacy-contracts.yaml``."""

    name: str
    description: str = ''
    paths: tuple[str, ...] = ()
    allowed: tuple[str, ...] = ()
    forbidden: tuple[str, ...] = ()
    owners: tuple[str, ...] = ()
    review_notes: str = ''

    def matches(self, path: str) -> bool:
        """Return whether a repository-relative path belongs to this contract."""
        return any(glob_match(path, pattern) for pattern in self.paths)


def normalize_path(path: str | Path) -> str:
    """Return a stable POSIX-style repository-relative path string.

    Backslashes are normalised to slashes, a leading ``./`` is removed, and
    leading slashes are dropped so absolute-looking inputs still compare
    predictably against contract globs.
    """
    value = str(path).strip().replace('\\', '/')
    while value.startswith('./'):
        value = value[2:]
    return value.lstrip('/')


def _translate_glob(pattern: str) -> str:
    """Translate a glob pattern into an anchored regular expression.

    ``**`` matches any number of path segments, including zero when written as
    ``**/``. A single ``*`` never crosses a path separator. This avoids the
    surprising ``pathlib.PurePosixPath.match`` behaviour where ``**`` can be
    treated like a single ``*``.
    """
    pattern = normalize_path(pattern)
    trailing_recursive = pattern.endswith('/**')
    if trailing_recursive:
        pattern = pattern[: -len('/**')]
    output: list[str] = ['^']
    index = 0
    length = len(pattern)

    while index < length:
        char = pattern[index]
        if char == '*':
            if index + 1 < length and pattern[index + 1] == '*':
                index += 2
                if index < length and pattern[index] == '/':
                    output.append('(?:.*/)?')
                    index += 1
                else:
                    output.append('.*')
            else:
                output.append('[^/]*')
                index += 1
        elif char == '?':
            output.append('[^/]')
            index += 1
        elif char == '[':
            end = index + 1
            if end < length and pattern[end] in '!^':
                end += 1
            if end < length and pattern[end] == ']':
                end += 1
            while end < length and pattern[end] != ']':
                end += 1
            if end >= length:
                output.append(r'\[')
                index += 1
            else:
                content = pattern[index + 1 : end]
                if content.startswith('!'):
                    content = '^' + content[1:]
                output.append('[' + content + ']')
                index = end + 1
        else:
            output.append(re.escape(char))
            index += 1

    if trailing_recursive:
        output.append('(?:/.*)?')
    output.append('$')
    return ''.join(output)


@lru_cache(maxsize=512)
def _compiled_glob(pattern: str) -> re.Pattern[str]:
    return re.compile(_translate_glob(pattern))


def glob_match(path: str | Path, pattern: str) -> bool:
    """Match a path against a contract glob, supporting ``**`` correctly."""
    return _compiled_glob(pattern).match(normalize_path(path)) is not None


def _as_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(str(item) for item in value)


def _build_contract(name: str, payload: dict[str, Any]) -> Contract:
    return Contract(
        name=name,
        description=str(payload.get('description', '') or '').strip(),
        paths=_as_tuple(payload.get('paths')),
        allowed=_as_tuple(payload.get('allowed')),
        forbidden=_as_tuple(payload.get('forbidden')),
        owners=_as_tuple(payload.get('owners')),
        review_notes=str(payload.get('review_notes', '') or '').strip(),
    )


def load_contracts(path: str | Path | None = None) -> dict[str, Contract]:
    """Load contracts from a YAML file.

    The document may either use a top-level ``contracts`` mapping or be a
    direct mapping of contract names to definitions.
    """
    contracts_path = Path(path) if path is not None else DEFAULT_CONTRACTS_PATH
    with contracts_path.open(encoding='utf-8') as handle:
        document = yaml.safe_load(handle)

    if not isinstance(document, dict):
        msg = f'Privacy contracts file must contain a mapping: {contracts_path}'
        raise ValueError(msg)

    raw_contracts = document.get('contracts', document)
    if not isinstance(raw_contracts, dict):
        msg = f'Privacy contracts section must contain a mapping: {contracts_path}'
        raise ValueError(msg)

    contracts: dict[str, Contract] = {}
    for name, payload in raw_contracts.items():
        if not isinstance(payload, dict):
            msg = f"Privacy contract '{name}' must be a mapping: {contracts_path}"
            raise ValueError(msg)
        contracts[str(name)] = _build_contract(str(name), payload)

    if not contracts:
        msg = f'No privacy contracts found in {contracts_path}'
        raise ValueError(msg)
    return contracts


def matched_contracts(path: str | Path, contracts: dict[str, Contract]) -> list[str]:
    """Return the names of contracts that own a repository-relative path."""
    return [name for name, contract in contracts.items() if contract.matches(path)]


def classify_files(paths: list[str | Path], contracts: dict[str, Contract], repo_root: str | Path | None = None) -> dict[str, list[str]]:
    """Map each input path to the contracts that own it.

    Keys are normalised repository-relative paths. Every input path appears in
    the result, even when no contract matches (with an empty list).
    """
    root = Path(repo_root).resolve() if repo_root is not None else None
    classification: dict[str, list[str]] = {}

    for raw_path in paths:
        relative = normalize_path(raw_path)
        if root is not None:
            candidate = Path(raw_path)
            if not candidate.is_absolute():
                candidate = root / candidate
            try:
                relative = candidate.resolve().relative_to(root).as_posix()
            except (OSError, ValueError):
                relative = normalize_path(raw_path)
        classification[relative] = matched_contracts(relative, contracts)

    return classification


def contracts_for_paths(paths: list[str | Path], contracts: dict[str, Contract], repo_root: str | Path | None = None) -> dict[str, Contract]:
    """Return the subset of contracts touched by any of the given paths."""
    matched: set[str] = set()
    for names in classify_files(paths, contracts, repo_root=repo_root).values():
        matched.update(names)
    return {name: contracts[name] for name in contracts if name in matched}


__all__ = [
    'DEFAULT_CONTRACTS_PATH',
    'Contract',
    'classify_files',
    'contracts_for_paths',
    'glob_match',
    'load_contracts',
    'matched_contracts',
    'normalize_path',
]
