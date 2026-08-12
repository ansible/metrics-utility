#!/usr/bin/env python3
"""Validate an anonymized rollup JSON against the AnonymizedPayload TypedDict.

Usage:
    uv run python tools/validate_segment_contract.py path/to/rollup.json
"""

import json
import sys

from pathlib import Path

from pydantic import ConfigDict, TypeAdapter, ValidationError, with_config

from metrics_utility.anonymized_rollups.types import AnonymizedPayload


StrictPayload = with_config(ConfigDict(extra='forbid'))(AnonymizedPayload)


def main():
    if len(sys.argv) < 2:
        print(f'Usage: {sys.argv[0]} path/to/rollup.json')
        sys.exit(1)

    json_path = Path(sys.argv[1])

    with open(json_path) as f:
        data = json.load(f)

    adapter = TypeAdapter(StrictPayload)

    try:
        adapter.validate_python(data, strict=True)
        print(f'Data: {json_path}')
        print(f'Keys: {sorted(data.keys())}')
        print()
        print('ALL CHECKS PASSED')
    except ValidationError as e:
        print(f'Data: {json_path}')
        print()
        print(f'ISSUES: {e.error_count()}')
        for err in e.errors():
            loc = '.'.join(str(p) for p in err['loc'])
            print(f'  {loc}: {err["msg"]}')
        sys.exit(1)


if __name__ == '__main__':
    main()
