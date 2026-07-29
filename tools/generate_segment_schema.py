#!/usr/bin/env python3
"""Generate OpenAPI schema from the AnonymizedPayload TypedDict.

Usage:
    uv run python tools/generate_segment_schema.py
    uv run python tools/generate_segment_schema.py -o schema.yaml
"""

import argparse
import sys

import yaml

from pydantic import ConfigDict, TypeAdapter, with_config

from metrics_utility.anonymized_rollups.types import AnonymizedPayload


StrictPayload = with_config(ConfigDict(extra='forbid'))(AnonymizedPayload)


def main():
    parser = argparse.ArgumentParser(description='Generate OpenAPI schema for the Segment anonymized payload.')
    parser.add_argument('-o', '--output', help='Write to file instead of stdout')
    args = parser.parse_args()

    schema = TypeAdapter(StrictPayload).json_schema(
        ref_template='#/components/schemas/{model}',
    )

    defs = schema.pop('$defs', {})
    openapi = {
        'openapi': '3.1.0',
        'info': {'title': 'Segment Anonymized Payload', 'version': '1.0.0'},
        'paths': {},
        'components': {
            'schemas': {
                'AnonymizedPayload': schema,
                **defs,
            },
        },
    }

    output = yaml.dump(openapi, default_flow_style=False, sort_keys=False)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(output)
        print(f'Schema written to {args.output}', file=sys.stderr)
    else:
        print(output)


if __name__ == '__main__':
    main()
