#!/usr/bin/env python3
"""Filter pg_dump output for stable, reviewable SQL files.

Used by both tools/docker/extract-awx-schema.sh and
.github/workflows/awx-schema.yml to ensure consistent output.

Usage:
    pg_dump -s ... | python3 filter_pgdump.py > latest.sql
    pg_dump --data-only ... | python3 filter_pgdump.py --normalize > initial.sql
"""

import re
import sys
import uuid


def pgdump_filter(lines):
    result = []
    prev_blank = False
    for line in lines:
        # strip pg_dump version comments — they change with postgres upgrades
        if line.startswith('-- Dumped from database version') or line.startswith('-- Dumped by pg_dump version'):
            continue
        # strip pg18+ security directives and settings not present in older versions
        if line.startswith('\\restrict ') or line.startswith('\\unrestrict'):
            continue
        if line.startswith('SET transaction_timeout'):
            continue
        # strip pg18+ named NOT NULL constraints back to plain NOT NULL
        line = re.sub(r' CONSTRAINT [a-z_]*_not_null\d* NOT NULL', ' NOT NULL', line)
        # collapse consecutive blank lines left by the above removals
        if line == '\n':
            if prev_blank:
                continue
            prev_blank = True
        else:
            prev_blank = False
        result.append(line)
    # strip trailing blank lines
    while result and result[-1] == '\n':
        result.pop()
    if result and not result[-1].endswith('\n'):
        result.append('\n')
    return result


SKIP_PREFIXES = ('COPY ', '--', 'ALTER ', 'SELECT ', 'SET ', '\\.', '\n')


def normalize_data(lines):
    ts_pat = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?\+00')
    iso_pat = re.compile(r'\d{8}T\d{6}Z')

    result = []
    for line in lines:
        line = ts_pat.sub('2000-01-01 00:00:00.000000+00', line)
        line = iso_pat.sub('20000101T000000Z', line)
        result.append(line)

    uuid_pat = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
    ns = uuid.UUID('12345678-1234-5678-1234-567812345678')

    seen = {}
    counter = 0
    for line in result:
        if not line.startswith(SKIP_PREFIXES):
            for m in uuid_pat.finditer(line):
                if m.group() not in seen:
                    counter += 1
                    seen[m.group()] = str(uuid.uuid5(ns, str(counter)))

    normalized = []
    for line in result:
        if line.startswith(SKIP_PREFIXES):
            normalized.append(line)
        else:
            normalized.append(uuid_pat.sub(lambda m: seen[m.group()], line))

    return normalized


def main():
    normalize = '--normalize' in sys.argv

    lines = sys.stdin.readlines()
    lines = pgdump_filter(lines)
    if normalize:
        lines = normalize_data(lines)

    sys.stdout.writelines(lines)


if __name__ == '__main__':
    main()
