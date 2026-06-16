#!/usr/bin/env python3

import argparse
import csv
import io
import json
import os
import re
import sys
import tarfile

from datetime import datetime
from pathlib import Path

import jsonschema


SCHEMAS_DIR = Path(__file__).resolve().parent.parent.parent / 'schemas'

REQUIRED_FILES = {'config.json', 'manifest.json'}
DCS_FILE = 'data_collection_status.csv'

BILLING_FILES = {
    'manifest.json',
    'config.json',
    'data_collection_status.csv',
    'job_host_summary.csv',
    'total_workers_vcpu.json',
}

VERSION_RE = re.compile(r'^\d+\.\d+$')


def find_schema(stem, version):
    versioned = SCHEMAS_DIR / f'{stem}-{version}.jsonschema'
    if versioned.exists():
        return versioned

    unversioned = SCHEMAS_DIR / f'{stem}.jsonschema'
    if unversioned.exists():
        return unversioned

    return None


def has_any_schema(stem):
    for p in SCHEMAS_DIR.glob(f'{stem}*.jsonschema'):
        if p.stem == stem or p.stem.startswith(f'{stem}-'):
            return True
    return False


def load_schema(path):
    with open(path) as f:
        return json.load(f)


def make_datetime_validator(schema):
    BaseVal = jsonschema.validators.validator_for(schema)

    def is_datetime(_, inst):
        try:
            datetime.fromisoformat(inst)
        except (TypeError, ValueError):
            return False
        return True

    checker = BaseVal.TYPE_CHECKER.redefine('datetime', is_datetime)
    return jsonschema.validators.extend(BaseVal, type_checker=checker)


def normalize_member_name(name):
    return name.removeprefix('./')


def read_member(tar, name):
    for prefix in ['', './']:
        try:
            member = tar.extractfile(prefix + name)
            if member is not None:
                return member.read()
        except KeyError:
            pass
    return None


def parse_csv_to_dicts(raw_bytes):
    text = raw_bytes.decode('utf-8')
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


INDIRECT_RENAME = {'indirect_nodes.csv': 'main_indirectmanagednodeaudit.csv'}


def validate_tarball(tarball_path, verbose, skip_dcs, aliases, strict_billing):
    errors = []
    skipped = []
    passed = []
    warnings = []

    def fail(msg):
        errors.append(msg)

    def skip(msg):
        skipped.append(msg)

    def ok(msg):
        passed.append(msg)

    def warn(msg):
        warnings.append(msg)

    try:
        tar = tarfile.open(tarball_path, 'r:gz')
    except Exception as e:
        fail(f'cannot open tarball: {e}')
        return errors, skipped, passed, warnings

    with tar:
        members = set()
        for m in tar.getmembers():
            if not m.isfile():
                continue
            members.add(normalize_member_name(m.name))

        required = set(REQUIRED_FILES)
        if not skip_dcs:
            required.add(DCS_FILE)

        for req in sorted(required):
            if req not in members:
                fail(f'missing required file: {req}')

        data_files = members - {'config.json', 'manifest.json', DCS_FILE}
        if not data_files:
            fail('no data files beyond config.json, manifest.json, and data_collection_status.csv')

        manifest_raw = read_member(tar, 'manifest.json')
        if manifest_raw is None:
            fail('cannot read manifest.json')
            return errors, skipped, passed, warnings

        try:
            manifest = json.loads(manifest_raw)
        except json.JSONDecodeError as e:
            fail(f'manifest.json is not valid JSON: {e}')
            return errors, skipped, passed, warnings

        if not isinstance(manifest, dict):
            fail(f'manifest.json must be an object, got {type(manifest).__name__}')
            return errors, skipped, passed, warnings

        manifest_schema_path = SCHEMAS_DIR / 'manifest.jsonschema'
        if manifest_schema_path.exists():
            manifest_schema = load_schema(manifest_schema_path)
            try:
                jsonschema.validate(manifest, manifest_schema)
                ok('manifest.json')
            except jsonschema.ValidationError as e:
                fail(f'manifest.json: schema validation failed: {e.message}')
        else:
            skip('manifest.json')

        aliases = aliases or {}
        reverse_aliases = {v: k for k, v in aliases.items()}

        for key, ver in manifest.items():
            if not isinstance(ver, str) or not VERSION_RE.match(ver):
                fail(f'manifest.json: invalid version {ver!r} for {key!r} (expected \\d+.\\d+)')
            if key not in members:
                alias_target = aliases.get(key)
                if alias_target and alias_target in members:
                    warn(f'manifest.json: lists {key!r} aliased to {alias_target!r}')
                else:
                    fail(f'manifest.json: lists {key!r} but file not found in tarball')

        dcs_files = set()
        if DCS_FILE in members:
            dcs_raw = read_member(tar, DCS_FILE)
            if dcs_raw is not None:
                rows = parse_csv_to_dicts(dcs_raw)
                if not rows:
                    fail(f'{DCS_FILE}: no data rows (header only)')
                else:
                    for row in rows:
                        fname = row.get('file_name', '')
                        status = row.get('status', '')
                        if fname:
                            dcs_files.add(fname)
                        if fname and fname not in members:
                            alias_target = aliases.get(fname)
                            if alias_target and alias_target in members:
                                warn(f'{DCS_FILE}: references {fname!r} aliased to {alias_target!r}')
                            else:
                                warn(f'{DCS_FILE}: references {fname!r} but file not found in tarball')
                        if status != 'ok':
                            fail(f"{DCS_FILE}: {fname!r} has status {status!r}, expected 'ok'")

        known_files = {'config.json', 'manifest.json', DCS_FILE}

        for filename in sorted(members):
            if filename == 'manifest.json':
                continue

            if strict_billing and (filename.endswith('.csv') or filename.endswith('.json')):
                if filename not in BILLING_FILES:
                    fail(f'{filename}: not accepted by the billing controller pipeline')
                    continue

            old_name = reverse_aliases.get(filename)
            version = manifest.get(filename) or (manifest.get(old_name) if old_name else None)

            stem = filename.rsplit('.', 1)[0] if '.' in filename else filename

            if version is None:
                if filename not in known_files and filename not in dcs_files:
                    warn(f'{filename} (not in manifest or data_collection_status)')
                else:
                    skip(f'{filename} (no version in manifest)')
                continue

            schema_path = find_schema(stem, version)

            if schema_path is None:
                if has_any_schema(stem):
                    fail(f'{filename}: schema exists for {stem!r} but not for version {version}')
                else:
                    skip(f'{filename} (no schema)')
                continue

            schema = load_schema(schema_path)

            raw = read_member(tar, filename)
            if raw is None:
                fail(f'{filename}: listed but cannot read from tarball')
                continue

            if filename.endswith('.csv'):
                try:
                    instance = parse_csv_to_dicts(raw)
                except Exception as e:
                    fail(f'{filename}: cannot parse CSV: {e}')
                    continue
            elif filename.endswith('.json'):
                try:
                    instance = json.loads(raw)
                except json.JSONDecodeError as e:
                    fail(f'{filename}: not valid JSON: {e}')
                    continue
            else:
                skip(f'{filename} (not csv or json)')
                continue

            try:
                Validator = make_datetime_validator(schema)
                Validator(schema=schema).validate(instance)
                ok(filename)
            except jsonschema.ValidationError as e:
                fail(f'{filename}: schema validation failed: {e.message}')

    return errors, skipped, passed, warnings


def main():
    parser = argparse.ArgumentParser(description='Validate metrics-utility tarballs against JSON schemas')
    parser.add_argument('-v', '--verbose', action='store_true', help='show SKIP/PASS/FAIL for every file')
    parser.add_argument('--skip-data-collection-status', action='store_true', help="don't require data_collection_status.csv")
    parser.add_argument(
        '--skip-indirect-rename', action='store_true', help='allow indirect_nodes.csv -> main_indirectmanagednodeaudit.csv rename in old tarballs'
    )
    parser.add_argument('--strict-billing-files', action='store_true', help='fail on csv/json files not accepted by the billing controller pipeline')
    parser.add_argument('tarballs', nargs='+', metavar='TARBALL', help='tarball(s) to validate')
    args = parser.parse_args()

    aliases = INDIRECT_RENAME if args.skip_indirect_rename else {}
    any_failure = False

    for tarball in args.tarballs:
        if not os.path.isfile(tarball):
            print(f'FAIL {tarball}: file not found', file=sys.stderr)
            any_failure = True
            continue

        errors, skipped, passed, warnings = validate_tarball(
            tarball, args.verbose, args.skip_data_collection_status, aliases, args.strict_billing_files
        )

        label = os.path.basename(tarball)

        if args.verbose:
            for name in skipped:
                print(f'SKIP {label}: {name}', file=sys.stderr)
            for name in passed:
                print(f'PASS {label}: {name}', file=sys.stderr)
            for msg in warnings:
                print(f'WARN {label}: {msg}', file=sys.stderr)

        if errors:
            any_failure = True
            for msg in errors:
                print(f'FAIL {label}: {msg}', file=sys.stderr)
        elif args.verbose:
            print(f'PASS {label}', file=sys.stderr)

    sys.exit(1 if any_failure else 0)


if __name__ == '__main__':
    main()
