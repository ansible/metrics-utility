"""Unit tests for automation_controller_billing/extract/base.py helper methods."""

import datetime
import io
import os
import tarfile
import tempfile

import pytest

from metrics_utility.automation_controller_billing.extract.base import Base, _safe_extract
from metrics_utility.exceptions import MetricsException


# ---------------------------------------------------------------------------
# Minimal concrete subclass (Base.iter_batches / fetch_partition_paths are
# abstract in spirit but not enforced; we only need the helpers under test)
# ---------------------------------------------------------------------------


def _make_extractor(extra_params=None):
    """Return a Base instance with minimal extra_params."""
    params = extra_params or {'ship_path': '/data', 'optional_sheets': None}
    instance = Base.__new__(Base)
    instance.extra_params = params
    instance.enabled_set = None
    return instance


# ---------------------------------------------------------------------------
# get_path_prefix
# ---------------------------------------------------------------------------


class TestGetPathPrefix:
    def test_returns_expected_path_structure(self):
        ext = _make_extractor({'ship_path': '/mydata', 'optional_sheets': None})
        date = datetime.date(2024, 3, 5)
        assert ext.get_path_prefix(date) == '/mydata/data/2024/03/05'

    def test_zero_pads_month_and_day(self):
        ext = _make_extractor({'ship_path': '/data', 'optional_sheets': None})
        date = datetime.date(2024, 1, 9)
        assert ext.get_path_prefix(date) == '/data/data/2024/01/09'

    def test_december_31(self):
        ext = _make_extractor({'ship_path': '/archive', 'optional_sheets': None})
        date = datetime.date(2023, 12, 31)
        assert ext.get_path_prefix(date) == '/archive/data/2023/12/31'


# ---------------------------------------------------------------------------
# sheet_enabled
# ---------------------------------------------------------------------------


class TestSheetEnabled:
    def test_none_optional_sheets_allows_everything(self):
        ext = _make_extractor({'ship_path': '/d', 'optional_sheets': None})
        assert ext.sheet_enabled(['managed_nodes']) is True

    def test_sheet_in_optional_sheets_returns_true(self):
        ext = _make_extractor({'ship_path': '/d', 'optional_sheets': ['managed_nodes', 'jobs']})
        assert ext.sheet_enabled(['managed_nodes']) is True

    def test_sheet_not_in_optional_sheets_returns_false(self):
        ext = _make_extractor({'ship_path': '/d', 'optional_sheets': ['jobs']})
        assert ext.sheet_enabled(['managed_nodes']) is False

    def test_any_match_in_list_returns_true(self):
        ext = _make_extractor({'ship_path': '/d', 'optional_sheets': ['inventory_scope']})
        assert ext.sheet_enabled(['managed_nodes', 'inventory_scope']) is True

    def test_no_match_returns_false(self):
        ext = _make_extractor({'ship_path': '/d', 'optional_sheets': ['jobs']})
        assert ext.sheet_enabled(['managed_nodes', 'inventory_scope']) is False


# ---------------------------------------------------------------------------
# csv_enabled
# ---------------------------------------------------------------------------


class TestCsvEnabled:
    def test_config_always_enabled_when_enabled_set_is_none(self):
        ext = _make_extractor({'ship_path': '/d', 'optional_sheets': None})
        ext.enabled_set = None
        # 'config' has no entry in CSV_SHEETS so sheet check passes; enabled_set is None → True
        assert ext.csv_enabled('config') is True

    def test_name_not_in_enabled_set_returns_false(self):
        ext = _make_extractor({'ship_path': '/d', 'optional_sheets': None})
        ext.enabled_set = ['main_host']
        assert ext.csv_enabled('main_jobevent') is False

    def test_name_in_enabled_set_returns_true(self):
        ext = _make_extractor({'ship_path': '/d', 'optional_sheets': None})
        ext.enabled_set = ['main_host', 'config']
        assert ext.csv_enabled('main_host') is True

    def test_csv_with_sheet_gating_blocked_when_sheet_absent(self):
        """main_jobevent sheets require e.g. 'usage_by_collections' to be listed."""
        ext = _make_extractor({'ship_path': '/d', 'optional_sheets': ['managed_nodes']})
        ext.enabled_set = None
        # main_jobevent maps to sheets not in optional_sheets → False
        assert ext.csv_enabled('main_jobevent') is False


# ---------------------------------------------------------------------------
# filter_tarball_paths
# ---------------------------------------------------------------------------


class TestFilterTarballPaths:
    def _ext(self):
        return _make_extractor()

    # Baseline: None collections returns all paths unchanged
    def test_none_collections_returns_all_paths(self):
        ext = self._ext()
        paths = ['/d/2024/01/01/uuid-1.tar.gz', '/d/2024/01/01/uuid-2.tar.gz']
        assert ext.filter_tarball_paths(paths, None) == paths

    # Forbidden collection names raise
    def test_data_collection_status_raises(self):
        ext = self._ext()
        with pytest.raises(MetricsException):
            ext.filter_tarball_paths(['/d/file.tar.gz'], ['data_collection_status'])

    def test_config_raises(self):
        ext = self._ext()
        with pytest.raises(MetricsException):
            ext.filter_tarball_paths(['/d/file.tar.gz'], ['config'])

    # Legacy filename (numeric suffix only) is always included
    def test_legacy_numeric_suffix_always_included(self):
        ext = self._ext()
        paths = ['/data/uuid-123.tar.gz']
        result = ext.filter_tarball_paths(paths, ['main_host'])
        assert '/data/uuid-123.tar.gz' in result

    # Modern filename matching collection name is included
    def test_filename_matching_collection_is_included(self):
        ext = self._ext()
        paths = ['/data/uuid-456-main_host.tar.gz']
        result = ext.filter_tarball_paths(paths, ['main_host'])
        assert '/data/uuid-456-main_host.tar.gz' in result

    def test_empty_paths_returns_empty_list(self):
        ext = self._ext()
        assert ext.filter_tarball_paths([], ['main_host']) == []

    def test_returns_list_type(self):
        ext = self._ext()
        paths = ['/data/uuid-1-main_host.tar.gz']
        result = ext.filter_tarball_paths(paths, ['main_host'])
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# _safe_extract (module-level function)
# ---------------------------------------------------------------------------


def _make_tarball(path, members):
    """Write a .tar.gz with the given (name, content) pairs into *path*."""
    with tarfile.open(path, 'w:gz') as tar:
        for name, content in members:
            data = content.encode() if isinstance(content, str) else content
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))


class TestSafeExtract:
    def test_extracts_csv_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = os.path.join(tmpdir, 'test.tar.gz')
            _make_tarball(tar_path, [('main_host.csv', 'col\nval\n')])

            extract_dir = os.path.join(tmpdir, 'out')
            _safe_extract(tar_path, extract_dir)

            assert os.path.exists(os.path.join(extract_dir, 'main_host.csv'))

    def test_extracts_json_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = os.path.join(tmpdir, 'test.tar.gz')
            _make_tarball(tar_path, [('config.json', '{"k": "v"}')])

            extract_dir = os.path.join(tmpdir, 'out')
            _safe_extract(tar_path, extract_dir)

            assert os.path.exists(os.path.join(extract_dir, 'config.json'))

    def test_skips_non_csv_non_json_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = os.path.join(tmpdir, 'test.tar.gz')
            _make_tarball(tar_path, [('script.sh', '#!/bin/bash'), ('data.csv', 'a,b\n')])

            extract_dir = os.path.join(tmpdir, 'out')
            _safe_extract(tar_path, extract_dir)

            assert not os.path.exists(os.path.join(extract_dir, 'script.sh'))
            assert os.path.exists(os.path.join(extract_dir, 'data.csv'))

    def test_enabled_set_filters_extraction(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = os.path.join(tmpdir, 'test.tar.gz')
            _make_tarball(
                tar_path,
                [
                    ('main_host.csv', 'col\nval\n'),
                    ('main_jobevent.csv', 'x\n1\n'),
                ],
            )

            extract_dir = os.path.join(tmpdir, 'out')
            _safe_extract(tar_path, extract_dir, enabled_set=['main_host', 'config'])

            assert os.path.exists(os.path.join(extract_dir, 'main_host.csv'))
            assert not os.path.exists(os.path.join(extract_dir, 'main_jobevent.csv'))

    def test_max_files_limit_stops_extraction(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = os.path.join(tmpdir, 'test.tar.gz')
            members = [(f'file{i}.csv', f'col\n{i}\n') for i in range(5)]
            _make_tarball(tar_path, members)

            extract_dir = os.path.join(tmpdir, 'out')
            _safe_extract(tar_path, extract_dir, max_files=2)

            extracted = [f for f in os.listdir(extract_dir) if f.endswith('.csv')]
            assert len(extracted) == 2

    def test_max_size_exceeded_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = os.path.join(tmpdir, 'test.tar.gz')
            large_content = 'x' * 1000
            _make_tarball(tar_path, [('big.csv', large_content)])

            extract_dir = os.path.join(tmpdir, 'out')
            with pytest.raises(ValueError, match='Maximum total size exceeded'):
                _safe_extract(tar_path, extract_dir, max_size=100)
