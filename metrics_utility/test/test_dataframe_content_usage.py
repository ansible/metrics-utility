"""Unit tests for DataframeContentUsage static regex helper methods."""

import datetime

from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage import (
    DataframeContentUsage,
)


# ---------------------------------------------------------------------------
# extract_collection_name
# ---------------------------------------------------------------------------


class TestExtractCollectionName:
    def test_fully_qualified_collection_module(self):
        """namespace.collection.module_name -> namespace.collection"""
        assert DataframeContentUsage.extract_collection_name('ansible.builtin.copy') == 'ansible.builtin'

    def test_four_part_fqcn(self):
        """namespace.collection.role.task -> namespace.collection"""
        assert DataframeContentUsage.extract_collection_name('community.general.git_config.subtask') == 'community.general'

    def test_two_part_name_returns_none(self):
        """namespace.collection (no third segment) -> None"""
        assert DataframeContentUsage.extract_collection_name('my_namespace.my_collection') is None

    def test_bare_module_name_returns_none(self):
        """plain module name like 'copy' -> None"""
        assert DataframeContentUsage.extract_collection_name('copy') is None

    def test_none_input_returns_none(self):
        assert DataframeContentUsage.extract_collection_name(None) is None

    def test_empty_string_returns_none(self):
        assert DataframeContentUsage.extract_collection_name('') is None

    def test_redhat_namespace(self):
        assert DataframeContentUsage.extract_collection_name('redhat.rhel_system_roles.selinux') == 'redhat.rhel_system_roles'

    def test_numeric_segments_not_matched(self):
        """Names containing numbers in the right places should still match."""
        assert DataframeContentUsage.extract_collection_name('acme.col1.task') == 'acme.col1'


# ---------------------------------------------------------------------------
# extract_role_name
# ---------------------------------------------------------------------------


class TestExtractRoleName:
    def test_fqcn_role_extracts_three_part_name(self):
        """namespace.collection.role -> namespace.collection.role"""
        result = DataframeContentUsage.extract_role_name('community.general.git_config')
        assert result == 'community.general.git_config'

    def test_four_part_fqcn_uses_role_segment(self):
        """For a.b.c.d the role is the third segment (c), not the trailing one."""
        result = DataframeContentUsage.extract_role_name('ansible.builtin.copy.something')
        assert result == 'ansible.builtin.copy'

    def test_three_part_fqcn_correct(self):
        result = DataframeContentUsage.extract_role_name('ansible.builtin.copy')
        assert result == 'ansible.builtin.copy'

    def test_standalone_two_part_role(self):
        """namespace.role -> namespace.role"""
        result = DataframeContentUsage.extract_role_name('my_ns.my_role')
        assert result == 'my_ns.my_role'

    def test_bare_single_part_returns_none(self):
        """single word -> None"""
        assert DataframeContentUsage.extract_role_name('copy') is None

    def test_none_input_returns_none(self):
        assert DataframeContentUsage.extract_role_name(None) is None

    def test_empty_string_returns_none(self):
        assert DataframeContentUsage.extract_role_name('') is None

    def test_redhat_namespace_role(self):
        result = DataframeContentUsage.extract_role_name('redhat.rhel_system_roles.selinux')
        assert result == 'redhat.rhel_system_roles.selinux'

    def test_deeply_nested_fqcn_uses_role_segment(self):
        """Extra trailing segments are ignored; the role stays the third segment."""
        result = DataframeContentUsage.extract_role_name('a.b.c.d.e')
        assert result == 'a.b.c'

    def test_trailing_junk_returns_none(self):
        """A malformed FQCN with non-word trailing junk no longer matches."""
        assert DataframeContentUsage.extract_role_name('ansible.builtin.copy.more extra') is None


# ---------------------------------------------------------------------------
# collection_regexp / standalone_role_regexp
# ---------------------------------------------------------------------------


class TestRegexpPatterns:
    def test_collection_regexp_is_non_empty_string(self):
        pattern = DataframeContentUsage.collection_regexp()
        assert isinstance(pattern, str)
        assert len(pattern) > 0

    def test_standalone_role_regexp_is_non_empty_string(self):
        pattern = DataframeContentUsage.standalone_role_regexp()
        assert isinstance(pattern, str)
        assert len(pattern) > 0


# ---------------------------------------------------------------------------
# build_dataframe - role_name
# ---------------------------------------------------------------------------


def _event(host, role, resolved_role=np.nan):
    return {
        'task_action': 'ansible.builtin.debug',
        'resolved_action': 'ansible.builtin.debug',
        'role': role,
        'resolved_role': resolved_role,
        'host_name': host,
        'job_remote_id': 1,
        'duration': 1.0,
    }


def _role_names(events):
    extractor = MagicMock()
    extractor.iter_batches.side_effect = lambda **kwargs: iter([{'main_jobevent': pd.DataFrame(events), 'config': {'install_uuid': 'uuid'}}])
    engine = DataframeContentUsage(extractor=extractor, month=datetime.date(2025, 2, 1), extra_params={})
    # one batch is enough, the engine iterates over every day of the month
    engine.dates = lambda: [datetime.date(2025, 2, 1)]
    df = engine.build_dataframe()
    return dict(zip(df['host_name'], df['role_name'], strict=True))


class TestBuildDataframeRoleName:
    def test_bare_local_role_is_kept(self):
        """A task from a local role ("webserver") must not be reported as "No role used"."""
        assert _role_names([_event('h1', 'webserver')]) == {'h1': 'webserver'}

    def test_non_identifier_galaxy_role_is_kept(self):
        """Standalone Galaxy role names may contain dashes (e.g. geerlingguy.php-versions)."""
        assert _role_names([_event('h1', 'geerlingguy.php-versions')]) == {'h1': 'geerlingguy.php-versions'}

    def test_dotted_and_missing_roles_unchanged(self):
        result = _role_names(
            [
                _event('h1', 'my_ns.my_role'),
                _event('h2', 'role', 'redhat.rhel_system_roles.selinux'),
                _event('h3', np.nan),
                _event('h4', ''),
            ]
        )
        assert result == {
            'h1': 'my_ns.my_role',
            'h2': 'redhat.rhel_system_roles.selinux',
            'h3': 'No role used',
            'h4': 'No role used',
        }
