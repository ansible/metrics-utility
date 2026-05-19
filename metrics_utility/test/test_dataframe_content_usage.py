"""Unit tests for DataframeContentUsage static regex helper methods."""

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

    def test_four_part_fqcn_uses_last_segment(self):
        """For a.b.c.d the regex captures the last repeated segment (d)."""
        result = DataframeContentUsage.extract_role_name('ansible.builtin.copy.something')
        assert result == 'ansible.builtin.something'

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
