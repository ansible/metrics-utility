from unittest.mock import Mock

import pytest

from metrics_utility.automation_controller_billing.dedup.ccsp import DedupCCSP
from metrics_utility.automation_controller_billing.dedup.factory import Factory
from metrics_utility.automation_controller_billing.dedup.renewal_guidance import (
    DedupRenewal,
    DedupRenewalExperimental,
    DedupRenewalHostname,
)
from metrics_utility.exceptions import NotSupportedFactory


class TestDedupFactory:
    """Test cases for the dedup factory."""

    @pytest.fixture
    def mock_dataframes(self):
        """Mock dataframes for testing."""
        return {
            'main_host': Mock(),
            'job_host_summary': Mock(),
            'host_metric': Mock(),
            'main_jobevent': Mock(),
            'data_collection_status': Mock(),
        }

    @pytest.fixture
    def base_extra_params(self):
        """Base extra parameters for testing."""
        return {
            'report_type': 'CCSP',
            'deduplicator': None,
            'report_renewal_guidance_dedup_iterations': '3',
        }

    def test_init(self, mock_dataframes, base_extra_params):
        """Test factory initialization."""
        factory = Factory(mock_dataframes, base_extra_params)
        assert factory.dataframes == mock_dataframes
        assert factory.extra_params == base_extra_params

    def test_create_ccsp_default(self, mock_dataframes, base_extra_params):
        """Test creating DedupCCSP with default settings for CCSP report type."""
        base_extra_params['report_type'] = 'CCSP'
        factory = Factory(mock_dataframes, base_extra_params)

        result = factory.create()

        assert isinstance(result, DedupCCSP)
        assert result.dataframes == mock_dataframes
        assert result.extra_params == base_extra_params
        assert result.experimental is False

    def test_create_ccspv2_default(self, mock_dataframes, base_extra_params):
        """Test creating DedupCCSP with default settings for CCSPv2 report type."""
        base_extra_params['report_type'] = 'CCSPv2'
        factory = Factory(mock_dataframes, base_extra_params)

        result = factory.create()

        assert isinstance(result, DedupCCSP)
        assert result.dataframes == mock_dataframes
        assert result.extra_params == base_extra_params
        assert result.experimental is False

    def test_create_renewal_guidance_default(self, mock_dataframes, base_extra_params):
        """Test creating DedupRenewal with default settings for RENEWAL_GUIDANCE report type."""
        base_extra_params['report_type'] = 'RENEWAL_GUIDANCE'
        factory = Factory(mock_dataframes, base_extra_params)

        result = factory.create()

        assert isinstance(result, DedupRenewal)
        # assert result.dataframes == mock_dataframes
        assert result.extra_params == base_extra_params

    def test_create_ccsp_deduplicator_explicit(self, mock_dataframes, base_extra_params):
        """Test creating DedupCCSP with explicit ccsp deduplicator."""
        base_extra_params['deduplicator'] = 'ccsp'
        factory = Factory(mock_dataframes, base_extra_params)

        result = factory.create()

        assert isinstance(result, DedupCCSP)
        assert result.dataframes == mock_dataframes
        assert result.extra_params == base_extra_params
        assert result.experimental is False

    def test_create_renewal_deduplicator_explicit(self, mock_dataframes, base_extra_params):
        """Test creating DedupRenewal with explicit renewal deduplicator."""
        base_extra_params['deduplicator'] = 'renewal'
        factory = Factory(mock_dataframes, base_extra_params)

        result = factory.create()

        assert isinstance(result, DedupRenewal)
        # assert result.dataframes == mock_dataframes
        assert result.extra_params == base_extra_params

    def test_create_ccsp_experimental_with_ccsp_report(self, mock_dataframes, base_extra_params):
        """Test creating DedupCCSP with experimental mode for CCSP report type."""
        base_extra_params['report_type'] = 'CCSP'
        base_extra_params['deduplicator'] = 'ccsp-experimental'
        factory = Factory(mock_dataframes, base_extra_params)

        result = factory.create()

        assert isinstance(result, DedupCCSP)
        assert result.dataframes == mock_dataframes
        assert result.extra_params == base_extra_params
        assert result.experimental is True

    def test_create_ccsp_experimental_with_ccspv2_report(self, mock_dataframes, base_extra_params):
        """Test creating DedupCCSP with experimental mode for CCSPv2 report type."""
        base_extra_params['report_type'] = 'CCSPv2'
        base_extra_params['deduplicator'] = 'ccsp-experimental'
        factory = Factory(mock_dataframes, base_extra_params)

        result = factory.create()

        assert isinstance(result, DedupCCSP)
        assert result.dataframes == mock_dataframes
        assert result.extra_params == base_extra_params
        assert result.experimental is True

    def test_create_ccsp_experimental_with_invalid_report_type(self, mock_dataframes, base_extra_params):
        """Test that ccsp-experimental with invalid report type raises exception."""
        base_extra_params['report_type'] = 'RENEWAL_GUIDANCE'
        base_extra_params['deduplicator'] = 'ccsp-experimental'
        factory = Factory(mock_dataframes, base_extra_params)

        with pytest.raises(NotSupportedFactory, match='Unknown report type: RENEWAL_GUIDANCE'):
            factory.create()

    def test_create_unsupported_report_type(self, mock_dataframes, base_extra_params):
        """Test that unsupported report type raises exception."""
        base_extra_params['report_type'] = 'UNSUPPORTED'
        factory = Factory(mock_dataframes, base_extra_params)

        with pytest.raises(NotSupportedFactory, match='Unknown report type: UNSUPPORTED'):
            factory.create()

    def test_create_unsupported_deduplicator(self, mock_dataframes, base_extra_params):
        """Test that unsupported deduplicator raises exception."""
        base_extra_params['deduplicator'] = 'unsupported'
        factory = Factory(mock_dataframes, base_extra_params)

        with pytest.raises(NotSupportedFactory, match='Factory for unsupported not supported'):
            factory.create()

    def test_create_none_deduplicator_with_various_report_types(self, mock_dataframes, base_extra_params):
        """Test create method with None deduplicator for various report types."""
        report_type_to_class = {
            'CCSP': DedupCCSP,
            'CCSPv2': DedupCCSP,
            'RENEWAL_GUIDANCE': DedupRenewal,
        }

        for report_type, expected_class in report_type_to_class.items():
            base_extra_params['report_type'] = report_type
            base_extra_params['deduplicator'] = None
            factory = Factory(mock_dataframes, base_extra_params)

            result = factory.create()

            assert isinstance(result, expected_class)
            # assert result.dataframes == mock_dataframes
            assert result.extra_params == base_extra_params

    def test_create_with_empty_extra_params(self, mock_dataframes):
        """Test create method with minimal extra parameters."""
        minimal_params = {
            'report_type': 'CCSP',
            'deduplicator': None,
        }
        factory = Factory(mock_dataframes, minimal_params)

        result = factory.create()

        assert isinstance(result, DedupCCSP)
        assert result.dataframes == mock_dataframes
        assert result.extra_params == minimal_params

    def test_create_with_additional_extra_params(self, mock_dataframes, base_extra_params):
        """Test create method with additional extra parameters."""
        base_extra_params.update(
            {
                'additional_param': 'value',
                'another_param': 123,
            }
        )
        factory = Factory(mock_dataframes, base_extra_params)

        result = factory.create()

        assert isinstance(result, DedupCCSP)
        assert result.dataframes == mock_dataframes
        assert result.extra_params == base_extra_params
        assert result.extra_params['additional_param'] == 'value'
        assert result.extra_params['another_param'] == 123

    def test_create_renewal_hostname_deduplicator(self, mock_dataframes, base_extra_params):
        """Test creating DedupRenewalHostname with renewal-hostname deduplicator."""
        base_extra_params['report_type'] = 'RENEWAL_GUIDANCE'
        base_extra_params['deduplicator'] = 'renewal-hostname'
        factory = Factory(mock_dataframes, base_extra_params)

        result = factory.create()

        assert isinstance(result, DedupRenewalHostname)
        assert result.extra_params == base_extra_params

    def test_create_renewal_experimental_deduplicator(self, mock_dataframes, base_extra_params):
        """Test creating DedupRenewalExperimental with renewal-experimental deduplicator."""
        base_extra_params['report_type'] = 'RENEWAL_GUIDANCE'
        base_extra_params['deduplicator'] = 'renewal-experimental'
        factory = Factory(mock_dataframes, base_extra_params)

        result = factory.create()

        assert isinstance(result, DedupRenewalExperimental)
        assert result.extra_params == base_extra_params

    def test_create_renewal_hostname_with_invalid_report_type(self, mock_dataframes, base_extra_params):
        """Test that renewal-hostname with invalid report type raises exception."""
        base_extra_params['report_type'] = 'CCSP'
        base_extra_params['deduplicator'] = 'renewal-hostname'
        factory = Factory(mock_dataframes, base_extra_params)

        with pytest.raises(NotSupportedFactory, match='renewal-hostname only supports RENEWAL_GUIDANCE'):
            factory.create()

    def test_create_renewal_experimental_with_invalid_report_type(self, mock_dataframes, base_extra_params):
        """Test that renewal-experimental with invalid report type raises exception."""
        base_extra_params['report_type'] = 'CCSP'
        base_extra_params['deduplicator'] = 'renewal-experimental'
        factory = Factory(mock_dataframes, base_extra_params)

        with pytest.raises(
            NotSupportedFactory,
            match='renewal-experimental only supports RENEWAL_GUIDANCE',
        ):
            factory.create()
