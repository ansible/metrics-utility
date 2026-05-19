"""Unit tests for the five Factory classes in automation_controller_billing.

Each factory is tested for:
- every valid type returns the correct class/instance
- an unknown type raises NotSupportedFactory
"""

import datetime

from unittest.mock import MagicMock

import pytest

from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_collection_status import (
    DataframeCollectionStatus,
)
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage import (
    DataframeContentUsage,
)
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_inventory_scope import (
    DataframeInventoryScope,
)
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_jobhost_summary_usage import (
    DataframeJobhostSummaryUsage,
)
from metrics_utility.automation_controller_billing.dataframe_engine.db_dataframe_host_metric import (
    DBDataframeHostMetric,
)
from metrics_utility.automation_controller_billing.dataframe_engine.factory import Factory as DataframeFactory
from metrics_utility.automation_controller_billing.dedup.factory import Factory as DedupFactory  # noqa: F401
from metrics_utility.automation_controller_billing.extract.extractor_directory import ExtractorDirectory
from metrics_utility.automation_controller_billing.extract.extractor_s3 import ExtractorS3
from metrics_utility.automation_controller_billing.extract.factory import Factory as ExtractFactory
from metrics_utility.automation_controller_billing.package.factory import Factory as PackageFactory
from metrics_utility.automation_controller_billing.package.package_crc import PackageCRC
from metrics_utility.automation_controller_billing.package.package_directory import PackageDirectory
from metrics_utility.automation_controller_billing.package.package_s3 import PackageS3
from metrics_utility.automation_controller_billing.report.factory import Factory as ReportFactory
from metrics_utility.automation_controller_billing.report.report_ccsp import ReportCCSP
from metrics_utility.automation_controller_billing.report.report_ccsp_v2 import ReportCCSPv2
from metrics_utility.automation_controller_billing.report.report_renewal_guidance import ReportRenewalGuidance
from metrics_utility.automation_controller_billing.report_saver.factory import Factory as ReportSaverFactory
from metrics_utility.automation_controller_billing.report_saver.report_saver_directory import ReportSaverDirectory
from metrics_utility.automation_controller_billing.report_saver.report_saver_s3 import ReportSaverS3
from metrics_utility.exceptions import NotSupportedFactory


# ---------------------------------------------------------------------------
# Extract factory
# ---------------------------------------------------------------------------


class TestExtractFactory:
    def _factory(self, ship_target):
        return ExtractFactory(ship_target=ship_target, extra_params={'ship_path': '/tmp', 'optional_sheets': None})

    def test_directory_creates_extractor_directory(self):
        result = self._factory('directory').create()
        assert isinstance(result, ExtractorDirectory)

    def test_s3_creates_extractor_s3(self):
        result = self._factory('s3').create()
        assert isinstance(result, ExtractorS3)

    def test_unknown_target_raises(self):
        with pytest.raises(NotSupportedFactory):
            self._factory('ftp').create()


# ---------------------------------------------------------------------------
# Report factory
# ---------------------------------------------------------------------------


def _ccsp_extra_params(report_type='CCSPv2'):
    return {
        'report_type': report_type,
        'price_per_node': 0,
        'report_period': '2024-01',
        'report_sku': 'MCT3695',
        'report_h1_heading': 'Heading',
        'report_po_number': 'PO123',
        'report_company_name': 'Acme',
        'report_email': 'billing@acme.com',
        'report_rhn_login': 'user',
        'report_company_business_leader': 'Leader',
        'report_company_procurement_leader': 'Procurement',
        'report_sku_description': 'Desc',
        'report_organization_filter': None,
        'optional_sheets': None,
        'deduplicator': None,
        # renewal guidance fields
        'report_renewal_guidance_title': 'Title',
        'report_renewal_guidance_description': 'Desc',
        'report_renewal_guidance_dedup_iterations': '3',
        'ephemeral_days': 90,
        'since_date': '2024-01-01T00:00:00',
        'until_date': '2024-02-01T00:00:00',
    }


class TestReportFactory:
    def test_ccsp_creates_report_ccsp(self):
        factory = ReportFactory(dataframes={}, extra_params=_ccsp_extra_params('CCSP'))
        result = factory.create()
        assert isinstance(result, ReportCCSP)

    def test_ccspv2_creates_report_ccsp_v2(self):
        factory = ReportFactory(dataframes={}, extra_params=_ccsp_extra_params('CCSPv2'))
        result = factory.create()
        assert isinstance(result, ReportCCSPv2)

    def test_renewal_guidance_creates_correct_report(self):
        factory = ReportFactory(dataframes={}, extra_params=_ccsp_extra_params('RENEWAL_GUIDANCE'))
        result = factory.create()
        assert isinstance(result, ReportRenewalGuidance)

    def test_unknown_type_raises(self):
        extra = _ccsp_extra_params('UNKNOWN')
        with pytest.raises(NotSupportedFactory):
            ReportFactory(dataframes={}, extra_params=extra).create()


# ---------------------------------------------------------------------------
# ReportSaver factory
# ---------------------------------------------------------------------------


def _saver_params():
    return {'report_spreadsheet_destination_path': '/tmp/report.xlsx'}


class TestReportSaverFactory:
    def test_directory_creates_directory_saver(self):
        result = ReportSaverFactory(ship_target='directory', extra_params=_saver_params()).create()
        assert isinstance(result, ReportSaverDirectory)

    def test_controller_db_creates_directory_saver(self):
        result = ReportSaverFactory(ship_target='controller_db', extra_params=_saver_params()).create()
        assert isinstance(result, ReportSaverDirectory)

    def test_s3_creates_s3_saver(self):
        result = ReportSaverFactory(ship_target='s3', extra_params=_saver_params()).create()
        assert isinstance(result, ReportSaverS3)

    def test_unknown_target_raises(self):
        with pytest.raises(NotSupportedFactory):
            ReportSaverFactory(ship_target='ftp', extra_params=_saver_params()).create()


# ---------------------------------------------------------------------------
# Package factory
# ---------------------------------------------------------------------------


class TestPackageFactory:
    def test_crc_returns_package_crc_class(self):
        result = PackageFactory(ship_target='crc').create()
        assert result is PackageCRC

    def test_directory_returns_package_directory_class(self):
        result = PackageFactory(ship_target='directory').create()
        assert result is PackageDirectory

    def test_s3_returns_package_s3_class(self):
        result = PackageFactory(ship_target='s3').create()
        assert result is PackageS3

    def test_unknown_target_raises(self):
        with pytest.raises(NotSupportedFactory):
            PackageFactory(ship_target='ftp').create()


# ---------------------------------------------------------------------------
# Dataframe engine factory
# ---------------------------------------------------------------------------


def _df_factory(report_type):
    extractor = MagicMock()
    month = datetime.date(2024, 1, 1)
    extra_params = {'report_type': report_type, 'deduplicator': None, 'optional_sheets': None}
    return DataframeFactory(extractor=extractor, month=month, extra_params=extra_params)


class TestDataframeFactory:
    def test_ccsp_returns_expected_engines(self):
        result = _df_factory('CCSP').create()
        assert isinstance(result['job_host_summary'], DataframeJobhostSummaryUsage)
        assert isinstance(result['main_jobevent'], DataframeContentUsage)
        assert isinstance(result['main_host'], DataframeInventoryScope)
        assert 'data_collection_status' not in result

    def test_ccspv2_includes_data_collection_status(self):
        result = _df_factory('CCSPv2').create()
        assert isinstance(result['job_host_summary'], DataframeJobhostSummaryUsage)
        assert isinstance(result['data_collection_status'], DataframeCollectionStatus)

    def test_renewal_guidance_returns_host_metric_engine(self):
        result = _df_factory('RENEWAL_GUIDANCE').create()
        assert isinstance(result['host_metric'], DBDataframeHostMetric)

    def test_unknown_type_raises(self):
        with pytest.raises(NotSupportedFactory):
            _df_factory('UNKNOWN').create()
