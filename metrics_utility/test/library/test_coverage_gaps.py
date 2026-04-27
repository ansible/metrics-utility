"""Tests targeting specific uncovered lines from the Sonar-Issues PR."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollup
from metrics_utility.automation_controller_billing.package.package_crc import PackageCRC
from metrics_utility.automation_controller_billing.report_saver.report_saver_s3 import ReportSaverS3
from metrics_utility.library.storage.crc import StorageCRC
from metrics_utility.management_utility import ManagementUtility


# --- BaseAnonymizedRollup.base() ---


def test_base_anonymized_rollup_base_returns_empty_dataframe():
    rollup = BaseAnonymizedRollup(rollup_name='test')
    result = rollup.base(pd.DataFrame({'col': [1, 2]}))
    assert isinstance(result, pd.DataFrame)
    assert result.empty


# --- ManagementUtility.get_commands() ---


def test_management_utility_get_commands_returns_dict():
    commands = ManagementUtility.get_commands()
    assert isinstance(commands, dict)
    assert all(v == 'metrics_utility' for v in commands.values())


# --- ReportSaverS3.report_exist() ---


def test_report_exist_returns_true_when_files_present():
    saver = ReportSaverS3.__new__(ReportSaverS3)
    saver.report_spreadsheet_destination_path = 'some/path'
    saver.s3_handler = MagicMock()
    saver.s3_handler.list_files.return_value = iter(['file1.xlsx'])
    assert saver.report_exist() is True


def test_report_exist_returns_false_when_no_files():
    saver = ReportSaverS3.__new__(ReportSaverS3)
    saver.report_spreadsheet_destination_path = 'some/path'
    saver.s3_handler = MagicMock()
    saver.s3_handler.list_files.return_value = iter([])
    assert saver.report_exist() is False


# --- StorageCRC (library/storage/crc.py) _put() RuntimeError ---


def test_crc_put_raises_runtime_error_on_bad_status():
    crc = StorageCRC(client_id='my-id', client_secret='my-secret')
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = 'Internal Server Error'
    with patch.object(crc, '_bearer', return_value='fake-token'), patch.object(crc, '_session') as mock_session:
        mock_session.return_value.post.return_value = mock_response
        with pytest.raises(RuntimeError, match='Upload failed with status 500'):
            crc._put(('artifact', b'data'))


# --- EventModulesAnonymizedRollup._merge_single_item() host_ids branch ---


def test_merge_single_item_sets_unique_hosts_total():
    rollup = EventModulesAnonymizedRollup()
    item = {'host_ids': ['host1', 'host2', 'host3'], 'total_count': 3}
    result = rollup._merge_single_item(item, None)
    assert result['unique_hosts_total'] == 3


# --- PackageCRC.is_shipping_configured() super() delegation ---


def test_package_crc_is_shipping_configured_returns_false_when_parent_returns_false():
    pkg = MagicMock(spec=PackageCRC)
    with patch('metrics_utility.base.Package.is_shipping_configured', return_value=False):
        result = PackageCRC.is_shipping_configured(pkg)
    assert result is False
