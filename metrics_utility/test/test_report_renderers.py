"""Behavioral checks for shared report definitions and spreadsheet output."""

import datetime
import gc
import json
import weakref

from pathlib import Path
from unittest.mock import Mock
from zipfile import ZipFile

import numpy as np
import pandas as pd
import pytest

from defusedxml import ElementTree
from openpyxl import load_workbook

from metrics_utility.automation_controller_billing.report.definition import (
    Alignment,
    Border,
    CellReference,
    Font,
    Multiply,
    PatternFill,
    Side,
    Sum,
    Workbook,
)
from metrics_utility.automation_controller_billing.report.renderers.ods import render as render_ods
from metrics_utility.automation_controller_billing.report.renderers.xlsx import render as render_xlsx
from metrics_utility.automation_controller_billing.report_saver.report_saver_directory import ReportSaverDirectory
from metrics_utility.automation_controller_billing.report_saver.report_saver_s3 import ReportSaverS3
from metrics_utility.test.util import run_build_int


TABLE = 'urn:oasis:names:tc:opendocument:xmlns:table:1.0'
OFFICE = 'urn:oasis:names:tc:opendocument:xmlns:office:1.0'
NS = {'table': TABLE, 'office': OFFICE}


def _definition():
    book = Workbook()
    book.remove(book.active)
    sheet = book.create_sheet('Usage')
    sheet.column_dimensions[1].width = 24
    sheet.row_dimensions[1].height = 25
    sheet.merge_cells(start_row=1, start_column=1, end_row=1, end_column=2)
    sheet.cell(1, 1).value = 'Usage report'
    sheet.cell(1, 1).font = Font(name='Arial', size=12, color='000000', bold=True)
    sheet.cell(2, 1).value = 3
    sheet.cell(2, 2).value = 12.5
    sheet.cell(2, 3).value = Multiply(CellReference(2, 1), CellReference(2, 2))
    sheet.cell(3, 3).value = Sum(CellReference(2, 3), CellReference(2, 3))
    sheet.cell(2, 3).number_format = '$#,##0.00'
    sheet.cell(2, 3).border = Border(left=Side(border_style='dotted', color='000000'))
    status = book.create_sheet('Collection status')
    status.cell(1, 1).value = 'Warning\nSecond line'
    status.cell(1, 1).fill = PatternFill('solid', fgColor='ffcc17')
    status.cell(1, 1).alignment = Alignment(horizontal='center', vertical='center')
    status.cell(2, 1).value = datetime.timedelta(hours=36)
    status.cell(3, 1).value = datetime.datetime(2024, 4, 17, 12, 19, 38, 762500)
    status.cell(4, 1).value = pd.NaT
    status.cell(5, 1).value = datetime.timedelta(hours=36, microseconds=123456)
    status.cell(6, 1).value = np.bool_(True)
    status.cell(7, 1).value = datetime.date(2024, 4, 17)
    status.cell(8, 1).value = datetime.time(12, 34, 56, 123456)
    status.cell(9, 1).fill = PatternFill('solid', fgColor='92d050')
    status.cell(10, 1).fill = PatternFill('solid', fgColor='FF0000')
    return book


def test_shared_definition_renders_formulas_styles_and_typed_values(tmp_path):
    definition = _definition()
    xlsx_path = tmp_path / 'report.xlsx'
    ods_path = tmp_path / 'report.ods'
    render_xlsx(definition).save(xlsx_path)
    render_ods(definition).save(ods_path)

    xlsx = load_workbook(xlsx_path)
    assert xlsx.sheetnames == ['Usage', 'Collection status']
    assert xlsx['Usage']['C2'].value == '=A2*B2'
    assert xlsx['Usage']['C3'].value == '=SUM(C2:C2)'
    assert 'A1:B1' in {str(item) for item in xlsx['Usage'].merged_cells.ranges}
    assert xlsx['Collection status']['A1'].fill.fgColor.rgb[-6:] == 'ffcc17'
    assert xlsx['Collection status']['A2'].value == datetime.timedelta(hours=36)

    with ZipFile(ods_path) as archive:
        assert archive.read('mimetype') == b'application/vnd.oasis.opendocument.spreadsheet'
        document = ElementTree.fromstring(archive.read('content.xml'))
        sheets = document.findall('.//table:table', NS)
        assert [sheet.attrib[f'{{{TABLE}}}name'] for sheet in sheets] == ['Usage', 'Collection status']
        cells = document.findall('.//table:table-cell', NS)
        formulas = [cell.attrib[f'{{{TABLE}}}formula'] for cell in cells if f'{{{TABLE}}}formula' in cell.attrib]
        assert formulas == ['of:=[.A2]*[.B2]', 'of:=SUM([.C2:.C2])']
        assert any(cell.attrib.get(f'{{{OFFICE}}}time-value') == 'PT129600S' for cell in cells)
        assert any(cell.attrib.get(f'{{{OFFICE}}}time-value') == 'PT129600.123456S' for cell in cells)
        assert any(cell.attrib.get(f'{{{OFFICE}}}boolean-value') == 'true' for cell in cells)
        assert any(cell.attrib.get(f'{{{OFFICE}}}date-value') == '2024-04-17T12:19:38.762500' for cell in cells)
        assert any(cell.attrib.get(f'{{{OFFICE}}}date-value') == '2024-04-17' for cell in cells)
        assert any(cell.attrib.get(f'{{{OFFICE}}}time-value') == 'PT45296.123456S' for cell in cells)
        assert any(cell.attrib.get(f'{{{TABLE}}}number-columns-spanned') == '2' for cell in cells)
        styles = archive.read('content.xml')
        assert b'#ffcc17' in styles
        assert b'#92d050' in styles
        assert b'#FF0000' in styles


def test_ods_saver_writes_exact_s3_temporary_path(tmp_path, monkeypatch):
    uploaded = {}

    def upload(path, destination):
        uploaded['destination'] = destination
        uploaded['bytes'] = Path(path).read_bytes()

    handler = Mock()
    handler.upload_file.side_effect = upload
    monkeypatch.setattr('metrics_utility.automation_controller_billing.report_saver.report_saver_s3.S3Handler', lambda **_: handler)
    saver = ReportSaverS3({'report_spreadsheet_destination_path': 'reports/sample.ods'})
    saver.save(render_ods(_definition()))
    assert uploaded['destination'] == 'reports/sample.ods'
    from io import BytesIO

    with ZipFile(BytesIO(uploaded['bytes'])) as archive:
        assert archive.read('mimetype') == b'application/vnd.oasis.opendocument.spreadsheet'


def test_ods_rejects_report_without_sheets():
    definition = Workbook()
    definition.remove(definition.active)
    with pytest.raises(ValueError, match='at least one sheet'):
        render_ods(definition)


def test_build_report_releases_ods_definition_before_saving(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[2]
    (tmp_path / 'data').symlink_to(root / 'metrics_utility/test/test_data/data', target_is_directory=True)
    snapshot = root / 'metrics_utility/test/snapshot_tests/CCSP/data/CCSPv2/snapshot_def_2024-04.json'
    env = json.loads(snapshot.read_text())['env_vars']
    env['METRICS_UTILITY_SHIP_PATH'] = str(tmp_path)
    captured = {}
    original_save = ReportSaverDirectory.save

    def track_definition(definition):
        captured['definition'] = weakref.ref(definition)
        return render_ods(definition)

    def verify_before_save(saver, spreadsheet):
        gc.collect()
        assert captured['definition']() is None
        original_save(saver, spreadsheet)

    monkeypatch.setattr('metrics_utility.automation_controller_billing.report.renderers.ods.render', track_definition)
    monkeypatch.setattr(ReportSaverDirectory, 'save', verify_before_save)
    run_build_int(env, {'month': '2024-04', 'format': 'ods'})
    assert (tmp_path / 'reports/2024/04/CCSPv2-2024-04.ods').is_file()


@pytest.mark.parametrize('report_type', ['CCSP', 'CCSPv2'])
def test_build_report_writes_xlsx_and_ods_to_separate_paths(tmp_path, report_type, monkeypatch):
    root = Path(__file__).resolve().parents[2]
    input_data = root / 'metrics_utility/test/test_data/data'
    (tmp_path / 'data').symlink_to(input_data, target_is_directory=True)
    snapshot = root / f'metrics_utility/test/snapshot_tests/CCSP/data/{report_type}/snapshot_def_2024-04.json'
    env = json.loads(snapshot.read_text())['env_vars']
    env['METRICS_UTILITY_SHIP_PATH'] = str(tmp_path)
    run_build_int(env, {'month': '2024-04'})
    run_build_int(env, {'month': '2024-04', 'format': 'ods'})

    report_dir = tmp_path / 'reports/2024/04'
    xlsx = report_dir / f'{report_type}-2024-04.xlsx'
    ods = report_dir / f'{report_type}-2024-04.ods'
    assert xlsx.is_file() and ods.is_file()
    assert load_workbook(xlsx).sheetnames[0] == 'Usage Reporting'
    with ZipFile(ods) as archive:
        document = ElementTree.fromstring(archive.read('content.xml'))
        assert document.find('.//table:table', NS).attrib[f'{{{TABLE}}}name'] == 'Usage Reporting'
    with monkeypatch.context() as patch:
        patch.setattr(
            'metrics_utility.automation_controller_billing.report.renderers.ods.render',
            Mock(side_effect=AssertionError('existing report must be skipped')),
        )
        run_build_int(env, {'month': '2024-04', 'format': 'ods'})
    ods.write_bytes(b'old report')
    run_build_int(env, {'month': '2024-04', 'format': 'ods', 'force': True})
    with ZipFile(ods) as archive:
        assert archive.read('mimetype') == b'application/vnd.oasis.opendocument.spreadsheet'


def test_ccspv2_ods_includes_optional_sheets_and_status_colors(tmp_path):
    root = Path(__file__).resolve().parents[2]
    (tmp_path / 'data').symlink_to(root / 'metrics_utility/test/test_data/data', target_is_directory=True)
    env = {
        'METRICS_UTILITY_SHIP_PATH': str(tmp_path),
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
        'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
        'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host,main_jobevent,main_indirectmanagednodeaudit',
        'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': (
            'ccsp_summary,jobs,managed_nodes,indirectly_managed_nodes,inventory_scope,usage_by_organizations,'
            'usage_by_collections,usage_by_roles,usage_by_modules,data_collection_status'
        ),
        'METRICS_UTILITY_DEDUPLICATOR': 'ccsp-experimental',
    }
    run_build_int(env, {'since': '2025-07-08', 'until': '2025-07-11', 'format': 'ods'})
    report = tmp_path / 'reports/2025/07/CCSPv2-2025-07-08--2025-07-11.ods'
    with ZipFile(report) as archive:
        content = archive.read('content.xml')
        doc = ElementTree.fromstring(content)
        names = [sheet.attrib[f'{{{TABLE}}}name'] for sheet in doc.findall('.//table:table', NS)]
        assert names == [
            'Usage Reporting',
            'Jobs',
            'Managed nodes',
            'Indirectly Managed nodes',
            'Inventory Scope',
            'Usage by organizations',
            'Usage by collections',
            'Usage by roles',
            'Usage by modules',
            'Data collection status',
        ]
        assert b'#FF0000' in content
        assert b'of:=SUM([.J7:.J12])' in content


def test_renewal_definition_renders_optional_sheets(setup_processed_dataframe, tmp_path):
    from metrics_utility.automation_controller_billing.report.report_renewal_guidance import ReportRenewalGuidance

    options = {
        'ephemeral_days': 30,
        'report_period': '2025-01-01, 2025-06-03',
        'report_type': 'RENEWAL_GUIDANCE',
        'since_date': '2025-01-01',
        'until_date': '2025-06-03',
        'optional_sheets': ['managed_nodes'],
    }
    report = ReportRenewalGuidance({'host_metric': setup_processed_dataframe}, options).build_definition()
    output = tmp_path / 'renewal.ods'
    render_ods(report).save(output)
    with ZipFile(output) as archive:
        doc = ElementTree.fromstring(archive.read('content.xml'))
        names = [sheet.attrib[f'{{{TABLE}}}name'] for sheet in doc.findall('.//table:table', NS)]
        assert names == [
            'Usage Reporting',
            'Managed nodes',
            'Managed nodes ephemeral',
            'Managed nodes ephemeral usage',
            'Deleted Managed nodes',
        ]
