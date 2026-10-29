"""Render a report definition as an OpenDocument spreadsheet."""

import datetime
import math

from decimal import Decimal
from numbers import Real

import numpy as np
import pandas as pd

from odf import teletype
from odf.number import CurrencyStyle, CurrencySymbol, DateStyle, Day, Hours, Minutes, Month, Number, Seconds, Text, TimeStyle, Year
from odf.opendocument import OpenDocumentSpreadsheet
from odf.style import ParagraphProperties, Style, TableCellProperties, TableColumnProperties, TableRowProperties, TextProperties
from odf.table import CoveredTableCell, Table, TableCell, TableColumn, TableRow
from odf.text import P
from openpyxl.utils import get_column_letter

from metrics_utility.automation_controller_billing.report.definition import Cell, Multiply, Sum


def _reference(ref):
    return f'.{get_column_letter(ref.column)}{ref.row}'


def _formula(value):
    if isinstance(value, Multiply):
        return f'of:=[{_reference(value.left)}]*[{_reference(value.right)}]'
    if isinstance(value, Sum):
        return f'of:=SUM([{_reference(value.start)}:{_reference(value.end)}])'
    raise TypeError(f'Unsupported report formula: {value!r}')


def _color(value):
    return f'#{value[-6:]}' if value else None


def _number_styles(document):
    currency = CurrencyStyle(name='currency')
    currency.addElement(CurrencySymbol(text='$'))
    currency.addElement(Number(decimalplaces=2, minintegerdigits=1, grouping='true'))
    document.automaticstyles.addElement(currency)

    date = DateStyle(name='datetime')
    for item in (
        Year(style='long'),
        Text(text='-'),
        Month(style='long'),
        Text(text='-'),
        Day(style='long'),
        Text(text=' '),
        Hours(style='long'),
        Text(text=':'),
        Minutes(style='long'),
        Text(text=':'),
        Seconds(style='long'),
    ):
        date.addElement(item)
    document.automaticstyles.addElement(date)

    date_only = DateStyle(name='date')
    for item in (Year(style='long'), Text(text='-'), Month(style='long'), Text(text='-'), Day(style='long')):
        date_only.addElement(item)
    document.automaticstyles.addElement(date_only)

    duration = TimeStyle(name='duration', truncateonoverflow='false')
    for item in (Hours(style='long'), Text(text=':'), Minutes(style='long'), Text(text=':'), Seconds(style='long')):
        duration.addElement(item)
    document.automaticstyles.addElement(duration)


def _border_properties(border):
    properties = {}
    for name in ('left', 'right', 'top', 'bottom'):
        side = getattr(border, name)
        if side and side.border_style:
            width = '1pt' if side.border_style in ('medium', 'thick') else '0.5pt'
            line = 'dotted' if side.border_style == 'dotted' else 'solid'
            properties[f'border{name}'] = f'{width} {line} {_color(side.color) or "#000000"}'
    return properties


def _cell_properties(source):
    properties = {}
    if source.fill and source.fill.pattern == 'solid':
        properties['backgroundcolor'] = _color(source.fill.fg_color)
    if source.alignment and source.alignment.vertical:
        properties['verticalalign'] = 'middle' if source.alignment.vertical == 'center' else source.alignment.vertical
    if source.border:
        properties.update(_border_properties(source.border))
    return properties


def _text_properties(font):
    properties = {}
    if font.name:
        properties['fontfamily'] = font.name
    if font.size:
        properties['fontsize'] = f'{font.size}pt'
    if font.color:
        properties['color'] = _color(font.color)
    if font.bold:
        properties['fontweight'] = 'bold'
    return properties


def _style(document, source, number_style, cache):
    key = (source.font, source.fill, source.border, source.alignment, number_style)
    if key in cache:
        return cache[key]

    params = {'name': f'cell{len(cache)}', 'family': 'table-cell'}
    if number_style:
        params['datastylename'] = number_style
    style = Style(**params)
    cell_properties = _cell_properties(source)
    if cell_properties:
        style.addElement(TableCellProperties(**cell_properties))
    text_properties = _text_properties(source.font) if source.font else {}
    if text_properties:
        style.addElement(TextProperties(**text_properties))
    if source.alignment and source.alignment.horizontal:
        style.addElement(ParagraphProperties(textalign=source.alignment.horizontal))
    document.automaticstyles.addElement(style)
    cache[key] = style
    return style


def _missing(value):
    if value is None:
        return True
    if isinstance(value, (str, Multiply, Sum)):
        return False
    result = pd.isna(value)
    return bool(result)


def _duration(value):
    if isinstance(value, pd.Timedelta):
        nanoseconds = value.value
    else:
        nanoseconds = ((value.days * 86400 + value.seconds) * 1000000 + value.microseconds) * 1000
    sign = '-' if nanoseconds < 0 else ''
    seconds, fraction = divmod(abs(nanoseconds), 1000000000)
    suffix = f'.{fraction:09d}'.rstrip('0') if fraction else ''
    return f'{sign}PT{seconds}{suffix}S'


def _value_attributes(value):
    """Return the ODF value attributes and any type-specific number style."""
    if isinstance(value, (Multiply, Sum)):
        return {'valuetype': 'float', 'formula': _formula(value)}, None
    if isinstance(value, (bool, np.bool_)):
        return {'valuetype': 'boolean', 'booleanvalue': str(bool(value)).lower()}, None
    if isinstance(value, (Real, Decimal)):
        if not math.isfinite(value):
            raise ValueError(f'Cannot write non-finite report value: {value}')
        return {'valuetype': 'float', 'value': str(value)}, None
    if isinstance(value, datetime.timedelta):
        return {'valuetype': 'time', 'timevalue': _duration(value)}, 'duration'
    if isinstance(value, datetime.datetime):
        return {'valuetype': 'date', 'datevalue': value.isoformat()}, 'datetime'
    if isinstance(value, datetime.date):
        return {'valuetype': 'date', 'datevalue': value.isoformat()}, 'date'
    if isinstance(value, datetime.time):
        if value.tzinfo is not None:
            raise ValueError('Timezone-aware times are not supported in reports')
        duration = datetime.timedelta(hours=value.hour, minutes=value.minute, seconds=value.second, microseconds=value.microsecond)
        return {'valuetype': 'time', 'timevalue': _duration(duration)}, 'duration'
    if isinstance(value, str):
        return {'valuetype': 'string'}, None
    raise TypeError(f'Unsupported report value: {type(value).__name__}')


def _cell(document, source, cache, span=None):
    value = source.value
    if source.number_format and source.number_format != '$#,##0.00':
        raise ValueError(f'Unsupported report number format: {source.number_format}')
    number_style = 'currency' if source.number_format else None
    attrs = {}
    if span:
        columns, rows = span
        if columns > 1:
            attrs['numbercolumnsspanned'] = columns
        if rows > 1:
            attrs['numberrowsspanned'] = rows
    if not _missing(value):
        value_attrs, value_style = _value_attributes(value)
        attrs.update(value_attrs)
        if value_style:
            number_style = value_style
    attrs['stylename'] = _style(document, source, number_style, cache)
    cell = TableCell(**attrs)
    if isinstance(value, str):
        paragraph = P()
        teletype.addTextToElement(paragraph, value.replace('\r\n', '\n'))
        cell.addElement(paragraph)
    return cell


class Spreadsheet:
    """A generated ODS document with the same save interface as XLSX."""

    def __init__(self, document):
        self.document = document

    def save(self, destination):
        self.document.save(str(destination), addsuffix=False)


def _add_columns(document, table, sheet, sheet_index, columns):
    for column in range(1, columns + 1):
        dimension = sheet.column_dimensions.get(column)
        if dimension and dimension.width is not None:
            style = Style(name=f'column{sheet_index}_{column}', family='table-column')
            style.addElement(TableColumnProperties(columnwidth=f'{dimension.width * 0.19:.2f}cm'))
            document.automaticstyles.addElement(style)
            table.addElement(TableColumn(stylename=style))
        else:
            table.addElement(TableColumn())


def _merged_cells(sheet):
    anchors = {}
    covered = set()
    for start_row, start_column, end_row, end_column in sheet.merges:
        anchors[(start_row, start_column)] = (end_column - start_column + 1, end_row - start_row + 1)
        covered.update(
            (row, column)
            for row in range(start_row, end_row + 1)
            for column in range(start_column, end_column + 1)
            if (row, column) != (start_row, start_column)
        )
    return anchors, covered


def _row(document, sheet, sheet_index, row):
    attrs = {}
    dimension = sheet.row_dimensions.get(row)
    if dimension and dimension.height is not None:
        style = Style(name=f'row{sheet_index}_{row}', family='table-row')
        style.addElement(TableRowProperties(rowheight=f'{dimension.height}pt'))
        document.automaticstyles.addElement(style)
        attrs['stylename'] = style
    return TableRow(**attrs)


def _add_cells(document, target, sheet, row, columns, cache, anchors, covered):
    for column in range(1, columns + 1):
        pos = (row, column)
        if pos in covered:
            target.addElement(CoveredTableCell())
        elif pos in sheet.cells:
            target.addElement(_cell(document, sheet.cells[pos], cache, anchors.get(pos)))
        elif pos in anchors:
            # A merged region may have an empty top-left cell.
            target.addElement(_cell(document, Cell(row, column), cache, anchors[pos]))
        else:
            target.addElement(TableCell())


def render(definition):
    if not definition.worksheets:
        raise ValueError('A report must have at least one sheet')
    document = OpenDocumentSpreadsheet()
    _number_styles(document)
    cache = {}
    for sheet_index, sheet in enumerate(definition.worksheets):
        table = Table(name=sheet.title)
        rows = max([row for row, _ in sheet.cells] + [merge[2] for merge in sheet.merges] + [0])
        columns = max([column for _, column in sheet.cells] + list(sheet.column_dimensions) + [merge[3] for merge in sheet.merges] + [0])
        _add_columns(document, table, sheet, sheet_index, columns)
        anchors, covered = _merged_cells(sheet)
        for row in range(1, rows + 1):
            target = _row(document, sheet, sheet_index, row)
            _add_cells(document, target, sheet, row, columns, cache, anchors, covered)
            table.addElement(target)
        document.spreadsheet.addElement(table)
    return Spreadsheet(document)
