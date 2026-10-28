"""Format-neutral structure for the contents and presentation of a report."""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Font:
    name: str | None = None
    size: float | None = None
    color: str | None = None
    bold: bool = False


@dataclass(frozen=True)
class Alignment:
    horizontal: str | None = None
    vertical: str | None = None


@dataclass(frozen=True)
class PatternFill:
    pattern: str
    fg_color: str


@dataclass(frozen=True)
class Side:
    border_style: str | None = None
    color: str | None = None


@dataclass(frozen=True)
class Border:
    left: Side | None = None
    right: Side | None = None
    top: Side | None = None
    bottom: Side | None = None


@dataclass(frozen=True)
class CellReference:
    row: int
    column: int


@dataclass(frozen=True)
class Multiply:
    left: CellReference
    right: CellReference


@dataclass(frozen=True)
class Sum:
    start: CellReference
    end: CellReference


@dataclass
class Cell:
    row: int
    column: int
    value: Any = None
    font: Font | None = None
    fill: PatternFill | None = None
    border: Border | None = None
    alignment: Alignment | None = None
    number_format: str | None = None


@dataclass
class Dimension:
    width: float | None = None
    height: float | None = None


@dataclass
class Worksheet:
    title: str
    cells: dict[tuple[int, int], Cell] = field(default_factory=dict)
    merges: list[tuple[int, int, int, int]] = field(default_factory=list)
    column_dimensions: dict[int, Dimension] = field(default_factory=lambda: defaultdict(Dimension))
    row_dimensions: dict[int, Dimension] = field(default_factory=lambda: defaultdict(Dimension))

    def cell(self, row, column):
        if row < 1 or column < 1:
            raise ValueError('Row and column indices must be positive')
        return self.cells.setdefault((row, column), Cell(row, column))

    def merge_cells(self, *, start_row, start_column, end_row, end_column):
        self.merges.append((start_row, start_column, end_row, end_column))

    def iter_rows(self, *, min_row, max_row, min_col, max_col):
        for row in range(min_row, max_row + 1):
            yield tuple(self.cell(row, column) for column in range(min_col, max_col + 1))


class Workbook:
    def __init__(self):
        self.worksheets = [Worksheet('Sheet')]

    @property
    def active(self):
        return self.worksheets[0] if self.worksheets else None

    def remove(self, sheet):
        self.worksheets.remove(sheet)

    def create_sheet(self, title):
        # Match spreadsheet title rules and avoid duplicate names as the XLSX
        # writer did before the report definition was introduced.
        if any(char in title for char in r'\*?:/[]'):
            raise ValueError(f'Invalid character in sheet title: {title}')
        original = title
        number = 1
        names = {sheet.title.lower() for sheet in self.worksheets}
        while title.lower() in names:
            title = f'{original}{number}'
            number += 1
        sheet = Worksheet(title)
        self.worksheets.append(sheet)
        return sheet
