"""Stub report implementations used by the library layer in tests."""

from contextlib import contextmanager

from .debug import indent, log


class BaseReport:
    """Stub base report that logs calls and returns fake spreadsheet paths."""

    def __init__(self, dataframes=None, extra_params=None):
        """Initialise the stub report.

        Args:
            dataframes: Dict of DataFrames (not used in the stub).
            extra_params: Configuration dict (not used in the stub).
        """
        log(f'library.reports {self.__class__.__name__}.__init__')
        self.dataframes = dataframes
        self.extra_params = extra_params

    def create(self):
        """Return self (stub — no-op factory method).

        Returns:
            The report instance itself.
        """
        log(f'library.reports {self.__class__.__name__}.create')
        return self

    def build_spreadsheet(self):
        """Return a fake spreadsheet path for testing.

        Returns:
            Path string to a fake ``/tmp/`` XLSX file.
        """
        log(f'library.reports {self.__class__.__name__}.build_spreadsheet')
        return f'/tmp/fake-spreadsheet-{self.__class__.__name__.lower()}.xlsx'

    @contextmanager
    def to_xlsx(self):
        """Context manager that yields the path to the (fake) built spreadsheet."""
        log(f'library.reports {self.__class__.__name__}.to_xlsx')
        indent(1)
        spreadsheet_path = self.build_spreadsheet()
        yield spreadsheet_path
        indent(-1)
        log(f'/library.reports {self.__class__.__name__}.to_xlsx')


class ReportCCSPv2(BaseReport):
    """Stub CCSPv2 report implementation for testing."""

    def build_spreadsheet(self):
        """Return a fake CCSPv2 report path."""
        log('library.reports ReportCCSPv2.build_spreadsheet')
        return '/tmp/fake-ccspv2-report.xlsx'


class ReportRenewalGuidance(BaseReport):
    """Stub Renewal Guidance report implementation for testing."""

    def build_spreadsheet(self):
        """Return a fake renewal guidance report path."""
        log('library.reports ReportRenewalGuidance.build_spreadsheet')
        return '/tmp/fake-renewal-guidance-report.xlsx'
