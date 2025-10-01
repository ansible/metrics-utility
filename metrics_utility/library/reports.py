from contextlib import contextmanager

from .debug import indent, log


class BaseReport:
    def __init__(self, dataframes=None, extra_params=None):
        log(f'library.reports {self.__class__.__name__}.__init__')
        self.dataframes = dataframes
        self.extra_params = extra_params

    def create(self):
        log(f'library.reports {self.__class__.__name__}.create')
        return self

    def build_spreadsheet(self):
        log(f'library.reports {self.__class__.__name__}.build_spreadsheet')
        return f'/tmp/fake-spreadsheet-{self.__class__.__name__.lower()}.xlsx'

    @contextmanager
    def to_xlsx(self):
        log(f'library.reports {self.__class__.__name__}.to_xlsx')
        indent(1)
        spreadsheet_path = self.build_spreadsheet()
        yield spreadsheet_path
        indent(-1)
        log(f'/library.reports {self.__class__.__name__}.to_xlsx')


class ReportCCSPv2(BaseReport):
    def build_spreadsheet(self):
        log('library.reports ReportCCSPv2.build_spreadsheet')
        return '/tmp/fake-ccspv2-report.xlsx'


class ReportRenewalGuidance(BaseReport):
    def build_spreadsheet(self):
        log('library.reports ReportRenewalGuidance.build_spreadsheet')
        return '/tmp/fake-renewal-guidance-report.xlsx'
