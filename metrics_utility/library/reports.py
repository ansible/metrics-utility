from contextlib import contextmanager
import tempfile
import os


class BaseReport:
    def __init__(self, dataframes=None, extra_params=None):
        print(f"library.reports {self.__class__.__name__}.__init__")
        self.dataframes = dataframes
        self.extra_params = extra_params

    def create(self):
        print(f"library.reports {self.__class__.__name__}.create")
        return self

    def build_spreadsheet(self):
        print(f"library.reports {self.__class__.__name__}.build_spreadsheet")
        return f"/tmp/fake-spreadsheet-{self.__class__.__name__.lower()}.xlsx"

    @contextmanager
    def to_xlsx(self):
        print(f"library.reports {self.__class__.__name__}.to_xlsx")
        spreadsheet_path = self.build_spreadsheet()
        yield spreadsheet_path

    @contextmanager
    def to_csv(self):
        print(f"library.reports {self.__class__.__name__}.to_csv")
        spreadsheet_path = self.build_spreadsheet()
        csv_path = spreadsheet_path.replace('.xlsx', '.csv')
        yield csv_path

    @contextmanager
    def to_pdf(self):
        print(f"library.reports {self.__class__.__name__}.to_pdf")
        spreadsheet_path = self.build_spreadsheet()
        pdf_path = spreadsheet_path.replace('.xlsx', '.pdf')
        yield pdf_path


class ReportCCSPv2(BaseReport):
    def build_spreadsheet(self):
        print("library.reports ReportCCSPv2.build_spreadsheet")
        return f"/tmp/fake-ccspv2-report.xlsx"


class ReportRenewalGuidance(BaseReport):
    def build_spreadsheet(self):
        print("library.reports ReportRenewalGuidance.build_spreadsheet")
        return f"/tmp/fake-renewal-guidance-report.xlsx"