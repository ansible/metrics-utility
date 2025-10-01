from contextlib import contextmanager


class BaseReport:
    def __init__(self, dataframes=None, extra_params=None):
        print(f"library.reports {self.__class__.__name__}.__init__")

    def create(self):
        print(f"library.reports {self.__class__.__name__}.create")
        return self

    @contextmanager
    def to_xlsx(self):
        print(f"library.reports {self.__class__.__name__}.to_xlsx")
        yield f"/tmp/fake-report-{self.__class__.__name__.lower()}.xlsx"


class ReportCCSPv2(BaseReport):
    pass


class ReportRenewalGuidance(BaseReport):
    pass