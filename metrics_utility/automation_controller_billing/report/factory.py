"""Factory for creating the correct report builder for the configured report type."""

from metrics_utility.automation_controller_billing.report.report_ccsp import ReportCCSP
from metrics_utility.automation_controller_billing.report.report_ccsp_v2 import ReportCCSPv2
from metrics_utility.automation_controller_billing.report.report_renewal_guidance import ReportRenewalGuidance
from metrics_utility.exceptions import NotSupportedFactory


class Factory:  # ReportFactory
    """Creates the correct report builder instance for the configured report type."""

    def __init__(self, dataframes, extra_params):
        """Initialise the report factory.

        Args:
            dataframes: Dict mapping dataframe names to DataFrames.
            extra_params: Dict containing at least ``'report_type'``.
        """
        self.dataframes = dataframes
        self.extra_params = extra_params

    def create(self):
        """Instantiate and return the report builder for the configured report type.

        Returns:
            A report builder instance with a ``build_spreadsheet()`` method.

        Raises:
            :exc:`~metrics_utility.exceptions.NotSupportedFactory`: For unknown
                report types.
        """
        report_type = self.extra_params['report_type']

        if report_type == 'CCSP':
            return ReportCCSP(self.dataframes, self.extra_params)

        if report_type == 'CCSPv2':
            return ReportCCSPv2(self.dataframes, self.extra_params)

        if report_type == 'RENEWAL_GUIDANCE':
            return ReportRenewalGuidance(self.dataframes, self.extra_params)

        raise NotSupportedFactory(f'Factory for {report_type} not supported')
