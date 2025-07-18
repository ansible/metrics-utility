from metrics_utility.automation_controller_billing.dedup.ccsp import DedupCCSP
from metrics_utility.automation_controller_billing.dedup.renewal_guidance import (
    DedupRenewal,
    DedupRenewalExperimental,
    DedupRenewalHostname,
)
from metrics_utility.exceptions import NotSupportedFactory


class Factory:  # DedupFactory
    def __init__(self, dataframes, extra_params):
        self.dataframes = dataframes
        self.extra_params = extra_params

    def create(self):
        deduplicator = self.extra_params['deduplicator']
        report_type = self.extra_params['report_type']

        kwargs = {
            'dataframes': self.dataframes,
            'extra_params': self.extra_params,
        }

        return self._get_deduplicator(deduplicator, report_type, kwargs)

    def _get_deduplicator(self, deduplicator, report_type, kwargs):
        if deduplicator is None:
            return self._get_default_deduplicator(report_type, kwargs)
        return self._get_custom_deduplicator(deduplicator, report_type, kwargs)

    def _get_default_deduplicator(self, report_type, kwargs):
        if report_type in {'CCSP', 'CCSPv2'}:
            return DedupCCSP(**kwargs)
        if report_type in {'RENEWAL_GUIDANCE'}:
            return DedupRenewal(**kwargs)
        raise NotSupportedFactory(f'Unknown report type: {report_type}')

    def _get_custom_deduplicator(self, deduplicator, report_type, kwargs):
        if deduplicator == 'ccsp':
            return DedupCCSP(**kwargs)
        if deduplicator == 'renewal':
            return DedupRenewal(**kwargs)
        if deduplicator == 'ccsp-experimental':
            self._validate_report_type(report_type, {'CCSP', 'CCSPv2'}, 'Unknown report type: {report_type}')
            return DedupCCSP(**kwargs, experimental=True)
        if deduplicator == 'renewal-hostname':
            self._validate_report_type(report_type, {'RENEWAL_GUIDANCE'}, 'renewal-hostname only supports RENEWAL_GUIDANCE, got: {report_type}')
            return DedupRenewalHostname(**kwargs)
        if deduplicator == 'renewal-experimental':
            self._validate_report_type(report_type, {'RENEWAL_GUIDANCE'}, 'renewal-experimental only supports RENEWAL_GUIDANCE, got: {report_type}')
            return DedupRenewalExperimental(**kwargs)
        raise NotSupportedFactory(f'Factory for {deduplicator} not supported')

    def _validate_report_type(self, report_type, allowed_types, error_message):
        if report_type not in allowed_types:
            raise NotSupportedFactory(error_message.format(report_type=report_type))
