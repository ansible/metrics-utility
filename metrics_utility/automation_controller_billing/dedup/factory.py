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

        if deduplicator is None:
            if report_type in {'CCSP', 'CCSPv2'}:
                return DedupCCSP(**kwargs)
            if report_type in {'RENEWAL_GUIDANCE'}:
                return DedupRenewal(**kwargs)

            raise NotSupportedFactory(f'Unknown report type: {report_type}')

        if deduplicator == 'ccsp':
            return DedupCCSP(**kwargs)
        if deduplicator == 'renewal':
            return DedupRenewal(**kwargs)

        if deduplicator == 'ccsp-experimental':
            if report_type not in {'CCSP', 'CCSPv2'}:
                raise NotSupportedFactory(f'Unknown report type: {report_type}')
            return DedupCCSP(**kwargs, experimental=True)

        # New renewal guidance deduplication modes
        if deduplicator == 'renewal-hostname':
            if report_type not in {'RENEWAL_GUIDANCE'}:
                raise NotSupportedFactory(f'renewal-hostname only supports RENEWAL_GUIDANCE, got: {report_type}')
            return DedupRenewalHostname(**kwargs)

        if deduplicator == 'renewal-experimental':
            if report_type not in {'RENEWAL_GUIDANCE'}:
                raise NotSupportedFactory(f'renewal-experimental only supports RENEWAL_GUIDANCE, got: {report_type}')
            return DedupRenewalExperimental(**kwargs)

        raise NotSupportedFactory(f'Factory for {deduplicator} not supported')
