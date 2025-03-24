import logging

import pandas as pd

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base
from metrics_utility.automation_controller_billing.helpers import merge_json_sets
from metrics_utility.debug_utils import print_data, print_debug


logger = logging.getLogger(__name__)

#######################################
# Code for building of the dataframe report based on MainHost table
######################################


class DataframeInventoryScope(Base):
    LOG_PREFIX = '[AAPBillingReport] '

    def build_dataframe(self):
        # A daily rollup dataframe

        billing_data_monthly_rollup = None

        for date in self.dates():
            ###############################
            # Generate the monthly dataset for report
            ###############################

            for data in self.extractor.iter_batches(date=date):
                # If the dataframe is empty, skip additional processing
                billing_data = data['main_host']
                if billing_data.empty:
                    continue

                print_debug(f'\nComputing data batch for {date}')
                print_data(billing_data, 'Newly loaded data')

                billing_data['organization_name'] = billing_data.organization_name.fillna('No organization name')
                billing_data['install_uuid'] = data['config']['install_uuid']

                # Store the original host name for mapping purposes
                billing_data['original_host_name'] = billing_data['host_name']
                if 'ansible_host_variable' in billing_data.columns:
                    # Replace missing ansible_host_variable with host name
                    billing_data['ansible_host_variable'] = billing_data.ansible_host_variable.fillna(billing_data['host_name'])
                    # And use the new ansible_host_variable instead of host_name, since
                    # what is in ansible_host_variable should be the actual host we count
                    billing_data['host_name'] = billing_data['ansible_host_variable']

                billing_data['last_automation'] = pd.to_datetime(billing_data['last_automation']).dt.tz_localize(None)

                ################################
                # Do the aggregation
                ################################

                print_data(billing_data, 'New loaded data batch')

                billing_data_group = billing_data.groupby(self.unique_index_columns(), dropna=False).agg(
                    organizations=('organization_name', lambda x: set(x)),
                    inventories=('inventory_name', lambda x: set(x)),
                    canonical_facts=('canonical_facts', lambda x: merge_json_sets(x)),
                    facts=('facts', lambda x: merge_json_sets(x)),
                    last_automation=('last_automation', 'max'),
                )
                print_data(billing_data_group, 'New data batch after aggregation')

                # Tweak types to match the table
                billing_data_group = self.cast_dataframe(billing_data_group, self.cast_types())

                ################################
                # Merge aggregations of multiple batches
                ################################
                if billing_data_monthly_rollup is None:
                    billing_data_monthly_rollup = billing_data_group
                else:
                    # Multipart collection, merge the dataframes and sum counts
                    billing_data_monthly_rollup = pd.merge(
                        billing_data_monthly_rollup.loc[:,], billing_data_group.loc[:,], on=self.unique_index_columns(), how='outer'
                    )
                    print_data(billing_data_monthly_rollup, 'Global data outer join batch data')

                    billing_data_monthly_rollup = self.summarize_merged_dataframes(
                        billing_data_monthly_rollup,
                        self.data_columns(),
                        operations={
                            'last_automation': 'max',
                            'organizations': 'combine_set',
                            'inventories': 'combine_set',
                            'canonical_facts': 'combine_json_values',
                            'facts': 'combine_json_values',
                        },
                    )

                    # Tweak types to match the table
                    billing_data_monthly_rollup = self.cast_dataframe(billing_data_monthly_rollup, self.cast_types())

                print_data(billing_data_monthly_rollup, 'Actual global data')

        if billing_data_monthly_rollup is None:
            return None

        return billing_data_monthly_rollup.reset_index()

    @staticmethod
    def unique_index_columns():
        return ['host_name', 'install_uuid']

    @staticmethod
    def data_columns():
        return ['last_automation', 'organizations', 'inventories', 'canonical_facts', 'facts']

    @staticmethod
    def cast_types():
        return {'last_automation': 'datetime64[ns]'}
