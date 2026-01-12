import pandas as pd

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base, merge_setdicts, merge_sets
from metrics_utility.automation_controller_billing.helpers import merge_json_sets, parse_json


def compute_serial(row):
    facts = parse_json(row['canonical_facts'])
    if pd.isnull(facts.get('ansible_product_serial')) or pd.isnull(facts.get('ansible_machine_id')):
        return None
    return facts.get('ansible_product_serial', '') + '/' + facts.get('ansible_machine_id', '')


# dataframe for main_host
class DataframeInventoryScope(Base):
    def build_dataframe(self):
        # A daily rollup dataframe

        billing_data_monthly_rollup = None

        for date in self.dates():
            ###############################
            # Generate the monthly dataset for report
            ###############################

            for data in self.extractor.iter_batches(date=date, collections=['main_host', 'main_host_daily'], optional=['config']):
                # If the dataframe is empty, skip additional processing
                billing_data = data['main_host']
                if billing_data.empty:
                    billing_data = data['main_host_daily']
                if billing_data.empty:
                    continue

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

                billing_data['last_automation'] = pd.to_datetime(billing_data['last_automation'], format='ISO8601').dt.tz_localize(None)

                billing_data['serial'] = billing_data.apply(compute_serial, axis=1)

                experimental_dedup = self.extra_params.get('deduplicator') == 'ccsp-experimental'
                if experimental_dedup:
                    billing_data['host_names_before_dedup'] = billing_data['host_name']
                else:
                    # Always create the column for consistent structure, but keep it empty when dedup is not enabled
                    billing_data['host_names_before_dedup'] = None

                ################################
                # Do the aggregation
                ################################

                billing_data_group = self.group(billing_data)

                ################################
                # Merge aggregations of multiple batches
                ################################

                billing_data_monthly_rollup = self.merge(billing_data_monthly_rollup, billing_data_group)

        if billing_data_monthly_rollup is None or billing_data_monthly_rollup.empty:
            return self.empty()

        return billing_data_monthly_rollup.reset_index()

    # Do the aggregation
    def group(self, dataframe):
        group = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            organizations=('organization_name', set),
            inventories=('inventory_name', set),
            canonical_facts=('canonical_facts', merge_json_sets),
            facts=('facts', merge_json_sets),
            last_automation=('last_automation', 'max'),
            serials=('serial', set),
            host_names_before_dedup=('host_names_before_dedup', set),
        )
        return self.cast_dataframe(group, self.cast_types())

    # Merge pre-aggregated
    def regroup(self, dataframe):
        return dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            organizations=('organizations', merge_sets),
            inventories=('inventories', merge_sets),
            canonical_facts=('canonical_facts', merge_setdicts),
            facts=('facts', merge_setdicts),
            last_automation=('last_automation', 'max'),
            serials=('serials', merge_sets),
            host_names_before_dedup=('host_names_before_dedup', merge_sets),
        )

    @staticmethod
    def unique_index_columns():
        return ['host_name', 'install_uuid']

    @staticmethod
    def data_columns():
        return ['last_automation', 'organizations', 'inventories', 'canonical_facts', 'facts', 'serials', 'host_names_before_dedup']

    @staticmethod
    def cast_types():
        return {'last_automation': 'datetime64[ns]'}

    @staticmethod
    def operations():
        return {
            'last_automation': 'max',
            'organizations': 'combine_set',
            'inventories': 'combine_set',
            'canonical_facts': 'combine_json_values',
            'facts': 'combine_json_values',
            'serials': 'combine_set',
            'host_names_before_dedup': 'combine_set',
        }
