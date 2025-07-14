import pandas as pd

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base, merge_setdicts, merge_sets
from metrics_utility.automation_controller_billing.helpers import merge_json_sets, parse_json


def compute_serial(row):
    facts = parse_json(row['canonical_facts'])
    if pd.isnull(facts['ansible_product_serial']) or pd.isnull(facts['ansible_machine_id']):
        return None
    return facts['ansible_product_serial'] + '/' + facts['ansible_machine_id']


# dataframe for main_host
class DataframeInventoryScope(Base):
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

                # Ensure host_names_before_dedup column exists for consistent structure
                if 'host_names_before_dedup' not in billing_data.columns:
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
        agg_dict = {
            'organizations': ('organization_name', set),
            'inventories': ('inventory_name', set),
            'canonical_facts': ('canonical_facts', merge_json_sets),
            'facts': ('facts', merge_json_sets),
            'last_automation': ('last_automation', 'max'),
            'serials': ('serial', set),
        }

        # Add host_names_before_dedup if it exists (after experimental deduplication)
        if 'host_names_before_dedup' in dataframe.columns:
            # Create a set from the values, properly handling both individual items and collections
            agg_dict['host_names_before_dedup'] = (
                'host_names_before_dedup',
                lambda x: set().union(*[item if isinstance(item, (set, list)) else {item} for item in x if item is not None]),
            )

        group = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(**agg_dict)
        return self.cast_dataframe(group, self.cast_types())

    # Merge pre-aggregated
    def regroup(self, dataframe):
        agg_dict = {
            'organizations': ('organizations', merge_sets),
            'inventories': ('inventories', merge_sets),
            'canonical_facts': ('canonical_facts', merge_setdicts),
            'facts': ('facts', merge_setdicts),
            'last_automation': ('last_automation', 'max'),
        }

        # Add serials if it exists (gets removed after deduplication mapping is created)
        if 'serials' in dataframe.columns:
            agg_dict['serials'] = ('serials', merge_sets)

        # Add host_names_before_dedup if it exists (after experimental deduplication)
        if 'host_names_before_dedup' in dataframe.columns:
            # Create a safe merge function that treats strings as single items
            def merge_hostname_sets(x):
                result_set = set()
                for item in x:
                    if item is not None:
                        if isinstance(item, (set, list)):
                            result_set.update(item)
                        else:
                            # Treat as single item (string)
                            result_set.add(item)
                return result_set

            agg_dict['host_names_before_dedup'] = ('host_names_before_dedup', merge_hostname_sets)

        return dataframe.groupby(self.unique_index_columns(), dropna=False).agg(**agg_dict)

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
