import pandas as pd

from metrics_utility.library.dataframes.base_traditional import BaseTraditional, merge_json_sets, merge_setdicts, merge_sets, parse_json


def compute_serial(row):
    facts = parse_json(row['canonical_facts'])

    if pd.isnull(facts.get('ansible_product_serial')) or pd.isnull(facts.get('ansible_machine_id')):
        return None

    return facts.get('ansible_product_serial', '') + '/' + facts.get('ansible_machine_id', '')


class DataframeMainHost(BaseTraditional):
    TARBALL_NAMES = ['main_host.csv', 'main_host_daily.csv', 'config.json']

    def prepare(self, tup):
        (mh, mhd, config) = tup

        if mh is not None:
            billing_data = mh
        elif mhd is not None:
            billing_data = mhd

        billing_data['organization_name'] = billing_data.organization_name.fillna('No organization name')
        billing_data['install_uuid'] = config['install_uuid']

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
        billing_data['host_names_before_dedup'] = billing_data['host_name']

        return billing_data

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
        return self.cast_dataframe(group)

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
