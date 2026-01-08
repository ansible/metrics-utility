import pandas as pd

from metrics_utility.library.dataframes.base_traditional import (
    BaseTraditional,
    combine_json_values,
    combine_set,
    merge_json_sets,
    merge_setdicts,
    merge_sets,
    parse_json,
)


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

        return group.astype({'last_automation': 'datetime64[ns]'})

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

    def merge(self, rollup, new_group):
        if rollup is None:
            return new_group

        df = pd.merge(rollup.loc[:,], new_group.loc[:,], on=self.unique_index_columns(), how='outer')

        # Apply aggregations directly
        df['last_automation'] = df[['last_automation_x', 'last_automation_y']].max(axis=1)
        df['organizations'] = df.apply(lambda row: combine_set(row.get('organizations_x'), row.get('organizations_y')), axis=1)
        df['inventories'] = df.apply(lambda row: combine_set(row.get('inventories_x'), row.get('inventories_y')), axis=1)
        df['canonical_facts'] = df.apply(lambda row: combine_json_values(row.get('canonical_facts_x'), row.get('canonical_facts_y')), axis=1)
        df['facts'] = df.apply(lambda row: combine_json_values(row.get('facts_x'), row.get('facts_y')), axis=1)
        df['serials'] = df.apply(lambda row: combine_set(row.get('serials_x'), row.get('serials_y')), axis=1)
        df['host_names_before_dedup'] = df.apply(
            lambda row: combine_set(row.get('host_names_before_dedup_x'), row.get('host_names_before_dedup_y')), axis=1
        )

        # Drop the _x and _y columns
        df = df.drop(
            columns=[
                'last_automation_x',
                'last_automation_y',
                'organizations_x',
                'organizations_y',
                'inventories_x',
                'inventories_y',
                'canonical_facts_x',
                'canonical_facts_y',
                'facts_x',
                'facts_y',
                'serials_x',
                'serials_y',
                'host_names_before_dedup_x',
                'host_names_before_dedup_y',
            ]
        )

        return df.astype({'last_automation': 'datetime64[ns]'})

    @staticmethod
    def unique_index_columns():
        return ['host_name', 'install_uuid']

    @staticmethod
    def data_columns():
        return ['last_automation', 'organizations', 'inventories', 'canonical_facts', 'facts', 'serials', 'host_names_before_dedup']

    @staticmethod
    def cast_types():
        return {'last_automation': 'datetime64[ns]'}
