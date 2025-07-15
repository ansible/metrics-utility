import ast
import json
import logging

import pandas as pd

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base, merge_setdicts, merge_sets
from metrics_utility.automation_controller_billing.helpers import merge_arrays, merge_json_sets, parse_json_array
from metrics_utility.metric_utils import DIRECT, INDIRECT, MANAGED_NODE_TYPES


logger = logging.getLogger(__name__)

DEVICE_TYPE_COL = 'device_type'
INFRASTRUCTURE_COL = 'infrastructure'


# dataframe for job_host_summary / indirect_nodes
class DataframeJobhostSummaryUsage(Base):
    def __init__(self, extractor, month, extra_params):
        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params

    def _get_infrastructure_from_facts(self, facts_dict):
        """
        Determines the infrastructure type from the 'infra_type' field in facts.
        Returns the infra_type value or 'Unknown Infrastructure' if not found.
        """
        if isinstance(facts_dict, dict) and 'infra_type' in facts_dict:
            infra_type = facts_dict['infra_type']
            if isinstance(infra_type, str) and infra_type.strip():
                return infra_type
        return 'Unknown Infrastructure'

    def _get_category_from_facts(self, facts_dict):
        """
        Determines the category from the 'infra_bucket' field in facts.
        Returns the infra_bucket value or 'Unknown Category' if not found.
        """
        if isinstance(facts_dict, dict) and 'infra_bucket' in facts_dict:
            infra_bucket = facts_dict['infra_bucket']
            if isinstance(infra_bucket, str) and infra_bucket.strip():
                return infra_bucket
        return 'Unknown Category'

    def _parse_facts_column_value(self, facts_data):
        """
        Parses a string representing JSON or a Python literal (like a dictionary)
        into a Python dictionary. Handles potential parsing errors and non-string inputs.
        """
        if isinstance(facts_data, str) and facts_data.strip():
            try:
                parsed_data = json.loads(facts_data)
                return parsed_data if isinstance(parsed_data, dict) else {}
            except json.JSONDecodeError:
                try:
                    parsed_data = ast.literal_eval(facts_data)
                    return parsed_data if isinstance(parsed_data, dict) else {}
                except (ValueError, SyntaxError) as e:
                    logger.warning(f"Could not parse facts string: '{facts_data}' using json.loads or ast.literal_eval. Error: {e}")
                    return {}
        elif isinstance(facts_data, dict):
            return facts_data
        return {}

    def build_dataframe(self):
        billing_data_monthly_rollup = None

        for date in self.dates():
            for data in self.extractor.iter_batches(date=date):
                print(f'DEBUG: Available data keys for date {date}: {list(data.keys())}')

                # Process DIRECT data if available
                direct_data = data['job_host_summary']
                if not direct_data.empty:
                    print(f'DEBUG: Processing DIRECT data: {len(direct_data)} rows')
                    billing_data = direct_data
                    managed_node_type = DIRECT

                    # Process this batch of DIRECT data
                    billing_data['managed_node_type'] = managed_node_type
                    billing_data['managed_node_type_string'] = MANAGED_NODE_TYPES[managed_node_type]

                    billing_data['organization_name'] = billing_data.organization_name.fillna('No organization name')
                    billing_data['install_uuid'] = data['config']['install_uuid']

                    billing_data['original_host_name'] = billing_data['host_name']
                    if 'ansible_host_variable' in billing_data.columns:
                        billing_data['ansible_host_variable'] = billing_data.ansible_host_variable.fillna(billing_data['host_name'])
                        billing_data['host_name'] = billing_data['ansible_host_variable']

                    # Summarize all task counts into 1 col (for use in DIRECT branch)
                    def sum_columns(row):
                        cols = ['dark', 'failures', 'ok', 'skipped', 'ignored', 'rescued']
                        return sum(row.get(i, 0) for i in cols)

                    # Summarize all reachable task counts into 1 col (for use in DIRECT branch)
                    def sum_reachable_columns(row):
                        cols = ['failures', 'ok', 'skipped', 'ignored', 'rescued']
                        return sum(row.get(i, 0) for i in cols)

                    # --- START: COMMON INITIALIZATION & PARSING FOR ALL hosts (DIRECT and INDIRECT) ---
                    # 1. Ensure core columns exist as pd.Series (fill with None if missing or entirely null)
                    if 'facts' not in billing_data.columns or billing_data['facts'].isnull().all():
                        billing_data['facts'] = pd.Series([None] * len(billing_data), index=billing_data.index)
                    if 'canonical_facts' not in billing_data.columns or billing_data['canonical_facts'].isnull().all():
                        billing_data['canonical_facts'] = pd.Series([None] * len(billing_data), index=billing_data.index)
                    if 'events' not in billing_data.columns or billing_data['events'].isnull().all():
                        billing_data['events'] = pd.Series([None] * len(billing_data), index=billing_data.index)

                    # 2. Parse raw data into Python objects for ALL hosts (DIRECT and INDIRECT)
                    billing_data['facts'] = billing_data['facts'].apply(self._parse_facts_column_value)
                    billing_data['canonical_facts'] = billing_data['canonical_facts'].apply(self._parse_facts_column_value)
                    billing_data['events'] = billing_data['events'].apply(parse_json_array)

                    # 3. Derive device_type, infrastructure, and category for ALL hosts based on parsed data
                    billing_data[DEVICE_TYPE_COL] = billing_data['facts'].apply(lambda x: x.get('device_type') if isinstance(x, dict) else None)
                    billing_data[INFRASTRUCTURE_COL] = billing_data['facts'].apply(self._get_infrastructure_from_facts)
                    billing_data['category'] = billing_data['facts'].apply(self._get_category_from_facts)
                    # --- END: Common initialization and parsing ---

                    # These calculations are specifically for DIRECT hosts using their detailed task counts
                    billing_data['task_runs'] = billing_data.apply(sum_columns, axis=1)
                    billing_data['reachable_task_runs'] = billing_data.apply(sum_reachable_columns, axis=1)
                    billing_data = billing_data[billing_data['reachable_task_runs'] > 0].copy()  # Filter unreachable DIRECT hosts

                    # Apply DIRECT-specific defaults (Physical Server, On-Premise)
                    billing_data[DEVICE_TYPE_COL] = billing_data[DEVICE_TYPE_COL].fillna('Physical Server')
                    billing_data[INFRASTRUCTURE_COL] = billing_data[INFRASTRUCTURE_COL].replace('Unknown Infrastructure', 'On-Premise')

                    # Print summary of data being processed
                    node_type_label = 'DIRECT'
                    print(f'Processing {node_type_label} data for date {date}: {len(billing_data)} rows')
                    if not billing_data.empty:
                        print(f'  Infrastructure types: {billing_data[INFRASTRUCTURE_COL].value_counts().to_dict()}')
                        print(f'  Device types: {billing_data[DEVICE_TYPE_COL].value_counts().to_dict()}')
                        print(f'  Categories: {billing_data["category"].value_counts().to_dict()}')

                    billing_data['created'] = pd.to_datetime(billing_data['created'], format='ISO8601').dt.tz_localize(None)
                    if 'job_created' in billing_data:
                        billing_data['job_created'] = pd.to_datetime(billing_data['job_created'], format='ISO8601').dt.tz_localize(None)
                    else:
                        billing_data['job_created'] = pd.NaT

                    ################################
                    # Do the aggregation for DIRECT data
                    ################################
                    billing_data_group = self.group(billing_data)

                    ################################
                    # Merge aggregations of multiple batches
                    ################################
                    billing_data_monthly_rollup = self.merge(billing_data_monthly_rollup, billing_data_group)

                # Process INDIRECT data if available
                indirect_data = data.get('indirect_nodes', pd.DataFrame())
                if not indirect_data.empty:
                    print(f'DEBUG: Processing INDIRECT data: {len(indirect_data)} rows')
                    billing_data = indirect_data
                    managed_node_type = INDIRECT

                    # Process this batch of INDIRECT data
                    billing_data['managed_node_type'] = managed_node_type
                    billing_data['managed_node_type_string'] = MANAGED_NODE_TYPES[managed_node_type]

                    billing_data['organization_name'] = billing_data.organization_name.fillna('No organization name')
                    billing_data['install_uuid'] = data['config']['install_uuid']

                    billing_data['original_host_name'] = billing_data['host_name']
                    if 'ansible_host_variable' in billing_data.columns:
                        billing_data['ansible_host_variable'] = billing_data.ansible_host_variable.fillna(billing_data['host_name'])
                        billing_data['host_name'] = billing_data['ansible_host_variable']

                    # --- START: COMMON INITIALIZATION & PARSING FOR ALL hosts (DIRECT and INDIRECT) ---
                    # 1. Ensure core columns exist as pd.Series (fill with None if missing or entirely null)
                    if 'facts' not in billing_data.columns or billing_data['facts'].isnull().all():
                        billing_data['facts'] = pd.Series([None] * len(billing_data), index=billing_data.index)
                    if 'canonical_facts' not in billing_data.columns or billing_data['canonical_facts'].isnull().all():
                        billing_data['canonical_facts'] = pd.Series([None] * len(billing_data), index=billing_data.index)
                    if 'events' not in billing_data.columns or billing_data['events'].isnull().all():
                        billing_data['events'] = pd.Series([None] * len(billing_data), index=billing_data.index)

                    # 2. Parse raw data into Python objects for ALL hosts (DIRECT and INDIRECT)
                    billing_data['facts'] = billing_data['facts'].apply(self._parse_facts_column_value)
                    billing_data['canonical_facts'] = billing_data['canonical_facts'].apply(self._parse_facts_column_value)
                    billing_data['events'] = billing_data['events'].apply(parse_json_array)

                    # 3. Derive device_type, infrastructure, and category for ALL hosts based on parsed data
                    billing_data[DEVICE_TYPE_COL] = billing_data['facts'].apply(lambda x: x.get('device_type') if isinstance(x, dict) else None)
                    billing_data[INFRASTRUCTURE_COL] = billing_data['facts'].apply(self._get_infrastructure_from_facts)
                    billing_data['category'] = billing_data['facts'].apply(self._get_category_from_facts)
                    # --- END: Common initialization and parsing ---

                    # For INDIRECT, task_runs might come directly from the raw 'task_runs' column if it exists in indirect_nodes,
                    # or default to 0 if not. No detailed counts (dark, ok, etc.) are available for summing.
                    # Ensure task_runs and reachable_task_runs exist as columns for consistency.
                    if 'task_runs' not in billing_data.columns:
                        billing_data['task_runs'] = 0  # Default if indirect_nodes doesn't have it
                    if 'reachable_task_runs' not in billing_data.columns:
                        billing_data['reachable_task_runs'] = 0  # Default if indirect_nodes doesn't have it

                    # Print summary of data being processed
                    node_type_label = 'INDIRECT'
                    print(f'Processing {node_type_label} data for date {date}: {len(billing_data)} rows')
                    if not billing_data.empty:
                        print(f'  Infrastructure types: {billing_data[INFRASTRUCTURE_COL].value_counts().to_dict()}')
                        print(f'  Device types: {billing_data[DEVICE_TYPE_COL].value_counts().to_dict()}')
                        print(f'  Categories: {billing_data["category"].value_counts().to_dict()}')

                    billing_data['created'] = pd.to_datetime(billing_data['created'], format='ISO8601').dt.tz_localize(None)
                    if 'job_created' in billing_data:
                        billing_data['job_created'] = pd.to_datetime(billing_data['job_created'], format='ISO8601').dt.tz_localize(None)
                    else:
                        billing_data['job_created'] = pd.NaT

                    ################################
                    # Do the aggregation for INDIRECT data
                    ################################
                    billing_data_group = self.group(billing_data)

                    ################################
                    # Merge aggregations of multiple batches
                    ################################
                    billing_data_monthly_rollup = self.merge(billing_data_monthly_rollup, billing_data_group)

        if billing_data_monthly_rollup is None or billing_data_monthly_rollup.empty:
            return self.empty()

        return billing_data_monthly_rollup.reset_index()

    def group(self, dataframe):
        group = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('task_runs', 'sum'),
            host_runs=('host_name', 'count'),
            first_automation=('created', 'min'),
            last_automation=('created', 'max'),
            job_created=('job_created', 'max'),
            managed_node_type=('managed_node_type', 'min'),
            managed_node_types_set=('managed_node_type_string', set),
            events=('events', lambda x: merge_arrays([item for item in x if item is not None])),
            canonical_facts=('canonical_facts', lambda x: merge_json_sets([item for item in x if item is not None])),
            facts=('facts', lambda x: merge_json_sets([item for item in x if item is not None])),
            device_type=(DEVICE_TYPE_COL, lambda x: x.dropna().iloc[0] if not x.dropna().empty else None),
            infrastructure=(INFRASTRUCTURE_COL, lambda x: x.dropna().iloc[0] if not x.dropna().empty else 'Unknown Infrastructure'),
            category=('category', lambda x: x.dropna().iloc[0] if not x.dropna().empty else 'Unknown Category'),
        )
        return self.cast_dataframe(group, self.cast_types())

    def regroup(self, dataframe):
        return dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('task_runs', 'sum'),
            host_runs=('host_runs', 'sum'),
            first_automation=('first_automation', 'min'),  # <--- This line should only appear ONCE
            last_automation=('last_automation', 'max'),
            job_created=('job_created', 'max'),
            managed_node_type=('managed_node_type', 'min'),
            managed_node_types_set=('managed_node_types_set', merge_sets),
            events=('events', merge_arrays),
            canonical_facts=('canonical_facts', merge_setdicts),
            facts=('facts', merge_setdicts),
            device_type=(DEVICE_TYPE_COL, lambda x: x.dropna().iloc[0] if not x.dropna().empty else None),
            infrastructure=(INFRASTRUCTURE_COL, lambda x: x.dropna().iloc[0] if not x.dropna().empty else 'Unknown Infrastructure'),
            category=('category', lambda x: x.dropna().iloc[0] if not x.dropna().empty else 'Unknown Category'),
        )

    @staticmethod
    def unique_index_columns():
        return ['organization_name', 'job_template_name', 'host_name', 'original_host_name', 'install_uuid', 'job_remote_id']

    @staticmethod
    def data_columns():
        return [
            'host_runs',
            'task_runs',
            'first_automation',
            'last_automation',
            'job_created',
            'managed_node_type',
            'managed_node_types_set',
            'canonical_facts',
            'facts',
            'events',
            DEVICE_TYPE_COL,
            INFRASTRUCTURE_COL,
            'category',
        ]

    @staticmethod
    def cast_types():
        return {
            'task_runs': int,
            'host_runs': int,
            'managed_node_type': int,
            'first_automation': 'datetime64[ns]',
            'last_automation': 'datetime64[ns]',
            'job_created': 'datetime64[ns]',
        }

    @staticmethod
    def operations():
        return {
            'first_automation': 'min',
            'last_automation': 'max',
            'job_created': 'max',
            'managed_node_type': 'min',
            'managed_node_types_set': 'combine_set',
            'events': 'combine_set',
            'canonical_facts': 'combine_json_values',
            'facts': 'combine_json_values',
            DEVICE_TYPE_COL: 'first_non_null',
            INFRASTRUCTURE_COL: 'first_non_null',
            'category': 'first_non_null',
        }
