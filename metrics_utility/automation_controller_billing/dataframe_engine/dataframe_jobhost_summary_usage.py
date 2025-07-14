import ast
import json
import logging
import os

import pandas as pd
import yaml

from metrics_utility.automation_controller_billing.dataframe_engine.base import (
    Base, merge_setdicts, merge_sets)
from metrics_utility.automation_controller_billing.helpers import (
    merge_arrays, merge_json_sets, parse_json_array)
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

        self.infrastructure_map = self._load_infrastructure_map()
        self.collection_to_infrastructure_map = self._reverse_infrastructure_map(self.infrastructure_map)

    def _load_infrastructure_map(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(base_dir)  # Go up one level from 'dataframe_engine'
        config_path = os.path.join(parent_dir, 'config', 'infrastructure_mapping.yml')

        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f).get('infrastructure_map', {})
        except FileNotFoundError:
            logger.error(f"Infrastructure mapping file not found at: {config_path}")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {config_path}: {e}")
            return {}

    def _reverse_infrastructure_map(self, infra_map):
        reversed_map = {}
        for infra_type, collections in infra_map.items():
            for collection_name in collections:
                reversed_map[collection_name] = infra_type
        return reversed_map

    def _get_infrastructure_from_events(self, events_list):
        """
        Determines the primary infrastructure type based on a list of Ansible collection names.
        Returns a single string (e.g., 'Public Cloud') or 'Unknown Infrastructure'.
        Prioritizes types if multiple are found, otherwise picks the first found.
        """
        if not events_list:
            return 'Unknown Infrastructure'

        priority_order = ['Public Cloud', 'Private Cloud', 'Networking', 'Storage', 'Database']

        found_infrastructures = []
        for event_collection in events_list:
            if isinstance(event_collection, str):
                infra_type = self.collection_to_infrastructure_map.get(event_collection)
                if infra_type:
                    found_infrastructures.append(infra_type)

        if found_infrastructures:
            found_infrastructures.sort(key=lambda x: priority_order.index(x) if x in priority_order else len(priority_order))
            return found_infrastructures[0]

        return 'Unknown Infrastructure'

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
                    logger.warning(
                        f"Could not parse facts string: '{facts_data}' "
                        f"using json.loads or ast.literal_eval. Error: {e}"
                    )
                    return {}
        elif isinstance(facts_data, dict):
            return facts_data
        return {}

    def build_dataframe(self):
        billing_data_monthly_rollup = None

        for date in self.dates():
            for data in self.extractor.iter_batches(date=date):
                billing_data = data['job_host_summary']
                managed_node_type = DIRECT

                if billing_data.empty:
                    billing_data = data['indirect_nodes']
                    managed_node_type = INDIRECT

                if billing_data.empty:
                    continue

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

                # 3. Derive device_type and infrastructure for ALL hosts based on parsed data
                billing_data[DEVICE_TYPE_COL] = billing_data['facts'].apply(
                    lambda x: x.get('device_type') if isinstance(x, dict) else None
                )
                billing_data[INFRASTRUCTURE_COL] = billing_data['events'].apply(self._get_infrastructure_from_events)
                # --- END: Common initialization and parsing ---


                # --- START: MANAGED_NODE_TYPE SPECIFIC LOGIC AND TASK_RUNS CALCULATION/FILTERING ---
                if managed_node_type == DIRECT:
                    # These calculations are specifically for DIRECT hosts using their detailed task counts
                    billing_data['task_runs'] = billing_data.apply(sum_columns, axis=1)
                    billing_data['reachable_task_runs'] = billing_data.apply(sum_reachable_columns, axis=1)
                    billing_data = billing_data[billing_data['reachable_task_runs'] > 0].copy() # Filter unreachable DIRECT hosts

                    # Apply DIRECT-specific defaults (Physical Server, On-Premise)
                    billing_data[DEVICE_TYPE_COL] = billing_data[DEVICE_TYPE_COL].fillna('Physical Server')
                    billing_data[INFRASTRUCTURE_COL] = billing_data[INFRASTRUCTURE_COL].replace('Unknown Infrastructure', 'On-Premise')

                elif managed_node_type == INDIRECT:
                    # For INDIRECT, task_runs might come directly from the raw 'task_runs' column if it exists in indirect_nodes,
                    # or default to 0 if not. No detailed counts (dark, ok, etc.) are available for summing.
                    # Ensure task_runs and reachable_task_runs exist as columns for consistency.
                    if 'task_runs' not in billing_data.columns:
                        billing_data['task_runs'] = 0 # Default if indirect_nodes doesn't have it
                    if 'reachable_task_runs' not in billing_data.columns:
                        billing_data['reachable_task_runs'] = 0 # Default if indirect_nodes doesn't have it
                    # No filter based on reachable_task_runs > 0 for INDIRECT, as it may lose valid hosts
                    # If INDIRECT hosts have their own criteria for being 'reachable' or counted, apply it here.
                    # For now, assuming all INDIRECT hosts passed to this point are 'countable' if they have task_runs > 0,
                    # but not filtering based on reachable_task_runs for INDIRECT.
                    # If you explicitly want to filter INDIRECT hosts with 0 task_runs, add:
                    # billing_data = billing_data[billing_data['task_runs'] > 0].copy()
                # --- END: MANAGED_NODE_TYPE SPECIFIC LOGIC ---


                # --- DEBUGGING WITH PRINT STATEMENTS (from previous step - keep or remove) ---
                print(f"--- DEBUGGING FOR DATE: {date} ---")
                print(f"DEBUG: 'facts' column head (after _parse_facts_column_value):\n{billing_data['facts'].head().to_markdown(index=False)}")
                print(f"DEBUG: 'canonical_facts' column head (after _parse_facts_column_value):\n{billing_data['canonical_facts'].head().to_markdown(index=False)}")
                print(f"DEBUG: 'events' column head (after parse_json_array):\n{billing_data['events'].head().to_markdown(index=False)}")
                print(f"--- END DEBUGGING FOR DATE: {date} ---\n")

                print(f"\n--- DEBUGGING billing_data before self.group() for date {date} ({'DIRECT' if managed_node_type == DIRECT else 'INDIRECT'}) ---")
                print(f"DEBUG: billing_data head (relevant cols):\n{billing_data[['host_name', 'managed_node_type', DEVICE_TYPE_COL, INFRASTRUCTURE_COL, 'facts', 'events', 'task_runs']].head().to_markdown(index=False)}")
                print(f"DEBUG: Unique device_types in this batch:\n{billing_data[DEVICE_TYPE_COL].value_counts(dropna=False).to_markdown()}")
                print(f"DEBUG: Unique infrastructure in this batch:\n{billing_data[INFRASTRUCTURE_COL].value_counts(dropna=False).to_markdown()}")
                print(f"DEBUG: Total rows in this batch: {len(billing_data)}")
                print("--- END DEBUGGING billing_data before self.group() ---\n")
                # --- END DEBUGGING ---

                billing_data['created'] = pd.to_datetime(billing_data['created'], format='ISO8601').dt.tz_localize(None)
                if 'job_created' in billing_data:
                    billing_data['job_created'] = pd.to_datetime(billing_data['job_created'], format='ISO8601').dt.tz_localize(None)
                else:
                    billing_data['job_created'] = pd.NaT

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

    def group(self, dataframe):
        # --- DEBUGGING: group method input for this batch ---
        print(f"\n--- DEBUGGING: group method input for this batch ---")
        print(f"DEBUG: Input dataframe head (for grouping):\n{dataframe[['host_name', 'managed_node_type', DEVICE_TYPE_COL, INFRASTRUCTURE_COL, 'facts', 'events']].head().to_markdown(index=False)}")
        print(f"DEBUG: Unique device_types in input to group():\n{dataframe[DEVICE_TYPE_COL].value_counts(dropna=False).to_markdown()}")
        print(f"DEBUG: Unique infrastructure in input to group():\n{dataframe[INFRASTRUCTURE_COL].value_counts(dropna=False).to_markdown()}")
        print(f"DEBUG: Unique managed_node_types in input to group():\n{dataframe['managed_node_type'].value_counts(dropna=False).to_markdown()}")
        print("--- END DEBUGGING: group method input ---\n")

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
        )
        # --- DEBUGGING: group method output for this batch ---
        print(f"\n--- DEBUGGING: group method output for this batch ---")
        print(f"DEBUG: Grouped DataFrame columns: {group.columns.tolist()}")
        print(f"DEBUG: Grouped DataFrame index levels: {group.index.names}")
        print(f"DEBUG: Grouped DataFrame head:\n{group.head().to_markdown()}")

        if DEVICE_TYPE_COL in group.columns:
            print(f"DEBUG: Unique device_types in grouped output:\n{group[DEVICE_TYPE_COL].value_counts(dropna=False).to_markdown()}")
        else:
            print(f"DEBUG: {DEVICE_TYPE_COL} not found in columns in grouped output.")

        if INFRASTRUCTURE_COL in group.columns:
            print(f"DEBUG: Unique infrastructure in grouped output:\n{group[INFRASTRUCTURE_COL].value_counts(dropna=False).to_markdown()}")
        else:
            print(f"DEBUG: {INFRASTRUCTURE_COL} not found in columns in grouped output.")

        print(f"DEBUG: Grouped DataFrame dtypes:\n{group.dtypes.to_markdown()}")
        print("--- END DEBUGGING: group method output ---\n")
        return self.cast_dataframe(group, self.cast_types())

    def regroup(self, dataframe):
        return dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('task_runs', 'sum'),
            host_runs=('host_runs', 'sum'),
            first_automation=('first_automation', 'min'), # <--- This line should only appear ONCE
            last_automation=('last_automation', 'max'),
            job_created=('job_created', 'max'),
            managed_node_type=('managed_node_type', 'min'),
            managed_node_types_set=('managed_node_types_set', merge_sets),
            events=('events', merge_arrays),
            canonical_facts=('canonical_facts', merge_setdicts),
            facts=('facts', merge_setdicts),
            device_type=(DEVICE_TYPE_COL, lambda x: x.dropna().iloc[0] if not x.dropna().empty else None),
            infrastructure=(INFRASTRUCTURE_COL, lambda x: x.dropna().iloc[0] if not x.dropna().empty else 'Unknown Infrastructure'),
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
        }
