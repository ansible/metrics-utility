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
    # --- NEW: __init__ method to load infrastructure map ---
    def __init__(self, extractor, month, extra_params):
        # Call the parent class's __init__ if Base expects arguments or needs setup.
        # Your Base class doesn't seem to have an __init__ in the provided code,
        # but if it did, you'd call super().__init__(*args, **kwargs) here.
        # For now, we'll assume Base doesn't need explicit super().__init__ if not defined.
        # If your Base has an __init__(self, config), you'd need to adapt this.
        # The key is to pass through extractor, month, extra_params for current class's use.
        # Assuming Base requires basic extractor setup, let's keep it minimal for now.
        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params

        self.infrastructure_map = self._load_infrastructure_map()
        self.collection_to_infrastructure_map = self._reverse_infrastructure_map(self.infrastructure_map)
    # --- END NEW ---

     # --- NEW: Methods to load and reverse infrastructure map ---
    def _load_infrastructure_map(self):
        # Assumes infrastructure_mapping.yml is in metrics_utility/automation_controller_billing/config/
        base_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(base_dir) # Go up one level from 'dataframe_engine'
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

        # Define a simple priority order if a host has events from multiple infra types
        # You can adjust this priority as needed based on your business logic.
        priority_order = ['Public Cloud', 'Private Cloud', 'Networking', 'Storage', 'Database']

        found_infrastructures = []
        for event_collection in events_list:
            if isinstance(event_collection, str): # Ensure it's a string before lookup
                infra_type = self.collection_to_infrastructure_map.get(event_collection)
                if infra_type:
                    found_infrastructures.append(infra_type)

        if found_infrastructures:
            # Sort by predefined priority. Items not in priority_order go to the end.
            found_infrastructures.sort(key=lambda x: priority_order.index(x) if x in priority_order else len(priority_order))
            return found_infrastructures[0] # Return the highest priority one

        return 'Unknown Infrastructure' # Default if no mapping is found for any event
    # --- END NEW ---
    def _parse_facts_column_value(self, facts_data):
        """
        Parses a string representing JSON or a Python literal (like a dictionary)
        into a Python dictionary. Handles potential parsing errors and non-string inputs.
        """
        if isinstance(facts_data, str) and facts_data.strip():
            try:
                # Attempt to parse as JSON first
                parsed_data = json.loads(facts_data)
                return parsed_data if isinstance(parsed_data, dict) else {}
            except json.JSONDecodeError:
                try:
                    # If JSON parsing fails, try ast.literal_eval for Python literals
                    parsed_data = ast.literal_eval(facts_data)
                    return parsed_data if isinstance(parsed_data, dict) else {}
                except (ValueError, SyntaxError) as e:
                    logger.warning(
                        f"Could not parse facts string: '{facts_data}' "
                        f"using json.loads or ast.literal_eval. Error: {e}"
                    )
                    return {}
        elif isinstance(facts_data, dict):
            # If it's already a dictionary (e.g., from an earlier stage), return it as is
            return facts_data
        # For None, NaN, or other non-string/non-dict inputs, return an empty dictionary
        return {}

    def build_dataframe(self):
        # A daily rollup dataframe
        billing_data_monthly_rollup = None

        for date in self.dates():
            ###############################
            # Generate the monthly dataset for report
            ###############################

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

                # Store the original host name for mapping purposes
                billing_data['original_host_name'] = billing_data['host_name']
                if 'ansible_host_variable' in billing_data.columns:
                    # Replace missing ansible_host_variable with host name
                    billing_data['ansible_host_variable'] = billing_data.ansible_host_variable.fillna(billing_data['host_name'])
                    # And use the new ansible_host_variable instead of host_name, since
                    # what is in ansible_host_variable should be the actual host we count
                    billing_data['host_name'] = billing_data['ansible_host_variable']

                # Summarize all task counts into 1 col
                def sum_columns(row):
                    return sum([row[i] for i in ['dark', 'failures', 'ok', 'skipped', 'ignored', 'rescued']])

                # Summarize all reachable task counts into 1 col
                def sum_reachable_columns(row):
                    return sum([row[i] for i in ['failures', 'ok', 'skipped', 'ignored', 'rescued']])

                if managed_node_type == DIRECT:
                    billing_data['task_runs'] = billing_data.apply(sum_columns, axis=1)

                    # Filter out managed nodes that were unreachable (represented as the dark counter).
                    # We want to count hosts that had at least one task running.
                    billing_data['reachable_task_runs'] = billing_data.apply(sum_reachable_columns, axis=1)
                    billing_data = billing_data[billing_data['reachable_task_runs'] > 0].copy()

                    # not collected for direct hosts
                    billing_data['facts'] = None
                    billing_data['canonical_facts'] = None
                    billing_data['events'] = None
                elif managed_node_type == INDIRECT:
                    # Load the events array safely
                    billing_data['events'] = billing_data['events'].apply(parse_json_array)

                 # --- DEBUGGING WITH PRINT STATEMENTS ---
                print(f"--- DEBUGGING FOR DATE: {date} ---")
                print(f"DEBUG: 'facts' column head (before _parse_facts_column_value):\n{billing_data['facts'].head().to_markdown(index=False)}")
                print(f"DEBUG: 'facts' column dtype (before _parse_facts_column_value): {billing_data['facts'].dtype}")
                print(f"DEBUG: Count of NaN in 'facts' (before _parse_facts_column_value): {billing_data['facts'].isna().sum()}")

                billing_data['facts'] = billing_data['facts'].apply(self._parse_facts_column_value)

                print(f"DEBUG: 'canonical_facts' column head (before _parse_facts_column_value):\n{billing_data['canonical_facts'].head().to_markdown(index=False)}")
                print(f"DEBUG: 'canonical_facts' column dtype (before _parse_facts_column_value): {billing_data['canonical_facts'].dtype}")
                print(f"DEBUG: Count of NaN in 'canonical_facts' (before _parse_facts_column_value): {billing_data['canonical_facts'].isna().sum()}")

                billing_data['canonical_facts'] = billing_data['canonical_facts'].apply(self._parse_facts_column_value)

                print(f"DEBUG: 'facts' column head (AFTER _parse_facts_column_value):\n{billing_data['facts'].head().to_markdown(index=False)}")
                print(f"DEBUG: 'facts' column dtype (AFTER _parse_facts_column_value): {billing_data['facts'].dtype}")
                print(f"DEBUG: 'canonical_facts' column head (AFTER _parse_facts_column_value):\n{billing_data['canonical_facts'].head().to_markdown(index=False)}")
                print(f"DEBUG: 'canonical_facts' column dtype (AFTER _parse_facts_column_value): {billing_data['canonical_facts'].dtype}")
                print(f"--- END DEBUGGING FOR DATE: {date} ---\n")
                # --- END DEBUGGING WITH PRINT STATEMENTS ---

                billing_data[DEVICE_TYPE_COL] = billing_data['facts'].apply(lambda x: x.get('device_type') if isinstance(x, dict) else None)

                billing_data['created'] = pd.to_datetime(billing_data['created'], format='ISO8601').dt.tz_localize(None)

                if 'job_created' in billing_data:
                    billing_data['job_created'] = pd.to_datetime(billing_data['job_created'], format='ISO8601').dt.tz_localize(None)
                else:
                    billing_data['job_created'] = pd.NaT

                ################################
                # Do the aggregation
                ################################

                # --- NEW DEBUG: Inspect billing_data before self.group() ---
                # Removed 'host_runs' from this print statement as it's not yet created.
                print(f"\n--- DEBUGGING billing_data before self.group() for date {date} ({'DIRECT' if managed_node_type == DIRECT else 'INDIRECT'}) ---")
                print(f"DEBUG: billing_data head (relevant columns only):\n{billing_data[['host_name', 'managed_node_type', DEVICE_TYPE_COL, 'facts']].head().to_markdown(index=False)}")
                print(f"DEBUG: Unique device_types in this batch:\n{billing_data[DEVICE_TYPE_COL].value_counts(dropna=False).to_markdown()}")
                print(f"DEBUG: Total rows in this batch: {len(billing_data)}")
                print("--- END DEBUGGING billing_data before self.group() ---\n")
                # --- END NEW DEBUG ---

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
         # --- NEW DEBUG: Inspect dataframe just before groupby ---
        print(f"\n--- DEBUGGING: group method input for this batch ---")
        print(f"DEBUG: Input dataframe head (for grouping):\n{dataframe[['host_name', 'managed_node_type', DEVICE_TYPE_COL, 'facts']].head().to_markdown(index=False)}")
        print(f"DEBUG: Unique device_types in input to group():\n{dataframe[DEVICE_TYPE_COL].value_counts(dropna=False).to_markdown()}")
        print(f"DEBUG: Unique managed_node_types in input to group():\n{dataframe['managed_node_type'].value_counts(dropna=False).to_markdown()}")
        # --- END NEW DEBUG ---

        group = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('task_runs', 'sum'),
            host_runs=('host_name', 'count'),
            first_automation=('created', 'min'),
            last_automation=('created', 'max'),
            job_created=('job_created', 'max'),
            managed_node_type=('managed_node_type', 'min'),
            managed_node_types_set=('managed_node_type_string', set),
            # TODO: optimize the aggregation to keep less rows around
            # job_ids=('inventory_name', set),
            events=('events', merge_arrays),
            canonical_facts=('canonical_facts', merge_json_sets),
            facts=('facts', merge_json_sets),
            device_type=(DEVICE_TYPE_COL, lambda x: x.dropna().iloc[0] if not x.dropna().empty else None),
        )
        # --- NEW DEBUG: Inspect 'group' DataFrame after aggregation ---
        print(f"\n--- DEBUGGING: group method output for this batch ---")
        print(f"DEBUG: Grouped DataFrame columns: {group.columns.tolist()}")
        print(f"DEBUG: Grouped DataFrame index levels: {group.index.names}") # Check index names
        print(f"DEBUG: Grouped DataFrame head:\n{group.head().to_markdown()}") # Print full head with index

        if DEVICE_TYPE_COL in group.columns:
            print(f"DEBUG: Unique device_types in grouped output:\n{group[DEVICE_TYPE_COL].value_counts(dropna=False).to_markdown()}")
        elif DEVICE_TYPE_COL in group.index.names:
            # If device_type is part of the index, we need to reset it to count values
            temp_df_for_counts = group.reset_index()
            if DEVICE_TYPE_COL in temp_df_for_counts.columns:
                 print(f"DEBUG: Unique device_types in grouped output (from index):\n{temp_df_for_counts[DEVICE_TYPE_COL].value_counts(dropna=False).to_markdown()}")
            else:
                 print(f"DEBUG: {DEVICE_TYPE_COL} not found in columns or index after reset for counts.")
        else:
            print(f"DEBUG: {DEVICE_TYPE_COL} not found in columns or index in grouped output.")

        print(f"DEBUG: Grouped DataFrame dtypes:\n{group.dtypes.to_markdown()}")
        print("--- END DEBUGGING: group method output ---\n")
        # --- END NEW DEBUG ---
        return self.cast_dataframe(group, self.cast_types())

    # Merge pre-aggregated
    def regroup(self, dataframe):
        return dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('task_runs', 'sum'),
            host_runs=('host_runs', 'sum'),
            first_automation=('first_automation', 'min'),
            last_automation=('last_automation', 'max'),
            job_created=('job_created', 'max'),
            managed_node_type=('managed_node_type', 'min'),
            managed_node_types_set=('managed_node_types_set', merge_sets),
            events=('events', merge_arrays),
            canonical_facts=('canonical_facts', merge_setdicts),
            facts=('facts', merge_setdicts),
            device_type=(DEVICE_TYPE_COL, lambda x: x.dropna().iloc[0] if not x.dropna().empty else None),
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
            DEVICE_TYPE_COL
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
        }
