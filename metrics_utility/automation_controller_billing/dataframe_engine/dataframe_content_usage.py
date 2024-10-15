import logging
import datetime
import pandas as pd
import re
from dateutil.relativedelta import relativedelta

from metrics_utility.automation_controller_billing.dataframe_engine.base \
    import Base, list_dates, granularity_cast

logger = logging.getLogger(__name__)

#######################################
# Code for building of the dataframe report based on Event table
######################################

class DataframeContentUsage(Base):
    LOG_PREFIX = "[AAPBillingReport] "

    def build_dataframe(self):
        # A monthly rollup dataframe
        content_explorer_rollup = None

        for date in self.dates():
            ###############################
            # Start a daily rollup code here
            ###############################
            for data in self.extractor.iter_batches(date=date):
                # If the dataframe is empty, skip additional processing
                events = data['main_jobevent']
                if events.empty:
                    continue

                # Filter non relevant rows
                events = events[events['task_action'].notnull()]
                events = events[events['host_name'].notnull()]

                # If the dataframe is empty, skip additional processing
                if events.empty:
                    continue

                events['install_uuid'] = data['config']['install_uuid']

                # If resolved_action resolved role are not there, fill them with task action
                # and role
                events['task_action'] = events.resolved_action.fillna(events.task_action).astype(str)
                events['role'] = events.resolved_role.fillna(events.role).astype(str)
                # Only get valid role names into role name
                events["role"] = events["role"].apply(
                    lambda x: self.extract_role_name(x))

                # Rename columns to match the reality, they are just names, not normalized cols anymore
                events.rename(columns={
                    'task_action': 'module_name',
                    'role': 'role_name'
                }, inplace=True)

                events['collection_name'] = events['module_name'].apply(
                    self.extract_collection_name)

                # Final cleanup if some module names didn't connect, otherwise this will fail
                # to insert with not null constraint on module_name
                events = events[events['module_name'].notnull()]

                # Set a human readable values for missing role and collection name
                events['role_name'] = events['role_name'].fillna("No role used").astype(str)
                events['collection_name'] = events['collection_name'].fillna("No collection used").astype(str)

                ################################
                # Do the aggregation
                ################################
                events_group = events.groupby(
                    self.unique_index_columns(), dropna=False
                ).agg(
                    task_runs=('module_name', 'count'),
                    duration=('duration', "sum"))

                # Duration is null in older versions of Controller
                events_group['duration'] = events_group.duration.fillna(0)
                # Tweak types to match the table
                events_group = self.cast_dataframe(events_group, self.cast_types())

                ################################
                # Merge aggregations of multiple batches
                ################################
                if content_explorer_rollup is None:
                    content_explorer_rollup = events_group
                else:
                    # Multipart collection, merge the dataframes and sum counts
                    content_explorer_rollup = pd.merge(
                        content_explorer_rollup.loc[:, ],
                        events_group.loc[:, ],
                        on=self.unique_index_columns(),
                        how='outer')

                    content_explorer_rollup = self.summarize_merged_dataframes(
                        content_explorer_rollup, self.data_columns())

                    # Tweak types to match the table
                    content_explorer_rollup = self.cast_dataframe(
                        content_explorer_rollup, self.cast_types())

        if content_explorer_rollup is None:
            return None

        return content_explorer_rollup.reset_index()

    @staticmethod
    def collection_regexp():
        return r'^(\w+)\.(\w+)\.((\w+)(\.|$))+'

    @staticmethod
    def standalone_role_regexp():
        return r'^(\w+)\.(\w+)$'

    @staticmethod
    def extract_collection_name(x):
        if x is None:
            return None

        m = re.match(DataframeContentUsage.collection_regexp(), x)

        if m:
            return f"{m.groups()[0]}.{m.groups()[1]}"
        else:
            return None

    @staticmethod
    def extract_role_name(x):
        if x is None:
            return None

        collection_role = re.match(DataframeContentUsage.collection_regexp(), x)
        standalone_role = re.match(DataframeContentUsage.standalone_role_regexp(), x)

        if collection_role:
            return f"{collection_role.groups()[0]}.{collection_role.groups()[1]}.{collection_role.groups()[2]}"
        elif standalone_role:
            return f"{standalone_role.groups()[0]}.{standalone_role.groups()[1]}"
        else:
            return None

    @staticmethod
    def unique_index_columns():
        return ['host_name', 'module_name', 'collection_name', 'role_name', 'install_uuid', 'job_remote_id']

    @staticmethod
    def data_columns():
        return ["task_runs", "duration"]

    @staticmethod
    def cast_types():
        return {
            'duration': 'float64',
            'task_runs': 'int64'}
