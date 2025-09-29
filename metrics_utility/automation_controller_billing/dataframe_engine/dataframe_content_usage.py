import re

import pandas as pd

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base


# dataframe for main_jobevent
class DataframeContentUsage(Base):
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

                # fill job_failed to false if empty and if column is not present and if present and empty = false
                # the same for job_created, job_started, job_finished - they should be not a timestamp
                if 'job_failed' not in events.columns:
                    events['job_failed'] = False
                else:
                    events['job_failed'] = events['job_failed'].fillna(False)

                if 'job_created' not in events.columns:
                    events['job_created'] = pd.NaT
                else:
                    events['job_created'] = events['job_created'].fillna(pd.NaT)

                if 'job_started' not in events.columns:
                    events['job_started'] = pd.NaT
                else:
                    events['job_started'] = events['job_started'].fillna(pd.NaT)

                if 'job_finished' not in events.columns:
                    events['job_finished'] = pd.NaT
                else:
                    events['job_finished'] = events['job_finished'].fillna(pd.NaT)

                events['install_uuid'] = data['config']['install_uuid']

                # If resolved_action resolved role are not there, fill them with task action
                # and role
                events['task_action'] = events.resolved_action.fillna(events.task_action).astype(str)
                events['role'] = events.resolved_role.fillna(events.role).astype(str)
                # Only get valid role names into role name
                events['role'] = events['role'].apply(lambda x: self.extract_role_name(x))

                # Rename columns to match the reality, they are just names, not normalized cols anymore
                events.rename(columns={'task_action': 'module_name', 'role': 'role_name'}, inplace=True)

                events['collection_name'] = events['module_name'].apply(self.extract_collection_name)

                # Final cleanup if some module names didn't connect, otherwise this will fail
                # to insert with not null constraint on module_name
                events = events[events['module_name'].notnull()]

                # Set a human readable values for missing role and collection name
                events['role_name'] = events['role_name'].fillna('No role used').astype(str)
                events['collection_name'] = events['collection_name'].fillna('No collection used').astype(str)

                ################################
                # Do the aggregation
                ################################

                events_group = self.group(events)

                ################################
                # Merge aggregations of multiple batches
                ################################

                content_explorer_rollup = self.merge(content_explorer_rollup, events_group)

        if content_explorer_rollup is None or content_explorer_rollup.empty:
            return self.empty()

        return content_explorer_rollup.reset_index()

    # Do the aggregation
    def group(self, dataframe):
        group = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('module_name', 'count'),
            duration=('duration', 'sum'),
            job_failed=('job_failed', 'first'),
            job_created=('job_created', 'first'),
            job_started=('job_started', 'first'),
            job_finished=('job_finished', 'first'),
            playbook=('playbook', 'first'),
        )

        # Duration is null in older versions of Controller
        group['duration'] = group.duration.fillna(0)

        # Tweak types to match the table
        return self.cast_dataframe(group, self.cast_types())

    # Merge pre-aggregated
    def regroup(self, dataframe):
        return dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('task_runs', 'sum'),
            duration=('duration', 'sum'),
            job_failed=('job_failed', 'first'),
            job_created=('job_created', 'first'),
            job_started=('job_started', 'first'),
            job_finished=('job_finished', 'first'),
            playbook=('playbook', 'first'),
        )

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
            return f'{m.groups()[0]}.{m.groups()[1]}'
        else:
            return None

    @staticmethod
    def extract_role_name(x):
        if x is None:
            return None

        collection_role = re.match(DataframeContentUsage.collection_regexp(), x)
        standalone_role = re.match(DataframeContentUsage.standalone_role_regexp(), x)

        if collection_role:
            return f'{collection_role.groups()[0]}.{collection_role.groups()[1]}.{collection_role.groups()[2]}'
        elif standalone_role:
            return f'{standalone_role.groups()[0]}.{standalone_role.groups()[1]}'
        else:
            return None

    @staticmethod
    def unique_index_columns():
        return ['host_name', 'module_name', 'collection_name', 'role_name', 'install_uuid', 'job_remote_id']

    @staticmethod
    def data_columns():
        return ['task_runs', 'duration']

    @staticmethod
    def cast_types():
        return {'duration': 'float64', 'task_runs': 'int64'}

    @staticmethod
    def operations():
        return {}
