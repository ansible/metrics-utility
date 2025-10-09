import re

from metrics_utility.library.dataframes.base_traditional import BaseTraditional


COLLECTION_REGEXP = r'^(\w+)\.(\w+)\.((\w+)(\.|$))+'


class DataframeMainJobevent(BaseTraditional):
    TARBALL_NAMES = ['main_jobevent.csv', 'config.json']

    def prepare(self, tup):
        (events, config) = tup

        # Filter non relevant rows
        events = events[events['task_action'].notnull()]
        events = events[events['host_name'].notnull()]

        # If the dataframe is empty, skip additional processing
        if events.empty:
            return None

        events['install_uuid'] = config['install_uuid']

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

        return events

    # Do the aggregation
    def group(self, dataframe):
        group = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('module_name', 'count'),
            duration=('duration', 'sum'),
        )

        # Duration is null in older versions of Controller
        group['duration'] = group.duration.fillna(0)

        # Tweak types to match the table
        return self.cast_dataframe(group)

    # Merge pre-aggregated
    def regroup(self, dataframe):
        return dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('task_runs', 'sum'),
            duration=('duration', 'sum'),
        )

    @staticmethod
    def extract_collection_name(x):
        if x is None:
            return None

        if m := re.match(COLLECTION_REGEXP, x):
            return f'{m.groups()[0]}.{m.groups()[1]}'

        return None

    @staticmethod
    def extract_role_name(x):
        if x is None:
            return None

        if collection_role := re.match(COLLECTION_REGEXP, x):
            return f'{collection_role.groups()[0]}.{collection_role.groups()[1]}.{collection_role.groups()[2]}'

        if standalone_role := re.match(r'^(\w+)\.(\w+)$', x):
            return f'{standalone_role.groups()[0]}.{standalone_role.groups()[1]}'

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
