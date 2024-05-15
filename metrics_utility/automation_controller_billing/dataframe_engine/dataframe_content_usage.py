import logging
import datetime
import pandas as pd
import re
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

#######################################
# Code for build the dataframe report and pushing it back to S3
######################################

def granularity_cast(date, granularity):
    if granularity == "monthly":
        return date.replace(day=1)
    elif granularity == "yearly":
        return date.replace(month=1, day=1)
    else:
        return date


def list_dates(start_date, end_date, granularity):
    # Given start date and end date, return list of dates in the given granularity
    # e.g. for daily it is a list of days withing the interval, for monthly it is a
    # list of months withing the interval, etc.
    start_date = granularity_cast(start_date, granularity)
    end_date = granularity_cast(end_date, granularity)

    dates_arr = []
    while start_date < end_date:
        dates_arr.append(start_date)

        if granularity == "monthly":
            start_date += relativedelta(months=+1)
        elif granularity == "yearly":
            start_date += relativedelta(years=+1)
        else:
            start_date += datetime.timedelta(days=1)

    dates_arr.append(end_date)

    return dates_arr

class DataframeContentUsage():
    LOG_PREFIX = "[AAPBillingReport] "

    def __init__(self, extractor, month, extra_params):
        self.logger = logger

        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params

        self.price_per_node = extra_params['price_per_node']

    @staticmethod
    def get_logger():
        return logging.getLogger(__name__)


    def build_dataframe(self):
        # A daily rollup dataframe
        billing_data_monthly_rollup = None

        # Get list of days of the specified month for the monthly report
        beginning_of_the_month = self.month.replace(day=1)
        end_of_the_month = beginning_of_the_month + relativedelta(months=1) - relativedelta(days=1)
        dates_list = list_dates(start_date=beginning_of_the_month,
                                end_date=end_of_the_month,
                                granularity="daily")

        granularity = "daily"
        table_name = "job_events_fk"

        # A monthly rollup dataframe
        content_explorer_rollup = None

        for date in dates_list:
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
                # events['role_collection_name'] = events['role_name'].apply(
                #     self.extract_collection_name)

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

        # order the columns right
        ccsp_report = content_explorer_rollup.reset_index()

        ccsp_report = ccsp_report.reindex(columns=[
            'host_name', 'module_name', 'collection_name', 'role_name', 'install_uuid', 'job_remote_id', 'task_runs', 'duration', ])
        return ccsp_report

    def cast_dataframe(self, df, types):
        levels = []
        for index, level in enumerate(df.index.levels):
            casted_level = df.index.levels[index].astype(object)
            levels.append(casted_level)

        df.index = df.index.set_levels(levels)

        return df.astype(types)

    def summarize_merged_dataframes(self, df, columns):
        for col in columns:
            df[col] = df[[f"{col}_x", f"{col}_y"]].sum(axis=1)
            del df[f"{col}_x"]
            del df[f"{col}_y"]
        return df

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
