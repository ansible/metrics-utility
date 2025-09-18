class Event_Anonymized_Rollups:
    """
    Event rollups operate over main_jobevent_service collector data
    """

    def base(dataframe):
        """
        This function will create first level aggregation of the event dataframe, the result is json

        Aggregations are:
            - Avg number of modules used in a playbook
            - Failure/Success rate of modules
            - Modules Used to Automate
            - Total number of modules automated
        """
        # add module column into the dataframe based on dataframe_content_usage.py approach
        dataframe['task_action'] = dataframe.resolved_action.fillna(dataframe.task_action).astype(str)
        dataframe.rename(columns={'task_action': 'module_name'}, inplace=True)

        # ------------------------------------------------------------
        # avg number of modules used in a playbook
        avg_modules = dataframe.groupby('playbook')['module_name'].nunique().mean()

        # ------------------------------------------------------------
        # failure/success rate of modules
        # Define event categories
        ok_events = {'runner_on_ok', 'runner_on_changed', 'runner_on_async_ok'}
        failed_events = {'runner_on_failed', 'runner_on_unreachable', 'runner_on_async_failed'}

        # Create new columns for classification
        dataframe['is_success'] = dataframe['event'].isin(ok_events)
        dataframe['is_failure'] = dataframe['event'].isin(failed_events)

        module_stats = dataframe.groupby('module_name').agg(failures=('is_failure', 'sum'), successes=('is_success', 'sum'))

        # total = failures + successes
        module_stats['total_runs'] = module_stats['failures'] + module_stats['successes']

        # failure rate = failures / total
        module_stats['failure_rate'] = module_stats['failures'] / module_stats['total_runs']

        # ------------------------------------------------------------
        # modules used to automate (sum of all distinct modules used)
        modules_used = dataframe['module_name'].nunique()

        # the dataframe should be converted to json
        json_data = {
            'avg_modules_per_playbook': avg_modules,
            'modules_used': modules_used,
            'modules_failure_rate': module_stats.reset_index().to_dict(orient='records'),
        }

        return json_data
