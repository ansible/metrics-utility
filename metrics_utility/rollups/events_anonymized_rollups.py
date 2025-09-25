class Event_Anonymized_Rollups:
    """
    Event rollups operate over main_jobevent_service collector data
    """

    @staticmethod
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

        # group by playbook, module, include job_failed column
        aggregations_by_playbook_module = (
            dataframe.groupby(['playbook', 'module_name'])
            .agg(
                job_failed=('job_failed', 'max'),
                job_total=('job_id', 'nunique'),
            )
            .reset_index()  # <-- make keys real columns
        )

        # modules used to automate
        list_of_modules_used_to_automate = aggregations_by_playbook_module['module_name'].unique().tolist()
        total_modules_used_to_automate = len(list_of_modules_used_to_automate)

        # return as object that can be converted to json
        return {
            'aggregations_by_playbook_module': aggregations_by_playbook_module.to_dict(orient='records'),
            'list_of_modules_used_to_automate': list_of_modules_used_to_automate,
            'total_modules_used_to_automate': total_modules_used_to_automate,
        }
