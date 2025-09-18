class Jobs_Anonymized_Rollups:
    """
    Collector - unified_jobs collector data
    """

    @staticmethod
    def base(dataframe):
        """
        This function will create first level aggregation of the job dataframe, the result is json

        Number of jobs executed 
        Number of jobs failed
        Number of jobs that succeeded 

        Job duration average in seconds - by template
        Job duration maximum seconds- by template
        Job duration minimum seconds - by template
        Job total seconds by template

        Active number of customer by Controller Version 
        Active number of Customers
        Active number of Customers (anonymized? - the same as above?)
        Number of templates executed by company
        """

        # create view from dataframe where finished is not null and started is not null
        dataframe = dataframe[dataframe['finished'].notna() & dataframe['started'].notna()]

        # compute job duration in seconds
        dataframe['duration'] = (dataframe['finished'] - dataframe['started']) / 1000

        # number of jobs executed, simply count of all rows
        number_of_jobs_executed = len(dataframe)

        # number of jobs failed, simply count of all rows where failed == true
        # sum failed column
        number_of_jobs_failed = dataframe['failed'].sum()

        # number of jobs succeeded should be total number of jobs executed - number of jobs failed
        number_of_jobs_succeeded = number_of_jobs_executed - number_of_jobs_failed

        # job duration average in seconds - by template
        # group by job_template_name and calculate the average of duration column
        job_duration_average_in_seconds_by_template = dataframe.groupby('job_template_name')['duration'].mean()

        # job duration maximum seconds - by template
        job_duration_maximum_seconds_by_template = dataframe.groupby('job_template_name')['duration'].max()

        # job duration minimum seconds - by template
        job_duration_minimum_seconds_by_template = dataframe.groupby('job_template_name')['duration'].min()

        # job total seconds by template
        job_total_seconds_by_template = dataframe.groupby('job_template_name')['duration'].sum()

        # active number of clusters - distinct of controller_node column
        active_number_of_customers = dataframe['controller_node'].nunique()

        # active number of clusters by controller version - column ansible_version
        # the same as above but group by ansible_version
        active_number_of_clusters_by_controller_version = dataframe.groupby('ansible_version')['controller_node'].nunique()

        # Number of templates executed by company
        # column job_template_name, group by controller_node, count distinct job_template_name
        number_of_templates_executed_by_company = dataframe.groupby('controller_node')['job_template_name'].nunique()
        
        return {
            'number_of_jobs_executed': number_of_jobs_executed,
            'number_of_jobs_failed': int(number_of_jobs_failed),
            'number_of_jobs_succeeded': int(number_of_jobs_succeeded),
            'job_duration_average_in_seconds_by_template': job_duration_average_in_seconds_by_template.to_dict(),
            'job_duration_maximum_seconds_by_template': job_duration_maximum_seconds_by_template.to_dict(),
            'job_duration_minimum_seconds_by_template': job_duration_minimum_seconds_by_template.to_dict(),
            'job_total_seconds_by_template': job_total_seconds_by_template.to_dict(),
            'active_number_of_customers': int(active_number_of_customers),
            'active_number_of_clusters_by_controller_version': active_number_of_clusters_by_controller_version.to_dict(),
            'number_of_templates_executed_by_company': number_of_templates_executed_by_company.to_dict(),
        }