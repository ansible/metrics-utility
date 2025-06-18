class DedupCCSP:
    def __init__(self, dataframes, extra_params):
        self.dataframes = dataframes
        self.extra_params = extra_params

    def run_deduplication(self):
        # FIXME - ccsp: jobhost, content, inventory ; ccspv2: jobhost, content, inventory, collection status
        return self.dataframes[0].copy()

    @staticmethod
    def do(df):
        # Store the original host name for mapping purposes
        df['original_host_name'] = df['host_name']
        if 'ansible_host_variable' in df.columns:
            # Replace missing ansible_host_variable with host name
            df['ansible_host_variable'] = df.ansible_host_variable.fillna(df['host_name'])
            # And use the new ansible_host_variable instead of host_name, since
            # what is in ansible_host_variable should be the actual host we count
            df['host_name'] = df['ansible_host_variable']
