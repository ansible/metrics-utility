import pandas as pd

class Dedup:
    def __init__(self, dataframe, extra_params):
        self.dataframe = dataframe
        self.extra_params = extra_params

    def run_deduplication(self):
        # Cleanup the null like values first
        self.dataframe['ansible_host_variable'] = self.dataframe['ansible_host_variable'].replace('', None)

        self.dataframe['ansible_product_serial'] = self.dataframe['ansible_product_serial'].replace('NA', None)
        self.dataframe['ansible_product_serial'] = self.dataframe['ansible_product_serial'].replace('', None)

        self.dataframe['ansible_machine_id'] = self.dataframe['ansible_machine_id'].replace('NA', None)
        self.dataframe['ansible_machine_id'] = self.dataframe['ansible_machine_id'].replace('', None)

        self.dataframe['ansible_host_variable'] = self.dataframe['ansible_host_variable'].replace('', None)
        deduped_list = []

        processed_dupes_index = set()
        for index, row in self.dataframe.iterrows():
            # Skip if index is in existing dupes_index
            if index in processed_dupes_index:
                continue

            # Start
            dupes = self.dataframe[self.dataframe['hostname']==row['hostname']]

            # We will do the search in iterations, to cover more indirect relationships
            iterations = int(self.extra_params['report_renewal_guidance_dedup_iterations'])
            for i in range(iterations):
                # Hostname dupe lookup
                dupes = self.find_dupes(dupes, 'hostname', dupes["hostname"])

                # Host variable dupe lookup
                dupes = self.find_dupes(dupes, 'ansible_host_variable', dupes["ansible_host_variable"])

                # Serial key dupes lookup
                dupes = self.find_dupes(dupes, 'ansible_product_serial', dupes["ansible_product_serial"])

                # ansible_machine_id key dupes lookup
                dupes = self.find_dupes(dupes, 'ansible_machine_id', dupes["ansible_machine_id"])

            processed_dupes_index.update(dupes['index'])

            # Take the last updated non deleted hostname with priority, to represent the
            # duplicate group
            latest_hostname = dupes.sort_values(by=['deleted', 'last_automation'],
                                                ascending=[True, False])['hostname'].iloc[0]
            deduped_list.append({
                'hostname': latest_hostname,
                'hostmetric_record_count': dupes['hostname'].nunique(),
                'hostmetric_record_count_active': dupes[~dupes["deleted"]==True]['hostname'].nunique(),
                'hostmetric_record_count_deleted': dupes[dupes["deleted"]==True]['hostname'].nunique(),
                'hostnames': self.stringify(set(dupes['hostname'])),
                'ansible_host_variables': self.stringify(set(dupes['ansible_host_variable'])),
                'ansible_product_serials': self.stringify(set(dupes['ansible_product_serial'])),
                'ansible_machine_ids': self.stringify(set(dupes['ansible_machine_id'])),
                'deleted': min(dupes['deleted']), # if there was at least one false, it's not deleted
                'first_automation':  min(dupes['first_automation']),
                'last_automation': max(dupes['last_automation']),
                'automated_counter': sum(dupes['automated_counter']),
                'deleted_counter': sum(dupes['deleted_counter']),
                'last_deleted':  max(dupes['last_deleted']),
            })

        return pd.DataFrame(deduped_list)

    def stringify(self, value):
        return ", ".join([v for v in list(value) if v is not None])

    def find_dupes(self, dupes, key, values):
        next_iteration_dupes = self.dataframe[self.dataframe[key].notnull() & self.dataframe[key].isin(values) ]
        dupes = pd.concat([dupes,next_iteration_dupes]).drop_duplicates().reset_index(drop=True)

        return dupes


