from collections import defaultdict


class DedupCCSP:
    def __init__(self, dataframes, extra_params, experimental=False):
        self.dataframes = dataframes
        self.extra_params = extra_params
        self.experimental = experimental

    def run(self):
        new = {}
        for name, dataframe in self.dataframes.items():
            new[name] = dataframe.build_dataframe()

        dedup_info = new['main_host']
        if dedup_info is None or dedup_info.empty:
            return new

        if not self.experimental:
            del dedup_info['serials']
            return new

        # each host_name in dedup_info has a list of combined serials
        # convert to a mapping from any hostname with that serial to a canonical hostname
        mapping = self.df_to_mapping(dedup_info)
        del dedup_info['serials']

        for v in ['job_host_summary', 'main_jobevent']:
            new[v] = self.dataframes[v].dedup(new[v], mapping)
        # no dedup on data_collection_status

        return new

    def df_to_mapping(self, df):
        serial_to_hosts = defaultdict(set)
        serial_to_first = {}

        for _, row in df.iterrows():
            host = row['host_name']
            serials = row['serials']

            if serials:
                for serial in serials:
                    if serial:
                        serial_to_hosts[serial].add(host)
                        if serial not in serial_to_first:
                            serial_to_first[serial] = host

        host_to_canonical = {}
        for serial, hosts in serial_to_hosts.items():
            canonical = serial_to_first[serial]
            for host in hosts:
                host_to_canonical[host] = canonical

        return host_to_canonical
