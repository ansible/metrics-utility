"""CCSP deduplication logic for billing dataframes."""

from collections import defaultdict


class DedupCCSP:
    """CCSP host deduplication engine.

    In standard mode, returns the built dataframes unchanged (no deduplication).
    In experimental mode, uses hardware serial numbers (``ansible_product_serial``
    combined with ``ansible_machine_id``) to detect duplicate host records and
    map them to a single canonical hostname.
    """

    def __init__(self, dataframes, extra_params, experimental=False):
        """Initialise the CCSP deduplicator.

        Args:
            dataframes: Dict mapping dataframe names to
                :class:`~metrics_utility.automation_controller_billing.dataframe_engine.base.Base`
                engine instances.
            extra_params: Dict of configuration parameters.
            experimental: When True, use serial-based deduplication.
        """
        self.dataframes = dataframes
        self.extra_params = extra_params
        self.experimental = experimental

    def run(self):
        """Build all dataframes and optionally apply serial-based deduplication.

        Returns:
            Dict mapping dataframe names to the (possibly deduplicated) DataFrames.
        """
        new = {}
        for name, dataframe in self.dataframes.items():
            new[name] = dataframe.build_dataframe()

        dedup_info = new['main_host']
        if dedup_info is None or dedup_info.empty:
            return new

        if not self.experimental:
            if 'serials' in dedup_info.columns:
                del dedup_info['serials']
            return new

        # each host_name in dedup_info has a list of combined serials
        # convert to a mapping from any hostname with that serial to a canonical hostname
        # Make a copy to avoid modifying the original dataframe
        dedup_info_copy = dedup_info.copy()
        mapping = self.df_to_mapping(dedup_info_copy)
        if 'serials' in dedup_info_copy.columns:
            del dedup_info_copy['serials']

        for v in ['job_host_summary', 'main_jobevent', 'main_host']:
            if v in new and new[v] is not None and not new[v].empty:
                # Pass main_host dataframe to job_host_summary for canonical facts enrichment
                if v == 'job_host_summary':
                    new[v] = self.dataframes[v].dedup(new[v], mapping, scope_dataframe=dedup_info_copy)
                else:
                    new[v] = self.dataframes[v].dedup(new[v], mapping)
        # no dedup on data_collection_status

        return new

    def df_to_mapping(self, df):
        """Build a hostname→canonical-hostname mapping from serial-number grouping.

        For each unique hardware serial, the first-seen hostname is chosen as the
        canonical representative.  All other hostnames sharing a serial are mapped
        to that canonical hostname.

        Args:
            df: The main_host DataFrame containing ``host_name`` and ``serials``
                columns.

        Returns:
            Dict mapping non-canonical hostnames to their canonical hostname.
        """
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
