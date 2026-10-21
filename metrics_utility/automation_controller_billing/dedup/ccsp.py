"""CCSP deduplication logic for billing dataframes."""

from collections import defaultdict
from collections.abc import Iterable


class _LazyDataFrameDict(dict):
    """Dict that builds DataFrames on first ``d[key]`` access.

    Eagerly-built entries are set with ``d[key] = df`` and behave like a
    normal dict.  Keys that have not been set yet are built from their
    engine on first access via ``__getitem__``.

    ``items()`` yields every entry (building lazily as needed) so that
    callers like the ``all_empty`` guard in ``build_report`` see all
    DataFrames, not only the eagerly-built ones.

    ``del d[key]`` removes both the built value and the engine so the
    memory can be reclaimed by the GC.
    """

    def __init__(self, engines):
        super().__init__()
        self._engines = dict(engines)

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            if key not in self._engines:
                raise
            df = self._engines[key].build_dataframe()
            super().__setitem__(key, df)
            return df

    def __contains__(self, key):
        return key in self._engines or super().__contains__(key)

    def items(self):
        """Yield all entries, building lazy ones on demand.

        Eagerly-built entries come first (via ``super().items()``),
        followed by any remaining engines that have not been accessed yet.
        """
        seen = set()
        for key, value in super().items():
            seen.add(key)
            yield key, value
        for key in self._engines:
            if key not in seen:
                yield key, self[key]

    def __delitem__(self, key):
        self._engines.pop(key, None)
        try:
            super().__delitem__(key)
        except KeyError:
            pass


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
        """Build dataframes and optionally apply serial-based deduplication.

        Returns a ``_LazyDataFrameDict`` so that callers can access each
        DataFrame via ``d[key]`` (triggering a build on first access) and
        ``del d[key]`` to release memory when the DataFrame is no longer
        needed.

        Returns:
            A dict-like mapping dataframe names to DataFrames.
        """
        if not self.experimental:
            return self._run_standard()
        return self._run_experimental()

    def _run_standard(self):
        """Non-experimental: build only ``job_host_summary`` eagerly.

        ``main_host``, ``main_jobevent``, and ``data_collection_status``
        stay lazy and are only built when the report builder accesses them.
        """
        result = _LazyDataFrameDict(self.dataframes)
        result['job_host_summary'] = self.dataframes['job_host_summary'].build_dataframe()
        return result

    def _run_experimental(self):
        """Experimental: build main_host, JHS, events eagerly for dedup."""
        result = _LazyDataFrameDict(self.dataframes)

        for name in ['main_host', 'job_host_summary', 'main_jobevent']:
            if name in self.dataframes:
                result[name] = self.dataframes[name].build_dataframe()

        dedup_info = result.get('main_host')
        if dedup_info is None or dedup_info.empty:
            return result

        dedup_info_copy = dedup_info.copy()
        mapping = self.df_to_mapping(dedup_info_copy)
        if 'serials' in dedup_info_copy.columns:
            del dedup_info_copy['serials']

        for name in ['job_host_summary', 'main_jobevent', 'main_host']:
            df = result.get(name)
            if df is not None and not df.empty:
                if name == 'job_host_summary':
                    result[name] = self.dataframes[name].dedup(df, mapping, scope_dataframe=dedup_info_copy)
                else:
                    result[name] = self.dataframes[name].dedup(df, mapping)

        return result

    def df_to_mapping(self, df):
        """Build a hostname->canonical-hostname mapping from serial-number grouping.

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

            if isinstance(serials, Iterable):
                for serial in serials:
                    if isinstance(serial, str) and serial:
                        serial_to_hosts[serial].add(host)
                        if serial not in serial_to_first:
                            serial_to_first[serial] = host

        host_to_canonical = {}
        for serial, hosts in serial_to_hosts.items():
            canonical = serial_to_first[serial]
            for host in hosts:
                host_to_canonical[host] = canonical

        return host_to_canonical
