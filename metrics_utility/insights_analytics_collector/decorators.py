def register(
    key,
    version,
    description=None,
    format="json",
    config=False,
    fnc_slicing=None,
    shipping_group="default",
    full_sync_interval_days=None,
):
    """
    A decorator used to register a function as a metric collector.

    Decorated functions should do the following based on format:
    - json: return JSON-serializable objects.
    - csv: write CSV data to a filename named 'key'

    :param output_type - 'data' or 'file_paths'

    @register('projects_by_scm_type', 1)
    def projects_by_scm_type():
        return {'git': 5, 'svn': 1}
    """

    def decorate(f):
        f.__insights_analytics_key__ = key
        f.__insights_analytics_version__ = version
        f.__insights_analytics_description__ = description
        f.__insights_analytics_type__ = format  # CSV/JSON
        f.__insights_analytics_config__ = config  # config
        f.__insights_analytics_fnc_slicing__ = fnc_slicing
        f.__insights_analytics_shipping_group__ = shipping_group
        f.__insights_analytics_full_sync_interval_days__ = full_sync_interval_days

        return f

    return decorate


def slicing(default=False):
    def decorate(f):
        f.__insights_analytics_slicing_name__ = f.__name__
        f.__insights_analytics_slicing_default__ = default
        return f

    return decorate
