def register(
    key,
    version,
    output_format='json',
    fnc_slicing=None,
):
    """
    A decorator used to register a function as a metric collector.

    Decorated functions should do the following based on output_format:
    - json: return JSON-serializable objects.
    - csv: write CSV data to a filename named 'key'

    @register('projects_by_scm_type', 1)
    def projects_by_scm_type():
        return {'git': 5, 'svn': 1}
    """

    def decorate(f):
        f.__insights_analytics_key__ = key
        f.__insights_analytics_version__ = version
        f.__insights_analytics_type__ = output_format  # 'csv' | 'json' (default)
        f.__insights_analytics_fnc_slicing__ = fnc_slicing

        return f

    return decorate
