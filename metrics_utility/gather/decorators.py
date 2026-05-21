def register(
    key,
    version,
    output_format='json',
    slicing=None,
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
        f._register_key_ = key
        f._register_version_ = version
        f._register_output_format_ = output_format
        f._register_slicing_ = slicing

        return f

    return decorate
