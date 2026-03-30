from ..util import DataframeOutput, collector


@collector
def feature_flags_service(*, db=None, output=DataframeOutput()):
    """
    Collect enabled feature flags from the controller.

    Returns a list of feature flags that are currently enabled, sourced from
    the dab_feature_flags_aapflag table. A flag is considered enabled when its
    condition is 'boolean' and its value is 'True'.
    """
    query = """
        SELECT
            name,
            condition,
            value,
            description,
            support_level,
            toggle_type,
            visibility
        FROM dab_feature_flags_aapflag
        WHERE condition = 'boolean'
            AND value = 'True'
        ORDER BY name ASC
    """

    return output.sql(db, query)
