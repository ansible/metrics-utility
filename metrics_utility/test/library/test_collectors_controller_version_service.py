from django.db import connection

from metrics_utility.library.collectors.controller.controller_version_service import controller_version_service
from metrics_utility.test.util import validate_dataframe


controller_version_service_lines = [
    'controller_version',
    '1.0',
    '23.5.0',
    '24.1.0',
    '24.2.0',
    '4.7.2',
]

controller_version_service_skip_columns = []


def test_controller_version_service_command():
    """Build and validate controller_version_service output from new library collector."""
    # Run the collector directly
    collector_instance = controller_version_service(db=connection)
    df = collector_instance.gather()

    assert df is not None, 'controller_version_service returned None'

    # Validate DataFrame content
    validate_dataframe(df, controller_version_service_lines, controller_version_service_skip_columns)

    # Verify that disabled instances are NOT included (24.3.0 should be filtered out)
    assert '24.3.0' not in df['controller_version'].values, (
        'Disabled instance version "24.3.0" should be filtered out by enabled=true filter, but it was found in the output'
    )

    # Verify that we have exactly 5 versions (all enabled instances from multiple SQL files)
    assert len(df) == 5, f'Expected 5 controller versions, got {len(df)}'

    # Verify versions are sorted ascending
    versions = df['controller_version'].tolist()
    assert versions == sorted(versions), 'Versions should be sorted in ascending order'
