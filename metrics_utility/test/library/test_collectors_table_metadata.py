from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.table_metadata import table_metadata


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_table_metadata_basic(mock_copy_pandas):
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = table_metadata(db=mock_db)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'main_jobevent' in query
    assert 'main_unifiedjob' in query
    assert 'main_jobhostsummary' in query
    assert 'estimated_row_count' in query
    assert isinstance(result, pd.DataFrame)
