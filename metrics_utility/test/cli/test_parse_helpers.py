from datetime import UTC
from unittest.mock import patch

import pytest

from metrics_utility.exceptions import MetricsError
from metrics_utility.management.validation import parse_date_param
from metrics_utility.test.util import utcdt


def test_parse_date_param():
    assert parse_date_param(None) is None
    assert parse_date_param('1d') is not None
    assert parse_date_param('1days') is not None
    assert parse_date_param('2mo') is not None
    assert parse_date_param('2months') is not None
    assert parse_date_param('3m') is not None
    assert parse_date_param('3minutes') is not None
    assert parse_date_param('2024-01-01') == utcdt('2024-01-01')
    assert parse_date_param('2024-01-01T12:31:00Z') == utcdt('2024-01-01T12:31:00')

    with patch('metrics_utility.management.validation.now') as mock:
        mock.return_value = utcdt('2024-02-29T13:59:00')

        assert parse_date_param('1d') == utcdt('2024-02-29')
        assert parse_date_param('2mo') == utcdt('2023-12-29')
        assert parse_date_param('3m') == utcdt('2024-02-29T13:56:00')

    # ensure timezone (including bare zoneless datetime)
    assert parse_date_param('2mo').tzinfo == UTC
    assert parse_date_param('3m').tzinfo == UTC
    assert parse_date_param('2024-01-01').tzinfo == UTC
    assert parse_date_param('2024-01-01T12:00:00').tzinfo == UTC

    # bare number invalid
    with pytest.raises(MetricsError):
        parse_date_param('3')

    # unknown suffix
    with pytest.raises(MetricsError):
        parse_date_param('4x')
