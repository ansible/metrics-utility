import json

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from metrics_utility.automation_controller_billing.collectors import get_hour_boundaries, total_workers_vcpu
from metrics_utility.exceptions import MetricsException, MissingRequiredEnvVar
from metrics_utility.test.util import temporary_env


class TestTotalWorkersVcpu:
    """Test suite for the total_workers_vcpu collector function."""

    def test_returns_none_when_not_in_optional_collectors(self):
        """Test that the function returns None when total_workers_vcpu is not in optional collectors."""
        with patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get:
            mock_get.return_value = []
            result = total_workers_vcpu(None, None, None)
            assert result is None

    def test_raises_missing_required_env_var_when_cluster_name_not_set(self):
        """Test that the function raises MissingRequiredEnvVar when METRICS_UTILITY_CLUSTER_NAME is not set."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.logger') as mock_logger,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': None}):
                with pytest.raises(MissingRequiredEnvVar) as exc_info:
                    total_workers_vcpu(None, None, None)

                assert 'environment variable METRICS_UTILITY_CLUSTER_NAME is not set' in str(exc_info.value)
                # Check that error was called with the log prefix format
                mock_logger.error.assert_called_once()
                call_args = mock_logger.error.call_args[0]
                assert '%s, environment variable METRICS_UTILITY_CLUSTER_NAME is not set' == call_args[0]
                assert '[METRICS_UTILITY_VCPU]:' in call_args[1]

    def test_returns_hardcoded_value_when_usage_based_billing_disabled(self):
        """Test that the function returns hardcoded value when METRICS_UTILITY_USAGE_BASED_METERING_ENABLED is not set or false (default behavior)."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.logger_info_level') as mock_logger_info,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Test when not set (default behavior)
            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster'}):
                result = total_workers_vcpu(None, None, None)
                assert result['cluster_name'] == 'test-cluster'
                assert result['total_workers_vcpu'] == 1
                assert 'timestamp' in result

                # Verify the logged JSON contains usage_based_billing_enabled = False and all required fields
                # The logger now logs twice: once for info, once for data
                assert mock_logger_info.info.call_count == 2
                # First call is info with log prefix
                first_call_args = mock_logger_info.info.call_args_list[0][0]
                assert first_call_args[0] == '%s info: %s'
                logged_json = json.loads(first_call_args[2])
                assert not logged_json['usage_based_billing_enabled']
                assert logged_json['total_workers_vcpu'] == 1
                assert 'cluster_name' in logged_json
                assert 'collection_timestamp' in logged_json
                assert 'start_timestamp' in logged_json
                assert 'end_timestamp' in logged_json

            # Test when explicitly set to false
            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'false'}):
                result = total_workers_vcpu(None, None, None)
                assert result['cluster_name'] == 'test-cluster'
                assert result['total_workers_vcpu'] == 1

                # Verify the logged JSON contains usage_based_billing_enabled = False
                first_call_args = mock_logger_info.info.call_args_list[0][0]
                logged_json = json.loads(first_call_args[2])
                assert not logged_json['usage_based_billing_enabled']

    def test_usage_based_billing_enabled_case_insensitive(self):
        """Test that METRICS_UTILITY_USAGE_BASED_METERING_ENABLED is case insensitive."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.PrometheusClient') as mock_prom_client_class,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Mock PrometheusClient
            mock_prom_client = MagicMock()
            mock_prom_client.get_current_value.return_value = 8.0
            mock_prom_client_class.return_value = mock_prom_client

            # Test TRUE (case insensitive)
            with temporary_env(
                {
                    'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
                    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'TRUE',
                    'METRICS_UTILITY_PROMETHEUS_URL': 'https://prometheus.example.com:9090',
                }
            ):
                result = total_workers_vcpu(None, None, None)
                assert result['cluster_name'] == 'test-cluster'
                assert result['total_workers_vcpu'] == 8

    def test_uses_default_prometheus_url_when_not_set(self):
        """Test that the function uses default Prometheus URL when METRICS_UTILITY_PROMETHEUS_URL is not set."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.PrometheusClient') as mock_prom_client_class,
            patch('metrics_utility.automation_controller_billing.collectors.get_total_workers_cpu') as mock_get_cpu,
            patch('metrics_utility.automation_controller_billing.collectors.get_cpu_timeline') as mock_get_timeline,
            patch('metrics_utility.automation_controller_billing.collectors.logger') as mock_logger,
            patch('metrics_utility.automation_controller_billing.collectors.logger_info_level') as mock_logger_info,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Mock PrometheusClient
            mock_prom_client = MagicMock()
            mock_prom_client_class.return_value = mock_prom_client

            # Mock helper functions
            mock_get_cpu.return_value = (16.0, 'max_over_time(sum(machine_cpu_cores)[59m59s:5m] @ 1234567890)')
            mock_get_timeline.return_value = [
                {'timestamp': '2023-01-01T10:00:00+00:00', 'cpu_sum': 16.0},
                {'timestamp': '2023-01-01T10:05:00+00:00', 'cpu_sum': 16.0},
            ]

            with temporary_env(
                {
                    'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
                    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true',
                    'METRICS_UTILITY_PROMETHEUS_URL': None,
                }
            ):
                result = total_workers_vcpu(None, None, None)

                # Verify it uses the default URL
                mock_prom_client_class.assert_called_once_with(url='https://prometheus-k8s.openshift-monitoring.svc.cluster.local:9091')

                # Check that the info message was logged with format string
                info_calls = [call[0] for call in mock_logger.info.call_args_list]
                assert any('%s environment variable METRICS_UTILITY_PROMETHEUS_URL is not set' in str(call) for call in info_calls), (
                    'Expected info log with format string for PROMETHEUS_URL not found'
                )

                # Verify the debug message was logged with format string
                debug_call_args = mock_logger.debug.call_args[0]
                assert debug_call_args[0] == '%s total_workers_vcpu: %s'
                assert debug_call_args[2] == 16.0

                # Verify helper functions were called
                mock_get_cpu.assert_called_once()
                mock_get_timeline.assert_called_once()

                # Verify result is returned successfully
                assert result['cluster_name'] == 'test-cluster'
                assert result['total_workers_vcpu'] == 16

                # Verify that the logged info contains all expected fields
                # The logger now logs twice: once for info, once for data
                assert mock_logger_info.info.call_count == 2
                first_call_args = mock_logger_info.info.call_args_list[0][0]
                assert first_call_args[0] == '%s info: %s'
                logged_json = json.loads(first_call_args[2])
                assert 'cluster_name' in logged_json
                assert 'collection_timestamp' in logged_json
                assert 'start_timestamp' in logged_json
                assert 'end_timestamp' in logged_json
                assert 'usage_based_billing_enabled' in logged_json
                assert 'promql_query' in logged_json
                assert 'timeline' in logged_json
                assert 'total_workers_vcpu' in logged_json
                assert logged_json['usage_based_billing_enabled'] is True
                assert 'max_over_time(sum(machine_cpu_cores)[59m59s:5m]' in logged_json['promql_query']

    def test_prometheus_client_creation_failure_raises_metrics_exception(self):
        """Test that PrometheusClient creation failure raises MetricsException."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.PrometheusClient') as mock_prom_client_class,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_prom_client_class.side_effect = Exception('Failed to create Prometheus client')

            with temporary_env(
                {
                    'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
                    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true',
                    'METRICS_UTILITY_PROMETHEUS_URL': 'https://prometheus.example.com:9090',
                }
            ):
                with pytest.raises(MetricsException) as exc_info:
                    total_workers_vcpu(None, None, None)

                assert 'Can not create a prometheus api client ERROR:' in str(exc_info.value)

    def test_prometheus_query_failure_raises_metrics_exception(self):
        """Test that Prometheus query failure raises MetricsException."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.PrometheusClient') as mock_prom_client_class,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Mock PrometheusClient that raises exception on query
            mock_prom_client = MagicMock()
            mock_prom_client.get_current_value.side_effect = Exception('Prometheus query failed')
            mock_prom_client_class.return_value = mock_prom_client

            with temporary_env(
                {
                    'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
                    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true',
                    'METRICS_UTILITY_PROMETHEUS_URL': 'https://prometheus.example.com:9090',
                }
            ):
                with pytest.raises(MetricsException) as exc_info:
                    total_workers_vcpu(None, None, None)

                assert 'Unexpected error when retrieving nodes:' in str(exc_info.value)

    def test_prometheus_query_returns_none_raises_metrics_exception(self):
        """Test that the function raises MetricsException when Prometheus query returns None (no data available)."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.PrometheusClient') as mock_prom_client_class,
            patch('metrics_utility.automation_controller_billing.collectors.logger') as mock_logger,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Mock PrometheusClient that returns None (no data available)
            mock_prom_client = MagicMock()
            mock_prom_client.get_current_value.return_value = None
            mock_prom_client_class.return_value = mock_prom_client

            with temporary_env(
                {
                    'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
                    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true',
                    'METRICS_UTILITY_PROMETHEUS_URL': 'https://prometheus.example.com:9090',
                }
            ):
                with pytest.raises(MetricsException) as exc_info:
                    total_workers_vcpu(None, None, None)

                # Verify the exception message
                assert 'No data availble yet, the cluster is probably running for less than an hour' in str(exc_info.value)

                # Verify the debug and warning messages were logged with format string
                debug_call_args = mock_logger.debug.call_args[0]
                assert debug_call_args[0] == '%s total_workers_vcpu: %s'
                assert debug_call_args[2] is None

                warning_call_args = mock_logger.warning.call_args[0]
                assert warning_call_args[0] == '%s No data availble yet, the cluster is probably running for less than an hour'
                assert '[METRICS_UTILITY_VCPU]:' in warning_call_args[1]

    def test_successful_prometheus_query_with_vcpu_calculation(self):
        """Test successful Prometheus query with vCPU calculation."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.PrometheusClient') as mock_prom_client_class,
            patch('metrics_utility.automation_controller_billing.collectors.logger_info_level') as mock_logger_info,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Mock PrometheusClient
            mock_prom_client = MagicMock()
            mock_prom_client.get_current_value.return_value = 24.5  # Float value from Prometheus
            mock_prom_client_class.return_value = mock_prom_client

            with temporary_env(
                {
                    'METRICS_UTILITY_CLUSTER_NAME': 'my-cluster',
                    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true',
                    'METRICS_UTILITY_PROMETHEUS_URL': 'https://prometheus.example.com:9090',
                }
            ):
                result = total_workers_vcpu(None, None, None)

                assert result['cluster_name'] == 'my-cluster'
                assert result['total_workers_vcpu'] == 24  # Should be converted to int
                assert 'timestamp' in result

                # Verify PrometheusClient was created with correct parameters
                mock_prom_client_class.assert_called_once_with(url='https://prometheus.example.com:9090')

                # Verify the query was called with correct PromQL
                mock_prom_client.get_current_value.assert_called_once()
                query_call = mock_prom_client.get_current_value.call_args[0][0]
                assert 'max_over_time(sum(machine_cpu_cores)[59m59s:5m]' in query_call
                assert '@' in query_call  # Should contain timestamp

                # Verify logging - logger now logs twice: once for info, once for data
                assert mock_logger_info.info.call_count == 2
                first_call_args = mock_logger_info.info.call_args_list[0][0]
                assert first_call_args[0] == '%s info: %s'
                logged_json = json.loads(first_call_args[2])
                assert logged_json['cluster_name'] == 'my-cluster'
                assert logged_json['total_workers_vcpu'] == 24
                assert logged_json['usage_based_billing_enabled']

    def test_prometheus_client_initialized_correctly(self):
        """Test that PrometheusClient is initialized correctly."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.PrometheusClient') as mock_prom_client_class,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Mock PrometheusClient
            mock_prom_client = MagicMock()
            mock_prom_client.get_current_value.return_value = 16.0
            mock_prom_client_class.return_value = mock_prom_client

            with temporary_env(
                {
                    'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
                    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true',
                    'METRICS_UTILITY_PROMETHEUS_URL': 'https://prometheus.example.com:9090',
                }
            ):
                total_workers_vcpu(None, None, None)

                # Verify PrometheusClient was created correctly
                mock_prom_client_class.assert_called_once_with(url='https://prometheus.example.com:9090')

    def test_prometheus_query_uses_correct_promql_with_hour_boundaries(self):
        """Test that the Prometheus query uses correct PromQL with hour boundaries."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.PrometheusClient') as mock_prom_client_class,
            patch('metrics_utility.automation_controller_billing.collectors.datetime') as mock_datetime,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Mock datetime to return fixed timestamp
            mock_now = datetime(2023, 12, 25, 15, 30, 45, tzinfo=timezone.utc)
            mock_datetime.now.return_value = mock_now
            mock_datetime.timezone = timezone
            mock_datetime.fromtimestamp = datetime.fromtimestamp  # Use real fromtimestamp

            # Mock PrometheusClient
            mock_prom_client = MagicMock()
            mock_prom_client.get_current_value.return_value = 12.0
            mock_prom_client_class.return_value = mock_prom_client

            with temporary_env(
                {
                    'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
                    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true',
                    'METRICS_UTILITY_PROMETHEUS_URL': 'https://prometheus.example.com:9090',
                }
            ):
                total_workers_vcpu(None, None, None)

                # Calculate expected timestamp
                current_ts = mock_now.timestamp()
                expected_prev_hour_start, _ = get_hour_boundaries(current_ts)

                # Verify the query was called with correct PromQL
                mock_prom_client.get_current_value.assert_called_once()
                query_call = mock_prom_client.get_current_value.call_args[0][0]
                expected_query = f'max_over_time(sum(machine_cpu_cores)[59m59s:5m] @ {expected_prev_hour_start})'
                assert query_call == expected_query

    def test_vcpu_value_converted_to_int(self):
        """Test that vCPU values from Prometheus (floats) are properly converted to integers."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.PrometheusClient') as mock_prom_client_class,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Mock PrometheusClient returning float value
            mock_prom_client = MagicMock()
            mock_prom_client.get_current_value.return_value = 15.9  # Float value
            mock_prom_client_class.return_value = mock_prom_client

            with temporary_env(
                {
                    'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
                    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true',
                    'METRICS_UTILITY_PROMETHEUS_URL': 'https://prometheus.example.com:9090',
                }
            ):
                result = total_workers_vcpu(None, None, None)

                assert result['total_workers_vcpu'] == 15  # Should be truncated to int
                assert isinstance(result['total_workers_vcpu'], int)

    @patch('metrics_utility.automation_controller_billing.collectors.datetime')
    def test_timestamp_in_output_with_hour_boundaries(self, mock_datetime):
        """Test that the function includes proper timestamp based on hour boundaries."""
        # Mock the datetime.now() call
        mock_now = datetime(2023, 12, 25, 15, 30, 45, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mock_now
        mock_datetime.timezone = timezone
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.PrometheusClient') as mock_prom_client_class,
            patch('metrics_utility.automation_controller_billing.collectors.logger_info_level') as mock_logger_info,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Mock PrometheusClient
            mock_prom_client = MagicMock()
            mock_prom_client.get_current_value.return_value = 8.0
            mock_prom_client_class.return_value = mock_prom_client

            with temporary_env(
                {
                    'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
                    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true',
                    'METRICS_UTILITY_PROMETHEUS_URL': 'https://prometheus.example.com:9090',
                }
            ):
                result = total_workers_vcpu(None, None, None)

                # Calculate expected timestamp
                current_ts = mock_now.timestamp()
                _, prev_hour_end = get_hour_boundaries(current_ts)
                expected_timestamp = datetime.fromtimestamp(prev_hour_end).isoformat()

                # Check result timestamp
                assert 'timestamp' in result
                assert result['timestamp'] == expected_timestamp

                # Check logged JSON - logger now logs twice: once for info, once for data
                assert mock_logger_info.info.call_count == 2
                first_call_args = mock_logger_info.info.call_args_list[0][0]
                assert first_call_args[0] == '%s info: %s'
                logged_json = json.loads(first_call_args[2])
                assert logged_json['end_timestamp'] == expected_timestamp
                assert logged_json['cluster_name'] == 'test-cluster'
                assert logged_json['total_workers_vcpu'] == 8
                assert logged_json['usage_based_billing_enabled']

    def test_usage_based_billing_disabled_unset_returns_hardcoded_value(self):
        """Test that when METRICS_UTILITY_USAGE_BASED_METERING_ENABLED is unset, it returns hardcoded value."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.logger_info_level') as mock_logger_info,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': None}):
                result = total_workers_vcpu(None, None, None)
                assert result['cluster_name'] == 'test-cluster'
                assert result['total_workers_vcpu'] == 1

                # Verify the logged JSON contains usage_based_billing_enabled = False
                # The logger now logs twice: once for info, once for data
                assert mock_logger_info.info.call_count == 2
                first_call_args = mock_logger_info.info.call_args_list[0][0]
                assert first_call_args[0] == '%s info: %s'
                logged_json = json.loads(first_call_args[2])
                assert not logged_json['usage_based_billing_enabled']


class TestGetHourBoundaries:
    """Test suite for the get_hour_boundaries helper function."""

    def test_get_hour_boundaries_calculation(self):
        """Test that get_hour_boundaries correctly calculates previous hour boundaries."""
        # Test with a specific timestamp: 2023-12-25 15:30:45 UTC
        test_datetime = datetime(2023, 12, 25, 15, 30, 45, tzinfo=timezone.utc)
        current_ts = test_datetime.timestamp()

        prev_hour_start, prev_hour_end = get_hour_boundaries(current_ts)

        # Previous hour should be 14:00:00 to 14:59:59
        expected_prev_hour_start = datetime(2023, 12, 25, 14, 0, 0, tzinfo=timezone.utc).timestamp()
        expected_prev_hour_end = datetime(2023, 12, 25, 14, 59, 59, tzinfo=timezone.utc).timestamp()

        assert prev_hour_start == expected_prev_hour_start
        assert prev_hour_end == expected_prev_hour_end

    def test_get_hour_boundaries_at_hour_boundary(self):
        """Test get_hour_boundaries when current time is exactly at hour boundary."""
        # Test at exactly 15:00:00
        test_datetime = datetime(2023, 12, 25, 15, 0, 0, tzinfo=timezone.utc)
        current_ts = test_datetime.timestamp()

        prev_hour_start, prev_hour_end = get_hour_boundaries(current_ts)

        # Previous hour should be 14:00:00 to 14:59:59
        expected_prev_hour_start = datetime(2023, 12, 25, 14, 0, 0, tzinfo=timezone.utc).timestamp()
        expected_prev_hour_end = datetime(2023, 12, 25, 14, 59, 59, tzinfo=timezone.utc).timestamp()

        assert prev_hour_start == expected_prev_hour_start
        assert prev_hour_end == expected_prev_hour_end

    def test_get_hour_boundaries_different_times(self):
        """Test get_hour_boundaries with different times throughout the day."""
        test_cases = [
            # (hour, expected_prev_hour)
            (1, 0),  # 01:xx -> previous hour is 00:xx
            (12, 11),  # 12:xx -> previous hour is 11:xx
            (23, 22),  # 23:xx -> previous hour is 22:xx
        ]

        for current_hour, expected_prev_hour in test_cases:
            test_datetime = datetime(2023, 12, 25, current_hour, 30, 0, tzinfo=timezone.utc)
            current_ts = test_datetime.timestamp()

            prev_hour_start, prev_hour_end = get_hour_boundaries(current_ts)

            expected_prev_hour_start = datetime(2023, 12, 25, expected_prev_hour, 0, 0, tzinfo=timezone.utc).timestamp()
            expected_prev_hour_end = datetime(2023, 12, 25, expected_prev_hour, 59, 59, tzinfo=timezone.utc).timestamp()

            assert prev_hour_start == expected_prev_hour_start
            assert prev_hour_end == expected_prev_hour_end
