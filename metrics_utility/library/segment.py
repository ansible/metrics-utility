"""
Library API for sending data to Segment.com.

This module provides a programmatic interface for sending metrics and analytics
data to Segment without using the CLI command.

Example usage:
    >>> from metrics_utility.library.segment import send_data, SegmentSender
    >>>
    >>> # Simple function API
    >>> send_data(
    ...     data=[{"hostname": "server1", "status": "active"}],
    ...     app="ansible-automation-platform",
    ...     write_key="your_segment_write_key",
    ... )
    >>>
    >>> # Class-based API
    >>> sender = SegmentSender(write_key="your_segment_write_key")
    >>> sender.send(
    ...     data=[{"metric": "cpu", "value": 75}],
    ...     app="awx",
    ...     user_id="admin@example.com",
    ... )
"""

import csv
import datetime
import json

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import segment.analytics as analytics


class SegmentError(Exception):
    """Base exception for Segment-related errors."""

    pass


class SegmentConfigurationError(SegmentError):
    """Raised when Segment configuration is invalid."""

    pass


class SegmentDataError(SegmentError):
    """Raised when data cannot be processed or sent."""

    pass


class SegmentSender:
    """
    Class-based API for sending data to Segment.

    Attributes:
        write_key: Segment write key for authentication
        debug: Enable debug logging in Segment SDK

    Example:
        >>> sender = SegmentSender(write_key="your_key")
        >>> sender.send(
        ...     data=[{"hostname": "server1"}],
        ...     app="ansible-platform",
        ... )
    """

    def __init__(self, write_key: str, debug: bool = False):
        """
        Initialize SegmentSender with write key.

        Args:
            write_key: Segment write key for authentication
            debug: Enable debug logging (default: False)

        Raises:
            SegmentConfigurationError: If write_key is not provided
        """
        if not write_key:
            raise SegmentConfigurationError('write_key is required')

        self.write_key = write_key
        self.debug = debug

    def send(
        self,
        data: Union[List[Dict[str, Any]], Dict[str, Any], str, Path],
        app: str,
        user_id: str = 'system',
        additional_properties: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Send data to Segment as a custom event.

        Args:
            data: Data to send (list of dicts, dict, file path, or Path object)
            app: Application identifier (used in event name and context)
            user_id: User ID for the event (default: 'system')
            additional_properties: Additional properties to include in event

        Returns:
            Dictionary with send results including:
                - success: Boolean indicating if send was successful
                - event_name: Name of the event sent
                - row_count: Number of data items sent
                - message: Success or error message

        Raises:
            SegmentConfigurationError: If required parameters are missing
            SegmentDataError: If data cannot be processed or sent

        Example:
            >>> sender.send(
            ...     data=[{"metric": "cpu", "value": 75}],
            ...     app="awx",
            ...     user_id="admin",
            ... )
            {'success': True, 'event_name': 'awx_data_upload', ...}
        """
        if not app:
            raise SegmentConfigurationError('app parameter is required')

        # Process data
        try:
            processed_data = self._process_data(data)
        except Exception as e:
            raise SegmentDataError(f'Failed to process data: {str(e)}')

        # Configure Segment SDK
        analytics.write_key = self.write_key
        analytics.debug = self.debug

        # Prepare event
        event_name = f'{app}_data_upload'
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        properties = {
            'data': processed_data['data'],
            'row_count': processed_data['row_count'],
            'data_type': processed_data['data_type'],
            'timestamp': timestamp,
        }

        # Add additional properties if provided
        if additional_properties:
            properties.update(additional_properties)

        context = {'app': {'name': app}}

        # Send to Segment
        try:
            analytics.track(
                user_id=user_id,
                event=event_name,
                properties=properties,
                context=context,
            )
            analytics.flush()

            return {
                'success': True,
                'event_name': event_name,
                'row_count': processed_data['row_count'],
                'message': (f'Successfully sent {processed_data["row_count"]} items to Segment as event "{event_name}"'),
            }

        except Exception as e:
            raise SegmentDataError(f'Failed to send data to Segment: {str(e)}')

    def _process_data(self, data: Union[List[Dict], Dict, str, Path]) -> Dict[str, Any]:
        """
        Process various data formats into a standardized format.

        Args:
            data: Data in various formats

        Returns:
            Dictionary with processed data, row count, and data type
        """
        # Handle file paths
        if isinstance(data, (str, Path)):
            return self._read_file(Path(data))

        # Handle list of dictionaries
        if isinstance(data, list):
            return {
                'data': data,
                'row_count': len(data),
                'data_type': 'list',
            }

        # Handle single dictionary
        if isinstance(data, dict):
            return {
                'data': [data],
                'row_count': 1,
                'data_type': 'dict',
            }

        raise SegmentDataError(f'Unsupported data type: {type(data)}. Expected list, dict, str (file path), or Path object.')

    def _read_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Read and parse file content.

        Args:
            file_path: Path to file

        Returns:
            Dictionary with parsed data, row count, and file type
        """
        if not file_path.exists():
            raise SegmentDataError(f'File not found: {file_path}')

        if not file_path.is_file():
            raise SegmentDataError(f'Path is not a file: {file_path}')

        file_extension = file_path.suffix.lower()

        if file_extension == '.csv':
            return self._read_csv(file_path)
        elif file_extension == '.json':
            return self._read_json(file_path)
        else:
            # Try CSV first, fall back to text
            try:
                return self._read_csv(file_path)
            except (csv.Error, UnicodeDecodeError):
                return self._read_text(file_path)

    def _read_csv(self, file_path: Path) -> Dict[str, Any]:
        """Read CSV file."""
        data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(dict(row))

        return {'data': data, 'row_count': len(data), 'data_type': 'csv'}

    def _read_json(self, file_path: Path) -> Dict[str, Any]:
        """Read JSON file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if isinstance(data, list):
            row_count = len(data)
        elif isinstance(data, dict):
            row_count = 1
            data = [data]
        else:
            row_count = 1
            data = [{'value': data}]

        return {'data': data, 'row_count': row_count, 'data_type': 'json'}

    def _read_text(self, file_path: Path) -> Dict[str, Any]:
        """Read plain text file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        lines = content.strip().split('\n')

        return {
            'data': {'content': content, 'lines': lines},
            'row_count': len(lines),
            'data_type': 'text',
        }


def send_data(
    data: Union[List[Dict[str, Any]], Dict[str, Any], str, Path],
    app: str,
    write_key: str,
    user_id: str = 'system',
    additional_properties: Optional[Dict[str, Any]] = None,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Send data to Segment (simple function API).

    This is a convenience function that creates a SegmentSender instance
    and sends data in a single call.

    Args:
        data: Data to send (list of dicts, dict, or file path)
        app: Application identifier
        write_key: Segment write key
        user_id: User ID for the event (default: 'system')
        additional_properties: Additional properties to include
        debug: Enable debug logging (default: False)

    Returns:
        Dictionary with send results

    Raises:
        SegmentConfigurationError: If configuration is invalid
        SegmentDataError: If data cannot be processed or sent

    Example:
        >>> result = send_data(
        ...     data=[{"metric": "cpu", "value": 75}],
        ...     app="awx",
        ...     write_key="your_key",
        ... )
        >>> print(result['success'])
        True
    """
    sender = SegmentSender(write_key=write_key, debug=debug)
    return sender.send(
        data=data,
        app=app,
        user_id=user_id,
        additional_properties=additional_properties,
    )


def send_csv_file(
    file_path: Union[str, Path],
    app: str,
    write_key: str,
    user_id: str = 'system',
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Send CSV file to Segment (convenience function).

    Args:
        file_path: Path to CSV file
        app: Application identifier
        write_key: Segment write key
        user_id: User ID for the event (default: 'system')
        debug: Enable debug logging (default: False)

    Returns:
        Dictionary with send results

    Example:
        >>> result = send_csv_file(
        ...     file_path="metrics.csv",
        ...     app="ansible-automation-platform",
        ...     write_key="your_key",
        ... )
    """
    return send_data(
        data=file_path,
        app=app,
        write_key=write_key,
        user_id=user_id,
        debug=debug,
    )


def send_json_file(
    file_path: Union[str, Path],
    app: str,
    write_key: str,
    user_id: str = 'system',
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Send JSON file to Segment (convenience function).

    Args:
        file_path: Path to JSON file
        app: Application identifier
        write_key: Segment write key
        user_id: User ID for the event (default: 'system')
        debug: Enable debug logging (default: False)

    Returns:
        Dictionary with send results

    Example:
        >>> result = send_json_file(
        ...     file_path="report.json",
        ...     app="awx",
        ...     write_key="your_key",
        ... )
    """
    return send_data(
        data=file_path,
        app=app,
        write_key=write_key,
        user_id=user_id,
        debug=debug,
    )


__all__ = [
    'SegmentSender',
    'SegmentError',
    'SegmentConfigurationError',
    'SegmentDataError',
    'send_data',
    'send_csv_file',
    'send_json_file',
]
