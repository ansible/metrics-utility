"""
Helper utilities for anonymized rollups.
"""

import json
import math

import pandas as pd
import yaml


try:
    import numpy as np

    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


def parse_yaml_json(variables_data):
    """
    Parse the variables field from raw YAML or JSON format.

    Args:
        variables_data: Raw variables field (YAML string, JSON string, dict, or None)

    Returns:
        dict: Parsed variables as a dictionary, or None if parsing fails
    """
    # Handle None, NaN, or empty string
    if pd.isna(variables_data) or not variables_data:
        return None

    # If already a dict, return as-is
    if isinstance(variables_data, dict):
        return variables_data

    # Must be a string at this point
    if not isinstance(variables_data, str):
        return None

    # Try JSON first (faster and more common)
    try:
        parsed = json.loads(variables_data)
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, TypeError, ValueError):
        pass

    # Try YAML if JSON failed
    try:
        parsed = yaml.safe_load(variables_data)
        if isinstance(parsed, dict):
            return parsed
    except (yaml.YAMLError, TypeError, ValueError):
        pass

    # Both parsing attempts failed
    return None


def sanitize_json(obj):
    """
    Sanitize a Python object to be JSON-serializable by replacing NaN and infinity values
    and converting NumPy types to native Python types.

    This function recursively traverses dictionaries, lists, and other data structures
    and replaces any NaN or infinity values with None (which becomes null in JSON),
    and converts NumPy types (int64, float64, etc.) to native Python types.

    Args:
        obj: The object to sanitize (can be dict, list, float, int, str, etc.)

    Returns:
        The sanitized object with all NaN and infinity values replaced with None
        and NumPy types converted to native Python types

    Examples:
        >>> sanitize_json({'value': float('nan')})
        {'value': None}

        >>> sanitize_json([1, float('inf'), 3])
        [1, None, 3]

        >>> sanitize_json({'nested': {'value': float('-inf')}})
        {'nested': {'value': None}}

        >>> import numpy as np
        >>> sanitize_json({'value': np.int64(42)})
        {'value': 42}
    """
    if isinstance(obj, dict):
        # Recursively sanitize dictionary values
        return {key: sanitize_json(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        # Recursively sanitize list items
        return [sanitize_json(item) for item in obj]
    elif isinstance(obj, tuple):
        # Recursively sanitize tuple items (convert to list for JSON)
        return [sanitize_json(item) for item in obj]
    elif HAS_NUMPY and isinstance(obj, (np.integer, np.floating)):
        # Convert NumPy integer and float types to native Python types
        # Check for NaN or infinity first
        if isinstance(obj, np.floating) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj.item()  # Convert NumPy scalar to native Python type
    elif HAS_NUMPY and isinstance(obj, np.ndarray):
        # Convert NumPy arrays to lists
        return sanitize_json(obj.tolist())
    elif isinstance(obj, float):
        # Check for NaN or infinity
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    else:
        # Return other types as-is (int, str, bool, None, etc.)
        return obj
