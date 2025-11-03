"""
Helper utilities for anonymized rollups.
"""

import math


def sanitize_json(obj):
    """
    Sanitize a Python object to be JSON-serializable by replacing NaN and infinity values.

    This function recursively traverses dictionaries, lists, and other data structures
    and replaces any NaN or infinity values with None (which becomes null in JSON).

    Args:
        obj: The object to sanitize (can be dict, list, float, int, str, etc.)

    Returns:
        The sanitized object with all NaN and infinity values replaced with None

    Examples:
        >>> sanitize_json({'value': float('nan')})
        {'value': None}

        >>> sanitize_json([1, float('inf'), 3])
        [1, None, 3]

        >>> sanitize_json({'nested': {'value': float('-inf')}})
        {'nested': {'value': None}}
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
    elif isinstance(obj, float):
        # Check for NaN or infinity
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    else:
        # Return other types as-is (int, str, bool, None, etc.)
        return obj
