"""
Helper utilities for anonymized rollups.
"""

import math

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


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
