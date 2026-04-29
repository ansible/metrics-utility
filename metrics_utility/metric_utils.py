"""Constants for managed node type classification in billing reports."""

DIRECT = 0
"""Integer code for directly managed nodes (automated by AWX)."""

INDIRECT = 1
"""Integer code for indirectly managed nodes (discovered via audit)."""
# later also EDGE = 2

MANAGED_NODE_TYPES = {DIRECT: 'DIRECT', INDIRECT: 'INDIRECT'}
"""Mapping from integer node-type codes to human-readable string labels."""
