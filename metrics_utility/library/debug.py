"""Lightweight indented debug-logging helpers used by the library layer."""

_indent = 0


def indent(increment):
    """Adjust the global indentation level for :func:`log` output.

    Args:
        increment: Number of levels to add (use negative values to decrease).
    """
    global _indent
    _indent += increment


def log(s):
    """Print *s* with the current indentation level prefixed.

    Args:
        s: The message to print.
    """
    print(f'{_indent * "  "}{s}')
