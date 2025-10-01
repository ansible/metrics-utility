_indent = 0


def indent(increment):
    global _indent
    _indent += increment


def log(s):
    print(f'{_indent * "  "}{s}')
