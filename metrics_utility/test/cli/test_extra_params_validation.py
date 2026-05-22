import pytest

from metrics_utility.exceptions import UnparsableParameter
from metrics_utility.test.util import run_gather_int


env_vars = {
    'METRICS_UTILITY_SHIP_PATH': '/tmp/nowrites',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
}


def handle_gather_exception(env_vars, params, klass):
    with pytest.raises(klass) as e:
        run_gather_int(env_vars, params)
    return e.value


def test_invalid_gather_argument_format():
    from metrics_utility.management.commands.gather_automation_controller_billing_data import Command

    bad_inputs = ['2', '2y', 'mo3', '3weeks', '3w']
    args = ['until', 'since']

    inp_errors = [
        'Bare integers are not allowed',
        "Invalid isoformat string: '2y'",
        None,
        None,
        None,
    ]
    arg_errors = [
        'End date for collection',
        'Start date for collection',
    ]

    for bad_input, err_input in zip(bad_inputs, inp_errors):
        for arg, err_arg in zip(args, arg_errors):
            cmd = Command()

            e = handle_gather_exception(env_vars, {arg: bad_input}, UnparsableParameter)

            assert (err_input or cmd.help_texts[arg]) in e.name
            assert err_arg in e.name
