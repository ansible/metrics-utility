"""Custom Django ManagementUtility for the metrics-utility CLI entry point."""

import os
import sys

from importlib import import_module
from importlib.metadata import version

import django.core.management as management

from metrics_utility.exceptions import MetricsException
from metrics_utility.logger import logger


class ManagementUtility(management.ManagementUtility):
    """Customised Django :class:`ManagementUtility` for the metrics-utility CLI.

    Limits the exposed commands to
    ``gather_automation_controller_billing_data``, and surfaces
    :class:`~metrics_utility.exceptions.MetricsException` errors as clean
    log messages with a non-zero exit code.
    """

    def execute(self):
        """
        Given the command-line arguments, figure out which subcommand is being
        run, create a parser appropriate to that command, and run it.
        """
        try:
            subcommand = self.argv[1]
        except IndexError:
            subcommand = 'help'  # Display help if no arguments were given.

        # Preprocess options to extract --settings and --pythonpath.
        # These options could affect the commands that are available, so they
        # must be processed early.
        parser = management.CommandParser(
            prog=self.prog_name,
            usage='%(prog)s subcommand [options] [args]',
            add_help=False,
            allow_abbrev=False,
        )
        parser.add_argument('--settings')
        # parser.add_argument("--pythonpath")
        parser.add_argument('args', nargs='*')  # catch-all
        try:
            options, args = parser.parse_known_args(self.argv[2:])
            # handle_default_options(options)
        except management.CommandError:
            pass  # Ignore any option errors at this point.

        # self.autocomplete()

        if subcommand == 'help':
            if '--commands' in args:
                sys.stdout.write(self.main_help_text(commands_only=True) + '\n')
            elif not options.args:
                sys.stdout.write(self.main_help_text() + '\n')
            else:
                self.fetch_command(options.args[0]).print_help(self.prog_name, options.args[0])
        # Special-cases: We want 'django-admin --version' and
        # 'django-admin --help' to work, for backwards compatibility.
        elif subcommand == 'version' or self.argv[1:] == ['--version']:
            sys.stdout.write(version('metrics-utility') + '\n')
        elif self.argv[1:] in (['--help'], ['-h']):
            sys.stdout.write(self.main_help_text() + '\n')
        else:
            self.run_subcommand(subcommand, self.argv)

    def main_help_text(self, commands_only=False):
        """Return the main help text.

        Args:
            commands_only: When True, return only the list of commands.

        Returns:
            Help text string.
        """
        commands = 'Commands: gather_automation_controller_billing_data'
        if commands_only:
            return commands
        else:
            return f'Usage: {os.path.basename(sys.argv[0])} <command> [options]\n{commands}'

    def fetch_command(self, subcommand):
        """Import and return the Command class for *subcommand*.

        Args:
            subcommand: Name of the management command to load.

        Returns:
            An instance of the command's ``Command`` class.

        Raises:
            Exception: If the command module cannot be imported.
        """
        try:
            module = import_module(f'metrics_utility.management.commands.{subcommand}')
        except Exception as ex:
            sys.stdout.write(f"Failed to import command '{subcommand}': {ex}")
            raise ex

        return module.Command()

    @staticmethod
    def get_commands():
        """Return a dict mapping each command name to ``'metrics_utility'``.

        Returns:
            Dict of ``{command_name: 'metrics_utility'}`` entries.
        """
        commands = {}
        path = os.path.join(os.path.dirname(__file__), 'management')
        commands.update({name: 'metrics_utility' for name in management.find_commands(path)})
        return commands

    def run_subcommand(self, subcommand, argv):
        """Execute *subcommand* and handle exceptions gracefully.

        Args:
            subcommand: The management command name to run.
            argv: Full argument list (including the program name).
        """
        try:
            self.fetch_command(subcommand).run_from_argv(argv)
        except MetricsException as e:
            logger.error(e.name)
            exit(1)
        except Exception as e:
            logger.exception(e)
            exit(1)
