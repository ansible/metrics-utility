"""Custom Django ManagementUtility for the metrics-utility CLI entry point."""

import os
import sys

from importlib import import_module
from importlib.metadata import version

import django.core.management as management

from metrics_utility.exceptions import MetricsError
from metrics_utility.logger import logger


class ManagementUtility(management.ManagementUtility):
    """Customised Django :class:`ManagementUtility` for the metrics-utility CLI.

    Limits the exposed commands to
    ``gather_automation_controller_billing_data``, and surfaces
    :class:`~metrics_utility.exceptions.MetricsError` errors as clean
    log messages with a non-zero exit code.
    """

    def execute(self):
        try:
            subcommand = self.argv[1]
        except IndexError:
            subcommand = 'help'

        if subcommand == 'help':
            if '--commands' in self.argv[2:]:
                sys.stdout.write(self.main_help_text(commands_only=True) + '\n')
            elif len(self.argv) <= 2:
                sys.stdout.write(self.main_help_text() + '\n')
            else:
                self.fetch_command(self.argv[2]).print_help(self.prog_name, self.argv[2])
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
        commands.update(dict.fromkeys(management.find_commands(path), 'metrics_utility'))
        return commands

    def run_subcommand(self, subcommand, argv):
        """Execute *subcommand* and handle exceptions gracefully.

        Args:
            subcommand: The management command name to run.
            argv: Full argument list (including the program name).
        """
        try:
            self.fetch_command(subcommand).run_from_argv(argv)
        except MetricsError as e:
            logger.error(e.name)
            exit(1)
        except Exception as e:
            logger.exception(e)
            exit(1)
