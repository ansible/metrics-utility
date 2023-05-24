import os
import sys
import django.core.management as management
from importlib import import_module


class ManagementUtility(management.ManagementUtility):
    def execute(self):
        """
        Given the command-line arguments, figure out which subcommand is being
        run, create a parser appropriate to that command, and run it.
        """
        try:
            subcommand = self.argv[1]
        except IndexError:
            subcommand = "help"  # Display help if no arguments were given.

        # Preprocess options to extract --settings and --pythonpath.
        # These options could affect the commands that are available, so they
        # must be processed early.
        parser = management.CommandParser(
            prog=self.prog_name,
            usage="%(prog)s subcommand [options] [args]",
            add_help=False,
            allow_abbrev=False,
        )
        parser.add_argument("--settings")
        # parser.add_argument("--pythonpath")
        parser.add_argument("args", nargs="*")  # catch-all
        try:
            options, args = parser.parse_known_args(self.argv[2:])
            # handle_default_options(options)
        except management.CommandError:
            pass  # Ignore any option errors at this point.

        # self.autocomplete()

        if subcommand == "help":
            if "--commands" in args:
                sys.stdout.write(self.main_help_text(commands_only=True) + "\n")
            elif not options.args:
                sys.stdout.write(self.main_help_text() + "\n")
            else:
                self.fetch_command(options.args[0]).print_help(
                    self.prog_name, options.args[0]
                )
        # Special-cases: We want 'django-admin --version' and
        # 'django-admin --help' to work, for backwards compatibility.
        elif subcommand == "version" or self.argv[1:] == ["--version"]:
            # sys.stdout.write(django.get_version() + "\n")
            sys.stdout.write("0.0.0.TODO" + "\n")
        elif self.argv[1:] in (["--help"], ["-h"]):
            sys.stdout.write(self.main_help_text() + "\n")
        else:
            # from metrics_utility.management.commands.host_metric import Command
            # Command().run_from_argv(self.argv)
            self.fetch_command(subcommand).run_from_argv(self.argv)

    def fetch_command(self, subcommand):
        module = import_module(f"metrics_utility.management.commands.{subcommand}")
        return module.Command()

    @staticmethod
    def get_commands():
        commands = {}
        path = os.path.join(os.path.dirname(__file__), "management")
        commands.update({name: 'metrics_utility' for name in management.find_commands(path)})
        return commands
