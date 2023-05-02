import django.core.management.base as base


class BaseCommand(base.BaseCommand):
    def execute(self, *args, **options):
        output = self.handle(*args, **options)
        if output:
            self.stdout.write(output)
        return output
