import django.core.management.base as base


class BaseCommand(base.BaseCommand):
    def execute(self, *args, **options):
        output = self.handle(*args, **options)
        if output:
            self.stdout.write(output)
        return output

    @staticmethod
    def dictfetchall(cursor):
        """
        Return all rows from a cursor as a dict.
        Assume the column names are unique.
        """
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
