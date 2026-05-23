"""CollectionOutput adapter – passed from the CLI layer into library collectors."""

import tempfile

from metrics_utility.gather.csv_file_splitter import CsvFileSplitter
from metrics_utility.library.collectors.util import DictOutput


class CollectionOutput(DictOutput):
    """Output adapter passed from the CLI to collectors.

    CSV collectors write files to ``full_path`` and return a list of file paths.
    JSON collectors return a dict (handled by the :class:`DictOutput` base class).
    """

    def __init__(self, full_path):
        """Initialise with the directory where CSV output files will be written.

        Args:
            full_path: Absolute path to the staging directory.
        """
        self.full_path = full_path

    # takes a list of filenames, returns the same
    def files(self, filenames):
        """Validate and return a list of CSV file paths.

        Args:
            filenames: Must be a list of path strings or None.

        Returns:
            The list unchanged, or None.

        Raises:
            Exception: If *filenames* is neither a list nor None.
        """
        if filenames is None:
            return None

        if type(filenames) is not list:
            raise Exception('filenames must be a list, or None')

        return filenames

    # takes a collector, returns a dict
    def as_dict(self, collector):
        """Gather from *collector* and return the result as a dict.

        Args:
            collector: A collector object with a ``gather(output=…)`` method.

        Returns:
            The gathered dict, or None.
        """
        return self.dict(collector.gather(output=self))

    # takes a collector, returns a list of filenames
    def as_files(self, collector):
        """Gather from *collector* and return the result as a list of file paths.

        Args:
            collector: A collector object with a ``gather(output=…)`` method.

        Returns:
            List of CSV file paths, or None.
        """
        return self.files(collector.gather(output=self))

    def sql(self, db, query):
        filespec = tempfile.mktemp(dir=self.full_path)  # NOT mkstemp - this is a prefix, can't have it get created
        return _copy_table_files(db, query, filespec)


def _copy_table_files(db, query, filespec):
    with CsvFileSplitter(filespec=filespec) as file:
        with db.cursor() as cursor:
            copy_query = f'COPY ({query}) TO STDOUT WITH CSV HEADER'

            with cursor.copy(copy_query) as copy:
                while data := copy.read():
                    byte_data = bytes(data)
                    file.write(byte_data.decode())

        return file.file_list(keep_empty=True)
