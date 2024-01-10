import io
import os

from .package import Package
from insights_analytics_collector import CsvFileSplitter as BaseCsvFileSplitter


class CsvFileSplitter(BaseCsvFileSplitter):
    """Helper for writing big data into multiple files splitted by size.
    Expects data written in CSV format (first line is header)
    Could be called from function decorated by @register (see Collector).
    :param max_file_size: determined by decorated function's attribute "max_data_size"
    """

    # TODO: make this configurable in the base class, that want file splitter
    # to return empty csv file, in case where we always need to send a payload,
    # to verify payload is being sent periodically.
    def file_list(self):
        """Returns list of written files"""
        self.currentfile.close()

        # If we only have one file, remove the suffix
        if len(self.files) == 1:
            filename = self.files.pop()
            new_filename = filename.replace("_split0", "")
            os.rename(filename, new_filename)
            self.files.append(new_filename)
        return self.files
