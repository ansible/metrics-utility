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

    def __init__(
        self, filespec=None, max_file_size=Package.MAX_DATA_SIZE, *args, **kwargs
    ):
        self.max_file_size = max_file_size
        self.filespec = filespec
        self.files = []
        self.currentfile = None
        self.header = None
        self.counter = 0
        self.cycle_file()

    # TODO: make this configurable in the base class, that we might want file splitter
    # to return empty csv file
    def file_list(self):
        """Returns list of written files"""
        self.currentfile.close()
        # Check for an empty dump
        # if len(self.header) + 1 == self.counter:
        #     os.remove(self.files[-1])
        #     self.files = self.files[:-1]
        # If we only have one file, remove the suffix
        if len(self.files) == 1:
            filename = self.files.pop()
            new_filename = filename.replace("_split0", "")
            os.rename(filename, new_filename)
            self.files.append(new_filename)
        return self.files
