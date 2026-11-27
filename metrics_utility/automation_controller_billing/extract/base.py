"""Base extractor class and safe tarball extraction utilities for billing data."""

import json
import os
import tarfile

import pandas as pd

from metrics_utility.exceptions import MetricsException
from metrics_utility.logger import logger


_main_host_sheets = [
    'inventory_scope',
    'jobs',
    'managed_nodes',
    'managed_nodes_by_organizations',
    'usage_by_collections',
    'usage_by_modules',
    'usage_by_roles',
]

# csv name => [ sheet_names ]
CSV_SHEETS = {
    'job_host_summary': [
        'ccsp_summary',
        'indirectly_managed_nodes',
        'inventory_scope',
        'jobs',
        'managed_nodes',
        'managed_nodes_by_organizations',
        'usage_by_organizations',
    ],
    'main_host': _main_host_sheets,
    'main_host_daily': _main_host_sheets,
    'main_indirectmanagednodeaudit': [
        'indirectly_managed_nodes',
        'infrastructure_summary',
        'managed_nodes',
        'usage_by_organizations',
    ],
    'main_jobevent': [
        'usage_by_collections',
        'usage_by_modules',
        'usage_by_organizations',
        'usage_by_roles',
    ],
    'data_collection_status': [
        'data_collection_status',
    ],
}


class Base:
    """Abstract base class for billing data extractors.

    Subclasses implement :meth:`iter_batches` to yield per-tarball data dicts
    and :meth:`fetch_partition_paths` to locate the tarballs for a given date.
    Common helpers for tarball extraction, CSV loading, and path construction
    are provided here.
    """

    LOG_PREFIX = '[ExtractorBase]'

    def __init__(self, extra_params):
        """Initialise the extractor with configuration parameters.

        Args:
            extra_params: Dict containing at least ``'ship_path'`` and
                ``'optional_sheets'`` keys.
        """
        self.extra_params = extra_params

    def load_config(self, file_path):
        """Read and return a JSON config file from *file_path*.

        Args:
            file_path: Absolute path to a ``config.json`` file extracted from
                a tarball.

        Returns:
            Parsed dict, or None if the file is not found.
        """
        try:
            with open(file_path) as f:
                return json.loads(f.read())
        except FileNotFoundError:
            logger.warning(f'{self.LOG_PREFIX} missing required file under path: {file_path} and date: {self.date}')

    def process_tarballs(self, path, temp_dir, enabled_set=None):
        """Extract a tarball and return a dict of DataFrames and config.

        Extracts the archive at *path* into *temp_dir*, then reads each
        requested CSV file into a pandas DataFrame.  The ``config`` key always
        holds the parsed ``config.json`` dict.

        Args:
            path: Path to the ``.tar.gz`` file to extract.
            temp_dir: Temporary directory to extract files into.
            enabled_set: Optional set of CSV names to extract; if None, all
                known CSVs are extracted.

        Returns:
            Dict with keys ``'config'``, ``'data_collection_status'``,
            ``'job_host_summary'``, ``'main_host'``, ``'main_host_daily'``,
            ``'main_indirectmanagednodeaudit'``, and ``'main_jobevent'``.
        """
        _safe_extract(path, temp_dir, enabled_set=enabled_set)
        self.enabled_set = enabled_set

        empty_dataframe = pd.DataFrame([{}])
        needed_data = {
            'config': None,
            'data_collection_status': empty_dataframe,
            'main_indirectmanagednodeaudit': empty_dataframe,
            'job_host_summary': empty_dataframe,
            'main_host': empty_dataframe,
            'main_host_daily': empty_dataframe,
            'main_jobevent': empty_dataframe,
        }

        if self.csv_enabled('config'):
            needed_data['config'] = self.load_config(os.path.join(temp_dir, 'config.json'))

        if self.csv_enabled('data_collection_status'):
            needed_data['data_collection_status'] = self.build_data_batch(temp_dir, 'data_collection_status')

        if self.csv_enabled('job_host_summary'):
            needed_data['job_host_summary'] = self.build_data_batch(temp_dir, 'job_host_summary')

        if self.csv_enabled('main_indirectmanagednodeaudit'):
            needed_data['main_indirectmanagednodeaudit'] = self.build_data_batch(temp_dir, 'main_indirectmanagednodeaudit')

        if self.csv_enabled('main_jobevent'):
            needed_data['main_jobevent'] = self.build_data_batch(temp_dir, 'main_jobevent')

        if self.csv_enabled('main_host'):
            needed_data['main_host'] = self.build_data_batch(temp_dir, 'main_host')

        if self.csv_enabled('main_host_daily'):
            needed_data['main_host_daily'] = self.build_data_batch(temp_dir, 'main_host_daily')

        return needed_data

    def build_data_batch(self, temp_dir, file_name):
        """
        Builds the report with only the necessary sheets.
        """

        if os.path.exists(os.path.join(temp_dir, f'{file_name}.csv')):
            return pd.read_csv(os.path.join(temp_dir, f'{file_name}.csv'), encoding='utf-8')
        else:
            return pd.DataFrame([{}])

    def csv_enabled(self, name):
        """
        Enable CSV extraction based on list of rendered sheets, but also enabled_set if set
        Only true if there's both a sheet that needs it and we're called by a dataframe that asks for it
        """
        if name in CSV_SHEETS and not self.sheet_enabled(CSV_SHEETS[name]):
            return False

        if self.enabled_set is None:
            return True

        return name in self.enabled_set

    def get_path_prefix(self, date):
        """Return the data/Y/m/d path"""
        ship_path = self.extra_params['ship_path']

        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')

        return f'{ship_path}/data/{year}/{month}/{day}'

    def sheet_enabled(self, sheets_required):
        """
        Checks if any sheets_required item is in METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS
        Returns a boolean so we know which sheets to provide in the report.
        """
        sheet_options = self.extra_params.get('optional_sheets')

        # no optional_sheets in rollups & generator - allow everything
        if sheet_options is None:
            return True

        return bool(set(sheet_options) & set(sheets_required))

    def filter_tarball_paths(self, paths, collections):
        """Skip partition markers and archive checksums, preserving candidate order.

        Package.make_tgz names a mixed archive after its last collection. Even a
        recognized suffix cannot rule out other collections inside the archive.
        S3 also supports renamed archives without a standard extension.

        Raises:
            :exc:`~metrics_utility.exceptions.MetricsException`: If
                ``'data_collection_status'`` or ``'config'`` is in *collections*
                (these are never used as tarball name filters).
        """
        if collections is not None:
            for key in ('data_collection_status', 'config'):
                if key in collections:
                    raise MetricsException(f'{key} is not a valid tarball name filter')

        return [path for path in paths if os.path.basename(path) not in ('', '_SUCCESS') and not path.endswith('.tar.gz.sha256')]


def _write_member(member_path, file_obj, max_size, total_extracted_size):
    with open(member_path, 'wb') as out_f:
        chunk_size = 1024 * 1024  # 1 MB buffer
        while True:
            data = file_obj.read(chunk_size)
            if not data:
                break

            total_extracted_size += len(data)
            if total_extracted_size > max_size:
                # Stop if we exceed total extraction size
                raise ValueError('Extraction aborted: Maximum total size exceeded.')

            out_f.write(data)

    return total_extracted_size


def _member_destination(extract_path_abs, name, enabled_set):
    basename = os.path.basename(name)
    if enabled_set and not any(basename.startswith(item + '.') for item in enabled_set):
        return None
    destination = os.path.abspath(os.path.join(extract_path_abs, name))
    if not destination.startswith(os.fspath(extract_path_abs) + os.sep):
        logger.warning(f'Skipping potentially unsafe file (path traversal): {name}')
        return None
    return destination


def _safe_extract(tar_path, extract_path, max_files=100, max_size=1024 * 1024 * 1024, enabled_set=None):
    """
    Safely extract a tar archive from 'tar_path' into 'extract_path' with constraints:
      - Only extract *.json or *.csv files
      - Skip directories, symbolic links, and hard links
      - Limit number of extracted files to 'max_files'
      - Limit total uncompressed size to 'max_size' bytes
    """
    extracted_files = []
    total_extracted_size = 0

    # Ensure the extraction directory exists
    os.makedirs(extract_path, exist_ok=True)
    extract_path_abs = os.path.abspath(extract_path)

    with tarfile.open(tar_path, 'r:*') as tar:
        # Stream members in one pass to avoid rewinding gzip archives.
        for member in tar:
            # Skip directories and links
            if member.isdir():
                continue
            if member.issym() or member.islnk():
                logger.warning(f'Skipping link: {member.name}')
                continue

            # Only allow .json or .csv
            if not member.name.endswith(('.json', '.csv')):
                continue

            member_path = _member_destination(extract_path_abs, member.name, enabled_set)
            if member_path is None:
                continue

            # Limit total files
            if len(extracted_files) >= max_files:
                logger.warning(f'Reached max file limit of {max_files}.')
                break

            # Extract file contents manually in chunks to avoid trusting the tar's metadata size
            file_obj = tar.extractfile(member)
            if file_obj is None:
                # Could not read the file content for some reason
                continue

            # Make sure the subdirectory structure exists
            os.makedirs(os.path.dirname(member_path), exist_ok=True)

            # Write out the file, limiting max size
            total_extracted_size = _write_member(member_path, file_obj, max_size, total_extracted_size)

            extracted_files.append(os.path.basename(member.name))

    file_count = len(extracted_files)
    file_list = '", "'.join(sorted(extracted_files))
    logger.debug(f'Extraction complete. Files extracted: {file_count} ("{file_list}"), Total size: {total_extracted_size} bytes.')
