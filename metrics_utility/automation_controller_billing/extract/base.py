import json
import os
import re
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
        'managed_nodes',
        'managed_nodes_by_organizations',
        'usage_by_organizations',
    ],
    'main_host': _main_host_sheets,
    'main_host_daily': _main_host_sheets,
    'main_indirectmanagednodeaudit': [
        'indirectly_managed_nodes',
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
    LOG_PREFIX = '[ExtractorBase]'

    def __init__(self, extra_params):
        self.extra_params = extra_params

    def load_config(self, file_path):
        try:
            with open(file_path) as f:
                return json.loads(f.read())
        except FileNotFoundError:
            logger.warning(f'{self.LOG_PREFIX} missing required file under path: {file_path} and date: {self.date}')

    def process_tarballs(self, path, temp_dir, enabled_set=None):
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
        # collections=['main_host', 'foo'] - returns only filenames matching *-main_host.tar.gz or *-foo.tar.gz
        if collections is None:
            return paths

        if 'data_collection_status' in collections:
            raise MetricsException('data_collection_status is not a valid tarball name filter')

        if 'config' in collections:
            raise MetricsException('config is not a valid tarball name filter')

        def match(s):
            # include all files produced by 0.6.0 and lower, and anything with an unexpected name
            if re.search(r'-\d+.tar.gz$', s):
                return True
            if re.search(r'-\d+-\w+.tar.gz$', s) is None:
                return True

            # should not happen, but make sure we're not ignoring data if it does
            if s.find('-unknown.') or s.find('-config.'):
                return True

            # match against collections
            for key in collections:
                if s.find(f'-{key}.') != -1:
                    return True

            return False

        paths = filter(match, paths)
        return list(paths)


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

    with tarfile.open(tar_path, 'r:*') as tar:
        for member in tar.getmembers():
            # Skip directories and links
            if member.isdir():
                continue
            if member.issym() or member.islnk():
                logger.warning(f'Skipping link: {member.name}')
                continue

            # Only allow .json or .csv
            if not (member.name.endswith('.json') or member.name.endswith('.csv')):
                continue

            # Only files from enabled_set (+ extension)
            basename = os.path.basename(member.name)
            if enabled_set:
                if not any(basename.startswith(item + '.') for item in enabled_set):
                    continue

            # Build a fully qualified path for this member and ensure it stays within extract_path
            member_path = os.path.abspath(os.path.join(extract_path, member.name))
            extract_path_abs = os.path.abspath(extract_path)
            if not member_path.startswith(extract_path_abs + os.sep):
                logger.warning(f'Skipping potentially unsafe file (path traversal): {member.name}')
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

            extracted_files.append(basename)

    file_count = len(extracted_files)
    file_list = '", "'.join(sorted(extracted_files))
    logger.debug(f'Extraction complete. Files extracted: {file_count} ("{file_list}"), Total size: {total_extracted_size} bytes.')
