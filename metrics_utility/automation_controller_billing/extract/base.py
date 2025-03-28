import json
import logging
import os
import tarfile

import pandas as pd

from metrics_utility.debug_utils import print_debug


CSV_SHEETS = {
    'job_host_summary': [
        'ccsp_summary',
        'indirectly_managed_nodes',
        'inventory_scope',
        'managed_nodes',
        'managed_nodes_by_organizations',
        'usage_by_organizations',
    ],
    'main_host': [
        'inventory_scope',
        'jobs',
        'managed_nodes',
        'managed_nodes_by_organizations',
        'usage_by_collections',
        'usage_by_modules',
        'usage_by_roles',
    ],
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
}


class Base:
    LOG_PREFIX = '[ExtractorBase]'

    def __init__(self, logger=logging.getLogger(__name__)):
        self.logger = logger

    def load_config(self, file_path):
        try:
            with open(file_path) as f:
                return json.loads(f.read())
        except FileNotFoundError:
            self.logger.warn(f'{self.LOG_PREFIX} missing required file under path: {file_path} and date: {self.date}')

    def process_tarballs(self, path, temp_dir):
        _safe_extract(path, temp_dir)
        config = self.load_config(os.path.join(temp_dir, 'config.json'))

        # # TODO: read the csvs in batches
        # for chunk in pd.read_csv(filename, chunksize=chunksize):
        # # chunk is a DataFrame. To "process" the rows in the chunk:
        # for index, row in chunk.iterrows():
        #     print(row)

        empty_dataframe = pd.DataFrame([{}])
        needed_data = {
            'config': config,
            'job_host_summary': empty_dataframe,
            'indirect_nodes': empty_dataframe,
            'main_jobevent': empty_dataframe,
            'main_host': empty_dataframe,
        }

        if self.csv_enabled('job_host_summary'):
            needed_data['job_host_summary'] = self.build_data_batch(temp_dir, 'job_host_summary')

        if self.csv_enabled('main_indirectmanagednodeaudit'):
            needed_data['indirect_nodes'] = self.build_data_batch(temp_dir, 'main_indirectmanagednodeaudit')

        if self.csv_enabled('main_jobevent'):
            needed_data['main_jobevent'] = self.build_data_batch(temp_dir, 'main_jobevent')

        if self.csv_enabled('main_host'):
            needed_data['main_host'] = self.build_data_batch(temp_dir, 'main_host')

        return needed_data

    def build_data_batch(self, temp_dir, file_name):
        """
        Builds the report with only the necessary sheets.
        """

        if os.path.exists(os.path.join(temp_dir, f'{file_name}.csv')):
            return pd.read_csv(os.path.join(temp_dir, f'{file_name}.csv'))
        else:
            return pd.DataFrame([{}])

    def csv_enabled(self, name):
        """Enable CSV extraction based on list of rendered sheets"""
        return self.sheet_enabled(CSV_SHEETS[name])

    def sheet_enabled(self, sheets_required):
        """
        Checks for METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS values and return those values.
        If none found, give it the default CCSP report sheet options.
        Returns a boolean so we know which sheets to provide in the report.
        """
        sheet_options = self.extra_params.get('optional_sheets')
        return bool(set(sheet_options) & set(sheets_required))


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


def _safe_extract(tar_path, extract_path, max_files=100, max_size=1024 * 1024 * 1024):
    """
    Safely extract a tar archive from 'tar_path' into 'extract_path' with constraints:
      - Only extract *.json or *.csv files
      - Skip directories, symbolic links, and hard links
      - Limit number of extracted files to 'max_files'
      - Limit total uncompressed size to 'max_size' bytes
    """
    extracted_files = 0
    total_extracted_size = 0

    # Ensure the extraction directory exists
    os.makedirs(extract_path, exist_ok=True)

    with tarfile.open(tar_path, 'r:*') as tar:
        for member in tar.getmembers():
            # 1) Skip directories and links
            if member.isdir():
                continue
            if member.issym() or member.islnk():
                print(f'Skipping link: {member.name}')
                continue

            # 2) Only allow .json or .csv
            if not (member.name.endswith('.json') or member.name.endswith('.csv')):
                continue

            # 3) Build a fully qualified path for this member
            #    and ensure it stays within extract_path.
            member_path = os.path.abspath(os.path.join(extract_path, member.name))
            extract_path_abs = os.path.abspath(extract_path)
            if not member_path.startswith(extract_path_abs + os.sep):
                print(f'Skipping potentially unsafe file (path traversal): {member.name}')
                continue

            # 4) Limit total files
            if extracted_files >= max_files:
                print(f'Reached max file limit of {max_files}.')
                break

            # 5) Extract file contents manually, in chunks,
            #    to avoid trusting the tar's metadata size.
            file_obj = tar.extractfile(member)
            if file_obj is None:
                # Could not read the file content for some reason
                continue

            # Make sure the subdirectory structure exists
            os.makedirs(os.path.dirname(member_path), exist_ok=True)

            # Write out the file, limiting max size
            total_extracted_size = _write_member(member_path, file_obj, max_size, total_extracted_size)

            extracted_files += 1

    print_debug(f'Extraction complete. Files extracted: {extracted_files}, Total size: {total_extracted_size} bytes.')
