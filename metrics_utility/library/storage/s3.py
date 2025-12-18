import fnmatch
import os
import tempfile

from contextlib import contextmanager

import boto3

from .helpers import load_csv, load_json, load_parquet
from .util import date_filter, dict_to_json_file


class StorageS3:
    def __init__(self, **settings):
        self.bucket = settings.get('bucket')
        self.endpoint = settings.get('endpoint')
        self.region = settings.get('region')
        self.access_key = settings.get('access_key')
        self.secret_key = settings.get('secret_key')

        if not self.bucket:
            raise Exception('StorageS3: bucket not set')

        self._client = None

    @property
    def client(self):
        if self._client is not None:
            return self._client

        self._client = boto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
        ).client('s3', endpoint_url=self.endpoint)

        return self._client

    # FIXME: also used by ExtractorS3 for now, replace with glob
    def list_files(self, prefix):
        paginator = self.client.get_paginator('list_objects')
        for resp in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for ret_value in resp.get('Contents', []):
                yield ret_value['Key']

    def glob(self, pattern, since=None, until=None):
        prefix = pattern.split('*')[0]
        globbed = fnmatch.filter(self.list_files(prefix), pattern)

        if not since and not until:
            return globbed

        return [filename for filename in globbed if date_filter(filename, since, until)]

    @contextmanager
    def get(self, remote):
        with tempfile.TemporaryDirectory() as directory:
            local_filename = os.path.join(directory, remote.split('/')[-1])
            self.client.download_file(Bucket=self.bucket, Key=remote, Filename=local_filename)
            yield local_filename

    def get_data(self, remote, format='auto'):
        """
        Retrieve data from S3 and return it parsed.

        Args:
            remote: Path to the object in S3
            format: Format of the data - 'auto' (detect from extension),
                   'json', 'csv', or 'parquet'

        Returns:
            For JSON: dict or list
            For CSV: list of dicts
            For Parquet: pandas DataFrame

        Raises:
            ValueError: If format is unsupported or cannot be auto-detected
            Exception: If the S3 object doesn't exist or download fails
        """
        # Auto-detect format from file extension
        if format == 'auto':
            remote_lower = remote.lower()
            if remote_lower.endswith('.json'):
                format = 'json'
            elif remote_lower.endswith('.csv'):
                format = 'csv'
            elif remote_lower.endswith('.parquet'):
                format = 'parquet'
            else:
                raise ValueError(f"Cannot auto-detect format for '{remote}'. Please specify format explicitly: 'json', 'csv', or 'parquet'")

        # Load the data using the appropriate helper
        with self.get(remote) as filename:
            if format == 'json':
                return load_json(filename)
            elif format == 'csv':
                return load_csv(filename)
            elif format == 'parquet':
                return load_parquet(filename)
            else:
                raise ValueError(f"Unsupported format: '{format}'. Supported formats: 'auto', 'json', 'csv', 'parquet'")

    def put(self, remote, *, filename=None, fileobj=None, dict=None):
        if filename:
            self.client.upload_file(Filename=filename, Bucket=self.bucket, Key=remote)

        if fileobj:
            self.client.upload_fileobj(Fileobj=fileobj, Bucket=self.bucket, Key=remote)

        if dict:
            with dict_to_json_file(dict) as filename:
                self.client.upload_file(Filename=filename, Bucket=self.bucket, Key=remote)

    def remove(self, remote):
        self.client.delete_object(Bucket=self.bucket, Key=remote)

    def exists(self, remote):
        # list_files uses remote as prefix, so we need exact match
        return remote in list(self.list_files(remote))
