import fnmatch
import os
import tempfile

from contextlib import contextmanager

import boto3

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
