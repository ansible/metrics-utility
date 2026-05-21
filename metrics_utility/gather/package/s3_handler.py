"""Thin boto3 wrapper for common S3 operations used by billing packages and savers."""

import os

import boto3

from botocore.exceptions import ClientError

from metrics_utility.logger import logger


class S3Handler:
    """Wrapper around boto3 providing upload, download, and list operations for a single S3 bucket."""

    def __init__(self, params):
        """Initialise the handler with S3 connection parameters.

        Args:
            params: Dict with optional keys ``'bucket_name'``, ``'bucket_endpoint'``,
                ``'bucket_region'``, ``'bucket_access_key'``, and ``'bucket_secret_key'``.
        """
        self.bucket_name = params.get('bucket_name')
        self.bucket_endpoint = params.get('bucket_endpoint')
        self.bucket_region = params.get('bucket_region')
        self.bucket_access_key = params.get('bucket_access_key')
        self.bucket_secret_key = params.get('bucket_secret_key')

        self._session = None

    @property
    def session(self):
        if self._session is not None:
            return self._session

        self._session = boto3.Session(
            aws_access_key_id=self.bucket_access_key,
            aws_secret_access_key=self.bucket_secret_key,
            region_name=self.bucket_region,
        )
        return self._session

    def get_s3_resource(self):
        """Return a boto3 S3 resource connected to the configured endpoint.

        Returns:
            A :class:`boto3.resources.factory.s3.ServiceResource` instance.
        """
        return self.session.resource('s3', endpoint_url=self.bucket_endpoint)

    def upload_file(self, file_name, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        try:
            s3_resource = self.get_s3_resource()
            s3_resource.meta.client.upload_file(file_name, self.bucket_name, object_name)
        except ClientError as e:
            logger.error(e)
            return False
