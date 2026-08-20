import os

import boto3

from botocore.exceptions import ClientError

from metrics_utility.logger import logger


class S3Handler:
    def __init__(self, params):
        self.bucket_name = params.get('bucket_name')
        self.bucket_endpoint = params.get('bucket_endpoint')
        self.bucket_region = params.get('bucket_region')
        self.bucket_access_key = params.get('bucket_access_key')
        self.bucket_secret_key = params.get('bucket_secret_key')

        if bool(self.bucket_access_key) != bool(self.bucket_secret_key):
            raise ValueError(
                'bucket_access_key and bucket_secret_key must both be provided or both be omitted'
                ' (omit both to use implicit credentials such as IRSA or EC2 instance profiles)'
            )

        self._session = None

    @property
    def session(self):
        if self._session is not None:
            return self._session

        session_kwargs = {'region_name': self.bucket_region}
        if self.bucket_access_key and self.bucket_secret_key:
            session_kwargs['aws_access_key_id'] = self.bucket_access_key
            session_kwargs['aws_secret_access_key'] = self.bucket_secret_key

        session = boto3.Session(**session_kwargs)

        if not self.bucket_access_key and session.get_credentials() is None:
            raise ValueError(
                'Unable to locate AWS credentials. '
                'Set the METRICS_UTILITY_BUCKET_ACCESS_KEY and METRICS_UTILITY_BUCKET_SECRET_KEY '
                'environment variables, or configure implicit credentials '
                '(IRSA, EC2 instance profile, ~/.aws/credentials, etc.).'
            )

        self._session = session
        return self._session

    def get_s3_resource(self):
        return self.session.resource('s3', endpoint_url=self.bucket_endpoint)

    def get_s3_client(self):
        return self.session.client('s3', endpoint_url=self.bucket_endpoint)

    def get_s3_bucket(self):
        return self.get_s3_resource().Bucket(self.bucket_name)

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

    def download_file(self, s3_filename, local_filename):
        """
        :param s3_filename - request_id
        :param local_filename - path to tmp
        """
        client = self.get_s3_client()

        full_name = s3_filename  # os.path.join(s3_path, s3_filename) if s3_path else s3_filename
        try:
            client.download_file(self.bucket_name, full_name, local_filename)
            status = True
        except ClientError as e:
            code = int(e.response['Error']['Code'])
            if code == 404:
                status = False
            else:
                raise e

        return status

    def list_files(self, prefix):
        s3_resource = self.get_s3_resource()

        paginator = s3_resource.meta.client.get_paginator('list_objects')
        for resp in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for ret_value in resp.get('Contents', []):
                yield ret_value['Key']
