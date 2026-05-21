"""Thin boto3 wrapper for S3 upload used by Package shipping."""

import boto3


def upload_file(params, file_name, object_name):
    session = boto3.Session(
        aws_access_key_id=params.get('bucket_access_key'),
        aws_secret_access_key=params.get('bucket_secret_key'),
        region_name=params.get('bucket_region'),
    )
    resource = session.resource('s3', endpoint_url=params.get('bucket_endpoint'))
    resource.meta.client.upload_file(file_name, params.get('bucket_name'), object_name)
