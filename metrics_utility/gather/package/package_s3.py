"""Package implementation that ships billing tarballs to an S3-compatible object store."""

from metrics_utility.gather.base.s3_handler import S3Handler
from metrics_utility.gather.package.package_local import PackageLocal


class PackageS3(PackageLocal):
    """Package that uploads the generated tarball into a date-partitioned S3 prefix."""

    def _do_ship(self, destination_path):
        s3_handler = S3Handler(params=self.collector.billing_provider_params)
        s3_handler.upload_file(self.tar_path, object_name=destination_path)
