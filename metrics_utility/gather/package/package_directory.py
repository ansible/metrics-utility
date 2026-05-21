"""Package implementation that ships billing tarballs to a local directory."""

import os
import shutil

from metrics_utility.gather.package.package_local import PackageLocal


class PackageDirectory(PackageLocal):
    """Package that copies the generated tarball into a local date-partitioned directory."""

    def _do_ship(self, destination_path):
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        shutil.copyfile(self.tar_path, destination_path)
