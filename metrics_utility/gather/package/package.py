import os
import pathlib
import shutil
import tarfile

from django.conf import settings

from metrics_utility.gather.collection.collection_data_status import CollectionDataStatus
from metrics_utility.gather.collection.collection_manifest import CollectionManifest
from metrics_utility.gather.package import crc_handler
from metrics_utility.gather.package.s3_handler import S3Handler
from metrics_utility.logger import logger


class Package:
    MAX_DATA_SIZE = 200 * 1048576

    def __init__(self, collector):
        self.collector = collector
        self.collections = []
        self.collection_keys = []
        self.data_collection_status = CollectionDataStatus(self.collector, self)
        self.manifest = CollectionManifest(collector)
        self.processed = False
        self.shipping_successful = None
        self.tar_path = None
        self.total_data_size = 0

    def add_collection(self, collection):
        self.collections.append(collection)
        self.collection_keys.append(collection.key)
        self.total_data_size = self.total_data_size + collection.data_size()

    def is_key_used(self, key):
        return key in self.collection_keys

    def delete_collected_files(self):
        for collection in self.collections:
            collection.cleanup()

    def has_free_space(self, requested_size):
        return self.total_data_size + requested_size <= self.MAX_DATA_SIZE

    def is_shipping_configured(self):
        if not self.tar_path:
            logger.error('Insights for Ansible Automation Platform TAR not found')
            return False

        if not os.path.exists(self.tar_path):
            logger.error(f'Insights for Ansible Automation Platform TAR {self.tar_path} not found')
            return False

        if 'Error:' in str(self.tar_path):
            return False

        if self.collector.ship_target == 'crc':
            if not crc_handler.is_shipping_configured():
                return False

        return True

    def make_tgz(self):
        target = self.collector.tmp_dir.parent
        try:
            tarname_base = self._tarname_base()
            path = pathlib.Path(target)
            index = len(list(path.glob(f'{tarname_base}-*.*')))
            tarname = f'{tarname_base}-{index}-unknown.tar.gz'

            with tarfile.open(target.joinpath(tarname), 'w:gz') as f:
                for collection in self.collections:
                    self._collection_to_tar(f, collection)

                self._config_to_tar(f)

                self._data_collection_status_to_tar(f)

                self._manifest_to_tar(f)

                self.tar_path = f.name

            try:
                orig_path = self.tar_path
                new_path = orig_path.replace('-unknown.', f'-{collection.key}.')
                os.rename(orig_path, new_path)
                self.tar_path = new_path
            except Exception as e:
                logger.error(f'Failed to identify collection type: {e}')

            return True
        except Exception as e:
            logger.exception(f'Failed to write analytics archive file: {e}')
            return False

    def ship(self):
        if not self.is_shipping_configured():
            self.shipping_successful = False
            return False

        logger.debug(f'shipping analytics file: {self.tar_path}')

        if self.collector.ship_target == 'crc':
            self.shipping_successful = crc_handler.ship(self.tar_path)
        elif self.collector.ship_target == 's3':
            destination_path = self._local_destination_path()
            S3Handler(params=self.collector.billing_provider_params).upload_file(self.tar_path, object_name=destination_path)
            self.shipping_successful = True
        else:
            destination_path = self._local_destination_path()
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            shutil.copyfile(self.tar_path, destination_path)
            self.shipping_successful = True

        logger.debug(f'shipping successful: {self.shipping_successful}')
        return self.shipping_successful

    def update_last_gathered_entries(self, updates_dict):
        if self.shipping_successful:
            for collection in self.collections:
                collection.update_last_gathered_entries(updates_dict)

    #
    # Private methods ---------------------------
    #

    def _tarname_base(self):
        if self.collector.ship_target == 'crc':
            timestamp = self.collector.gather_until
            return f'{settings.SYSTEM_UUID}-{timestamp.strftime("%Y-%m-%d-%H%M%S%z")}'
        else:
            since, until = self._batch_since_and_until()
            return f'{settings.INSTALL_UUID}-{since.strftime("%Y-%m-%d-%H%M%S%z")}-{until.strftime("%Y-%m-%d-%H%M%S%z")}'

    def _batch_since_and_until(self):
        return self.collections[0].since, self.collections[0].until

    def _local_destination_path(self):
        since, _ = self._batch_since_and_until()
        base_path = self.collector.billing_provider_params['ship_path']
        filename = os.path.basename(self.tar_path)

        year = since.strftime('%Y')
        month = since.strftime('%m')
        day = since.strftime('%d')

        return os.path.join(base_path, f'data/{year}/{month}/{day}', filename)

    def _collection_to_tar(self, tar, collection):
        try:
            if not collection.is_empty():
                collection.add_to_tar(tar)
                self.manifest.add_collection(collection)
        except Exception as e:
            logger.exception(f'Could not generate metric {collection.filename}: {e}')
            return None

    def _config_to_tar(self, tar):
        if self.collector.collections['config'] is None:
            logger.error("'config' collector data is missing, and is required to ship.")
            return False
        else:
            self._collection_to_tar(tar, self.collector.collections['config'])

        return True

    def _data_collection_status_to_tar(self, tar):
        try:
            self.data_collection_status.gather()
            self.data_collection_status.add_to_tar(tar)
            self.manifest.add_collection(self.data_collection_status)
        except Exception as e:
            logger.exception(f'Could not generate {self.data_collection_status.filename}: {e}')

    def _manifest_to_tar(self, tar):
        try:
            self.manifest.gather()
            self.manifest.add_to_tar(tar)
            self.add_collection(self.manifest)
        except Exception as e:
            logger.exception(f'Could not generate {self.manifest.filename}: {e}')
