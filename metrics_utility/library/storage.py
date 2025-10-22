from contextlib import contextmanager

import os
import json

from .debug import indent, log

from datetime import datetime


class StorageCRCMutual:
    def __init__(self, settings):
        log('library.storage StorageCRCMutual.__init__')
        self.settings = settings

    def ship(self, data):
        log('library.storage StorageCRCMutual.ship')
        return True


class StorageS3:
    def __init__(self, settings):
        log('library.storage StorageS3.__init__')
        self.settings = settings

    def ship(self, tarball):
        log('library.storage StorageS3.ship')
        return True

    def glob(self, glob, since=None, until=None):
        log('library.storage StorageS3.glob')
        return [f'fake-file-{i}.tar.gz' for i in range(3)]

    @contextmanager
    def get(self, remote):
        log('library.storage StorageS3.get')
        indent(1)
        yield f'/tmp/local-{remote}'
        indent(-1)
        log('/library.storage StorageS3.get')

    def put(self, path=None, data=None, file=None):
        log('library.storage StorageS3.put')
        return True

    def remove(self, files):
        log('library.storage StorageS3.remove')
        return True


class StorageCRC:
    def __init__(self, settings):
        log('library.storage StorageCRC.__init__')
        self.settings = settings

    def ship(self, data):
        log('library.storage StorageCRC.ship')
        return True


class StorageDirectory:
    def __init__(self, settings):
        log('library.storage StorageDirectory.__init__')
        self.settings = settings

    def ship(self, data):
        log('library.storage StorageDirectory.ship')
        return True

    def glob(self, glob, since=None, until=None):
        log('library.storage StorageDirectory.glob')
        return [f'fake-file-{i}.tar.gz' for i in range(3)]

    @contextmanager
    def get(self, remote):
        log('library.storage StorageDirectory.get')
        indent(1)
        yield f'/tmp/local-{remote}'
        indent(-1)
        log('/library.storage StorageDirectory.get')

    def put(self, path=None, data=None, file=None):
        log('library.storage StorageDirectory.put')
        return True

    def remove(self, files):
        log('library.storage StorageDirectory.remove')
        return True


class StorageSegment:
    def __init__(self, **segment_config):
        log('library.storage StorageSegment.__init__')
        self.segment_config = segment_config
        self.write_key = segment_config.get('write_key')
        self.endpoint = segment_config.get('endpoint', 'https://api.segment.io/v1/track')
        self.debug = segment_config.get('debug', False)

    def put(self, name, filename):
        """
        Upload an artifact (tarball, parquet file, etc.) to Segment analytics
        
        Args:
            name: Identifier for the uploaded artifact
            filename: Path to the file to upload
        """
        log(f'library.storage StorageSegment.put name={name} filename={filename}')

        _, ext = os.path.splitext(filename)

        if ext == ".json" or ext == ".jsn":
            with open(filename) as f:
                data = json.loads(f.read())
        else:
            raise Exception(f"Unsupported upload type {ext} in filename")
        
        try:
            import segment.analytics as analytics
            
            # Configure Segment client
            analytics.write_key = self.write_key
            analytics.debug = self.debug
            
            # Send a track event for the uploaded artifact
            analytics.track(
                user_id=self.segment_config.get('user_id', 'unknown'),
                event='Metrics Artifact Upload',
                properties={
                    'artifact_name': name,
                    'data': data,
                    'upload_timestamp': datetime.now().isoformat(),
                }
            )
            
            # Flush to ensure event is sent
            analytics.flush()
            
            log(f'Successfully uploaded {name} to Segment')
            return True
            
        except ImportError:
            log('analytics-python package not installed')
            return False
        except Exception as e:
            log(f'Failed to upload to Segment: {e}')
            return False
