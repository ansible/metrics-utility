import datetime

import segment.analytics as analytics


class StorageSegment:
    def __init__(self, **settings):
        self.debug = settings.get('debug', False)
        self.user_id = settings.get('user_id', 'unknown')
        self.write_key = settings.get('write_key')

        if not self.write_key:
            raise Exception('StorageSegment: write_key not set')

    def put(self, artifact_name, *, filename=None, fileobj=None, dict=None):
        if filename or fileobj or not dict:
            raise Exception('StorageSegment: filename= & fileobj= not supported, use dict=')

        # Configure Segment client
        analytics.write_key = self.write_key
        analytics.debug = self.debug

        # Send a track event for the uploaded artifact
        analytics.track(
            user_id=self.user_id,
            event='Metrics Artifact Upload',
            properties={
                'artifact_name': artifact_name,
                'data': dict,
                'upload_timestamp': datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
            },
        )

        # Flush to ensure event is sent
        analytics.flush()
