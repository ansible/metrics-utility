import shutil

from unittest.mock import MagicMock, patch

from metrics_utility.gather.package.package import Package
from metrics_utility.test.gather.support.analytics_collector import AnalyticsCollector
from metrics_utility.test.util import utcdt


def _make_collector():
    c = AnalyticsCollector(collection_type=AnalyticsCollector.DRY_RUN)
    c._gather_initialize(None, utcdt('2024-01-01'), utcdt('2024-01-02'))
    return c


def _cleanup(collector):
    if collector.tmp_dir:
        shutil.rmtree(collector.tmp_dir, ignore_errors=True)


# --- is_shipping_configured ---


def test_is_shipping_configured_no_tar_path():
    collector = _make_collector()
    try:
        pkg = Package(collector)
        assert pkg.is_shipping_configured() is False
    finally:
        _cleanup(collector)


def test_is_shipping_configured_tar_not_found():
    collector = _make_collector()
    try:
        pkg = Package(collector)
        pkg.tar_path = '/nonexistent/path.tar.gz'
        assert pkg.is_shipping_configured() is False
    finally:
        _cleanup(collector)


def test_is_shipping_configured_crc_not_configured():
    collector = _make_collector()
    collector.ship_target = 'crc'
    try:
        pkg = Package(collector)
        pkg.tar_path = __file__  # any existing file
        with patch('metrics_utility.gather.package.package.crc_handler') as mock_crc:
            mock_crc.is_shipping_configured.return_value = False
            assert pkg.is_shipping_configured() is False
    finally:
        _cleanup(collector)


# --- ship ---


def test_ship_not_configured():
    collector = _make_collector()
    try:
        pkg = Package(collector)
        result = pkg.ship()
        assert result is False
        assert pkg.shipping_successful is False
    finally:
        _cleanup(collector)


def test_ship_crc():
    collector = _make_collector()
    collector.ship_target = 'crc'
    try:
        pkg = Package(collector)
        pkg.tar_path = __file__
        with patch('metrics_utility.gather.package.package.crc_handler') as mock_crc:
            mock_crc.is_shipping_configured.return_value = True
            mock_crc.ship.return_value = None
            result = pkg.ship()
        assert result is True
        assert pkg.shipping_successful is True
        mock_crc.ship.assert_called_once_with(__file__)
    finally:
        _cleanup(collector)


def test_ship_exception():
    collector = _make_collector()
    collector.ship_target = 'crc'
    try:
        pkg = Package(collector)
        pkg.tar_path = __file__
        with patch('metrics_utility.gather.package.package.crc_handler') as mock_crc:
            mock_crc.is_shipping_configured.return_value = True
            mock_crc.ship.side_effect = RuntimeError('network error')
            result = pkg.ship()
        assert result is False
        assert pkg.shipping_successful is False
    finally:
        _cleanup(collector)


# --- make_tgz error paths ---


def test_make_tgz_rename_exception():
    collector = _make_collector()
    try:
        pkg = Package(collector)

        mock_collection = MagicMock()
        mock_collection.key = 'test'
        mock_collection.is_empty.return_value = False
        mock_collection.filename = 'test.json'
        mock_collection.version = '1.0'
        mock_collection.output_format = 'json'
        mock_collection.data = '{"test": true}'
        mock_collection.data_size.return_value = 10
        pkg.add_collection(mock_collection)

        collector.config_collection = MagicMock()
        collector.config_collection.is_empty.return_value = True

        with patch('os.rename', side_effect=OSError('rename failed')):
            result = pkg.make_tgz()
        assert result is True
        assert pkg.tar_path is not None
    finally:
        _cleanup(collector)


def test_make_tgz_exception():
    collector = _make_collector()
    try:
        pkg = Package(collector)
        collector.tmp_dir = None  # will cause exception in make_tgz
        result = pkg.make_tgz()
        assert result is False
    finally:
        pass


# --- _collection_to_tar exception ---


def test_collection_to_tar_exception():
    collector = _make_collector()
    try:
        pkg = Package(collector)

        mock_collection = MagicMock()
        mock_collection.is_empty.return_value = False
        mock_collection.add_to_tar.side_effect = RuntimeError('tar error')
        mock_collection.filename = 'broken.csv'

        mock_tar = MagicMock()
        result = pkg._collection_to_tar(mock_tar, mock_collection)
        assert result is None
    finally:
        _cleanup(collector)


# --- _config_to_tar missing config ---


def test_config_to_tar_missing():
    collector = _make_collector()
    collector.config_collection = None
    try:
        pkg = Package(collector)
        mock_tar = MagicMock()
        result = pkg._config_to_tar(mock_tar)
        assert result is False
    finally:
        _cleanup(collector)


# --- _data_collection_status_to_tar exception ---


def test_data_collection_status_to_tar_exception():
    collector = _make_collector()
    try:
        pkg = Package(collector)
        mock_tar = MagicMock()
        mock_tar.addfile.side_effect = RuntimeError('write error')

        mock_collection = MagicMock()
        mock_collection.gathering_successful = True
        mock_collection.gathering_started_at = utcdt('2024-01-01')
        mock_collection.gathering_finished_at = utcdt('2024-01-01T00:01:00')
        mock_collection.since = utcdt('2024-01-01')
        mock_collection.until = utcdt('2024-01-02')
        mock_collection.filename = 'test.csv'
        pkg.collections = [mock_collection]

        pkg._data_collection_status_to_tar(mock_tar)
        # should not raise, just log
    finally:
        _cleanup(collector)


# --- _manifest_to_tar exception ---


def test_manifest_to_tar_exception():
    collector = _make_collector()
    try:
        pkg = Package(collector)
        mock_tar = MagicMock()
        mock_tar.addfile.side_effect = RuntimeError('write error')
        pkg.manifest_data = {'test.csv': '1.0'}

        pkg._manifest_to_tar(mock_tar)
        # should not raise, just log
    finally:
        _cleanup(collector)
