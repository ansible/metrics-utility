"""Test release selection without networking or the project's Python dependencies."""

import json
import subprocess
import unittest

from pathlib import Path


ARCHIVE = 'linux_amd64.tar.gz'
CHECKSUM = f'{ARCHIVE}.md5'
SELECTOR = Path(__file__).with_name('select_seaweedfs_release.jq')


def release(tag, published_at, assets=(ARCHIVE, CHECKSUM), **overrides):
    """Build inert GitHub release metadata, with asset URLs tied to the tag."""
    return {
        'tag_name': tag,
        'published_at': published_at,
        'draft': False,
        'prerelease': False,
        'assets': [{'name': name, 'state': 'uploaded', 'browser_download_url': f'https://example.invalid/{tag}/{name}'} for name in assets],
        **overrides,
    }


class SelectSeaweedFSReleaseTests(unittest.TestCase):
    """Require a complete stable release, not just the newest tag or an archive alone."""

    def select(self, pages):
        """Execute the same jq filter as the workflow."""
        return subprocess.run(
            ['jq', '--exit-status', '--raw-output', '--from-file', str(SELECTOR)],
            input=json.dumps(pages),
            capture_output=True,
            text=True,
            check=False,
        )

    def assert_selected(self, pages, tag):
        """Check that the archive and checksum both belong to the selected release."""
        result = self.select(pages)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip().split('\t'),
            [tag, f'https://example.invalid/{tag}/{ARCHIVE}', f'https://example.invalid/{tag}/{CHECKSUM}'],
        )

    def test_fall_back_when_latest_release_has_no_linux_assets(self):
        self.assert_selected(
            [[release('4.49', '2026-10-23T07:53:00Z', assets=()), release('4.48', '2026-09-28T15:53:48Z')]],
            '4.48',
        )

    def test_advance_when_newest_release_has_both_assets(self):
        self.assert_selected([[release('4.49', '2026-10-23T07:53:00Z'), release('4.48', '2026-09-28T15:53:48Z')]], '4.49')

    def test_do_not_mix_archive_and_checksum_from_different_releases(self):
        self.assert_selected(
            [
                [
                    release('archive-only', '2026-10-23T00:00:00Z', assets=(ARCHIVE,)),
                    release('checksum-only', '2026-10-22T00:00:00Z', assets=(CHECKSUM,)),
                    release('complete', '2026-10-21T00:00:00Z'),
                ]
            ],
            'complete',
        )

    def test_ignore_drafts_and_prereleases(self):
        self.assert_selected(
            [
                [
                    release('draft', '2026-10-23T00:00:00Z', draft=True),
                    release('prerelease', '2026-10-22T00:00:00Z', prerelease=True),
                    release('unpublished', None),
                    release('stable', '2026-10-21T00:00:00Z'),
                ]
            ],
            'stable',
        )

    def test_search_every_page_and_compare_publication_dates_not_tags(self):
        self.assert_selected(
            [
                [release('9.0', '2026-09-28T00:00:00Z')],
                [release('10.0', '2026-10-23T00:00:00Z')],
                [release('11.0', '2026-10-22T00:00:00Z')],
            ],
            '10.0',
        )

    def test_skip_assets_that_are_still_uploading(self):
        uploading = release('uploading', '2026-10-23T00:00:00Z')
        uploading['assets'][0]['state'] = 'new'
        self.assert_selected([[uploading, release('complete', '2026-10-22T00:00:00Z')]], 'complete')

    def test_fail_clearly_when_no_compatible_release_exists(self):
        for pages in ([], [[]], [[release('incomplete', '2026-10-23T00:00:00Z', assets=(ARCHIVE,))]]):
            with self.subTest(pages=pages):
                result = self.select(pages)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, '')
                self.assertIn('No stable SeaweedFS release has both uploaded', result.stderr)


if __name__ == '__main__':
    unittest.main()
