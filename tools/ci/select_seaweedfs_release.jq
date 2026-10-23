# Input: paginated release arrays from gh api --paginate --slurp.
# Output: tag, archive URL, and checksum URL from the newest compatible stable release.
[
  .[][]
  | select(.draft == false and .prerelease == false and .published_at != null)
  | . as $release
  | (.assets | map(select(.state == "uploaded")) | INDEX(.name)) as $assets
  | select($assets["linux_amd64.tar.gz"] != null and $assets["linux_amd64.tar.gz.md5"] != null)
  | {
      tag: $release.tag_name,
      published_at: $release.published_at,
      archive: $assets["linux_amd64.tar.gz"].browser_download_url,
      checksum: $assets["linux_amd64.tar.gz.md5"].browser_download_url
    }
]
| max_by(.published_at)
| if . == null then
    error("No stable SeaweedFS release has both uploaded linux_amd64.tar.gz and linux_amd64.tar.gz.md5 assets")
  else
    [.tag, .archive, .checksum] | @tsv
  end
