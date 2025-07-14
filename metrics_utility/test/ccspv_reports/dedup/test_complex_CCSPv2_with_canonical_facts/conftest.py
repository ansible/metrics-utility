

def pytest_addoption(parser):
    """Add command line options for bidirectional regeneration between MD files and tarballs.

    Usage examples:

    # Regenerate MD files from tarball CSV data (when CSV data changes):
    docker compose -f tools/docker/docker-compose.yaml exec metrics-utility-env \
        uv run pytest metrics_utility/test/ccspv_reports/dedup/test_complex_CCSPv2_with_canonical_facts/ \
        test_complex_CCSPv2_with_canonical_facts.py::test_command_with_extended_canonical_facts \
        --regenerate-md

    # Regenerate tarball CSV data from MD files (when MD files are manually edited):
    docker compose -f tools/docker/docker-compose.yaml exec metrics-utility-env \
        uv run pytest metrics_utility/test/ccspv_reports/dedup/test_complex_CCSPv2_with_canonical_facts/ \
        test_complex_CCSPv2_with_canonical_facts.py::test_command_with_extended_canonical_facts \
        --regenerate-tarballs

    # Regenerate both (useful for initial setup or major changes):
    docker compose -f tools/docker/docker-compose.yaml exec metrics-utility-env \
        uv run pytest metrics_utility/test/ccspv_reports/dedup/test_complex_CCSPv2_with_canonical_facts/ \
        test_complex_CCSPv2_with_canonical_facts.py::test_command_with_extended_canonical_facts \
        --regenerate-md --regenerate-tarballs
    """
    parser.addoption(
        "--regenerate-md",
        action="store_true",
        default=False,
        help="Regenerate MD files from CSV data extracted from tarballs"
    )
    parser.addoption(
        "--regenerate-tarballs",
        action="store_true",
        default=False,
        help="Regenerate tarball CSV data from existing MD files"
    )
