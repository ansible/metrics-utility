#!/bin/bash
# Sync requirements files from uv.lock
# This script generates both production and development requirements files

set -e

# Parse command line arguments
CHECK_ONLY=false
if [[ "${1:-}" == "--check" ]]; then
    CHECK_ONLY=true
fi

if [[ "$CHECK_ONLY" == "true" ]]; then
    echo "🔍 Checking if requirements files are in sync with uv.lock..."
else
    echo "🔄 Syncing requirements files from uv.lock..."
fi

# Check if uv is installed and find the correct path
UV_CMD="uv"
if ! command -v uv &> /dev/null; then
    # Try finding uv in the virtual environment
    if [[ -n "${VIRTUAL_ENV:-}" ]] && [[ -f "${VIRTUAL_ENV}/bin/uv" ]]; then
        UV_CMD="${VIRTUAL_ENV}/bin/uv"
    elif [[ -f ".venv/bin/uv" ]]; then
        UV_CMD=".venv/bin/uv"
    else
        echo "❌ Error: uv is not installed or not in PATH"
        echo "Please install uv first: https://docs.astral.sh/uv/getting-started/installation/"
        exit 1
    fi
fi

if [[ "$CHECK_ONLY" == "true" ]]; then
    # Check mode: generate temporary files and compare
    $UV_CMD export --format requirements-txt --no-dev > requirements.txt.tmp 2>/dev/null
    $UV_CMD export --format requirements-txt > dev-requirements.txt.tmp 2>/dev/null
    
    SYNC_NEEDED=false
    
    if [[ ! -f "requirements.txt" ]]; then
        echo "❌ requirements.txt not found"
        SYNC_NEEDED=true
    elif ! diff -q requirements.txt requirements.txt.tmp > /dev/null 2>&1; then
        echo "❌ requirements.txt is out of sync with uv.lock"
        SYNC_NEEDED=true
    fi
    
    if [[ ! -f "dev-requirements.txt" ]]; then
        echo "❌ dev-requirements.txt not found"
        SYNC_NEEDED=true
    elif ! diff -q dev-requirements.txt dev-requirements.txt.tmp > /dev/null 2>&1; then
        echo "❌ dev-requirements.txt is out of sync with uv.lock"
        SYNC_NEEDED=true
    fi
    
    # Clean up temp files
    rm -f requirements.txt.tmp dev-requirements.txt.tmp
    
    if [[ "$SYNC_NEEDED" == "true" ]]; then
        echo ""
        echo "💡 Run './sync-requirements.sh' or 'make sync-requirements' to fix."
        exit 1
    else
        echo "✅ Requirements files are in sync with uv.lock"
        exit 0
    fi
else
    # Sync mode: generate and update files
    echo "📦 Generating requirements.txt (production dependencies only)..."
    $UV_CMD export --format requirements-txt --no-dev > requirements.txt

    echo "🛠️  Generating dev-requirements.txt (all dependencies)..."
    $UV_CMD export --format requirements-txt > dev-requirements.txt

    echo "✅ Requirements files synced successfully!"
    echo ""
    echo "📄 Files generated:"
    echo "   - requirements.txt: $(wc -l < requirements.txt) lines (production only)"
    echo "   - dev-requirements.txt: $(wc -l < dev-requirements.txt) lines (includes dev tools)"
    echo ""
    echo "💡 These files are automatically synced when uv.lock changes via GitHub Actions."
fi
