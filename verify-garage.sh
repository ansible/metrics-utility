#!/bin/bash
# Quick verification script for Garage S3 setup

set -e

echo "=== Garage Setup Verification ==="
echo ""

# Check if docker/podman is available
if command -v docker &> /dev/null; then
    DOCKER_CMD="docker"
elif command -v podman &> /dev/null; then
    DOCKER_CMD="podman"
else
    echo "Error: Neither docker nor podman found"
    exit 1
fi

echo "Using: $DOCKER_CMD"
echo ""

# Check if garage container is running
echo "1. Checking Garage container status..."
if $DOCKER_CMD ps | grep -q garage; then
    echo "   ✓ Garage container is running"
else
    echo "   ✗ Garage container is not running"
    echo "   Run: docker compose -f tools/docker/docker-compose.yaml up -d"
    exit 1
fi
echo ""

# Check Garage health endpoint
echo "2. Checking Garage health..."
if curl -s http://localhost:3900/health > /dev/null; then
    echo "   ✓ Garage is healthy"
else
    echo "   ✗ Garage health check failed"
    exit 1
fi
echo ""

# Check if uv is available (check venv first, then PATH)
echo "3. Checking for uv..."
if [ -f ".venv/bin/uv" ]; then
    UV_CMD=".venv/bin/uv"
    echo "   ✓ Using uv from .venv"
elif command -v uv &> /dev/null; then
    UV_CMD="uv"
    echo "   ✓ Using uv from PATH"
else
    echo "   ✗ uv not found. Please install it with: pip install uv"
    echo "   Or run: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi
echo ""

# Test S3 API with Python
echo "4. Testing S3 API with boto3..."

# Use environment variables if set, otherwise use generated Garage credentials
ACCESS_KEY="${METRICS_UTILITY_BUCKET_ACCESS_KEY:-GK160f780fdab0f547be610987}"
SECRET_KEY="${METRICS_UTILITY_BUCKET_SECRET_KEY:-078af014efeae3636bd090275e3cbd9624b44625f9597a5723eb8a6b9c8fe113}"

$UV_CMD run python << EOF
import boto3
try:
    client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='$ACCESS_KEY',
        aws_secret_access_key='$SECRET_KEY',
        region_name='us-east-1'
    )
    buckets = client.list_buckets()
    bucket_names = [b['Name'] for b in buckets.get('Buckets', [])]
    
    if 'metricsutilitys3' in bucket_names:
        print("   ✓ S3 API is working")
        print(f"   ✓ Found bucket: metricsutilitys3")
    else:
        print("   ✗ Bucket 'metricsutilitys3' not found")
        print(f"   Found buckets: {bucket_names}")
        exit(1)
except Exception as e:
    print(f"   ✗ S3 API test failed: {e}")
    exit(1)
EOF
echo ""

# Test file upload/download
echo "5. Testing S3 operations..."
$UV_CMD run python << EOF
import boto3

try:
    client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='$ACCESS_KEY',
        aws_secret_access_key='$SECRET_KEY',
        region_name='us-east-1'
    )
    
    # Upload test
    test_key = 'test-verification-file.txt'
    test_content = b'Hello from Garage!'
    client.put_object(Bucket='metricsutilitys3', Key=test_key, Body=test_content)
    print("   ✓ Upload successful")
    
    # Download test
    response = client.get_object(Bucket='metricsutilitys3', Key=test_key)
    downloaded = response['Body'].read()
    
    if downloaded == test_content:
        print("   ✓ Download successful")
    else:
        print("   ✗ Download content mismatch")
        exit(1)
    
    # Cleanup
    client.delete_object(Bucket='metricsutilitys3', Key=test_key)
    print("   ✓ Delete successful")
    
except Exception as e:
    print(f"   ✗ S3 operations failed: {e}")
    exit(1)
EOF
echo ""

echo "=== All checks passed! ==="
echo ""
echo "Garage is properly configured and working."
echo "You can now run the test suite with:"
echo "  uv run pytest -s -v metrics_utility/test/library/test_storage_s3.py"
echo ""
