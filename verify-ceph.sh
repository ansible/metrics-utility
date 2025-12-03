#!/bin/bash
# Quick verification script for Ceph RGW S3 setup

set -e

echo "=== Ceph RGW Setup Verification ==="
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

# Check if ceph-rgw container is running
echo "1. Checking Ceph RGW container status..."
if $DOCKER_CMD ps | grep -q ceph-rgw; then
    echo "   ✓ Ceph RGW container is running"
else
    echo "   ✗ Ceph RGW container is not running"
    echo "   Run: docker compose -f tools/docker/docker-compose.yaml up -d"
    exit 1
fi
echo ""

# Check Ceph RGW endpoint
echo "2. Checking Ceph RGW health..."
if curl -s http://localhost:9000 > /dev/null 2>&1; then
    echo "   ✓ Ceph RGW is responding"
else
    echo "   ✗ Ceph RGW health check failed"
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

# Use environment variables if set, otherwise use default credentials
ACCESS_KEY="${METRICS_UTILITY_BUCKET_ACCESS_KEY:-myuseraccesskey}"
SECRET_KEY="${METRICS_UTILITY_BUCKET_SECRET_KEY:-myusersecretkey}"

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
    test_content = b'Hello from Ceph RGW!'
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
echo "Ceph RGW is properly configured and working."
echo "You can now run the test suite with:"
echo "  uv run pytest -s -v metrics_utility/test/library/test_storage_s3.py"
echo ""

