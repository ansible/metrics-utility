#!/bin/bash
set -e

echo "Waiting for Ceph RGW to be ready..."
until curl -s http://ceph-rgw:7480 >/dev/null 2>&1; do
  sleep 2
done
sleep 3

echo "Ceph RGW is ready! Setting up S3..."

# Ceph demo mode already creates a user with specified credentials
# Let's verify and create the bucket
ACCESS_KEY="${CEPH_DEMO_ACCESS_KEY:-myuseraccesskey}"
SECRET_KEY="${CEPH_DEMO_SECRET_KEY:-myusersecretkey}"
BUCKET_NAME="${CEPH_DEMO_BUCKET:-metricsutilitys3}"
ENDPOINT="http://ceph-rgw:7480"

# Install AWS CLI for S3 operations
echo "Installing AWS CLI..."
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
cd /tmp
unzip -q awscliv2.zip
./aws/install

# Configure AWS CLI
mkdir -p ~/.aws
cat > ~/.aws/credentials << EOF
[default]
aws_access_key_id = $ACCESS_KEY
aws_secret_access_key = $SECRET_KEY
EOF

cat > ~/.aws/config << EOF
[default]
region = us-east-1
output = json
EOF

# Wait a bit more for RGW to be fully ready
sleep 5

# Create bucket using AWS CLI
echo "Creating bucket: $BUCKET_NAME..."
if aws s3 mb "s3://$BUCKET_NAME" --endpoint-url "$ENDPOINT" 2>/dev/null; then
  echo "✓ Bucket created successfully"
else
  echo "✓ Bucket already exists or created"
fi

# Verify bucket
echo "Verifying bucket..."
if aws s3 ls --endpoint-url "$ENDPOINT" | grep -q "$BUCKET_NAME"; then
  echo "✓ Bucket verification successful"
else
  echo "✗ Bucket verification failed"
  exit 1
fi

# Test upload/download
echo "Testing S3 operations..."
echo "Hello from Ceph RGW!" > /tmp/test-file.txt
if aws s3 cp /tmp/test-file.txt "s3://$BUCKET_NAME/test-file.txt" --endpoint-url "$ENDPOINT" 2>/dev/null; then
  echo "✓ Upload test successful"
else
  echo "✗ Upload test failed"
  exit 1
fi

if aws s3 cp "s3://$BUCKET_NAME/test-file.txt" /tmp/test-download.txt --endpoint-url "$ENDPOINT" 2>/dev/null; then
  echo "✓ Download test successful"
else
  echo "✗ Download test failed"
  exit 1
fi

# Cleanup test file
aws s3 rm "s3://$BUCKET_NAME/test-file.txt" --endpoint-url "$ENDPOINT" 2>/dev/null || true

echo ""
echo "=== Ceph RGW Setup Complete ==="
echo ""
echo "S3 Credentials:"
echo "  Access Key: $ACCESS_KEY"
echo "  Secret Key: $SECRET_KEY"
echo "  Endpoint: $ENDPOINT"
echo "  Bucket: $BUCKET_NAME"
echo "  Region: us-east-1"
echo ""
echo "External endpoint (from host): http://localhost:9000"
echo ""

