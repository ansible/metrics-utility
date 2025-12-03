#!/bin/bash
# Helper script to get Ceph RGW S3 credentials

echo "Fetching Ceph RGW credentials..."
echo ""

# Check if container is running
if ! docker ps | grep -q ceph-rgw; then
    echo "Error: ceph-rgw container is not running"
    echo "Start it with: docker-compose up -d ceph-rgw"
    exit 1
fi

# Get user info from radosgw-admin
USER_INFO=$(docker exec ceph-rgw radosgw-admin user info --uid=demo-user 2>/dev/null)

if [ $? -eq 0 ]; then
    ACCESS_KEY=$(echo "$USER_INFO" | grep -A 1 '"access_key"' | tail -1 | sed 's/.*"\(.*\)".*/\1/')
    SECRET_KEY=$(echo "$USER_INFO" | grep -A 1 '"secret_key"' | tail -1 | sed 's/.*"\(.*\)".*/\1/')
    
    echo "=== Ceph RGW Credentials ==="
    echo ""
    echo "User: demo-user"
    echo "Access Key: $ACCESS_KEY"
    echo "Secret Key: $SECRET_KEY"
    echo ""
    echo "Environment variables:"
    echo "export METRICS_UTILITY_BUCKET_ACCESS_KEY=$ACCESS_KEY"
    echo "export METRICS_UTILITY_BUCKET_SECRET_KEY=$SECRET_KEY"
    echo "export METRICS_UTILITY_BUCKET_ENDPOINT=http://localhost:9000"
    echo "export METRICS_UTILITY_BUCKET_NAME=metricsutilitys3"
    echo "export METRICS_UTILITY_BUCKET_REGION=us-east-1"
    echo ""
else
    echo "Could not retrieve credentials. Check if ceph-rgw is properly initialized."
    echo ""
    echo "Default credentials from docker-compose.yaml:"
    echo "export METRICS_UTILITY_BUCKET_ACCESS_KEY=myuseraccesskey"
    echo "export METRICS_UTILITY_BUCKET_SECRET_KEY=myusersecretkey"
    echo "export METRICS_UTILITY_BUCKET_ENDPOINT=http://localhost:9000"
    echo "export METRICS_UTILITY_BUCKET_NAME=metricsutilitys3"
    echo "export METRICS_UTILITY_BUCKET_REGION=us-east-1"
    echo ""
fi

