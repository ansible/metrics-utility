# Ceph RGW S3 Credentials

Ceph RGW allows you to configure custom access keys through environment variables in the demo container.

## Current Credentials

The default credentials are configured in `docker-compose.yaml`:

```bash
METRICS_UTILITY_BUCKET_ACCESS_KEY=myuseraccesskey
METRICS_UTILITY_BUCKET_SECRET_KEY=myusersecretkey
```

## Using the Credentials

### Option 1: Export as Environment Variables

```bash
export METRICS_UTILITY_BUCKET_ACCESS_KEY=myuseraccesskey
export METRICS_UTILITY_BUCKET_SECRET_KEY=myusersecretkey
export METRICS_UTILITY_BUCKET_ENDPOINT=http://localhost:9000
export METRICS_UTILITY_BUCKET_NAME=metricsutilitys3
export METRICS_UTILITY_BUCKET_REGION=us-east-1
```

### Option 2: Create a `.env` file

Create `.env` in the project root:

```bash
METRICS_UTILITY_BUCKET_ACCESS_KEY=myuseraccesskey
METRICS_UTILITY_BUCKET_SECRET_KEY=myusersecretkey
METRICS_UTILITY_BUCKET_ENDPOINT=http://localhost:9000
METRICS_UTILITY_BUCKET_NAME=metricsutilitys3
METRICS_UTILITY_BUCKET_REGION=us-east-1
```

Then source it:
```bash
set -a; source .env; set +a
```

### Option 3: Modify docker-compose.yaml

Update the environment variables in `tools/docker/docker-compose.yaml` for the `ceph-rgw` service:

```yaml
environment:
  CEPH_DEMO_ACCESS_KEY: "your-custom-access-key"
  CEPH_DEMO_SECRET_KEY: "your-custom-secret-key"
  CEPH_DEMO_BUCKET: "metricsutilitys3"
```

## Creating Additional Users

You can create additional users using the radosgw-admin CLI:

```bash
# Access the Ceph container
docker exec -it ceph-rgw bash

# Create a new user
radosgw-admin user create \
  --uid=testuser \
  --display-name="Test User" \
  --access-key=testaccesskey \
  --secret-key=testsecretkey

# List all users
radosgw-admin user list

# Get user info
radosgw-admin user info --uid=demo-user
```

## Managing Access Keys

### View existing keys

```bash
docker exec ceph-rgw radosgw-admin user info --uid=demo-user
```

### Create a new access key for existing user

```bash
docker exec ceph-rgw radosgw-admin key create \
  --uid=demo-user \
  --key-type=s3 \
  --access-key=newkey \
  --secret-key=newsecret
```

### Remove an access key

```bash
docker exec ceph-rgw radosgw-admin key rm \
  --uid=demo-user \
  --key-type=s3 \
  --access-key=oldkey
```

## Testing

Once you've set the credentials, test with:

```bash
# Run verification script
./verify-ceph.sh

# Or test directly with Python
uv run python << 'EOF'
import boto3
import os

client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id=os.getenv('METRICS_UTILITY_BUCKET_ACCESS_KEY', 'myuseraccesskey'),
    aws_secret_access_key=os.getenv('METRICS_UTILITY_BUCKET_SECRET_KEY', 'myusersecretkey'),
    region_name='us-east-1'
)
print(client.list_buckets())
EOF
```

## Note on Persistence

In the demo mode with tmpfs storage:
- Credentials defined in environment variables persist across container restarts
- User data stored in Ceph is lost when containers are removed
- For production, use persistent volumes instead of tmpfs

## Bucket Policies

You can set bucket policies using the S3 API:

```python
import boto3
import json

client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='myuseraccesskey',
    aws_secret_access_key='myusersecretkey',
    region_name='us-east-1'
)

# Example: Make bucket publicly readable (use with caution!)
policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": "*",
        "Action": ["s3:GetObject"],
        "Resource": ["arn:aws:s3:::metricsutilitys3/*"]
    }]
}

client.put_bucket_policy(
    Bucket='metricsutilitys3',
    Policy=json.dumps(policy)
)
```

## Security Best Practices

1. **Change default credentials** in production environments
2. **Use strong, random keys** for access and secret keys
3. **Limit bucket policies** to minimum required permissions
4. **Rotate keys regularly** in production systems
5. **Use HTTPS** for production deployments (configure TLS in Ceph RGW)

## Resources

- [Ceph RGW Admin Guide](https://docs.ceph.com/en/latest/radosgw/admin/)
- [Ceph RGW S3 API](https://docs.ceph.com/en/latest/radosgw/s3/)
- [User Management](https://docs.ceph.com/en/latest/radosgw/admin/#user-management)

