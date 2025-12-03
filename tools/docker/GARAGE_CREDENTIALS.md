# Garage S3 Credentials

Garage generates its own secure key IDs (starting with "GK") instead of allowing custom values like "myuseraccesskey".

## Current Credentials

Get the latest credentials from the setup logs:

```bash
docker logs docker-garage-setup-1 | grep -A 3 "Generated Key"
```

Example output:
```
Generated Key ID: GK160f780fdab0f547be610987
Generated Key Secret: 078af014efeae3636bd090275e3cbd9624b44625f9597a5723eb8a6b9c8fe113
```

## Using the Credentials

### Option 1: Export as Environment Variables

```bash
export METRICS_UTILITY_BUCKET_ACCESS_KEY=GK160f780fdab0f547be610987
export METRICS_UTILITY_BUCKET_SECRET_KEY=078af014efeae3636bd090275e3cbd9624b44625f9597a5723eb8a6b9c8fe113
```

### Option 2: Create a `.env` file

Create `.env` in the project root:

```bash
METRICS_UTILITY_BUCKET_ACCESS_KEY=GK160f780fdab0f547be610987
METRICS_UTILITY_BUCKET_SECRET_KEY=078af014efeae3636bd090275e3cbd9624b44625f9597a5723eb8a6b9c8fe113
METRICS_UTILITY_BUCKET_ENDPOINT=http://localhost:9000
METRICS_UTILITY_BUCKET_NAME=metricsutilitys3
METRICS_UTILITY_BUCKET_REGION=us-east-1
```

Then source it:
```bash
set -a; source .env; set +a
```

### Option 3: Update docker-compose.yaml

Update the environment variables in `tools/docker/docker-compose.yaml` for the `metrics-utility` and `metrics-utility-env` services to use the generated credentials.

## Testing

Once you've set the credentials, test with:

```bash
# Run verification (update verify-garage.sh to use the new credentials)
./verify-garage.sh

# Or test directly with Python
uv run python << 'EOF'
import boto3
import os

client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id=os.getenv('METRICS_UTILITY_BUCKET_ACCESS_KEY', 'GK160f780fdab0f547be610987'),
    aws_secret_access_key=os.getenv('METRICS_UTILITY_BUCKET_SECRET_KEY', '078af014efeae3636bd090275e3cbd9624b44625f9597a5723eb8a6b9c8fe113'),
    region_name='us-east-1'
)
print(client.list_buckets())
EOF
```

## Note on Persistence

The credentials are generated when the `garage-setup` container runs. If you restart the Garage containers from scratch, new credentials will be generated. To avoid this:

1. Keep the Garage container running
2. Use `docker-compose restart` instead of `down` + `up`
3. Or note the credentials and reuse them

## Helper Script

Use `tools/docker/get-garage-credentials.sh` to fetch the current key ID (note: secret is only shown during creation):

```bash
./tools/docker/get-garage-credentials.sh
```

