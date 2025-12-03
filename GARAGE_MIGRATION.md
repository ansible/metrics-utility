# Migration from MinIO to Garage

This document describes the changes made to replace MinIO with [Garage](https://garagehq.deuxfleurs.fr/) as the S3-compatible object storage solution for the metrics-utility project.

## What is Garage?

Garage is a lightweight, geo-distributed, S3-compatible object storage system designed for self-hosting. It offers:

- Full S3 API compatibility (works with boto3 and AWS CLI tools)
- Lightweight and efficient design
- Easy setup for development and testing
- Open source (AGPLv3)

## Changes Made

### 1. Docker Compose Configuration (`tools/docker/docker-compose.yaml`)

- **Replaced** `minio` service with `garage` service
  - Garage S3 API runs on port 3902 (mapped to 9000 for compatibility with existing scripts)
  - Admin API on port 3900
  - RPC on port 3901
  - Configuration is mounted from `tools/docker/garage.toml`

- **Replaced** `minio-setup` service with `garage-setup` service
  - Uses Alpine Linux with Garage CLI
  - Automatically configures Garage cluster layout
  - Creates the `metricsutilitys3` bucket
  - Sets up access key: `myuseraccesskey` / `myusersecretkey`
  - Setup script is in `tools/docker/garage-setup.sh`

- **Updated** environment variables in dependent services
  - Changed `METRICS_UTILITY_BUCKET_ENDPOINT` from `http://minio:9000` to `http://garage:3902`

### 2. New Configuration Files

- **`tools/docker/garage.toml`** - Garage server configuration
- **`tools/docker/garage-setup.sh`** - Automated setup script for bucket and access keys

### 3. Scripts

- **Updated** `run-s3-gather-build` with comments noting that MinIO client (mc) works with any S3-compatible storage, including Garage
- No functional changes needed since the endpoint and credentials remain the same

### 4. Documentation

- **Updated** `README.md` to reference Garage instead of MinIO
- **Updated** `metrics_utility/library/README.md` to clarify S3-compatible storage options
- **Updated** test file comments to reference Garage instead of MinIO

### New Files Created

- `GARAGE_MIGRATION.md` - Comprehensive migration guide with testing instructions
- `verify-garage.sh` - Quick verification script to test the Garage setup
- `tools/docker/garage.toml` - Garage server configuration
- `tools/docker/garage-setup.sh` - Setup script for initializing Garage

### 5. Source Code

**No changes required!** The existing boto3-based S3 client code works seamlessly with Garage because:
- Garage implements the S3 API
- boto3 is S3-compatible storage agnostic
- Only the endpoint URL needs to be different (handled via environment variables)

## Testing the Migration

### Quick Test

1. Start the services:
   ```bash
   docker compose -f tools/docker/docker-compose.yaml up -d
   ```

2. Wait for Garage to be ready (check logs):
   ```bash
   docker logs garage-setup
   # Should show "Garage setup complete!"
   ```

3. Run S3 storage tests:
   ```bash
   uv run pytest -s -v metrics_utility/test/library/test_storage_s3.py
   ```

4. Run S3 gather tests:
   ```bash
   uv run pytest -s -v metrics_utility/test/gather/test_s3_gather.py
   ```

### Full Test Suite

Run the full test suite:
```bash
docker compose -f tools/docker/docker-compose.yaml --profile=pytest up
```

Or interactively:
```bash
docker compose -f tools/docker/docker-compose.yaml --profile=env up -d
docker exec -it metrics-utility-env /bin/sh
uv run pytest -s -v
```

### Manual Testing with MinIO Client

The MinIO client (mc) works with Garage:

```bash
# Configure the alias
mc alias set garage http://localhost:9000 myuseraccesskey myusersecretkey

# List buckets
mc ls garage/

# List objects in bucket
mc ls garage/metricsutilitys3/
```

### Testing with AWS CLI

You can also use the AWS CLI:

```bash
export AWS_ACCESS_KEY_ID=myuseraccesskey
export AWS_SECRET_ACCESS_KEY=myusersecretkey
export AWS_DEFAULT_REGION=us-east-1

# List buckets
aws --endpoint-url http://localhost:9000 s3 ls

# List objects
aws --endpoint-url http://localhost:9000 s3 ls s3://metricsutilitys3/
```

## Port Mapping

| Service | Internal Port | External Port | Purpose |
|---------|---------------|---------------|---------|
| Garage S3 API | 3902 | 9000 | S3-compatible API (mapped to 9000 for compatibility) |
| Garage Admin | 3900 | 3900 | Admin API for cluster management |
| Garage RPC | 3901 | 3901 | Internal cluster communication |

The S3 API port is mapped from 3902 to 9000 to maintain compatibility with existing scripts and tests that expect port 9000.

## Compatibility Notes

- **boto3**: Full compatibility ✅
- **AWS CLI**: Full compatibility ✅  
- **MinIO Client (mc)**: Full compatibility ✅
- **Existing code**: No changes required ✅

## Troubleshooting

### Error: "executable file `sh` not found in $PATH"

This error occurred in early implementations where we tried to use shell commands in the Garage container. The fix:
- Configuration is now in `tools/docker/garage.toml` (mounted as a volume)
- Setup script uses Alpine Linux which has a proper shell
- Garage server just runs with the mounted config file

If you see this error, make sure you have the latest version of the configuration.

### Garage container fails to start

Check logs:
```bash
docker logs garage
```

Ensure:
1. The `tools/docker/garage.toml` file exists
2. tmpfs mounts are working properly
3. Port 9000, 3900, and 3901 are not already in use

### Setup script fails

Check setup logs:
```bash
docker logs garage-setup
```

The setup script should show:
- "Garage is ready!"
- "Configuring node layout..."
- "Creating access key..."
- "Creating bucket..."
- "Granting bucket permissions..."
- "Garage setup complete!"

### S3 operations fail

1. Verify Garage is running:
   ```bash
   curl http://localhost:3900/health
   ```

2. Check environment variables are set correctly:
   ```bash
   echo $METRICS_UTILITY_BUCKET_ENDPOINT  # Should be http://localhost:9000 or http://garage:3902
   ```

3. Test S3 connection:
   ```bash
   uv run python -c "
   import boto3
   client = boto3.client(
       's3',
       endpoint_url='http://localhost:9000',
       aws_access_key_id='myuseraccesskey',
       aws_secret_access_key='myusersecretkey',
       region_name='us-east-1'
   )
   print(client.list_buckets())
   "
   ```

## Benefits of Garage

1. **Lightweight**: Smaller resource footprint than MinIO
2. **Simpler**: Easier configuration for single-node development
3. **Modern**: Built in Rust, actively maintained
4. **Geo-distributed**: Designed for distributed deployments (useful for future scaling)
5. **Compatible**: Drop-in replacement for MinIO in S3-compatible scenarios

## Rollback

If you need to rollback to MinIO, the previous configuration is preserved in git history. The changes are minimal and can be easily reverted.

## References

- [Garage Documentation](https://garagehq.deuxfleurs.fr/)
- [Garage GitHub Repository](https://github.com/deuxfleurs-org/garage)
- [Garage S3 API Compatibility](https://garagehq.deuxfleurs.fr/documentation/reference-manual/s3-compatibility/)

