# ✅ Ceph RGW Setup Complete!

Your metrics-utility project has been successfully configured to use Ceph RGW for S3-compatible storage!

## 🎉 What's Working

- ✅ Ceph RGW S3-compatible storage is running
- ✅ Access credentials are configured
- ✅ Bucket `metricsutilitys3` exists
- ✅ All S3 operations (upload/download/delete) work

## 🔑 Your Credentials

The current Ceph RGW credentials are:

```bash
METRICS_UTILITY_BUCKET_ACCESS_KEY=myuseraccesskey
METRICS_UTILITY_BUCKET_SECRET_KEY=myusersecretkey
METRICS_UTILITY_BUCKET_ENDPOINT=http://localhost:9000
METRICS_UTILITY_BUCKET_NAME=metricsutilitys3
METRICS_UTILITY_BUCKET_REGION=us-east-1
```

**Note:** These credentials are set via environment variables in the Ceph RGW demo container. They persist as long as the container is running but can be customized by modifying the docker-compose.yaml file.

## 🚀 Quick Start

### 1. Set Environment Variables

```bash
export METRICS_UTILITY_BUCKET_ACCESS_KEY=myuseraccesskey
export METRICS_UTILITY_BUCKET_SECRET_KEY=myusersecretkey
export METRICS_UTILITY_BUCKET_ENDPOINT=http://localhost:9000
export METRICS_UTILITY_BUCKET_NAME=metricsutilitys3
export METRICS_UTILITY_BUCKET_REGION=us-east-1
```

### 2. Verify the Setup

```bash
./verify-ceph.sh
```

You should see:
```
=== All checks passed! ===
```

### 3. Run Tests

```bash
# Run S3 storage tests
uv run pytest -s -v metrics_utility/test/library/test_storage_s3.py

# Run S3 gather tests  
uv run pytest -s -v metrics_utility/test/gather/test_s3_gather.py
```

## 📝 Daily Usage

### Start Ceph RGW

```bash
cd tools/docker
docker-compose up -d ceph-rgw
```

### Stop Ceph RGW

```bash
cd tools/docker
docker-compose down ceph-rgw
```

### View Logs

```bash
docker logs ceph-rgw
docker logs ceph-rgw-setup
```

### Access Ceph Admin Commands

```bash
# Access the Ceph container
docker exec -it ceph-rgw bash

# List buckets
radosgw-admin bucket list

# Get user info
radosgw-admin user info --uid=demo-user

# Create a new user
radosgw-admin user create --uid=newuser --display-name="New User" --access-key=mykey --secret-key=mysecret
```

## 🔧 Troubleshooting

### If containers stop or restart

The Ceph data is stored in tmpfs (temporary filesystem), so it persists while containers are running but is lost on container removal. To maintain your data:

- Use `docker-compose restart` instead of `down` + `up`
- Or back up your data before stopping containers

### If permissions are denied

Check the user permissions using radosgw-admin:

```bash
docker exec ceph-rgw radosgw-admin user info --uid=demo-user
```

### If you need to change credentials

Edit the `tools/docker/docker-compose.yaml` file and update the environment variables:

```yaml
environment:
  CEPH_DEMO_ACCESS_KEY: "your-access-key"
  CEPH_DEMO_SECRET_KEY: "your-secret-key"
```

Then restart the container:

```bash
cd tools/docker
docker-compose down
docker-compose up -d
```

### Connection Issues

If you're getting connection timeouts, check:

1. Container is running: `docker ps | grep ceph-rgw`
2. Port is accessible: `curl -v http://localhost:9000`
3. Endpoint URL is correct: `http://localhost:9000` for host, `http://ceph-rgw:7480` for containers

## 📚 Documentation

- **Ceph RGW official docs**: https://docs.ceph.com/en/latest/radosgw/
- **S3 API compatibility**: https://docs.ceph.com/en/latest/radosgw/s3/
- **Admin Operations**: https://docs.ceph.com/en/latest/radosgw/admin/

## ✨ Key Features of Ceph RGW

1. **S3 Compatibility**: Full S3 API support for standard operations

2. **Ports**: 
   - RGW S3 API runs on port 7480 (mapped to 9000 for compatibility with existing scripts)

3. **Management**: Use `radosgw-admin` CLI for administrative tasks

4. **Multi-tenancy**: Supports multiple users, buckets, and access policies

5. **Performance**: Production-ready storage backend used in large-scale deployments

## 🎯 Next Steps

Your environment is ready! You can now:

1. Run your existing metrics-utility workflows with S3 storage
2. Execute the test suite
3. Develop and test new features

Everything that worked with Garage or MinIO should work with Ceph RGW since it's fully S3-compatible!

---

**Happy coding!** 🚀

