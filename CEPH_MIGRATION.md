# Migration to Ceph RGW

This document describes the migration from Garage to Ceph RGW for S3-compatible object storage.

## Why Ceph RGW?

Ceph RGW (RADOS Gateway) is a production-ready S3-compatible object storage interface that:

- **Production-Ready**: Used in large-scale enterprise deployments
- **Fully S3-Compatible**: Implements the complete S3 API
- **Scalable**: Designed for horizontal scaling
- **Feature-Rich**: Supports multi-tenancy, bucket policies, versioning, and more
- **Well-Documented**: Extensive documentation and community support
- **Industry Standard**: Part of the widely-adopted Ceph distributed storage platform

## What Changed

### Files Modified

1. **docker-compose.yaml**
   - Replaced `garage` service with `ceph-rgw`
   - Updated service endpoint from `garage:3902` to `ceph-rgw:7480`
   - Changed setup container from `garage-setup` to `ceph-rgw-setup`

2. **Setup Scripts**
   - Created `ceph-rgw-setup.sh` to initialize Ceph RGW
   - Removed dependency on `garage-setup.sh`

3. **Verification**
   - Created `verify-ceph.sh` for testing Ceph RGW setup
   - Replaces `verify-garage.sh`

4. **Documentation**
   - Created `CEPH_SETUP_COMPLETE.md`
   - Created `tools/docker/CEPH_CREDENTIALS.md`
   - Created this migration guide

5. **Scripts**
   - Updated `run-s3-gather-build` to reference Ceph RGW

### Files Removed/Deprecated

- `garage.toml` - No longer needed (Ceph uses different config)
- `garage-setup.sh` - Replaced by `ceph-rgw-setup.sh`
- `get-garage-credentials.sh` - Credentials now managed via environment variables
- `verify-garage.sh` - Replaced by `verify-ceph.sh`
- `GARAGE_SETUP_COMPLETE.md` - Replaced by `CEPH_SETUP_COMPLETE.md`
- `GARAGE_MIGRATION.md` - Historical reference
- `tools/docker/GARAGE_CREDENTIALS.md` - Replaced by `CEPH_CREDENTIALS.md`

## Configuration Changes

### Port Mapping

**Garage:**
```yaml
ports:
  - "3900:3900" # Admin API
  - "3901:3901" # RPC
  - "9000:3902" # S3 API
```

**Ceph RGW:**
```yaml
ports:
  - "9000:7480" # RGW S3 API
```

### Endpoints

**From containers:**
- Before: `http://garage:3902`
- After: `http://ceph-rgw:7480`

**From host:**
- Before: `http://localhost:9000`
- After: `http://localhost:9000` (unchanged)

### Credentials

**Garage:**
- Generated automatically with GK-prefixed keys
- Retrieved via Admin API or logs

**Ceph RGW:**
- Configured via environment variables
- Default: `myuseraccesskey` / `myusersecretkey`
- Customizable in docker-compose.yaml

## Migration Steps

### 1. Stop Old Services

```bash
cd tools/docker
docker-compose down
```

### 2. Pull New Changes

```bash
git pull origin CephRGW
```

### 3. Start Ceph RGW

```bash
cd tools/docker
docker-compose up -d ceph-rgw ceph-rgw-setup
```

### 4. Verify Setup

```bash
./verify-ceph.sh
```

### 5. Update Environment Variables

If you had custom Garage credentials set, update them:

```bash
# Old Garage credentials
unset METRICS_UTILITY_BUCKET_ACCESS_KEY
unset METRICS_UTILITY_BUCKET_SECRET_KEY

# New Ceph RGW credentials
export METRICS_UTILITY_BUCKET_ACCESS_KEY=myuseraccesskey
export METRICS_UTILITY_BUCKET_SECRET_KEY=myusersecretkey
export METRICS_UTILITY_BUCKET_ENDPOINT=http://localhost:9000
export METRICS_UTILITY_BUCKET_NAME=metricsutilitys3
export METRICS_UTILITY_BUCKET_REGION=us-east-1
```

### 6. Test

```bash
# Run S3 tests
uv run pytest -s -v metrics_utility/test/library/test_storage_s3.py

# Run gather tests with S3
uv run pytest -s -v metrics_utility/test/gather/test_s3_gather.py
```

## Compatibility

### What Stays the Same

✅ S3 API operations (upload, download, delete, list)
✅ Bucket names and structure
✅ Application code (no changes needed)
✅ Test suite
✅ External endpoint port (9000)
✅ Environment variable names

### What's Different

🔄 Container service name (`garage` → `ceph-rgw`)
🔄 Internal port (3902 → 7480)
🔄 Credential management (API → environment variables)
🔄 Admin interface (Garage API → radosgw-admin CLI)
🔄 Configuration file (garage.toml → Ceph env vars)

## Troubleshooting

### Issue: Container won't start

**Solution:**
```bash
# Check logs
docker logs ceph-rgw

# Ensure no port conflicts
lsof -i :9000

# Try recreating container
docker-compose down
docker-compose up -d
```

### Issue: Bucket not found

**Solution:**
```bash
# Check if setup completed
docker logs ceph-rgw-setup

# Manually create bucket
docker exec -it ceph-rgw bash
aws s3 mb s3://metricsutilitys3 --endpoint-url http://localhost:7480
```

### Issue: Access denied

**Solution:**
```bash
# Verify credentials
docker exec ceph-rgw radosgw-admin user info --uid=demo-user

# Check environment variables
env | grep METRICS_UTILITY_BUCKET
```

### Issue: Connection timeout

**Solution:**
```bash
# Check if RGW is responding
curl -v http://localhost:9000

# Check container network
docker network inspect docker_default

# Ensure container is healthy
docker ps
docker inspect ceph-rgw
```

## Advanced Configuration

### Enable Logging

Add to docker-compose.yaml:

```yaml
ceph-rgw:
  environment:
    RGW_LOGLEVEL: "20"  # Debug level logging
```

### Persistent Storage

Replace tmpfs with volumes for data persistence:

```yaml
ceph-rgw:
  volumes:
    - ceph-etc:/etc/ceph
    - ceph-lib:/var/lib/ceph

volumes:
  ceph-etc:
  ceph-lib:
```

### Custom User

Modify docker-compose.yaml:

```yaml
ceph-rgw:
  environment:
    CEPH_DEMO_UID: "your-username"
    CEPH_DEMO_ACCESS_KEY: "your-access-key"
    CEPH_DEMO_SECRET_KEY: "your-secret-key"
```

### Production Deployment

For production use:

1. **Use persistent volumes** instead of tmpfs
2. **Enable TLS/SSL** for encrypted connections
3. **Configure proper authentication** with strong keys
4. **Set up monitoring** using Ceph metrics
5. **Configure backup** and disaster recovery
6. **Use a proper Ceph cluster** instead of demo mode

## Performance Considerations

### Demo Mode Limitations

The current setup uses Ceph in "demo" mode with tmpfs storage:

- ⚠️ Data is stored in memory (tmpfs)
- ⚠️ Limited by available RAM
- ⚠️ Data lost on container removal
- ⚠️ Not suitable for production

### Production Setup

For production workloads:

1. Deploy a full Ceph cluster with OSDs
2. Use block devices or persistent volumes
3. Configure replication for redundancy
4. Set up monitoring and alerting
5. Tune performance parameters

## References

- [Ceph Documentation](https://docs.ceph.com/)
- [Ceph RGW Documentation](https://docs.ceph.com/en/latest/radosgw/)
- [Ceph RGW S3 API](https://docs.ceph.com/en/latest/radosgw/s3/)
- [Ceph RGW Admin Operations](https://docs.ceph.com/en/latest/radosgw/admin/)
- [Ceph RGW Configuration Reference](https://docs.ceph.com/en/latest/radosgw/config-ref/)

## Support

For issues or questions:

1. Check container logs: `docker logs ceph-rgw`
2. Review Ceph RGW documentation
3. Verify network connectivity and credentials
4. Test with AWS CLI or boto3 directly

---

**Migration Date:** December 2025
**Ceph Version:** Quincy (latest)
**S3 API Version:** Compatible with AWS S3

