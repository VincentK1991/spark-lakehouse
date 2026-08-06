#!/bin/sh
# One-shot bucket + access-key bootstrap for MinIO, run by the mc-init service.
# Idempotent: safe to re-run (docker compose up recreates this container each time).
set -eu

echo "Waiting for MinIO..."
until mc alias set local "http://minio:9000" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  sleep 1
done

mc mb --ignore-existing "local/$MINIO_BUCKET"

cat > /tmp/lakekeeper-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": [
        "arn:aws:s3:::$MINIO_BUCKET",
        "arn:aws:s3:::$MINIO_BUCKET/*"
      ]
    }
  ]
}
EOF

mc admin policy create local lakekeeper-bucket-access /tmp/lakekeeper-policy.json 2>/dev/null || \
  mc admin policy update local lakekeeper-bucket-access /tmp/lakekeeper-policy.json 2>/dev/null || true

mc admin user add local "$MINIO_LAKEKEEPER_ACCESS_KEY" "$MINIO_LAKEKEEPER_SECRET_KEY" 2>/dev/null || true
mc admin policy attach local lakekeeper-bucket-access --user "$MINIO_LAKEKEEPER_ACCESS_KEY" 2>/dev/null || true

# Lakekeeper console's browser-side DuckDB-WASM preview sends *unsigned* GET
# requests straight to MinIO (it doesn't sign/vend credentials to the
# browser for this static-credential warehouse). Anonymous download is the
# only way to make that succeed. Dev-only trade-off: this makes every object
# in the bucket world-readable to anyone who can reach MinIO's S3 port
# directly, bypassing Lakekeeper/OpenFGA's access control entirely — see
# docs/initial_stack.md.
mc anonymous set download "local/$MINIO_BUCKET"

echo "MinIO ready: bucket '$MINIO_BUCKET', access key '$MINIO_LAKEKEEPER_ACCESS_KEY' scoped to it."
