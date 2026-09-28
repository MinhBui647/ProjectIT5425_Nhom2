#!/usr/bin/env python3
"""Check MinIO connection and write test data."""

import sys
sys.path.insert(0, "src")

from datetime import datetime, timezone

def check_minio_connection():
    """Check MinIO connection."""
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        print("boto3 not installed, skipping connection test")
        return None

    from lakehouse_storage.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET

    endpoint = f"http://{MINIO_ENDPOINT}"
    access_key = MINIO_ACCESS_KEY
    secret_key = MINIO_SECRET_KEY
    bucket = MINIO_BUCKET

    print(f"Testing MinIO connection to {endpoint}...")
    print(f"Access Key: {access_key}")
    print(f"Bucket: {bucket}")

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
        )

        # List buckets
        response = s3.list_buckets()
        print(f"\nBuckets: {[b['Name'] for b in response['Buckets']]}")

        # Check if bucket exists
        try:
            s3.head_bucket(Bucket=bucket)
            print(f"Bucket '{bucket}' exists!")
        except Exception:
            print(f"Bucket '{bucket}' not found, creating...")
            s3.create_bucket(Bucket=bucket)
            print(f"Created bucket '{bucket}'")

        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False


def test_delta_write():
    """Test writing data to a Delta table on MinIO."""
    from deltalake import write_deltalake, DeltaTable
    from deltalake.writer import WriterProperties
    import polars as pl

    from lakehouse_storage.config import MINIO_BUCKET, get_storage_options

    bucket = MINIO_BUCKET
    storage_opts = get_storage_options()

    table_uri = f"s3://{bucket}/test_sensor"

    print(f"\nTesting Delta write to {table_uri}...")

    # Create test data
    df = pl.DataFrame({
        "timestamp": [datetime.now(timezone.utc)],
        "line_id": ["LINE_TEST"],
        "batch_id": ["BATCH_TEST"],
        "preheat_temp": [80.5],
        "uht_temp": [140.0],
        "homo_press_stage1": [180.0],
        "homo_press_stage2": [40.0],
        "flow_rate": [5000.0],
        "conductivity": [4.8],
        "power_kw": [75.0],
        "ingestion_date": [datetime.now().date()],
    })

    try:
        write_deltalake(
            table_or_uri=table_uri,
            data=df.to_arrow(),
            mode="append",
            partition_by=["line_id", "ingestion_date"],
            storage_options=storage_opts,
            writer_properties=WriterProperties(compression="ZSTD"),
        )

        # Read back
        dt = DeltaTable(table_uri, storage_options=storage_opts)
        print(f"Delta table version: {dt.version()}")
        print(f"Rows written: {dt.to_pyarrow_table().num_rows}")

        return True
    except Exception as e:
        print(f"ERROR writing Delta: {e}")
        return False


def test_sensor_generator():
    """Test generating and writing sensor data."""
    from lakehouse_storage.mock_sensor_stream import generate_sensor_batch, write_sensor_batch
    from deltalake import DeltaTable
    from lakehouse_storage.config import MINIO_BUCKET, get_storage_options

    bucket = MINIO_BUCKET
    storage_opts = get_storage_options()
    table_uri = f"s3://{bucket}/sensor_telemetry"

    print("\nTesting sensor generator...")

    try:
        # Generate 60 samples
        df = generate_sensor_batch(n_samples=60, seed=42)
        print(f"Generated {len(df)} sensor samples")
        print(f"Columns: {df.columns}")

        # Write to Delta (using silver transform)
        version = write_sensor_batch(df, table_uri)
        print(f"Written to Delta, version: {version}")

        # Verify
        dt = DeltaTable(table_uri, storage_options=storage_opts)
        print(f"Total rows in table: {dt.to_pyarrow_table().num_rows}")

        return True
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("MinIO Integration Check")
    print("=" * 60)

    results = {}

    print("\n1. Checking MinIO connection...")
    results["connection"] = check_minio_connection()

    if results["connection"]:
        print("\n2. Testing Delta write...")
        results["delta_write"] = test_delta_write()

        print("\n3. Testing sensor generator...")
        results["sensor_generator"] = test_sensor_generator()

    print("\n" + "=" * 60)
    print("Results:")
    for name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {name}: {status}")
    print("=" * 60)
