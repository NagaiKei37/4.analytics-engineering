import os
import requests
import pandas as pd
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq


# Base URL for the NYC TLC trip data hosted on GitHub
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

# Target GCS bucket — override via environment variable or change the default
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dbt-nyctaxi")

# Number of rows per chunk — keeps memory usage low
# when processing large files like yellow taxi data.
# 1M rows is a good balance between speed and memory.
CHUNK_SIZE = 1_000_000

# Explicit dtypes per service to prevent inconsistent schema inference.
# Without this, pandas infers types per chunk independently:
#   - Columns with all nulls in a chunk become float64 instead of string
#   - Integer columns with some nulls become float64 in some chunks but int64 in others
# This causes ParquetWriter and BigQuery to fail on schema mismatches.
DTYPES = {
    'yellow': {
        'VendorID': 'Int64',
        'tpep_pickup_datetime': 'str',
        'tpep_dropoff_datetime': 'str',
        'passenger_count': 'Int64',
        'trip_distance': 'float64',
        'RatecodeID': 'Int64',
        'store_and_fwd_flag': 'str',
        'PULocationID': 'Int64',
        'DOLocationID': 'Int64',
        'payment_type': 'Int64',
        'fare_amount': 'float64',
        'extra': 'float64',
        'mta_tax': 'float64',
        'tip_amount': 'float64',
        'tolls_amount': 'float64',
        'improvement_surcharge': 'float64',
        'total_amount': 'float64',
        'congestion_surcharge': 'float64',
    },
    'green': {
        'VendorID': 'Int64',
        'lpep_pickup_datetime': 'str',
        'lpep_dropoff_datetime': 'str',
        'store_and_fwd_flag': 'str',
        'RatecodeID': 'Int64',
        'PULocationID': 'Int64',
        'DOLocationID': 'Int64',
        'passenger_count': 'Int64',
        'trip_distance': 'float64',
        'fare_amount': 'float64',
        'extra': 'float64',
        'mta_tax': 'float64',
        'tip_amount': 'float64',
        'tolls_amount': 'float64',
        'ehail_fee': 'float64',
        'improvement_surcharge': 'float64',
        'total_amount': 'float64',
        'payment_type': 'Int64',
        'trip_type': 'Int64',
        'congestion_surcharge': 'float64',
    },
}


def upload_to_gcs(bucket, object_name, local_file):
    """
    Upload a local file to a GCS bucket.
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    
    Uses GOOGLE_APPLICATION_CREDENTIALS env var for authentication automatically.
    """
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    """
    Downloads NYC TLC trip data from GitHub, converts CSV to Parquet,
    and uploads to GCS. Processes files in chunks to avoid OOM kills
    on large datasets (e.g. yellow taxi has ~10M+ rows per month).
    
    Files are uploaded to GCS under: {service}/{service}_tripdata_{year}-{month}.parquet
    """

    # Validate that we have dtype definitions for this service
    if service not in DTYPES:
        print(f"ERROR: No dtype definition for service '{service}'. Skipping.")
        return

    dtype = DTYPES[service]

    # Loop through all 12 months
    for i in range(12):
        month = f"{i+1:02d}"
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        request_url = f"{init_url}{service}/{file_name}"

        # --- Step 1: Download the compressed CSV from GitHub ---
        print(f"Downloading: {file_name}")
        r = requests.get(request_url)

        # Skip this month if the file doesn't exist (e.g. future months)
        if r.status_code != 200:
            print(f"WARNING: Failed to download {file_name} (status {r.status_code}). Skipping.")
            continue

        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # --- Step 2: Convert CSV to Parquet using chunked reading ---
        # ParquetWriter writes chunks incrementally to a single file
        # without holding the entire dataset in memory.
        # The schema is locked from the first chunk and enforced on all subsequent chunks.
        parquet_file = file_name.replace('.csv.gz', '.parquet')
        writer = None

        try:
            for chunk in pd.read_csv(file_name, compression='gzip', low_memory=False,
                                     chunksize=CHUNK_SIZE, dtype=dtype):
                if 'store_and_fwd_flag' in chunk.columns:
                    chunk['store_and_fwd_flag'] = chunk['store_and_fwd_flag'].map({'Y': True, 'N': False}).astype('boolean')
                # Convert pandas DataFrame chunk to a PyArrow table
                table = pa.Table.from_pandas(chunk)

                # Initialize writer with schema from the first chunk
                if writer is None:
                    writer = pq.ParquetWriter(parquet_file, table.schema)

                # Append this chunk to the parquet file
                writer.write_table(table)

        except Exception as e:
            print(f"ERROR: Failed to convert {file_name} to parquet: {e}")
            if writer:
                writer.close()
            continue
        finally:
            # Always close the writer to flush remaining data
            if writer:
                writer.close()

        print(f"Parquet: {parquet_file}")

        # --- Step 3: Upload to GCS ---
        try:
            upload_to_gcs(BUCKET, f"{service}/{parquet_file}", parquet_file)
            print(f"GCS: {service}/{parquet_file}")
        except Exception as e:
            print(f"ERROR: Failed to upload {parquet_file} to GCS: {e}")
            continue
        finally:
            # Clean up local files regardless of upload success
            # to prevent disk space issues in constrained environments like Codespaces
            if os.path.exists(parquet_file):
                os.remove(parquet_file)
            if os.path.exists(file_name):
                os.remove(file_name)


# --- Run the pipeline ---
# Uncomment the services/years you want to process

web_to_gcs('2019', 'green')
web_to_gcs('2020', 'green')
web_to_gcs('2019', 'yellow')
web_to_gcs('2020', 'yellow')