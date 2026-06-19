import os
import boto3
from snowflake.connector import connect
from dotenv import load_dotenv

load_dotenv()

# 1. Connect to local MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=f"http://localhost:{os.getenv('MINIO_PORT', '9000')}",
    aws_access_key_id=os.getenv('MINIO_ROOT_USER', 'admin_user'),
    aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD', 'super_secure_minio_pass'),
    region_name='us-east-1'
)
BUCKET_NAME = 'banking-data-lake'

# 2. Connect to Cloud Snowflake
sf_conn = connect(
    user='sridhar19',
    password=os.getenv('SNOWFLAKE_PASSWORD', 'YOUR_ACTUAL_SNOWFLAKE_PASSWORD'), # Set this in your .env or paste it here
    account='UEQXJAQ-JE08800',
    warehouse='BANKING_WH',
    database='BANKING_LAKEHOUSE',
    schema='RAW',
    role='DBT_ROLE'
)
cursor = sf_conn.cursor()

print("Scanning local MinIO data lake for raw CDC files...")

# List all objects in the bucket
response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix='raw/')

if 'Contents' not in response:
    print("No files found in MinIO yet. Make sure your kafka_to_minio.py script is running and capturing events!")
else:
    for obj in response['Contents']:
        s3_key = obj['Key']
        if not s3_key.endswith('.json'):
            continue
            
        # Determine which table this file belongs to based on its path
        table_name = ""
        if "users/" in s3_key:
            table_name = "raw_users"
        elif "accounts/" in s3_key:
            table_name = "raw_accounts"
        elif "transactions/" in s3_key:
            table_name = "raw_transactions"
            
        if table_name:
            local_filename = os.path.basename(s3_key)
            # Download file locally temporary
            s3_client.download_file(BUCKET_NAME, s3_key, local_filename)
            
            print(f"Uploading {local_filename} to Snowflake Stage...")
            # Push file to Snowflake Internal Stage
            cursor.execute(f"PUT file://{os.path.abspath(local_filename)} @banking_raw_stage auto_compress=true;")
            
            print(f"Ingesting into Snowflake table: {table_name}...")
            # Copy data from Stage into the Raw JSON Variant table
            cursor.execute(f"""
                COPY INTO {table_name} (json_data)
                FROM @banking_raw_stage/{local_filename}.gz
                FILE_FORMAT = (TYPE = 'JSON');
            """)
            
            # Clean up local temporary file
            os.remove(local_filename)

print("🚀 Sync Complete! All data has successfully migrated from your local data lake to Snowflake RAW.")
cursor.close()
sf_conn.close()