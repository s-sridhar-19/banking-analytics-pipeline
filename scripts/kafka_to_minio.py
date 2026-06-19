import os
import json
import boto3
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# 1. Initialize S3 Client pointing to our local MinIO container
s3_client = boto3.client(
    's3',
    endpoint_url=f"http://localhost:{os.getenv('MINIO_PORT', '9000')}",
    aws_access_key_id=os.getenv('MINIO_ROOT_USER', 'admin_user'),
    aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD', 'super_secure_minio_pass'),
    region_name='us-east-1'  # Default dummy region for MinIO
)

BUCKET_NAME = 'banking-data-lake'

# 2. Configure Kafka Consumer Connection
conf = {
    'bootstrap.servers': f"localhost:{os.getenv('KAFKA_PORT', '9092')}",
    'group.id': 'minio-ingestion-group',   # Fixed: Using strict dot notation 
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

# Subscribe to the CDC topics created by Debezium
topics = ['banking_cdc.public.users', 'banking_cdc.public.accounts', 'banking_cdc.public.transactions']
consumer.subscribe(topics)

print(f"🚀 Streaming worker active. Listening for changes on topics: {topics}...")

try:
    while True:
        # Poll Kafka for new messages every 1 second
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        
        # Error Handling Block
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                # Fixed: Ignore and bypass safely if Debezium hasn't triggered the topic yet
                continue
            else:
                print(f"❌ Kafka Error: {msg.error()}")
                break

        # Extract event metadata
        topic = msg.topic()
        table_name = topic.split('.')[-1]  # Extracts 'users', 'accounts', or 'transactions'
        payload = json.loads(msg.value().decode('utf-8'))

        # Generate an enterprise-grade partitioned path and filename using timestamps
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        s3_key = f"raw/{table_name}/{datetime.now().strftime('%Y-%m-%d')}/{table_name}_{timestamp}.json"

        # Upload the raw JSON CDC payload directly into MinIO
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(payload, indent=2)
        )
        print(f"💾 Captured change from DB table [{table_name}] -> Saved to S3://{BUCKET_NAME}/{s3_key}")

except KeyboardInterrupt:
    print("\n🛑 Stopping streaming worker gracefully...")
finally:
    # Safely close the consumer connection
    consumer.close()