# End-to-End Modern Banking Analytics Pipeline

 ## Architecture:

[PostgreSQL Database] 
       │  (Captures live banking transactions as they happen)
       ▼
[Debezium CDC + Kafka] 
       │  (Streams raw transactional logs out of the database)
       ▼
[MinIO Local Data Lake] 
       │  (Packs raw events into JSON objects inside an S3 bucket)
       ▼
[Python Bridge Tunnel] 
       │  (Securely ships local files straight up to the cloud)
       ▼
[Snowflake RAW Stage] 
       │  (Lands raw JSON payloads directly inside cloud database storage)
       ▼
[dbt Transformation Hub] 
       │  (Unpacks nested variants into clean rows & aggregates account balances)
       ▼
📈 [Analytics-Ready Marts Layer]  <-- YOU ARE HERE!
