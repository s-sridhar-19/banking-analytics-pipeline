{{ config(materialized='view') }}

with raw_source as (
    select json_data from {{ source('snowflake_raw', 'raw_transactions') }}
),

unpacked as (
    select
        json_data:payload:after:transaction_id::int as transaction_id,
        json_data:payload:after:account_id::string as account_id,
        json_data:payload:after:amount::numeric(15, 2) as amount,
        json_data:payload:after:transaction_type::string as transaction_type,
        json_data:payload:after:transaction_status::string as transaction_status,
        
        -- Convert Debezium MicroTimestamp (microseconds) to readable Snowflake timestamp
        to_timestamp_ntz(json_data:payload:after:timestamp::int / 1000000) as transaction_at,
        to_timestamp_ntz(json_data:payload:source:ts_ms::int / 1000) as cdc_captured_at
    from raw_source
)

select * from unpacked