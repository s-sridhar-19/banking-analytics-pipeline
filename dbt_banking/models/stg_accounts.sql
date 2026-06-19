{{ config(materialized='view') }}

with raw_source as (
    select json_data from {{ source('snowflake_raw', 'raw_accounts') }}
),

unpacked as (
    select
        json_data:payload:after:account_id::string as account_id,
        json_data:payload:after:user_id::int as user_id,
        json_data:payload:after:account_type::string as account_type,
        json_data:payload:after:balance::numeric(15, 2) as balance,
        to_timestamp_ntz(json_data:payload:source:ts_ms::int / 1000) as cdc_captured_at
    from raw_source
)

select * from unpacked