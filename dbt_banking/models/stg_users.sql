{{ config(materialized='view') }}

with raw_source as (
    select json_data from {{ source('snowflake_raw', 'raw_users') }}
),

unpacked as (
    select
        json_data:payload:after:user_id::int as user_id,
        json_data:payload:after:first_name::string as first_name,
        json_data:payload:after:last_name::string as last_name,
        json_data:payload:after:email::string as email,
        to_timestamp_ntz(json_data:payload:source:ts_ms::int / 1000) as cdc_captured_at
    from raw_source
)

select * from unpacked