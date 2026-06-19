{{ config(materialized='table') }}

with users as (
    select * from {{ ref('stg_users') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

transactions as (
    select * from {{ ref('stg_transactions') }}
),

-- Aggregate transaction behavior per account
account_activity as (
    select
        account_id,
        count(transaction_id) as total_transactions,
        sum(case when transaction_type = 'DEPOSIT' then amount 
                 when transaction_type = 'WITHDRAWAL' then -amount 
                 else 0 end) as net_transaction_volume,
        max(transaction_at) as last_transaction_date
    from transactions
    where transaction_status = 'SUCCESS'
    group by account_id
),

-- Join everything together into a unified business profile
final as (
    select
        u.user_id,
        u.first_name,
        u.last_name,
        u.email,
        a.account_id,
        a.account_type,
        a.balance as official_database_balance,
        coalesce(aa.total_transactions, 0) as total_transactions,
        coalesce(aa.net_transaction_volume, 0) as net_transaction_volume,
        aa.last_transaction_date
    from users u
    join accounts a on u.user_id = a.user_id
    left join account_activity aa on a.account_id = aa.account_id
)

select * from final