with double_entry_book as (
    -- debits
    select to_address as address, value as value, block_timestamp
    from `bigquery-public-data.ethereum_blockchain.traces`
    where to_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    union all
    -- credits
    select from_address as address, -value as value, block_timestamp
    from `bigquery-public-data.ethereum_blockchain.traces`
    where from_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    union all
    -- transaction fees debits
    select miner as address, sum(cast(receipt_gas_used as numeric) * cast(gas_price as numeric)) as value, block_timestamp
    from `bigquery-public-data.ethereum_blockchain.transactions` as transactions
    join `bigquery-public-data.ethereum_blockchain.blocks` as blocks on blocks.number = transactions.block_number
    group by blocks.miner, block_timestamp
    union all
    -- transaction fees credits
    select from_address as address, -(cast(receipt_gas_used as numeric) * cast(gas_price as numeric)) as value, block_timestamp
    from `bigquery-public-data.ethereum_blockchain.transactions`
),
double_entry_book_grouped_by_date as (
    select address, sum(value) as balance_increment, date(block_timestamp) as date
    from double_entry_book
    group by address, date
),
daily_balances_with_gaps as (
    select address, date, sum(balance_increment) over (partition by address order by date) as balance,
    lead(date, 1, current_date()) over (partition by address order by date) as next_date
    from double_entry_book_grouped_by_date
),
calendar AS (
    select date from unnest(generate_date_array('2015-07-30', current_date())) as date
),
daily_balances as (
    select address, calendar.date, balance
    from daily_balances_with_gaps
    join calendar on daily_balances_with_gaps.date <= calendar.date and calendar.date < daily_balances_with_gaps.next_date
)
select address, date, balance
from daily_balances