with 
d0 as (
    select
        array_to_string(x.addresses,',') as address,
        date(block_timestamp) as date,
        value
        from `bigquery-public-data.crypto_dogecoin.transactions` join unnest(outputs) as x
)
,daily_incomes as (
    select
        address,
        date,
        sum(value) as income
        from d0
        GROUP BY address, date
)
,total_income as (
    select
        date,
        sum(income) as daily_income
    from daily_incomes
    group by date
)
,ranked_daily_incomes as (
    select 
        daily_incomes.date,
        income,
        row_number() over (partition by daily_incomes.date order by income desc) as rank
    from daily_incomes
    join total_income on daily_incomes.date = total_income.date 
    where safe_divide(income, daily_income) >= 0.0001
)
select 
    date, 
    -- (1 − 2B) https://en.wikipedia.org/wiki/Gini_coefficient
    1 - 2 * sum((income * (rank - 1) + income / 2)) / count(*) / sum(income) as gini
from ranked_daily_incomes
group by date
order by date asc