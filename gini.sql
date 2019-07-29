with ts AS (
  SELECT
  TIMESTAMP "2010-01-01 00:00:00" AS start,
  1629000 AS supply
  ),
  double_entry_book as (
  -- debits
  SELECT
  array_to_string(inputs.addresses, ",") as address
  , -inputs.value as value
  FROM `bigquery-public-data.crypto_bitcoin.inputs` as inputs
  WHERE block_timestamp<=(SELECT start from ts)

  UNION ALL
  -- credits
  SELECT
  array_to_string(outputs.addresses, ",") as address
  , outputs.value as value
  FROM `bigquery-public-data.crypto_bitcoin.outputs` as outputs
  WHERE block_timestamp<=(SELECT start from ts)
  ),
  ordered_balances as (
  select address, sum(value)/1e8 as balance
  from double_entry_book
  group by address
  order by balance desc
  limit 10000
  ),
  positive_balances as (
  select sum(value)/1e8 as balance
  from double_entry_book
  group by address
  having balance>0.0001
  ),
  ranked_balances AS (
  SELECT balance AS balance,
  sum(balance) over (order by balance DESC) AS cum_balance,
  row_number()  over (order by balance DESC) AS rank
  from ordered_balances
  )
  SELECT
  -- (1 − 2B) https://en.wikipedia.org/wiki/Gini_coefficient
  1 - 2 * sum((balance * (rank - 1) + balance / 2)) / count(*) / sum(balance) AS gini,
  sum(case when cum_balance < (SELECT supply FROM ts)*0.51 then 1 else 0 end) AS nakamoto,
  (select count(*)  FROM positive_balances) AS holders,
  ( select supply from ts) AS supply
  from ranked_balances