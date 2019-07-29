with double_entry_book as (
    -- debits
    select array_to_string(array(select * from unnest(outputs.addresses) order by 1), ',') as address, 
        outputs.value as value
    from bitcoin_blockchain.transactions as transactions,
    unnest(transactions.outputs) as outputs
    union all
    -- credits
    select array_to_string(array(select * from unnest(inputs.addresses) order by 1), ',') as address, 
        -inputs.value as value
    from bitcoin_blockchain.transactions as transactions,
    unnest(transactions.inputs) as inputs
)
select address, sum(value) as balance
from double_entry_book
group by address
order by balance desc
limit 100