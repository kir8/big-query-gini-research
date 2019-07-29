with balances as (
    select '2018-01-01' as date, balance
    from unnest([1,2,3,4,5]) as balance -- Gini coef: 0.2666666666666667
    union all
    select '2018-01-02' as date, balance
    from unnest([3,3,3,3]) as balance -- Gini coef: 0.0
    union all
    select '2018-01-03' as date, balance
    from unnest([4,5,1,8,6,45,67,1,4,11]) as balance -- Gini coef: 0.625
),
ranked_balances as (
    select date, balance, row_number() over (partition by date order by balance desc) as rank
    from balances
)
select date, 
    -- (1 − 2B) https://en.wikipedia.org/wiki/Gini_coefficient
    1 - 2 * sum((balance * (rank - 1) + balance / 2)) / count(*) / sum(balance) AS gini
from ranked_balances
group by date
having sum(balance) > 0
order by date asc
-- verify here http://shlegeris.com/gini

