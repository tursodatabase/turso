-- LIMBO_SKIP: subquery in where not supported


select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= '1997-06-01'
	and o_orderdate < '1997-09-01' -- modified not to include cast({'month': 3} as interval)
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority;
