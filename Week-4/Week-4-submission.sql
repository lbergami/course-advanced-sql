/* 
	The query is structured as follows:
		- Get the (i) list of the customers in the automobile segment and (ii) the list of urgent orders and 
          combine the two.
        - Get top 3 line item by spend for each customer 
    	- Get a last order date; top 3 order numbers and total spend for each customer and combine with the three 
          single items
*/

with 

    automobile_customers as (

    	select 
        	c_custkey
        from snowflake_sample_data.tpch_sf1.customer
        where c_mktsegment = 'AUTOMOBILE'
        
    ), 
    
    urgent_orders as (

		select 
        	o_custkey,
            o_orderkey, 
            o_orderdate
        from snowflake_sample_data.tpch_sf1.orders
        where o_orderpriority = '1-URGENT'
        
    ), 

    urgent_automobile_orders as (

    	select 
        	c_custkey, 
            o_orderkey, 
            o_orderdate
        from automobile_customers
        inner join urgent_orders
        	on automobile_customers.c_custkey = urgent_orders.o_custkey
            
    ), 

    top_3_expensive_line_items as (

    	select 
    	    orders.c_custkey,
            orders.o_orderkey,
            orders.o_orderdate, 
            line_item.l_partkey::integer as key, 
            line_item.l_quantity::integer as quantity, 
            line_item.l_extendedprice::integer price, 
            rank() over(partition by orders.c_custkey
    					order by line_item.l_extendedprice desc) as rank_line
    	from urgent_automobile_orders as orders  
    	left join snowflake_sample_data.tpch_sf1.lineitem as line_item
    		on orders.o_orderkey = line_item.l_orderkey 
    	qualify rank_line < 4 
         
	), 

    line_item_rank_1 as (

        select 
        	c_custkey, 
            key as part_1_key, 
            quantity as part_1_quantity, 
            price as part_1_total_spent
        from top_3_expensive_line_items 
        where rank_line = 1
        
    ), 

    line_item_rank_2 as (

        select 
        	c_custkey, 
            key as part_2_key, 
            quantity as part_2_quantity, 
            price as part_2_total_spent
        from top_3_expensive_line_items 
        where rank_line = 2
        
    ), 
    
    line_item_rank_3 as (

        select 
        	c_custkey, 
            key as part_3_key, 
            quantity as part_3_quantity, 
            price as part_3_total_spent
        from top_3_expensive_line_items 
        where rank_line = 3
        
    ), 

    customers_summary as (

        select 
    		c_custkey,
        	max(o_orderdate) as last_order_date, 
        	listagg(o_orderkey, ', ') as order_numbers, 
        	sum(price) as total_spent 
    	from top_3_expensive_line_items
    	group by 1

    )

    select
    	customers_summary.c_custkey, 
        last_order_date,
        order_numbers, 
        total_spent, 
        part_1_key, 
        part_1_quantity, 
        part_1_total_spent,
        part_2_key, 
        part_2_quantity, 
        part_2_total_spent,
        part_3_key, 
        part_3_quantity, 
        part_3_total_spent
    from customers_summary
    left join line_item_rank_1
    	on customers_summary.c_custkey = line_item_rank_1.c_custkey
    left join line_item_rank_2
    	on customers_summary.c_custkey = line_item_rank_2.c_custkey
    left join line_item_rank_3
    	on customers_summary.c_custkey = line_item_rank_3.c_custkey
 	order by customers_summary.last_order_date desc
	limit 100


    with urgent_orders as (
    select
    	o_orderkey,
    	o_orderdate,
        c_custkey,
        p_partkey,
        l_quantity,
        l_extendedprice,
        row_number() over (partition by c_custkey order by l_extendedprice desc) as price_rank
    from snowflake_sample_data.tpch_sf1.orders as o
    inner join snowflake_sample_data.tpch_sf1.customer as c on o.o_custkey = c.c_custkey
    inner join snowflake_sample_data.tpch_sf1.lineitem as l on o.o_orderkey = l.l_orderkey
    inner join snowflake_sample_data.tpch_sf1.part as p on l.l_partkey = p.p_partkey
    where c.c_mktsegment = 'AUTOMOBILE'
    	and o.o_orderpriority = '1-URGENT'
    order by 1, 2),

top_orders as (
    select
    	c_custkey,
        max(o_orderdate) as last_order_date,
        listagg(o_orderkey, ', ') as order_numbers,
        sum(l_extendedprice) as total_spent
    from urgent_orders
    where price_rank <= 3
    group by 1
    order by 1)

select 
	t.c_custkey,
    t.last_order_date,
    t.order_numbers,
    t.total_spent,
    u.p_partkey as part_1_key,
    u.l_quantity as part_1_quantity,
    u.l_extendedprice as part_1_total_spent,
    u2.p_partkey as part_2_key,
    u2.l_quantity as part_2_quantity,
    u2.l_extendedprice as part_2_total_spent,
    u3.p_partkey as part_3_key,
    u3.l_quantity as part_3_quantity,
    u3.l_extendedprice as part_3_total_spent
from top_orders as t
inner join urgent_orders as u on t.c_custkey = u.c_custkey
inner join urgent_orders as u2 on t.c_custkey = u2.c_custkey
inner join urgent_orders as u3 on t.c_custkey = u3.c_custkey
where u.price_rank = 1 and u2.price_rank = 2 and u3.price_rank = 3
order by t.last_order_date desc
limit 100
