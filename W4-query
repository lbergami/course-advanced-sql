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
        customers_summary.last_order_date,
        customers_summary.order_numbers, 
        customers_summary.total_spent, 
        line_item_rank_1.part_1_key, 
        line_item_rank_1.part_1_quantity, 
        line_item_rank_1.part_1_total_spent,
        line_item_rank_2.part_2_key, 
        line_item_rank_2.part_2_quantity, 
        line_item_rank_2.part_2_total_spent,
        line_item_rank_3.part_3_key, 
        line_item_rank_3.part_3_quantity, 
        line_item_rank_3.part_3_total_spent
    from customers_summary
    left join line_item_rank_1
    	on customers_summary.c_custkey = line_item_rank_1.c_custkey
    left join line_item_rank_2
    	on customers_summary.c_custkey = line_item_rank_2.c_custkey
    left join line_item_rank_3
    	on customers_summary.c_custkey = line_item_rank_3.c_custkey
    order by customers_summary.last_order_date desc
    limit 100


   
