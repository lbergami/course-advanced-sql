# Week 1 - Question 1

The query is structered as follows:
* Creation of three tables: US city tables; Potential customers and; suppliers
* Pairwise combination of customers and suppliers 
* Filter closest supplier for each customer based on geometric distance
* Join to the resulting customer-supplier list required attributes 

`   with lkpUScities as (
    select 
        lower(trim(city_name)) as city_name, 
        lower(trim(state_abbr)) as state_abbr, 
        lat, long
    from (
        select 
            lower(trim(city_name)) as city_name, lower(trim(state_abbr)) as state_abbr, 
            lat, long, 
            row_number() over(partition by city_name, state_abbr order by city_name, state_abbr) as row_number
         from vk_data.resources.us_cities
         
    )
    where row_number = 1
    ),

    lkpCustomersCities as (
    select 
        customer_id, 
        lower(trim(customer_city)) as customer_city, 
        lower(trim(customer_state)) as customer_state
    from vk_data.customers.customer_address 
    ),

    lkpCustomersGeoCoords as (
    select 
        customer_id, 
        lat as customer_lat,
        long as customer_long
    from lkpCustomersCities
    inner join lkpUScities
        on (lkpCustomersCities.customer_city = lkpUScities.city_name) and (lkpCustomersCities.customer_state = lkpUScities.state_abbr)
    ),

    lkpSupplier as (
    select 
        supplier_id,  
        lower(trim(supplier_city)) as supplier_city,
        lower(trim(supplier_state)) as supplier_state 
    from vk_data.suppliers.supplier_info
    ), 

    lkpSupplierGeoCoords as (
    select 
        supplier_id, 
        lat as supplier_lat,
        long as supplier_long
    from lkpSupplier
    left join lkpUScities
        on (lkpSupplier.supplier_city = lkpUScities.city_name) and (lkpSupplier.supplier_state = lkpUScities.state_abbr)

    ), 

    lkpConsumersSuppliersFullList as (
    select 
        a.customer_id, 
        b.supplier_id, 
        st_distance(st_makepoint(lkpCustomers.customer_long, lkpCustomers.customer_lat), st_makepoint(lkpSuppliers.supplier_long, lkpSuppliers.supplier_lat)) / 1000         as km_distance
    from (select customer_id from lkpCustomersGeoCoords) as a 
    cross join (select supplier_id from lkpSupplierGeoCoords) as b
    left join lkpCustomersGeoCoords as lkpCustomers
        on a.customer_id = lkpCustomers.customer_id
    left join lkpSupplierGeoCoords as lkpSuppliers
        on b.supplier_id = lkpSuppliers.supplier_id
    ), 

    lkpConsumersSuppliersList as (
    select 
        customer_id, 
        supplier_id, 
        round(km_distance) as shopping_distance_km    
    from (select 
            customer_id,
            supplier_id, 
            km_distance, 
            row_number() over(partition by customer_id order by km_distance) as row_number
          from lkpConsumersSuppliersFullList)
    where row_number = 1 
    )
    select 
    main.customer_id,
    c.first_name, 
    c.last_name, 
    c.email,
    main.supplier_id,
    s.supplier_name,
    main.shopping_distance_km
    from lkpConsumersSuppliersList as main 
    left join vk_data.customers.customer_data as c
    on main.customer_id = c.customer_id
    left join vk_data.suppliers.supplier_info as s
    on main.supplier_id = s.supplier_id
`



    
