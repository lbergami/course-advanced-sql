with 

    /* Clean US cities data and extract geo coordinates for the Chicago and Gary*/
    
    us_cities_geo_coords as (

    select 
        trim(lower(city_name)) as city_name,
        trim(lower(state_abbr)) as state_abbr, 
        geo_location 
    from vk_data.resources.us_cities us 

    ),

    chicago_gary_geo_coords as (

    select 
        city_name, 
        geo_location
    from us_cities_geo_coords
    where (city_name = 'chicago' and state_abbr = 'il') or (city_name = 'gary' and state_abbr = 'in')  
    
    ), 

    /* Extract list of affected customers */
    
    customer_names as (
    
    select
        customer_id, 
        first_name || ' ' || last_name as customer_name
    from vk_data.customers.customer_data

    ),
    
    customers_in_affected_cities as (
    
    select 
        customer_id, 
        trim(lower(customer_city)) as customer_city,
        trim(lower(customer_state)) as customer_state
    from vk_data.customers.customer_address
    where 
        (trim(lower(customer_state)) = 'ky' and 
            (trim(lower(customer_city)) ilike any ('%concord%', '%georgetown%', '%ashland%'))
        ) or
        (trim(lower(customer_state)) = 'ca' and 
            (trim(lower(customer_city)) ilike any ('%oakland%', '%pleasant hill%'))
        ) or
        (trim(lower(customer_state)) = 'tx' and 
            (trim(lower(customer_city)) ilike any ('%arlington%', '%brownsville%'))
        ) 
    ),

    active_customers_n_preferences as (
    
    select 
        customer_id,
        count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1

    ), 

    affected_customers as (

    select 
        customer_names.customer_name, 
        customers_in_affected_cities.customer_city,
        customers_in_affected_cities.customer_state,
        active_customers_n_preferences.food_pref_count, 
        us_cities_geo_coords.geo_location as customer_city_geo_coords
    from customers_in_affected_cities
    inner join customer_names
        on customers_in_affected_cities.customer_id = customer_names.customer_id
    inner join active_customers_n_preferences
        on customers_in_affected_cities.customer_id = active_customers_n_preferences.customer_id
    inner join us_cities_geo_coords
        on (customers_in_affected_cities.customer_city = us_cities_geo_coords.city_name) 
        and (customers_in_affected_cities.customer_state = us_cities_geo_coords.state_abbr)    

    ), 

    /* Compute distances between afftected customers and Chicago and Gary */
    
    affected_customers_geo_distances as (
    
    select 
        affected_customers.customer_name,
        initcap(affected_customers.customer_city) as customer_city,
        upper(affected_customers.customer_state) as customer_state,
        affected_customers.food_pref_count, 
        city_name, 
        (st_distance(affected_customers.customer_city_geo_coords, chicago_gary_geo_coords.geo_location) / 1609)::int as distance_miles
    from affected_customers
    cross join chicago_gary_geo_coords

    ) 
    
    select *
    from affected_customers_geo_distances
    pivot(
        sum(distance_miles) for city_name in ('chicago', 'gary'))
        as pivot_values (customer_name, customer_city, customer_state, 
                         food_pref_count, 
                         chicago_distance_miles, gary_distance_miles)









