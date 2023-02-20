/* 
	The query is structured as follows:
		- Get the number of unique sessions by date
    	- Get the average length of session (in sec) by day in two steps:
        	(1) length of each session and;
            (2) avg length session by day 
    	- Get the list of sessions ending with a viewed recipe. The table is then used as input to compute:
    		- Avg number of searches per day; 
        	- Most popular recipes IDs per day. When there is a tie, I report all them in an array  
*/


with 
	
    daily_n_session as (
		
        select
            to_date(event_timestamp) as date,
            count(distinct session_id) as n_sessions
        from vk_data.events.website_activity as n_sessions  
        group by 1
            
    ), 

    session_length as (

    	select 
        	to_date(event_timestamp) as date,
            session_id, 
    		timestampdiff(second, min(event_timestamp), max(event_timestamp)) as session_length_seconds
            from vk_data.events.website_activity 
            group by 1, 2
            
    ), 

    daily_average_session_length as (

    	select 
        	date, 
            round(avg(session_length_seconds)) as avg_session_length_seconds
        from session_length   
        group by 1
    
    ),
    
    sessions_with_viewed_recipes as (

        select 
            to_date(event_timestamp) as date,
        	session_id, 
            trim(parse_json(event_details):"recipe_id",'""') as recipe_id
        from vk_data.events.website_activity 
    	where contains(event_details, 'view_recipe')	
    ), 

    recipe_search_per_sessions as (

    	select 
    		sessions_with_viewed_recipes.date, 
        	sessions_with_viewed_recipes.session_id,
        	count(trim(parse_json(vk_data.events.website_activity.event_details):"event",'""')) as n_recipe_search
    from sessions_with_viewed_recipes
    inner join vk_data.events.website_activity 
    	on sessions_with_viewed_recipes.session_id  = vk_data.events.website_activity.session_id
    where contains(vk_data.events.website_activity.event_details, 'search')
    group by 1, 2

    ), 

    daily_average_n_recipe_search as (
    
    	select 
        	date, 
            round(avg(n_recipe_search)) as avg_n_recipe_search
        from recipe_search_per_sessions
    	group by 1 

    ), 
    
    daily_most_viewed_recipe_id as (

    	select 
        	date, 
            recipe_id
		from sessions_with_viewed_recipes
    	group by 1, 2
    	qualify rank() over(partition by date order by count(*) desc) = 1

    ), 

    daily_agg_most_viewed_recipe_id as (

    	select 
    		date, 
            array_agg(recipe_id) as most_viewed_recipe_id
    from daily_most_viewed_recipe_id
    group by 1 
    
    )

    select 
    	daily_n_session.date, 
        daily_n_session.n_sessions, 
        daily_average_session_length.avg_session_length_seconds, 
        daily_average_n_recipe_search.avg_n_recipe_search, 
        daily_agg_most_viewed_recipe_id.most_viewed_recipe_id
    from daily_n_session
    left join daily_average_session_length
    	on daily_n_session.date = daily_average_session_length.date
    left join daily_average_n_recipe_search
    	on daily_n_session.date = daily_average_n_recipe_search.date
    left join daily_agg_most_viewed_recipe_id
    	on daily_n_session.date = daily_agg_most_viewed_recipe_id.date
        
            



 



