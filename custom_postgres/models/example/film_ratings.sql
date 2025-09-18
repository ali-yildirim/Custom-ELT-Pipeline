WITH films_with_ratings as (
    select 
        film_id,
        title,
        release_date,
        price,
        rating,
        user_rating,
        case
            when user_rating >= 4.5 then 'Excellent'
            when user_rating >= 4.0   then 'Good'
            when user_rating >= 3 then 'Average'
            else 'poor'
        end as rating_category
    from 
        {{ ref('films')}}
),
films_with_actors as (
    select 
        f.film_id,
        f.title,
        STRING_AGG(a.actor_name,',') as actors
    from
        {{ ref('films')}} f
    left join {{ref('film_actors')}} fa ON f.film_id = fa.film_id
    left join {{ref('actors')}} a on fa.actor_id = a.actor_id
    group by f.film_id, f.title
)
select 
    fwf.*,
    fwa.actors
from films_with_ratings fwf
left join films_with_actors fwa on fwf.film_id = fwa.film_id
