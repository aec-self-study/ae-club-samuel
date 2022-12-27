with pageviews_source as (

    select
        visitor_id,
        session_number,
        min(pageview_timestamp) as session_started_at,
        sum(minutes_between_pageviews) as session_minutes
    from {{ ref('fct_pageviews') }}
    group by 1, 2

)

select
    generate_uuid() as surrogate_key,
    visitor_id,
    session_number,
    session_started_at,
    session_minutes,
    timestamp_add(session_started_at, interval session_minutes minute) as session_ended_at
from pageviews_source
