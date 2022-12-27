with pageviews_source as (

    select
        pageview_id,
        visitor_id,
        customer_id,
        device_type,
        pageview_timestamp,
        page_name
    from {{ ref('stg_web_tracking__pageviews') }}

), real_visitor_id_source as (

    select 
        customer_id,
        visitor_id
    from {{ ref('stg_web_tracking__pageviews') }}
    qualify row_number() over (partition by customer_id order by pageview_timestamp) = 1
    order by customer_id

), joined as (

    select
        coalesce(real_visitor_id_source.visitor_id, pageviews_source.visitor_id) as visitor_id,
        pageviews_source.pageview_id,
        pageviews_source.customer_id,
        pageviews_source.device_type,
        pageviews_source.pageview_timestamp,
        pageviews_source.page_name,
        row_number() over (
            partition by coalesce(real_visitor_id_source.visitor_id, pageviews_source.visitor_id)
            order by pageviews_source.pageview_timestamp
        ) as event_number,
        row_number() over (
            partition by coalesce(real_visitor_id_source.visitor_id, pageviews_source.visitor_id)
            order by pageviews_source.pageview_timestamp
        ) = 1 as is_first_pageview
    from pageviews_source
        left join real_visitor_id_source
            on pageviews_source.customer_id = real_visitor_id_source.customer_id
    order by pageviews_source.customer_id, pageviews_source.pageview_timestamp

), sessioned as(

    select
        visitor_id,
        pageview_id,
        customer_id,
        device_type,
        pageview_timestamp,
        page_name,
        event_number,
        is_first_pageview,
        timestamp_diff(
            lead(pageview_timestamp, 1) over (
                partition by visitor_id
                order by pageview_timestamp
            ),
            pageview_timestamp,
            minute
        ) as minutes_between_pageviews,
        timestamp_diff(
            lead(pageview_timestamp, 1) over (
                partition by visitor_id
                order by pageview_timestamp
            ),
            pageview_timestamp,
            minute
        ) > 30
        or is_first_pageview
        as is_new_session
    from joined

)

select 
    *,
    sum(if(is_new_session, 1, 0)) over (partition by visitor_id order by event_number) as session_number
from sessioned
