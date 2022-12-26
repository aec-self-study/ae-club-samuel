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
        visitor_id,
        row_number() over (partition by customer_id order by pageview_timestamp) as event_number
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
        pageviews_source.page_name
    from pageviews_source
        left join real_visitor_id_source
            on pageviews_source.customer_id = real_visitor_id_source.customer_id
    order by pageviews_source.customer_id, pageviews_source.pageview_timestamp

)

select * from joined
