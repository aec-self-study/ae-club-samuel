with pageviews_source as (

    select * from {{ source('web_tracking', 'pageviews') }}

), renamed as (

    select
        id as pageview_id,
        customer_id,
        visitor_id,
        device_type,
        timestamp as pageview_timestamp,
        page as page_name
    from pageviews_source

)

select * from renamed
