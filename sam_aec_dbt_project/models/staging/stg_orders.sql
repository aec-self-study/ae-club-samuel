with orders as (

    select
        id as order_id,
        created_at as order_created_at,
        customer_id,
        total as order_total_dollars,
        address as shipping_address,
        state as shipping_state,
        zip as shipping_zip
    from {{ source('coffee_shop', 'orders') }}

)

select * from orders
