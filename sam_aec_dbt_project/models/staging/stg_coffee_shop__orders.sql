with orders as (

    select
        id as order_id,
        created_at,
        customer_id,
        total,
        address as shipping_address,
        state as shipping_state,
        zip as shipping_zip
    from {{ source('coffee_shop', 'orders') }}

)

select * from orders
