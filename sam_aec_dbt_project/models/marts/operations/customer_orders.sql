with customers as (

    select 
        customer_id,
        customer_name,
        customer_email
    from {{ ref('stg_coffee_shop__customers') }}

), orders as (
    select
        order_id,
        customer_id,
        created_at,
        total,
        shipping_address,
        shipping_state,
        shipping_zip
    from {{ ref('stg_coffee_shop__orders') }}

), joined as (

    select
        customers.customer_id,
        orders.order_id,
        customers.customer_name,
        customers.customer_email,
        orders.created_at,
        orders.total,
        orders.shipping_address,
        orders.shipping_state,
        orders.shipping_zip
    from customers
        left join orders on customers.customer_id = orders.customer_id

)

select * from joined
