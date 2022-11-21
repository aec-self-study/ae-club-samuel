with customers as (

    select 
        customer_id,
        customer_name,
        customer_email
    from {{ ref('stg_customers') }}

), orders as (
    select
        order_id,
        customer_id,
        order_created_at,
        order_total_dollars,
        shipping_address,
        shipping_state,
        shipping_zip
    from {{ ref('stg_orders') }}

), joined as (

    select
        customers.customer_id,
        orders.order_id,
        customers.customer_name,
        customers.customer_email,
        orders.order_created_at,
        orders.order_total_dollars,
        orders.shipping_address,
        orders.shipping_state,
        orders.shipping_zip
    from customers
        left join orders on customers.customer_id = orders.customer_id

)

select * from joined
