with customers as (

    select * from {{ ref('stg_coffee_shop__customers') }}

), orders as (

    select * from {{ ref('stg_coffee_shop__orders') }}

), order_items as (

    select * from {{ ref('stg_coffee_shop__order_items') }}

), products as (

    select * from {{ ref('stg_coffee_shop__products') }}

), product_prices as (

    select * from {{ ref('stg_coffee_shop__product_prices') }}

), joined as (

    select
        products.product_id,
        orders.order_id,
        customers.customer_id,
        products.product_name,
        products.category,
        product_prices.price as revenue,
        orders.created_at as ordered_at,
        row_number() over (partition by customers.customer_id order by orders.created_at) > 1 as is_return_customer
    from orders
    left join customers
        on orders.customer_id = customers.customer_id
    left join order_items
        on orders.order_id = order_items.order_id
    left join products
        on order_items.product_id = products.product_id
    left join product_prices
        on products.product_id = product_prices.product_id
    where orders.created_at >= product_prices.created_at
    qualify row_number() over (partition by orders.order_id, products.product_id order by product_prices.created_at desc) = 1

)

select * from joined
