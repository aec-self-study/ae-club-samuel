with customers_source as (

    select
        customers.customer_id,
        min(orders.created_at) as customer_first_ordered_at
    from {{ ref('stg_coffee_shop__customers') }} customers
    left join {{ ref('stg_coffee_shop__orders') }} orders
        on customers.customer_id = orders.customer_id
    group by 1

), date_spine_source as (

    select *
    from
        unnest(generate_date_array('2010-01-01', current_date(), interval 1 day)) as date_day

), customer_dates as (

    select
        customers_source.customer_id,
        date_spine_source.date_day
    from customers_source
    left join date_spine_source
        on date(customers_source.customer_first_ordered_at) <= date_spine_source.date_day

), products_source as (

    select
        product_id,
        product_name,
        category
    from {{ ref('stg_coffee_shop__products') }}

), orders_source as (

    select
        order_id,
        customer_id,
        date(created_at) as created_date,
        total
    from {{ ref('stg_coffee_shop__orders') }}

), order_items_source as (

    select
        order_id,
        product_id
    from {{ ref('stg_coffee_shop__order_items') }}

), joined as (

    select
        customer_dates.date_day,
        customer_dates.customer_id,
        orders_source.order_id,
        order_items_source.product_id,
        products_source.product_name,
        products_source.category,
        ifnull(sum(orders_source.total), 0) as revenue
    from customer_dates
    left join orders_source
        on customer_dates.date_day = orders_source.created_date
            and customer_dates.customer_id = orders_source.customer_id
    left join order_items_source
        on orders_source.order_id = order_items_source.order_id
    left join products_source
        on order_items_source.product_id = products_source.product_id
    group by 1, 2, 3, 4, 5, 6

), by_week as (

    select
        date_trunc(date_day, week) as date_week,
        customer_id,
        ifnull(sum(revenue), 0) as sum_revenue
    from joined
    group by 1, 2

), window_sum as (

    select
        date_week,
        customer_id,
        sum_revenue,
        sum(sum_revenue) over (partition by customer_id order by date_week) as revenue_cumulative
    from by_week
    group by 1, 2, 3

)

select * from window_sum
        
