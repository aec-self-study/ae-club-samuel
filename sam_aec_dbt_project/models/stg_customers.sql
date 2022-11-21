with customers as (

    select
        id as customer_id,
        name as customer_name,
        email as customer_email
    from 'analytics-engineers-club.coffee_shop.customers'

)

select * from customers
