with customers as (

  select 
    id as customer_id, 
    name, 
    email 
  from `analytics-engineers-club.coffee_shop.customers`

), orders as (
  
  select 
    customer_id, 
    min(created_at) as first_order_at, 
    count(*) as orders 
  from `analytics-engineers-club.coffee_shop.orders` 
  group by 1
  
)

select
  customers.customer_id,
  customers.name,
  customers.email,
  orders.first_order_at,
  orders.orders
from customers
left join orders
  on customers.customer_id = orders.customer_id
order by first_order_at
