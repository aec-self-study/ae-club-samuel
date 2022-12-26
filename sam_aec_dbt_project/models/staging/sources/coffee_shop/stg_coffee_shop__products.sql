with products_source as (

    select * from {{ source('coffee_shop', 'products') }}

), renamed as (

    select
        id as product_id,
        name as product_name,
        category,
        created_at

    from products_source

)

select * from renamed
