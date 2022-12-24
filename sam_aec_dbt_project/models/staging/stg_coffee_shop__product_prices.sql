with product_prices_source as (

    select * from {{ source('coffee_shop', 'product_prices') }}

),

renamed as (

    select
        id as product_prices_id,
        product_id,
        price,
        created_at,
        ended_at
    from product_prices_source

)

select * from renamed
