SELECT
    item_id::integer        as item_id,
    order_id::integer       as order_id,
    product_id::integer     as product_id,
    quantity::integer       as quantity,
    unit_price::double      as unit_price
FROM {{ source('staging', 'order_items') }}
