SELECT
    product_id::integer     as product_id,
    name,
    category,
    cost::double            as cost
FROM {{ source('staging', 'products') }}
