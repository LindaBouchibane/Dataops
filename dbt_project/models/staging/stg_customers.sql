SELECT
    customer_id::integer    as customer_id,
    country,
    signup_date::date       as signup_date,
    segment
FROM {{ source('staging', 'customers') }}
