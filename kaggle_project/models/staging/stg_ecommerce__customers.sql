with source as (select *
                from {{ source('kaggle', 'customers_raw') }}),

     prepared as (SELECT customer_id                                    as customer_id,
                         customer_unique_id                             as customer_unique_id,
                         cast(customer_zip_code_prefix as number(5, 0)) AS customer_zip_code_prefix,
                         customer_city                                  as customer_city,
                         customer_state                                 as customer_state
                  FROM source)

SELECT *
FROM prepared