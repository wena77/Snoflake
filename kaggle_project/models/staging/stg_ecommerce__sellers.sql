with source as (select *
                from {{ source('kaggle', 'sellers_raw') }}),

     prepared as (SELECT seller_id                                    as seller_id,
                         cast(seller_zip_code_prefix as number(5, 0)) AS seller_zip_code_prefix,
                         seller_city                                  as seller_city,
                         seller_state                                 as seller_state
                  FROM source)

SELECT *
FROM prepared