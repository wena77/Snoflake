with source as (select *
                from {{ source('kaggle', 'order_items_raw') }}),

     prepared as (SELECT order_id                              as order_id,
                         cast(order_item_id as number(2, 0))   as order_item_id,
                         product_id                            as product_id,
                         seller_id                             as seller_id,
                         cast(shipping_limit_date as datetime) AS shipping_limit_date,
                         cast(price as double)                 as price,
                         cast(freight_value as double)         as freight_value
                  FROM source)

SELECT *
FROM prepared