with source as (select *
                from {{ source('kaggle', 'orders_raw') }}),

     prepared as (SELECT order_id                                        as order_id,
                         customer_id                                     as customer_id,
                         order_status                                    as order_status,
                         cast(order_purchase_timestamp as datetime)      AS order_purchase_timestamp,
                         cast(order_approved_at as datetime)             as order_approved_at,
                         cast(order_delivered_carrier_date as datetime)  AS order_delivered_carrier_date,
                         cast(order_delivered_customer_date as datetime) AS order_delivered_customer_date,
                         cast(order_estimated_delivery_date as datetime) AS order_estimated_delivery_date
                  FROM source)

SELECT *
FROM prepared