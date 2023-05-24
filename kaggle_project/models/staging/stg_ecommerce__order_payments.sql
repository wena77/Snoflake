with source as (select *
                from {{ source('kaggle', 'order_payments_raw') }}),

     prepared as (SELECT order_id                                   as order_id,
                         cast(payment_sequential as number(2, 0))   as payment_sequential,
                         replace(payment_type, '_', ' ')            as payment_type,
                         cast(payment_installments as number(2, 0)) as payment_installments,
                         cast(payment_value as double)              as payment_value
                  FROM source)

SELECT *
FROM prepared