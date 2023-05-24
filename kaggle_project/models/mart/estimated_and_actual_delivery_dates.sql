{{
    config(
        materialized='view'
    )
}}

SELECT order_id,
       order_estimated_delivery_date as estimated_delivery_date,
       order_delivered_customer_date as delivered_customer_date,
       CASE when delivered_customer_date > estimated_delivery_date then 'Oops' else 'OK' END as  has_come_true
FROM edw.orders
WHERE order_status = 'delivered'