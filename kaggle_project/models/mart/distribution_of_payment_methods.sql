{{
    config(
        materialized='view'
    )
}}


with payment_methods as (SELECT order_id,
                                payment_type
                         FROM edw.order_payments),

     orders as (SELECT o.order_id,
                       o.customer_id,
                       payment_methods.payment_type
                FROM edw.orders o
                         JOIN payment_methods ON o.order_id = payment_methods.order_id),

     customer as (SELECT orders.payment_type,
                         c.customer_city,
                         c.customer_state
                  FROM orders
                           JOIN edw.customers c ON c.customer_id = orders.customer_id)

SELECT customer_state as state,
       customer_city as city,
       payment_type,
       count(payment_type) as number_of_payments
FROM customer
GROUP BY payment_type, customer_city, customer_state
