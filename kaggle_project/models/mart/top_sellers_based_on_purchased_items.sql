{{
    config(
        materialized='view'
    )
}}

with order_items as (SELECT seller_id,
                            count(seller_id) as sold_items
                     FROM edw.order_items
                     GROUP BY seller_id)


SELECT order_items.seller_id,
       order_items.sold_items,
       seller.seller_zip_code_prefix,
       seller.seller_city,
       seller.seller_state

FROM order_items
         JOIN edw.sellers seller on order_items.seller_id = seller.seller_id