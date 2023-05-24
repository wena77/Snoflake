{{
    config(
        materialized='view'
    )
}}

with order_items as (SELECT product_id,
                            count(product_id) as sold_items
                     FROM edw.products
                     GROUP BY product_id),

     sold as (SELECT order_items.product_id,
                     order_items.sold_items,
                     product.product_category_name,
                     product.product_category_name_english

              FROM order_items
                       JOIN edw.products product on order_items.product_id = product.product_id)

SELECT product_category_name,
       product_category_name_english,
       sum(sold_items) as sold_items
FROM sold
GROUP BY product_category_name, product_category_name_english


