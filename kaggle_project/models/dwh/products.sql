{{
    config(
        materialized='table'
    )
}}

with products_english as (SELECT *
                          FROM {{ ref('stg_ecommerce__product_category_name_translation') }}),
     products as (SELECT *
                  FROM {{ ref('stg_ecommerce__products') }}),
     mapped_products as (SELECT product_id,
                                products.product_category_name,
                                product_name_lenght,
                                product_description_lenght,
                                product_photos_qty,
                                product_weight_g,
                                product_length_cm,
                                product_height_cm,
                                product_width_cm,
                                product_category_name_english

                         FROM products
                                  LEFT JOIN products_english
                                            ON products.product_category_name = products_english.product_category_name)
SELECT *
FROM mapped_products