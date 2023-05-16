with source as (select * from {{ source('kaggle', 'product_category_name_translation_raw') }}),

     prepared as (SELECT product_category_name         as product_category_name,
                         product_category_name_english as product_category_name_english
                  FROM source)

SELECT *
FROM prepared