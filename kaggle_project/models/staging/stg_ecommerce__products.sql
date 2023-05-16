with source as (select *
                from {{ source('kaggle', 'products_raw') }}),

     prepared as (SELECT product_id                                       as product_id,
                         product_category_name                            as product_category_name,
                         cast(product_name_lenght as number(2, 0))        as product_name_lenght,
                         cast(product_description_lenght as number(4, 0)) as product_description_lenght,
                         cast(product_photos_qty as number(2, 0))         as product_photos_qty,
                         cast(product_weight_g as number(6, 0))           as product_weight_g,
                         cast(product_length_cm as number(3, 0))          as product_length_cm,
                         cast(product_height_cm as number(3, 0))          as product_height_cm,
                         cast(product_width_cm as number(3, 0))           as product_width_cm
                  FROM source)

SELECT *
FROM prepared