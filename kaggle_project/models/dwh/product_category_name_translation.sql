{{
    config(
        materialized='table'
    )
}}

SELECT *
FROM {{ ref('stg_ecommerce__product_category_name_translation') }}