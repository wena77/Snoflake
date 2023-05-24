{{
    config(
        materialized='table'
    )
}}

SELECT *
FROM {{ ref('stg_ecommerce__order_reviews') }}