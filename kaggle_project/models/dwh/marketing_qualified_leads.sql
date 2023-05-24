{{
    config(
        materialized='table'
    )
}}

SELECT *
FROM {{ ref('stg_ecommerce__marketing_qualified_leads') }}