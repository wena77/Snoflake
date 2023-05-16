with source as (select *
                from {{ source('kaggle', 'marketing_qualified_leads_raw') }}),

     prepared as (SELECT mql_id                           as mql_id,
                         cast(first_contact_date as date) AS first_contact_date,
                         landing_page_id                  as landing_page_id,
                         origin                           as origin
                  FROM source)

SELECT *
FROM prepared