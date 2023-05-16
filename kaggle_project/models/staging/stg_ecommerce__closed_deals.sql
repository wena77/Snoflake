with source as (select *
                from {{ source('kaggle', 'closed_deals_raw') }}),

     prepared as (SELECT mql_id                                        as mql_id,
                         seller_id                                     as seller_id,
                         sdr_id                                        as sdr_id,
                         sr_id                                         as sr_id,
                         cast(won_date as datetime)                    AS won_date,
                         business_segment                              as business_segment,
                         lead_type                                     as lead_type,
                         lead_behaviour_profile                        as lead_behaviour_profile,
                         has_company                                   as has_company,
                         has_gtin                                      as has_gtin,
                         average_stock                                 as average_stock,
                         business_type                                 as business_type,
                         cast(declared_product_catalog_size as double) as declared_product_catalog_size,
                         cast(declared_monthly_revenue as double)      as declared_monthly_revenue
                  FROM source)

SELECT *
FROM prepared