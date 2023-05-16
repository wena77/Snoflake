with source as (select *
                from {{ source('kaggle', 'geolocation_raw') }}),

     prepared as (SELECT cast(geolocation_zip_code_prefix as number(5, 0)) AS geolocation_zip_code_prefix,
                         geolocation_lat                                   as geolocation_lat,
                         geolocation_lng                                   as geolocation_lng,
                         geolocation_city                                  as geolocation_city,
                         geolocation_state                                 as geolocation_state
                  FROM source)

SELECT *
FROM prepared