{{
    config(
        materialized='view'
    )
}}

with sellers as (SELECT seller_zip_code_prefix,
                        seller_city,
                        seller_state,
                        count(seller_zip_code_prefix, seller_city, seller_state) as total_sellers
                 FROM edw.sellers
                 GROUP BY seller_zip_code_prefix, seller_city, seller_state),
     customers as (SELECT customer_zip_code_prefix,
                          customer_city,
                          customer_state,
                          count(customer_zip_code_prefix,
                                customer_city,
                                customer_state) as total_customers
                   FROM edw.customers
                   GROUP BY customer_zip_code_prefix, customer_city, customer_state),
     geolocation as (SELECT distinct geolocation_zip_code_prefix,
                                     geolocation_city,
                                     geolocation_state
                     FROM edw.geolocations)

SELECT distinct geolocation_state,
                geolocation_city,
                geolocation_zip_code_prefix,
                sel.total_sellers,
                cus.total_customers
FROM geolocation geo
         LEFT JOIN sellers sel ON (sel.seller_zip_code_prefix = geo.geolocation_zip_code_prefix AND
                                   sel.seller_city = geo.geolocation_city AND
                                   sel.seller_state = geo.geolocation_state)
         LEFT JOIN customers cus ON (cus.customer_zip_code_prefix = geo.geolocation_zip_code_prefix AND
                                     cus.customer_city = geo.geolocation_city AND
                                     cus.customer_state = geo.geolocation_state)