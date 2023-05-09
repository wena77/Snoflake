SELECT A.ID
    , FIRST_NAME
    , LAST_NAME
    , birthdate
    , BOOKING_REFERENCE
    , HOTEL
    , BOOKING_DATE
    , COST
FROM {{ref('customers')}}  A
JOIN {{ref('combined_bookings')}} B
on A.ID = B.ID