with source as (select *
                from {{ source('kaggle', 'order_reviews_raw') }}),

     prepared as (SELECT review_id                                 as review_id,
                         order_id                                  as order_id,
                         cast(review_score as number(5, 0))        as review_score,
                         review_comment_title                      as review_comment_title,
                         review_comment_message                    as review_comment_message,
                         cast(review_creation_date as datetime)    AS review_creation_date,
                         cast(review_answer_timestamp as datetime) AS review_answer_timestamp
                  FROM source)

SELECT *
FROM prepared