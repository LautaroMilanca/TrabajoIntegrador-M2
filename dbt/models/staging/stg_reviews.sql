with src as (
  select listing_id, payload from {{ source('raw','raw_reviews_json') }}
)
select
  listing_id,
  (payload->>'review_id')::bigint as review_id,
  (payload->>'rating')::int       as rating,
  (payload->>'review_date')::date as review_date
from src
where payload ? 'review_id'
