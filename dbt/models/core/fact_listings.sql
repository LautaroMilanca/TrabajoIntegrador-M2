with s as (select * from {{ ref('stg_listings') }})
select
  listing_id,
  {{ dbt_utils.generate_surrogate_key(['host_id']) }}                               as host_key,
  {{ dbt_utils.generate_surrogate_key(['neighbourhood_group','neighbourhood']) }}   as neighbourhood_key,
  {{ dbt_utils.generate_surrogate_key(['room_type']) }}                              as room_type_key,
  price, minimum_nights, number_of_reviews, reviews_per_month,
  calculated_host_listings_count, availability_365, last_review,
  latitude, longitude
from s
