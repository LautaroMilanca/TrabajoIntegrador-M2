select
  f.listing_id,
  count(r.review_id)             as reviews_count,
  avg(r.rating)::numeric(4,2)    as avg_rating,
  max(r.review_date)             as last_review_date
from {{ ref('fact_listings') }} f
left join {{ ref('stg_reviews') }} r
  on r.listing_id = f.listing_id
group by 1
