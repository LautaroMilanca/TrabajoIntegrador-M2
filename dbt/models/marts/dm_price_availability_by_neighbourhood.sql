select
  d.neighbourhood_group,
  d.neighbourhood,
  avg(f.price)::numeric(10,2)              as avg_price,
  percentile_cont(0.5) within group (order by f.price) as median_price,
  avg(f.availability_365)::numeric(10,2)   as avg_availability_365,
  count(*)                                 as listings_count
from {{ ref('fact_listings') }} f
join {{ ref('dim_neighbourhood') }} d using (neighbourhood_key)
group by 1,2
