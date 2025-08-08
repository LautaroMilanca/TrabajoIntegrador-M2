select
  h.host_id,
  h.host_name,
  count(*)                        as total_listings,
  avg(f.price)::numeric(10,2)     as avg_price
from {{ ref('fact_listings') }} f
join {{ ref('dim_host') }} h using (host_key)
group by 1,2
order by total_listings desc
