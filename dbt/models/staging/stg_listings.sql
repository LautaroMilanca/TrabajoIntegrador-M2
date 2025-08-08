with src as (
  select * from {{ source('raw','raw_listings') }}
)
select
  cast(id as bigint)                          as listing_id,
  trim(name)                                  as name,
  cast(host_id as bigint)                     as host_id,
  trim(host_name)                             as host_name,
  lower(trim(neighbourhood_group))            as neighbourhood_group,
  lower(trim(neighbourhood))                  as neighbourhood,
  cast(latitude as double precision)          as latitude,
  cast(longitude as double precision)         as longitude,
  lower(trim(room_type))                      as room_type,
  cast(price as integer)                      as price,
  cast(minimum_nights as integer)             as minimum_nights,
  cast(number_of_reviews as integer)          as number_of_reviews,
  cast(calculated_host_listings_count as int) as calculated_host_listings_count,
  cast(availability_365 as integer)           as availability_365,
  {{ dbt_utils.safe_cast("last_review", "date") }} as last_review,
  cast(reviews_per_month as double precision) as reviews_per_month
from src
where price >= 0
  and minimum_nights >= 1
  and latitude between -90 and 90
  and longitude between -180 and 180
  and availability_365 between 0 and 365
