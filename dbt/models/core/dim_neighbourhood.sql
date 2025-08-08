select distinct
  {{ dbt_utils.generate_surrogate_key(['neighbourhood_group','neighbourhood']) }} as neighbourhood_key,
  neighbourhood_group,
  neighbourhood
from {{ ref('stg_listings') }}
