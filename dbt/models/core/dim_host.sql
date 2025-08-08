select distinct
  {{ dbt_utils.generate_surrogate_key(['host_id']) }} as host_key,
  host_id,
  host_name
from {{ ref('stg_listings') }}
