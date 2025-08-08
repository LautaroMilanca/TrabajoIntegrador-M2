select distinct
  {{ dbt_utils.generate_surrogate_key(['room_type']) }} as room_type_key,
  room_type
from {{ ref('stg_listings') }}
