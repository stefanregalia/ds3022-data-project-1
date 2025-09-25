-- Quickly checking a sample of fct_trips_enriched to ensure all columns loaded and calculated correctly

{{ config(materialized='table') }}

-- one day slice + hard row cap for speed
select *
from {{ ref('fct_trips_enriched') }}
where pickup_ts >= timestamp '2015-01-01'
  and pickup_ts <  timestamp '2015-01-02'
limit 1000
