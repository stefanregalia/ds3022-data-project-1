{{ config(materialized='table') }}

select
  upper(taxi_type) as taxi_type,
  count(*) as n_trips,
  min(date_trunc('year', pickup_ts)) as first_year_seen,
  max(date_trunc('year', pickup_ts)) as last_year_seen
from {{ ref('fct_trips') }}
where pickup_ts >= timestamp '2015-01-01'
  and pickup_ts <  timestamp '2025-01-01'
group by 1
order by n_trips desc
