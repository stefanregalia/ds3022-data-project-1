-- Combine yellow and green taxi trips into a single fact table

{{ config(materialized='view') }}

with trips as (
  select * from {{ ref('stg_yellow') }}
  union all
  select * from {{ ref('stg_green') }}
)
select
  taxi_type,
  pickup_ts,
  dropoff_ts,
  passenger_count,
  trip_distance
from trips
where pickup_ts >= timestamp '2015-01-01'
  and pickup_ts <  timestamp '2025-01-01'
