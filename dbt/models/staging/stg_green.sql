-- Normalizing green_clean into a consistent schema for downstream facts.

with src as (
  select * from "emissions"."main"."green_clean"
)
select
  src.lpep_pickup_datetime::timestamp  as pickup_ts,
  src.lpep_dropoff_datetime::timestamp as dropoff_ts,
  src.trip_distance::double            as trip_distance,
  src.passenger_count::int             as passenger_count,
  'GREEN'                              as taxi_type
from src

