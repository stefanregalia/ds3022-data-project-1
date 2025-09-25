-- Normalizing yellow_clean into a consistent schema for downstream facts.

with src as (
  select * from "emissions"."main"."yellow_clean"
)
select
  src.tpep_pickup_datetime::timestamp   as pickup_ts,
  src.tpep_dropoff_datetime::timestamp  as dropoff_ts,
  src.trip_distance::double             as trip_distance,
  src.passenger_count::int              as passenger_count,
  'YELLOW'                              as taxi_type
from src


