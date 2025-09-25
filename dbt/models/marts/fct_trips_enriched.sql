-- models/marts/fct_trips_enriched.sql
-- Inserted columns are all in this table

{{ config(materialized='view') }}


with base as (
  select *
  from {{ ref('fct_trips') }}
  where pickup_ts >= timestamp '2015-01-01'
    and pickup_ts <  timestamp '2025-01-01'
),
em as (
  select
    upper(taxi_type)           as taxi_type,
    co2_grams_per_mile::double as co2_grams_per_mile
  from {{ source('lookup', 'vehicle_emissions') }}
),
seconds as (
  select
    b.*,
    greatest(date_diff('second', b.pickup_ts, b.dropoff_ts), 0) as trip_seconds
  from base b
)

select
  s.taxi_type,
  s.pickup_ts,
  s.dropoff_ts,
  s.passenger_count,
  s.trip_distance,

  -- durations
  (s.trip_seconds / 60.0)                                  as trip_duration_minutes,
  (s.trip_seconds / 3600.0)                                as trip_duration_hours,

  -- avg mph: 0 if duration is 0
  case when s.trip_seconds > 0
       then s.trip_distance / (s.trip_seconds / 3600.0)
       else 0
  end                                                      as avg_mph,

  -- CO2 in kilograms via live lookup, inner-join guarantees non-null
  (s.trip_distance * e.co2_grams_per_mile) / 1000.0        as trip_co2_kgs,

  -- time features
  extract('hour'  from s.pickup_ts)                        as hour_of_day,
  extract('dow'   from s.pickup_ts)                        as day_of_week,
  extract('week'  from s.pickup_ts)                        as week_of_year,
  extract('month' from s.pickup_ts)                        as month_of_year

from seconds s
join em e
  on e.taxi_type = upper(s.taxi_type)
