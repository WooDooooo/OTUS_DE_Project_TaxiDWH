select
    trips.vendor_id,
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.passenger_count,
    trips.trip_distance,
    trips.rate_code_id,
    trips.store_and_fwd_flag,
    trips.pickup_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    pickup_zone.service_zone as pickup_service_zone,
    trips.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    dropoff_zone.service_zone as dropoff_service_zone,
    trips.payment_type,
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.congestion_surcharge,
    trips.airport_fee
from {{ ref('stg_trips') }} as trips
left join {{ ref('stg_zones') }} as pickup_zone
    on trips.pickup_location_id = pickup_zone.location_id
left join {{ ref('stg_zones') }} as dropoff_zone
    on trips.dropoff_location_id = dropoff_zone.location_id
