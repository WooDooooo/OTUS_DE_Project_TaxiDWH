select
    LocationID as location_id,
    Borough as borough,
    Zone as zone,
    service_zone
from {{ source('nyc_taxi', 'raw_zones') }}
