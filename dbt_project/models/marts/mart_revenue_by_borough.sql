select
    pickup_borough as borough,
    count() as trip_count,
    sum(total_amount) as total_revenue
from {{ ref('int_trips_with_zones') }}
group by pickup_borough
