-- Deduplicating on trip_id
SELECT trip_id, COUNT(*) AS duplicate_count
FROM trips
GROUP BY trip_id
HAVING COUNT(*) > 1 ;

-- trip status

select distinct(trip_status)
from trips;


-- day of week

select distinct(day_type)
from trips;

select distinct(is_ghost_trip)
from trips;

select distinct(vehicle_type)
from trips;
