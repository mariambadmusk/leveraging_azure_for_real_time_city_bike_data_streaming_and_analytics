-- I'll be running these on postgres. would be a job i need to orchestrate with airflow or adf
INSERT INTO station_status
        (station_id, network_id, timestamp_id, free_bikes, empty_slots, 
        renting, returning, ebikes, normal_bikes, slots)
SELECT station_id, network_id, timestamp_id, free_bikes, empty_slots,
        renting, returning, ebikes, normal_bikes, slots
FROM stg_station_status
ON CONFLICT (station_id, timestamp_id) DO UPDATE
SET
    free_bikes = EXCLUDED.free_bikes,
    empty_slots = EXCLUDED.empty_slots,
    renting = EXCLUDED.renting,
    returning = EXCLUDED.returning,
    ebikes = EXCLUDED.ebikes,
    normal_bikes = EXCLUDED.normal_bikes,
    slots = EXCLUDED.slots;