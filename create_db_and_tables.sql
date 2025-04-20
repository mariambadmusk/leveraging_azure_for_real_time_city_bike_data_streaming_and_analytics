%sql

CREATE DATABASE IF NOT EXISTS "bike_networks_monitor";

USE "bike_networks_monitor";

-- The following table is used to store the bike networks
-- dim_bike_networks
CREATE TABLE IF NOT EXISTS bike_networks_monitor.dim_bike_networks(
    "network_id" INT PRIMARY KEY,
    "name" VARCHAR(255),
    "latitude" FLOAT,
    "longitude" FLOAT,
    "city" VARCHAR(255),
    "country" VARCHAR(255),
    "company" VARCHAR(255),
    PRIMARY KEY ("network_id")
);


-- The following table is used to store the status of the bike stations as at runtime
-- dime_stations_status
CREATE TABLE IF NOT EXISTS bike_networks_monitor.fact_stations_status(
    "station_id" VARCHAR(255) REFERENCES dim_station(station_id),
    "network_id" VARCHAR (255) REFERENCES dim_network(network_id),
    "timestamp_id" TIMESTAMP REFERENCES dim_time(timestamp_id),
    "free_bikes" INT,
    "empty_slots" INT,
    "ebikes" INT,
    "normal_bikes" INT,
    "renting" BOOLEAN,
    "returning" BOOLEAN,
    "slots" INT,
    -- creating a composite primary key
    PRIMARY KEY (station_id, timestamp_id)
);

-- create staging fact table for upserts(to ensure idempotency)
CREATE TABLE IF NOT EXISTS bike_networks_monitor.staging_station_status(
    "station_id" VARCHAR(255) PRIMARY KEY,
    "network_id" VARCHAR(255),
    "timestamp_id" TIMESTAMP,
    "free_bikes" INT,
    "empty_slots" INT,
    "ebikes" INT,
    "normal_bikes" INT,
    "slots" INT
);

-- The following table is used to store the time dimension 
-- fact_time
CREATE TABLE IF NOT EXISTS bike_networks_monitor.fact_time(
    "timestamp_id" TIMESTAMP PRIMARY KEY,
    "year" INT,
    "month" INT,
    "day" INT,
    "hour" INT,
    "minute" INT,
    "second" INT
);

-- The following table is used to store the bike stations for each network
-- fact_bike_stations
CREATE TABLE IF NOT EXISTS bike_networks_monitor.dim_bike_stations(
    "station_id" VARCHAR(255) PRIMARY KEY,
    "network_id" VARCHAR(255) REFERENCES bike_networks_monitor.dim_bike_networks("network_id")
    "station_name" VARCHAR(255),
    "city" VARCHAR(255),
    "country" VARCHAR(255),
    "latitude" DOUBLE PRECISION,
    "longitude" DOUBLE PRECISION,
    "uid" VARCHAR(255),
    "address" VARCHAR(255),
    "renting" INT,
    "returning" INT,
    "has_ebikes" BOOLEAN
);


-- The following table is used to store the weather data for each city of the bike stations
-- fact_weather
CREATE TABLE IF NOT EXIST bike_networks_monitor.fact_weather(
    "city" VARCHAR (255) PRIMARY KEY,
    "localtime" TIMESTAMP, 
    "last_updated" TIMESTAMP,
    "condition" VARCHAR(255),
    "temp_c" FLOAT,
    "feelslike_c" FLOAT,
    "heat_index" FLOAT,
    "wind_mph" FLOAT,
    "wind_degree" INTEGER,
    "wind_dir" VARCHAR (255),
    "humidity" INTEGER,
    "cloud" INTEGER
    FOREIGN KEY (city) REFERENCES bike_networks_monitor.dim_bike_stations(city)
);






-- learn about creating indexes for query performance optimization


