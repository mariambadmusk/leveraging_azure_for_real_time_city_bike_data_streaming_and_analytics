%sql

CREATE DATABASE IF NOT EXISTS "bike_networks_monitor";

USE "bike_networks_monitor";

-- The following table is used to store the bike networks
-- dime_bike_networks
CREATE TABLE IF NOT EXISTS bike_networks_monitor.dim_bike_networks(
    "network_id" INT PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "latitude" VARCHAR(255),
    "longitude" VARCHAR(255),
    "city" VARCHAR(255),
    "country" VARCHAR(255),
    "company" VARCHAR(255),
    PRIMARY KEY ("network_id")
);


-- The following table is used to store the status of the bike stations as at runtime
-- dime_stations_status
CREATE TABLE IF NOT EXISTS bike_networks_monitor.dim_stations_status(
    "station_id" VARCHAR(255) PRIMARY KEY,
    "timestamp" TIMESTAMP,
    "free_bikes" INT,
    "empty_slots" INT,
    "total_slots" INT,
    "has_ebikes" BOOLEAN,
    "ebikes" INT,
    "normal_bikes" INT,
    "renting" BOOLEAN,
    "returning" BOOLEAN,
    "last_updated" TIMESTAMP,

);

-- The following table is used to store the bike stations for each network
-- fact_bike_stations
CREATE TABLE IF NOT EXISTS bike_networks_monitor.fact_bike_stations(
    "station_id" VARCHAR(255) PRIMARY KEY,
    "network_id" VARCHAR(255),
    "name" VARCHAR(255)
    "uid" VARCHAR(255),
    FOREIGN KEY ("network_id") REFERENCES bike_networks_monitor.dim_bike_networks("network_id")
);


-- The following table is used to store the weather data for each city of the bike stations
-- fact_weather
CREATE TABLE IF NOT EXIST bike_networks_monitor.fact_weather(
    city VARCHAR (255),
    localtime TIMESTAMP, 
    last_updated TIMESTAMP,
    condition VARCHAR(255),
    temp_c FLOAT,
    feelslike_c FLOAT,
    heat_index FLOAT,
    wind_mph FLOAT,
    wind_degree INTEGER,
    wind_dir VARCHAR (255),
    humidity INTEGER,
    cloud INTEGER
    FOREIGN KEY (city) REFERENCES bike_networks_monitor.dim_city(city)
);


-- The following table is used to store the location of the bike stations
-- dim_City
CREATE TABLE IF NOT EXISTS bike_networks_monitor.dim_city(
    "city" VARCHAR(255) PRIMARY KEY,
    "country" VARCHAR(255),
    "latitude" FLOAT,
    "longitude" FLOAT,
    "timezone" VARCHAR(255)
    );



-- create dimension table for bike networks
CREATE TABLE IF NOT EXISTS dim_network (
    network_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    city VARCHAR,
    country VARCHAR,
    company VARCHAR
);

-- create dimension table for bike stations
CREATE TABLE IF NOT EXISTS dim_station (
    station_id VARCHAR PRIMARY KEY,
    network_id VARCHAR REFERENCES dim_network(network_id),
    name VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    uid VARCHAR,
    address VARCHAR,
    number INT
);

-- create dimension table for time
CREATE TABLE IF NOT EXISTS dim_time (
    timestamp_id TIMESTAMP PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    hour INT,
    minute INT,
    second INT
);

-- create fact table
CREATE TABLE IF NOT EXISTS station_status (
    station_id VARCHAR REFERENCES dim_station(station_id),
    network_id VARCHAR REFERENCES dim_network(network_id),
    timestamp_id TIMESTAMP REFERENCES dim_time(timestamp_id),
    free_bikes INT,
    empty_slots INT,
    renting INT,
    returning INT,
    ebikes INT,
    normal_bikes INT,
    slots INT,
    -- creating a composite primary key
    PRIMARY KEY (station_id, timestamp_id)
);

-- create staging fact table for upserts(to ensure idempotency)
CREATE TABLE IF NOT EXISTS stg_station_status (
    station_id VARCHAR,
    network_id VARCHAR,
    timestamp_id TIMESTAMP,
    free_bikes INT,
    empty_slots INT,
    renting INT,
    returning INT,
    ebikes INT,
    normal_bikes INT,
    slots INT
);

-- learn about creating indexes for query performance optimization



