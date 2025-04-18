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




