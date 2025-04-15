%sql

CREATE DATABASE IF NOT EXISTS "bike_networks_monitor";

USE "bike_networks_monitor";

-- The following table is used to store the bike networks
CREATE TABLE IF NOT EXISTS "bike_networks_monitor"."bike_networks" (
    "network_id" INT NOT NULL ,
    "name" VARCHAR(255) NOT NULL,
    "latitude" VARCHAR(255) NOT NULL,
    "longitude" VARCHAR(255) NOT NULL,
    "city" VARCHAR(255) NOT NULL,
    "country" VARCHAR(255) NOT NULL,
    "company" VARCHAR(255) NOT NULL,
    PRIMARY KEY ("network_id")
);


-- The following table is used to store the status of the bike stations as at runtime
CREATE TABLE IF NOT EXISTS "bike_networks_monitor.stations_status"(
    "station_id" VARCHAR(255) NOT NULL,
    "timestamp" TIMESTAMP NOT NULL,
    "free_bikes" INT NOT NULL,
    "empty_slots" INT NOT NULL,
    "total_slots" INT NOT NULL,
    "has_ebikes" BOOLEAN NOT NULL,
    "ebikes" INT NOT NULL,
    "normal_bikes" INT NOT NULL,
    "renting" BOOLEAN NOT NULL,
    "returning" BOOLEAN NOT NULL,
    "last_updated" TIMESTAMP NOT NULL,

);

-- The following table is used to store the bike stations for each network
CREATE TABLE IF NOT EXISTS "bike_networks_monitor.bike_stations"(
    "network_id" VARCHAR(255) NOT NULL,
    "station_id" VARCHAR(255) NOT NULL,
    "name" VARCHAR(255) NOT NULL,
    "latitude" VARCHAR(255) NOT NULL,
    "longitude" VARCHAR(255) NOT NULL,
    "address" VARCHAR(255) NOT NULL,
    "uid" VARCHAR(255) NOT NULL
);


-- The following table is used to store the location of the bike stations
CREATE TABLE IF NOT EXISTS "bike_networks_monitor.location_stations"(
    "station_id" VARCHAR(255) NOT NULL,
    "city" VARCHAR(255) NOT NULL,
    "country" VARCHAR(255) NOT NULL,
    "latitude" VARCHAR(255) NOT NULL,
    "longitude" VARCHAR(255) NOT NULL,
    );




