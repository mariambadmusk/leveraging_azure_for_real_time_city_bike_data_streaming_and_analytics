
CREATE TABLE IF NOT EXISTS dim_bike_networks (
    network_id VARCHAR(255) PRIMARY KEY,
    network_name VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    city VARCHAR(255),
    country VARCHAR(255),
    company VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS fact_time (
    timestamp_id TIMESTAMP PRIMARY KEY,
    year_ INT,
    month_ INT,
    day_ INT,
    hour_ INT,
    minute_ INT,
    second_ INT
);

CREATE TABLE IF NOT EXISTS staging_station_status (
    station_id VARCHAR(255) PRIMARY KEY,
    network_id VARCHAR(255),
    timestamp_id TIMESTAMP,
    free_bikes INT,
    empty_slots INT,
    ebikes INT,
    normal_bikes INT,
    slots INT,
    FOREIGN KEY (network_id) REFERENCES dim_bike_networks(network_id),
    FOREIGN KEY (timestamp_id) REFERENCES fact_time(timestamp_id)
);

CREATE TABLE IF NOT EXISTS dim_bike_stations (
    station_id VARCHAR(255) PRIMARY KEY,
    network_id VARCHAR(255),
    street VARCHAR(255),
    city VARCHAR(255),
    country VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    uid VARCHAR(255),
    address VARCHAR(255),
    renting INT,
    is_returning INT,
    has_ebikes BOOLEAN,
    FOREIGN KEY (network_id) REFERENCES dim_bike_networks(network_id)
);

CREATE TABLE IF NOT EXISTS fact_stations_status (
    station_id VARCHAR(255) PRIMARY KEY,
    network_id VARCHAR(255),
    timestamp_id TIMESTAMP,
    free_bikes INT,
    empty_slots INT,
    ebikes INT,
    normal_bikes INT,
    is_renting BOOLEAN,
    is_returning BOOLEAN,
    slots INT,
    FOREIGN KEY (network_id) REFERENCES dim_bike_networks(network_id),
    FOREIGN KEY (timestamp_id) REFERENCES fact_time(timestamp_id)
);




CREATE TABLE IF NOT EXISTS fact_weather (
    street VARCHAR(255) PRIMARY KEY,
    localtime_tz TIMESTAMP, 
    last_updated_tz TIMESTAMP,
    weather_condition VARCHAR(255),
    temp_c FLOAT,
    feelslike_c FLOAT,
    heat_index FLOAT,
    wind_mph FLOAT,
    wind_degree INTEGER,
    wind_dir VARCHAR(255),
    humidity INTEGER,
    cloud_condition INTEGER,
    FOREIGN KEY (city) REFERENCES dim_bike_stations(city)
);
