# Revision History

| Version |       Author       |     Date     |                         Description                       |
| ------- | ------------------ | ------------ | --------------------------------------------------------- |
|   1.0   |   Mariam Badmus    |  12-04-2025  |     Updated the project section                           |
|   2.0   |   Mariam Badmus    |  17-04-2025  | Updated project goals & part of the data ifecyce overiew  |

# Leveraging Azure For Real Time City Bike Data Streaming And Analytics

## Project Goals
The primary goal of the City Bike Streaming ETL Project is to build a real-time data pipeline that continuously ingests, processes, and stores city bike station data for analytical and reporting purposes. The project aims to fetch live data every five minutes using asynchronous API calls and stream the data into Azure Events Hub with Kafka Protocol to ensure scalability and resilience. Using PySpark Structured Streaming, the data is then transformed and modelled into a star schema, enabling efficient querying and business intelligence insights. The transformed data is stored is loaded into a database for downstream use. The entire system is designed to be low-cost, modular, and easily scalable using Azure services. Monitoring and logging mechanisms are also implemented to ensure pipeline reliability and traceability.

The final output produces a set of analytical tables in a star schema format, including a fact table for bike station metrics and dimension tables for station details and timestamp breakdowns. The cleaned and structured data is made available for visualisation through BI tools to support stakeholders in tracking bike availability, usage trends, and operational KPIs in near real-time. 


  Stakeholders 
  
  |       name        |       role       |     GitHub Profile    |      
  | ----------------- | ---------------- | ------------------    | 
  |   Mariam Badmus   |                  |    mariambadmusk      |  
  
## Project Plan
  Phase 1: Requirement Gathering
  * Choose APIs: City Bike API 
  * Design Data Schema and Architecture
    
  Phase 2: Data Ingestion
  * Extract Data from:
    * City Bike API
    * Weather API
  * Unit Tests:
    * Validate each API response and schemas
      
  * Integeration Tests

 Phase 3: Data Transformation
* Use Azure Databricks (PySpark) to:
   * Clean and transform data
   * Select and merge data
   * Create aggregated data

 Phase 4: Data Loading
 * Load transformed data to:
   * Azure PostgreSQL
  
 Phase 5: Data Orchestration
 Phase 6: Data Testing & Validation
  
## Project Success Criteria 
 - Data Freshness
 - Scalability
 - Automation
 - Monitoring & Logging
 - Documentation

## Requirements
  - technical requirements? like system requirement etc
  - 
## Technology Stack
  - Azure Event Hub
  - Azure Postgresql
  - Azure Managed Airflow
  - Azure Databricks
    
## Data Architecture Diagram
## Data Workflow Diagram
    

# Data Lifecycle Overview
  ## Data source
  
- **All data acquired:** 
All the data used in this project was acquired from an open public API that provides live status updates of city bike stations. The primary data source is located at: `https://api.citybik.es/v2/` *https://api.citybik.es/v2/networks*, which returns real-time JSON-formatted data about each station’s availability, including the number of available bikes. The method of acquisition involved making repeated asynchronous API calls using the `aiohttp` library in Python, scheduled to run every five minutes to ensure the most up-to-date data is collected without overloading the source server.
 
    
 ## Data Description 
 
 #### Format of the Data

- **Type:** JSON
- **Structure:** Hierarchical, with two main sources:
  - `networks`: Metadata about bike-sharing networks
  - `stations`: Real-time status of individual bike stations
  - `weather`: Real-time status of weather across the cities

---

#### Size of the Data
  - `networks`: < 400 KB
  - `stations`: < 
  - `weather`: < 


- **Networks**
  - Records: ~700+ networks globally (varies based on API)
  - Fields per record: 8+ fields
      
        
- **Stations**
  - Records: Varies by network (for example `Accès Vélo` has ~10–100 stations)
  - Fields per station: 18 fields
 
- **Weather**
  - Records: Varies by customised settings on the Weather API web
  - Fields selected per query: 21 fields


#### 📈 Aggregate Statistics (Example - )

- **Total Stations**: 
- **Average Free Bikes**: 
- **Average Empty Slots**: 
- **% of Stations with eBikes**: 
- **Most Common Station Name Prefix**: 
---

#### Evaluation Against Requirements

| Requirement                          |    Status     | Notes                                                                 |
|--------------------------------------|------------- |----------------------------------------------------------------------  |
| Real-time bike availability          |     ✅       | Data refreshes frequently via API with timestamps                     |
| Location metadata                    |     ✅       | Latitude, longitude, and city provided                                |
| Station identification               |     ✅       | Unique `station_id` and `uid` present                                 |
| E-bike differentiation               |     ✅       | `has_ebikes`, `ebikes`, and `normal_bikes` fields available           |
| Scalability across multiple cities   |     ✅       | Data spans global networks in United States, Canada, Great Britain    |

---

#### Business Terms

| Term              | Description                                                     |
|-------------------|-----------------------------------------------------------------|
| `free_bikes`      | Number of bikes available for rent at the station               |
| `empty_slots`     | Number of docks available for returning bikes                   |
| `has_ebikes`      | Whether the station includes electric bikes                     |
| `timestamp`       | Time of the most recent status update renamed to lasst_updated  |
| `renting`         | Indicates if bikes can be rented from the station               |
| `returning`       | Indicates if bikes can be returned to the station               |

---

#### Sensitive Information Present?

- **No personally identifiable information (PII)** is present in the dataset.
- Coordinates are for station locations and not linked to individuals. 

  
## Data Quality

#### Completeness

- **Networks Data:** Complete
- **Stations Data:** Largely complete; however, some fields like `uid`, `address`, `ebikes`, `normal_bikes`, or `free_bikes` may be missing or return `null` occasionally. This issue arises primarily because of the dynamic schema from the stations API, which may omit data that isn't available at the time of the fetch.
- **Weather Data:** Complete

#### Accuracy 

- **Networks Data:** Generally accurate as fetched from a trusted source.
- **Stations Data:** Bike and slot numbers are usually accurate but can become stale between fetch intervals.
- **Weather Data:** Moderate accuracy; values are fetched using rounded `latitude` and `longitude values`, leading to possible inaccuracies when the city's weather differs from the station’s microclimate.


#### Timeliness
- **Networks Data:** Updates are infrequent but acceptable due to low volatility.
- **Stations Data:** Fetched every 10–15 minutes. This may not always reflect real-time changes, especially during peak usage times.
- **Weather Data:** Timeliness may be affected by API latency and update frequency, potentially showing slightly outdated conditions.

#### Uniqueness
- **Networks Data:** Unique by `network_id`; no duplication observed.
- **Stations Data:** Composite primary key on `station_id` and `timestamp` prevents duplication. However, we do not currently check if station status has changed since the last ingestion, which may lead to storage of identical rows.
- **Weather Data:** Same city weather may be recorded multiple times without changes due to a lack of change-detection logic.

#### Conformity
- **Networks Data:** High conformity with consistent use of naming conventions and data types.
- **Stations Data:** Field formats (e.g., coordinates, booleans) are consistent. When missing, fields are either null or omitted, which could be filled with standard placeholders like `unknown` or `not available`.
- **Weather Data:** Mostly conforms to schema. Field types are consistent, although some optional fields are missing depending on API response.

#### Validity
- **Networks Data:** Valid; schema constraints such as `PRIMARY KEY` and correct types are enforced.
- **Stations Data:** Referential integrity is upheld via foreign keys. Latitude and longitude values are within acceptable ranges.
- **Weather Data:** Fields like `temp_c`, `humidity`, and `wind_mph` fall within logical ranges. Timestamps are valid and parsable.
  

#### Missing Values

| Field           | Issue                                   | Frequency       |
|-----------------|------------------------------------------|-----------------|
| `ebikes`        | Missing or null in some stations         | Occasionally    |
| `normal_bikes`  | Sometimes `0` or omitted                 | Occasionally    |
| `free_bikes`    | May be `0` at inactive stations          | Low             |
| `last_updated`  | Rarely missing, but may be outdated      | Rare            |


#### Data Quality Verification Results

- Random station records were sampled across multiple networks to check field consistency
- Issues detected include:
  - Slight latency between timestamp and real-world availability.
  - Dynamic fields for some stations
  - 
####  Solutions for Data Quality Issues
- **Schema Enforcement:** Apply schema validation in PySpark to catch and fill missing fields with default values (e.g., `0` for bike counts).
- **Null Handling:** Replace missing numerical fields with zeros and flag records for potential review.
- **Data Cleaning:** Apply transformations to standardise formats and impute missing fields where feasible.

#### Possible Solutions

| Issue Identified                       | Suggested Solution                                                                  |
|----------------------------------------|-------------------------------------------------------------------------------------|
| Missing fields in station data         | Implement fallback values or enrichment (e.g., geocoding `address` from coordinates)|
| Weather inaccuracy due to rounding     | Use more precise coordinates where possible, or fetch per station if feasible       |
| Timeliness delay                       | Reduce fetch intervals or implement streaming if the API supports push updates      |
| Lack of change-detection               | Introduce hashing or comparison logic before ingestion to prevent duplicates        |
| Null values reducing conformity        | Use placeholders like `"unknown"` or `"not provided"` for better readability        |

      
## Data Transformation\Selection\Modelling


#### Goals of Transformation & Modelling

- Ensure **schema uniformity** from dynamically structured JSON
- Maintain **data integrity** and **referential consistency**
- Generate **dimensions** and **facts** for analytical modelling
- Map data to a **star schema** for performance and simplicity


#### Data Selection

#### Fields Selected

| Table                     | Fields Used                                                                 |
|---------------------------|------------------------------------------------------------------------------|
| `dim_bike_networks`       | `network_id`, `name`, `latitude`, `longitude`, `city`, `country`, `company` |
| `dim_bike_stations`       | `station_id`, `network_id`, `name`, `city`, `country`, `latitude`, `longitude`, `uid`, `address` |
| `fact_stations_status`    | `station_id`, `network_id`, `timestamp_id`, `free_bikes`, `empty_slots`, `ebikes`, `normal_bikes`, `renting`, `returning`, `slots` |
| `fact_weather`            | `city`, `localtime`, `last_updated`, `condition`, `temp_c`, `feelslike_c`, `humidity`, `wind_dir`, etc. |
| `dim_time`                | `timestamp_id`, `year`, `month`, `day`, `hour`, `minute`, `second`          |

#### Relevance to Project Goals

- **City**, **station**, and **network IDs** help group and segment bike availability and demand.
- **`free_bikes`**, **`ebikes`**, and **`empty_slots`** drive operational monitoring and supply/demand forecasting.
- **Weather features** like `humidity`, `temp_c`, and `condition` allow for correlation analysis between weather and bike usage.
- **Timestamps** standardised via `dim_time` enable time series and trend analysis.

#### Technical Constraints Considered

| Constraint                 |                         Description                                         |
|----------------------------|-----------------------------------------------------------------------------|
| **Latency & Timeliness**   | Updated every 10–15 minutes, tolerable for near-real-time analysis          |
| **Dynamic Schema**         | Stations API may omit fields                                                |
| **Data Types**             | Typed explicitly in SQL (e.g., `BOOLEAN`, `INT`, `VARCHAR`) to avoid issues |
| **Relational Mapping**     | Keys like `network_id` and `station_id` are reused across dimensions/facts  |

---

#### Transformation Logic by Table

#### 1. `dim_bike_networks`

| Raw Field          | Transformed Field | Description                           |
|--------------------|-------------------|---------------------------------------|
| `id`               | `network_id`      | Renamed for clarity                   |
| `location.city`    | `city`            | Extracted from nested JSON            |
| `company[0]`       | `company`         | Selected first listed company         |

---

#### 2. `dim_bike_stations`

| Raw Field       | Transformed Field | Description                                       |
|-----------------|-------------------|---------------------------------------------------|
| `extra.uid`     | `uid`             | Extracted from nested field `extra`              |
| `extra.address` | `address`         | May be missing; fallback applied if not present  |
| `network`       | `network_id`      | Linked to parent network                         |

---

#### 3. `fact_stations_status`

| Derived Field   | Logic                                                             |
|-----------------|-------------------------------------------------------------------|
| `normal_bikes`  | `free_bikes - ebikes`                                             |
| `slots`         | `free_bikes + empty_slots`                                        |
| `timestamp_id`  | Captured at ingestion time                                        |
| `renting`, `returning` | Coerced to `BOOLEAN`, ensuring consistency                |

---

#### 4. `fact_weather`

| Field          | Transformed From  | Description                                      |
|----------------|-------------------|--------------------------------------------------|
| `city`         | `location.name`   | Matched with station `city`                     |
| `temp_c`       | `current.temp_c`  | Rounded to one decimal                          |
| `condition`    | `current.condition.text` | Flattened from nested structure          |

---

#####  Transformation Flow Diagram (Text Representation)

```text
External APIs (Networks, Stations, Weather)
          ↓
    Raw JSON -> Staging Tables
          ↓
    Transformation Logic
     - Field Selection
     - Schema Normalisation
     - Type Casting
     - Enrichment & Derived Fields
          ↓
   Star Schema (Facts + Dimensions)

```
  

    
  ### Data Definition
  | Table   |      Column      | Business Description |     Data Type     |   Nullable Y/N?  |
  | ------- | ---------------- | -------------------- | ----------------  | ---------------- |
  |         |                  |                      |                   |                  |

  ### Data Mapping
  | Table   |       Column       |     Source   |          
  | ------- | ------------------ | ------------ | 
  |         |                    |              |

# Security Access
* Access controlled
* Sensitive data? If yes? Anonymisation/encryption
  
# Monitoring and Logging
* Data pipeline failuress wil be monitored
* Alert mechanism
  
  
# CI/CD Automation
* how deployments or tests will be automated
      

