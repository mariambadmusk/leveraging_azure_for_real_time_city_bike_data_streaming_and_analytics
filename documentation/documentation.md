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

---

#### Size of the Data

- **Networks**
  - Records: ~700+ networks globally (varies based on API)
  - Fields per record: ~7 top-level fields including nested `location` and `company` arrays
    
  - Networks Table
      - `id`
      - `name`
      - `location.latitude`
      - `location.longitude`
      - `location.city`
      - `location.country`
      - `href`
      - `company`
      - `gbfs_href`
        
- **Stations**
  - Records: Varies by network (for example `Accès Vélo` has ~10–100 stations)
  - Fields per station: ~15+ fields

    - Stations Table
      - `id`
      - `station_id`
      - `name`
      - `latitude`
      - `longitude`
      - `timestamp`
      - `free_bikes`
      - `empty_slots`
      - `uid`
      - `renting`
      - `returning`
      - `last_updated`
      - `address`
      - `has_ebikes`
      - `ebikes`
      - `normal_bikes`


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

- **Networks Data:** Largely complete, with fields such as `id`, `name`, `location`, and `gbfs_href` consistently present.
- **Stations Data:** Generally complete; however, some fields like `ebikes`, `normal_bikes`, or `free_bikes` may be missing or return `null` occasionally 
    depending on the network status.

#### Accuracy / Error Presence

- **Timestamp Fields:** Timestamps are accurate and in ISO 8601 format (`UTC`), matching real-time expectations.
- **Coordinates:** Latitude and longitude fields appear valid and consistent with actual city geography.
- **Boolean Fields:** Fields like `renting` and `returning` correctly reflect operational status.

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
      
  ## Data Transformation
  
  ## Data Selection
  * data(fields/columns) to be used
  * relevance to project goals
  * technical constraints like data volume and data types


  ### Data Modeling
  Transformation required
  Loading the data/Data storage

  ### Data Cleaning/Transformation
  * Derived columns e.g data aggregation
  * Data merged
  * Data reformatted/columns reordered
  * Data standardisation/ Data mapping (new schema design)
    
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
      

