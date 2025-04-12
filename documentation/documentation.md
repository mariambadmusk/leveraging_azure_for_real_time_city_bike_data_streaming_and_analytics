# Revision History

| Version |       Author       |     Date     |            Description                |
| ------- | ------------------ | ------------ | ------------------------------------- |
|   1.0   |   Mariam Badmus    |  12-04-2025  |     Updated the project section       |

# Project Title

## Project Goals
A automated scalable ETL pipline on that uses the power of Azure cloud to integrate New York City's transport, weather, and traffic data to provide actionable insights on bus performance, delays, and route efficiency in New York City. The project goal is to support smarter transportation planning, improve commuting efficiency, and enable real-time data-driven decisions. 


  Stakeholders 
  
  |       name        |       role       |     GitHub Profile    |      
  | ----------------- | ---------------- | ------------------    | 
  |   Mariam Badmus   |                  |    mariambadmusk      |  
  
## Project Output
## Project Plan
  Phase 1: Requirement Gathering
  * Choose APIs: MTA Bus Time API, 511NY Traffic API, Weather API
  * Design Data Schema and Architecture
    
  Phase 2: Data Ingestion
  * Extract Data from:
    * MTA Bus Time API
    * 511Ny Traffic API
    * Weather API
  * Unit Tests:
    * Validate each API response and schemas
    * 
  * Integeration Tests

 Phase 3: Data Transformation
* Use Azure Databricks (PySpark) to:
   * Clean and transform data
   * Select and merge data
   * Create aggregated data

 Phase 4: Data Loading
 * Load transformed data to:
   * Azure Synapse Analytics
  
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
  - Azure Synapse Analytics/Azure Postgresql
  - Azure Managed Airflow
  - Azure Databricks
    
## Data Architecture Diagram
## Data Workflow Diagram
    

# Data Lifecycle Overview
  ## Data source
  * all data acquired
  * location   (e.g URL, API, file path)
  * method of acquisition  (e.g scraping, API calls)
  * problems encountered
    
  ## Data Description 
  Format of the data
  Size of data (records, fields, tables)
  Column names
  Aggregate statistics
  Evaluation of data against requirements
  Business terms
  Sensitive information present?   
  
  ## Data Quality
  Completeness
  Accuracy / Error presence
  Missing values (format, frequency)
  Data quality verification results, if there is a probem with data quality: Solutions?
      
  ## Data Transformation
  ### Data Selection
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
      

