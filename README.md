# Levergaing Azure For Real-Time City Bike Data Streaming And Analytics

This project implements a **real-time data pipeline** that streams, transforms, and analyses city bike station data from the [CityBikes API](https://api.citybik.es/) using **Azure services**.. It captures detailed station level metrics such as:

- Bike availability (`free_bikes`, `empty_slots`)
- Geolocation (`latitude`, `longitude`)
- Network and station metadata (`name`, `uid`, `address`, `company`)
- Last updated time (`timestamp`)

### 🔧 Tools & Technologies

- `aiohttp` for asynchronous data extraction
- **Apache Kafka** for real-time data ingestion
- **PySpark Structured Streaming with Azure Databricks** for scalable transformation
- **Azure Event Hub** for scalability and integration
- **Azure PostgreSQL** for storage
- **Power BI** for visualisation
- **Star Schema Design** for analytical queries


### Setup
### Contributions
### License
