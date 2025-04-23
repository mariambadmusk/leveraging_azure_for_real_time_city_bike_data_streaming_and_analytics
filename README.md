# Levergaing Azure For Real-Time City Bike Data Streaming And Analytics

This project implements a **near-real-time data pipeline** that streams, transforms, and analyses city bike station data from the [CityBikes API](https://api.citybik.es/) and [weather api](https://www.weatherapi.com/) using **Azure services**.. It captures detailed station level metrics such as:

- Bike availability (`free_bikes`, `empty_slots`)
- Geolocation (`latitude`, `longitude`)
- Network and station metadata (`name`, `uid`, `address`, `company`)
- Last updated time (`timestamp`)

### Tools & Technologies

- `aiohttp` for asynchronous data extraction
- **PySpark Structured Streaming with Azure Databricks** for scalable transformation
- **Azure Event Hub** for scalability and integration
- **Azure PostgreSQL** for storage
- **Power BI** for visualisation
- **Star Schema Design** for analytical queries


## Setup

### 1. Set Up Azure Resources

### Azure Databricks Workspace
1. Go to the Azure Portal.
2. Search for **Azure Databricks** and click **Create**.
3. Configure:
   - Workspace name
   - Region (same across all services)
   - Pricing Tier
   - Resource Group
4. Click **Review + Create**, then **Create**.
5. Launch the workspace and create a **cluster**.



### Azure Database for PostgreSQL
1. In Azure Portal > search “Azure Database for PostgreSQL”.
2. Click **Create** > Choose **Single Server** or **Flexible Server**.
3. Configure:
   - Server name
   - Region (same as Databricks)
   - Admin username & password
4. Under **Networking**, allow public access.
5. Save server credentials.
6. Create a **database** under the server.

> Allow Databricks access under "Connection Security" by enabling access from Azure services or adding Databricks’ IP Or you an Allow other Azure services to connect during configuration set up.



### Azure Event Hubs
1. In Azure Portal, search **Event Hubs** > click **Create**.
2. Create a **Namespace** and choose region.
3. Inside the namespace:
   - Create a new **Event Hub** for stations_event_hub and another for weather_event_hub
   - Under **Settings/Shared Access Policies** , use or create `RootManageSharedAccessKey` or a `sharedAccessKey`
4. Save:
   - Namespace name
   - Event Hub name
   - Connection string


### 2. Clone the Project
```bash
git clone https://github.com/mariambadmusk/leveraging_azure_for_real_time_city_bike_data_streaming_and_analytics

```

### 3. Link Databricks to GitHub

In Databricks > User icon > User Settings > Git Integration tab > Choose GitHub, authenticate, and save.

### 4. Create a .env file add the below configurations

```bash

STATION_EVENTHUB_KEY=""
STATION_EVENTHUB_NAMESPACE=""
STATION_EVENTHUB_NAME=

WEATHER_EVENTHUB_KEY=
WEATHER_EVENTHUB_NAMESPACE=
WEATHER_EVENTHUB_NAME=

AZURE_POSTGRESQL_PASSWORD=
WEATHER_API_KEY=

JDBC_URL=""
AZURE_POSTGRESQL_USER=
DB_DRIVER=org.postgresql.Driver

SQL_CONN_URL = "postgresql://<username>:<password>@<server-name>.postgres.database.azure.com:5432/<database>"


```

### 5. Install Required Libraries

- Install pypi packages in Databricks:

    Cluster > Libraries > Install New > PyPI

```bash

aiohttp, pandas, azure-eventhub, azure.eventhub, asyncio, SQLAlchemy, python-dotenv

```

- Install Maven Packages:

    Cluster > Libraries > Install New > Maven

```bash

com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21

```



### Contributions
### License
