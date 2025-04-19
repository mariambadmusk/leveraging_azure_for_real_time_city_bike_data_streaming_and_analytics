from utils import config_logging, publish_to_eventhub
import json
import asyncio
import aiohttp
import csv


logger = config_logging()



def read_network_ids():
    """ Read bike network ids from CSV file """

    try:
        with open("bike_networks.csv", "r", encoding="utf-8") as f:
            data = csv.reader(f)
            all_ids = list(data)

            logger.info("Network IDs read successfully.")
            return all_ids
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return None



async def fetch(url: str):
    """ Fetch data from API asynchronously """

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.text()
                    return data
                logger.info("Data fetched successfully.")
    except aiohttp.ClientError as e:
        logger.error(f"Error fetching data: {e}")
        return None
    

    
async def fetch_all_stations(event_hub_name, event_hub_namespace, all_ids) -> None:

    """ Fetch all bike networks asynchronously """

    try:
        
        
        url = "http://api.citybik.es/v2/networks"
        urls = [f"{url}/{network_id}" for network_id in all_ids]

        tasks = []
        for url in urls:
            tasks.append(fetch(url))    
            

        responses = await asyncio.gather(*tasks)


        for response in responses:
            if response:
                response = json.loads(response)  # convert text (str) to json to extract the networks

            tasks_stations = response.get("network", {}).get("stations", [])
            

            for stations in tasks_stations:
                streaming_data = ({
                    "network_id": response.get("network", {}).get("id"),
                    "station_id": stations["id"],
                    "station_name": stations["name"],
                    "latitude": stations["latitude"],
                    "longitude": stations["longitude"],
                    "last_updated": stations["timestamp"],  # rename timestamp key to last_updated
                    "free_bikes": stations["free_bikes"],
                    "empty_slots": stations["empty_slots"],
                    "uid": stations["extra"].get("uid", None),
                    "renting": stations["extra"].get("renting", 0),
                    "returning": stations["extra"].get("returning", 0),
                    "address": stations["extra"].get("address", None),
                    "has_ebikes": stations["extra"].get("has_ebikes", 0),
                    "ebikes": stations["extra"].get("ebikes", 0),
                    "normal_bikes": stations["extra"].get("normal_bikes", 0),
                    "number": stations["extra"].get("number", 0),
                    "slots": stations["extra"].get("slots", 0),
                })
                
                await publish_to_eventhub(event_hub_name, event_hub_namespace, streaming_data)
                        
        logger.info("Streaming data sent to event stations event hub")
        
    except aiohttp.ClientError as e:
        logger.error(f"Error fetching data: {e}")
        return None




    


