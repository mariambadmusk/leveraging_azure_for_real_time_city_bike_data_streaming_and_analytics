from scripts.utils import config_logging, publish_to_eventhub
import json
import asyncio
import aiohttp
import pandas as pd
import time


logger = config_logging()



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
    

    
async def fetch_all_stations(connection_str,event_hub_name, all_ids) -> None:

    """ Fetch all bike networks asynchronously """

    try:      
        url = "http://api.citybik.es/v2/networks"
        urls = [f"{url}/{network_id}" for network_id in all_ids]

        tasks = []
        for url in urls:
            tasks.append(fetch(url))    
            
        responses = await asyncio.gather(*tasks)

        await publish_to_eventhub(connection_str,event_hub_name, responses)
                        
        logger.info("Streaming data sent to event stations event hub")      
    except aiohttp.ClientError as e:
        logger.error(f"Error fetching data: {e}")
        return None



if __name__ = "__main__":
    pass


