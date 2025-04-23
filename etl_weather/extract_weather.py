from utils import config_logging, publish_to_eventhub
import aiohttp
import asyncio
import pandas as pd



logger = config_logging()


def get_coordinates(path: str) -> list:
    df = pd.read_csv(path)
    coordinates = df[["latitude", "longtitude"]].to_dict(orient="records")

    return coordinates
    


async def fetch(url: str):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    logger.error(f"Error fetching data from {url}: {response.status}")
                    return None      
    except aiohttp.ClientError as e:
        logger.error(f"Error fetching data: {e}")
        return None


async def fetch_all_weather(api_key: str, coordinates: list, weather_event_hub_connstr: str, weather_event_hub_name: str) -> None: 
                       
    tasks = []

    for row in coordinates():
        lat = row["latitude"]
        lon = row["longtitude"]
        url = f"https://api.weatherapi.com/v1/current.json?key={api_key}&q={lat},{lon}"
        tasks.append(fetch(url))


    responses = await asyncio.gather(*tasks)

    # Send data to event hub
    await publish_to_eventhub(weather_event_hub_connstr, weather_event_hub_name, responses)
                                                
    logger.info(f"Streaming data sent to Weather Event Hub")
                




if __name__ == "__main__":
    pass
   