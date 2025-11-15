import requests
import os
from loguru import logger

from typing import List, Dict, Any


API_URL = "https://fake-api-vycpfa6oca-uc.a.run.app/"
AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")


def get_sales(date: str) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.

    :param date: data retrieve the data from
    :return: list of records
    """
    sales_data = []
    MAX_PAGES = 500 

    for page in range(1,MAX_PAGES+1):

        params: dict[str, str] = {"date": date, "page": page}
        headers: dict = {"Authorization": AUTH_TOKEN}

        try:
            request_url: str = requests.Request("GET", API_URL+"sales", headers=headers, params=params)
            logger.info(f"Fetching from URL: {request_url}")

            response: requests.Response = requests.get(API_URL+"sales", headers=headers, params=params)
            response.raise_for_status()

            page_data = response.json()

            if not page_data:  
                break
            
            for doc in page_data:
                sales_data.append(doc)

        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                break
            else:
                logger.error(f"Request failed: {e}")
                break

        except requests.exceptions.RequestException as e:
            logger.error("RequestException: %s", e)
            break
    
    return sales_data