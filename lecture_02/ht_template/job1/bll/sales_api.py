import os
import shutil
from loguru import logger

from lecture_02.ht_template.job1.dal.sales_api import get_sales
from lecture_02.ht_template.job1.dal.local_disk import save_to_disk


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    
    if os.path.exists(raw_dir):
        shutil.rmtree(raw_dir)
    
    os.makedirs(raw_dir, exist_ok=True)

    raw_dir_path = os.path.join(raw_dir, f"sales_{date}.json")

    sales_data = get_sales(date)
    logger.info("\tPages received:", len(sales_data))
    
    save_to_disk(sales_data, raw_dir_path)

    logger.info(f"\tI'm in get_sales(...) function!")
