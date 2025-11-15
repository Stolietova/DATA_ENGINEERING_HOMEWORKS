import os
import shutil
from loguru import logger

from lecture_02.ht_template.job2.dal import local_disk


def save_sales_to_local_disk(raw_dir: str, stg_dir: str) -> None:

    if os.path.exists(stg_dir):
        shutil.rmtree(stg_dir)
    os.makedirs(stg_dir, exist_ok=True)

    for fname in sorted(os.listdir(raw_dir)):
        if not fname.endswith(".json"):
            continue
        json_path: str = os.path.join(raw_dir, fname)
        avro_path: str = os.path.join(stg_dir, f"{fname[:-5]}.avro")
        
        local_disk.convert_json_to_avro(json_path, avro_path)

    logger.info(f"All files loaded!")
