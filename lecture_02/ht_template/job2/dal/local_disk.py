import json
import fastavro
from loguru import logger
from typing import List, Dict, Any



def convert_json_to_avro(json_path: str, avro_path:str) -> None:
      """
      Get data from json file and converts to avro

      """
    
      with open(json_path, "r", encoding="utf-8") as f:
         sales_data = json.load(f)

         if isinstance(sales_data, dict):
            sales_data = [sales_data]

         first_record = sales_data[0]
         fields: list = []
         field_type: str = ''
         for key, value in first_record.items():
            if isinstance(value, int):
               field_type = 'int'
            elif isinstance(value, float):
               field_type = 'float'
            else:
               field_type = 'string'
            fields.append({'name': key, 'type': field_type})
        
         schema = {'type': 'record',
                   'name': 'sales',
                   'fields': fields}
        
         with open(avro_path, "wb") as out:
            fastavro.writer(out, schema, sales_data)

      logger.info(f"Saved records to {avro_path}")