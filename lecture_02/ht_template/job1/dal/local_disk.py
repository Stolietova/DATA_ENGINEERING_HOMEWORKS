import os
import json
from typing import List, Dict, Any


def save_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:

     with open(path, "w", encoding="utf-8") as f:
        json.dump(json_content, f, ensure_ascii=False, indent=2)
