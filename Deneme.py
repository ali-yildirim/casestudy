from dataclasses import dataclass
from typing import Any, Dict, Optional
import os
import json
import mysql.connector
from typing import List
import logging

logger = logging.getLogger(__name__)

def process_json_file(file_path: str) -> List[Dict]:
        """Process a JSON file containing hotel provider data"""
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            
            # Extract the hotel data from the numbered keys
            processed_data = list(data.values())
            
            return processed_data
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format: {e}")
            raise
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error processing JSON file: {e}")
            raise

@dataclass
class Category:
    id: int
    name: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Category':
        return cls(
            id=data["category"]['id'],
            name=data["category"]['name']
        )
data = process_json_file('example_hotel_data.json')



for item in data:
    obje = Category.from_dict(item)
    print(obje)