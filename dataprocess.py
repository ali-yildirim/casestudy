from DataMigrationSystem import DataMigrationClass
import json
from DataClasses import Category, Chain, Hotel
from typing import Dict, List
from dataclasses import dataclass
from typing import Any, Dict, Optional
import os


@dataclass
class Deneme:
    id: int
    name: str
    category_id: int
    chain_id: Optional[int]
    location: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Hotel':
        return cls(
            id=data['property_id'],
            name=data['name'],
            category_id=data['category_id'],
            chain_id=data.get('chain_id'),  # Chain might be optional
            location=data['location'])



file_path = 'example_hotel_data.json'
with open("example_hotel_data.json", 'r') as file:
    data = json.load(file)
            
hotels= []
for key, value in data.items():
    hotels.append(Deneme.from_dict(value, []))


