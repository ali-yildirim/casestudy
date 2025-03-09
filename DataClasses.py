
from dataclasses import dataclass
from typing import Any, Dict, Optional

@dataclass
class Category:
    id: str
    name: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Category':
        return cls(
            id=data["category"]["id"],
            name=data["category"]["name"]
        )

@dataclass
class Chain:
    id: str
    name: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Chain':
        return cls(
            id=data["chain"]["id"],
            name=data["chain"]["name"]
        )

@dataclass
class Hotel:
    
    
    id: int
    name: str
    category_id: Optional[int]
    chain_id: Optional[int]
    location: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Hotel':
        
        if data["location"]["obfuscation_required"] == True:
            location_str = f"{data['location']['obfuscated_coordinates']['latitude']},{data['location']['obfuscated_coordinates']['longitude']}"
        else:
            location_str = f"{data['location']['coordinates']['latitude']},{data['location']['coordinates']['longitude']}"
        return cls(
            id=data["property_id"],
            name=data["name"],
            category_id=data["category"].get("id"),
            chain_id=data["chain"].get("id"), 
            location=location_str
        )


    @classmethod
    def field_from_id(cls, chain_id, category_id, cursor):
        query = "SELECT name FROM Category WHERE id = %s"
        cursor.execute(query, (category_id,))
        category_name = cursor.fetchone()[0]
        query = "SELECT name FROM _Chain_ WHERE id = %s"
        cursor.execute(query, (chain_id,))
        chain_name = cursor.fetchone()[0]
        return category_name, chain_name