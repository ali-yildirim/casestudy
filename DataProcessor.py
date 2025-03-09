from dataclasses import dataclass
from typing import Any, Dict, List
import json
import logging
from DataClasses import Category, Chain, Hotel

# Set up logger
logger = logging.getLogger(__name__)

class DataProcessorClass:
    """Processes and validates JSON data from hotel providers"""
    
    @staticmethod
    def process_json_file(file_path: str) -> List[Dict]:
        """Process a JSON file containing hotel provider data"""
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            
            # Extract the hotel data from the json
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
    
    @staticmethod
    def extract_categories(data: List[Dict]) -> List[Category]:
        """Extract category objects from JSON data"""
        categories = []
        
        for item in data:
            try:
                if 'category' in item:
                    # Create Category directly from the 'category' field
                    category = Category.from_dict(item)
                    categories.append(category)
            except KeyError as e:
                logger.warning(f"Skipping category due to missing key: {e}")
            except Exception as e:
                logger.warning(f"Error processing category: {e}")
        
        return categories
    
    @staticmethod
    def extract_chains(data: List[Dict]) -> List[Chain]:
        """Extract chain objects from JSON data"""
        chains = []
        
        for item in data:
            try:
                if 'chain' in item:
                    # Create Chain directly from the 'chain' field
                    chain = Chain.from_dict(item)
                    chains.append(chain)

            except KeyError as e:
                logger.warning(f"Skipping chain due to missing key: {e}")
            except Exception as e:
                logger.warning(f"Error processing chain: {e}")
        
        return chains
    
    @staticmethod
    def extract_hotels(data: List[Dict]) -> List[Hotel]:
        """Extract hotel objects from JSON data"""
        hotels = []
        
        for item in data:
            try:
                
                hotel = Hotel.from_dict(item)
                hotels.append(hotel)
              
            except KeyError as e:
                logger.warning(f"Skipping hotel due to missing key: {e}")
            except Exception as e:
                logger.warning(f"Error processing hotel: {e}")
        
        return hotels
    
