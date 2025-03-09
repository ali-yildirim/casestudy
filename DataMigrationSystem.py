from DataProcessor import DataProcessorClass
from DBManager import DatabaseConnector
from typing import Dict
import logging

logger = logging.getLogger(__name__)

class DataMigrationClass:
    """Main system to coordinate the data migration process"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.db_connector = DatabaseConnector(db_config)
        self.processor = DataProcessorClass()
    
    def migrate_data(self, file_path: str) -> None:
        """Process data from JSON file and migrate it to the database"""
        try:
            # Process JSON data
            data = self.processor.process_json_file(file_path)
            
            # Extract objects
            categories = self.processor.extract_categories(data)
            chains = self.processor.extract_chains(data)
            hotels = self.processor.extract_hotels(data)

            # Connect to database
            self.db_connector.connect()
            
            # Create tables
            self.db_connector.create_tables()
            
            # Insert data
            self.db_connector.insert_categories(categories)
            self.db_connector.insert_chains(chains)
            self.db_connector.insert_hotels(hotels)
            
            logger.info("Data migration completed successfully")
            
        except Exception as e:
            logger.error(f"Data migration failed: {e}")
            raise
        finally:
            # Ensure database connection is closed
            self.db_connector.disconnect()
