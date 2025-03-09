from dataclasses import dataclass
from typing import Any, Dict, Optional, List
import json
import logging
import mysql.connector
from mysql.connector import Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class Category:
    id: str  # Using string to match the JSON input format
    name: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Category':
        return cls(
            id=data['id'],
            name=data['name']
        )

@dataclass
class Chain:
    id: str  # Using string to match the JSON input format
    name: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Chain':
        return cls(
            id=data['id'],
            name=data['name']
        )

@dataclass
class Coordinates:
    latitude: Dict
    longitude: float
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Coordinates':
        return cls(
            latitude=data['latitude'],
            longitude=data['longitude']
        )

@dataclass
class Location:
    coordinates: Coordinates
    obfuscation_required: bool
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Location':
        return cls(
            coordinates=Coordinates.from_dict(data['coordinates']),
            obfuscation_required=data['obfuscation_required']
        )
    
    def to_string(self) -> str:
        """Convert location to string format for database storage"""
        return json.dumps({
            "latitude": self.coordinates.latitude,
            "longitude": self.coordinates.longitude,
            "obfuscation_required": self.obfuscation_required
        })

@dataclass
class Hotel:
    id: int
    name: str
    category_id: str
    chain_id: str
    location: str
    
    @classmethod
    def from_raw_dict(cls, data: Dict[str, Any]) -> 'Hotel':
        """Create Hotel instance from raw JSON data"""
        # Extract location data
        location_obj = Location.from_dict(data['location'])
        location_str = location_obj.to_string()
        
        return cls(
            id=data['property_id'],
            name=data['name'],
            category_id=data['category']['id'],
            chain_id=data['chain']['id'],
            location=location_str
        )


class DatabaseConnector:
    """Handles database connection and operations."""
    
    def __init__(self, host: str, user: str, password: str, database: str):
        """Initialize the database connector with connection parameters."""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
    
    def connect(self):
        """Establish a connection to the MySQL database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                logger.info("Connected to MySQL database")
                return True
        except Error as e:
            logger.error(f"Error connecting to MySQL database: {e}")
            return False
    
    def disconnect(self):
        """Close the database connection."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("MySQL connection closed")
    
    def create_tables(self):
        """Create the necessary tables if they don't exist."""
        if not self.connection or not self.connection.is_connected():
            logger.error("No active database connection")
            return False
        
        cursor = self.connection.cursor()
        
        try:
            # Create Category table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Category (
                    category_id VARCHAR(50) PRIMARY KEY,
                    category_name VARCHAR(255) NOT NULL
                )
            """)
            
            # Create Chain table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Chain (
                    chain_id VARCHAR(50) PRIMARY KEY,
                    chain_name VARCHAR(255) NOT NULL
                )
            """)
            
            # Create Hotel table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Hotel (
                    hotel_id INT PRIMARY KEY,
                    hotel_name VARCHAR(255) NOT NULL,
                    category_id VARCHAR(50),
                    chain_id VARCHAR(50),
                    location TEXT,
                    FOREIGN KEY (category_id) REFERENCES Category(category_id),
                    FOREIGN KEY (chain_id) REFERENCES Chain(chain_id)
                )
            """)
            
            self.connection.commit()
            logger.info("Tables created successfully")
            return True
        except Error as e:
            logger.error(f"Error creating tables: {e}")
            return False
        finally:
            cursor.close()


class DataProcessor:
    """Processes JSON data and extracts entities."""
    
    @staticmethod
    def parse_json(json_data: str) -> Dict[str, Any]:
        """Parse JSON string into a dictionary."""
        try:
            return json.loads(json_data)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {e}")
            raise
    
    @staticmethod
    def extract_categories(data: Dict[str, Any]) -> List[Category]:
        """Extract unique categories from raw data."""
        categories = {}
        
        for _, hotel_data in data.items():
            category_data = hotel_data['category']
            category_id = category_data['id']
            
            if category_id not in categories:
                categories[category_id] = Category.from_dict(category_data)
        
        return list(categories.values())
    
    @staticmethod
    def extract_chains(data: Dict[str, Any]) -> List[Chain]:
        """Extract unique chains from raw data."""
        chains = {}
        
        for _, hotel_data in data.items():
            chain_data = hotel_data['chain']
            chain_id = chain_data['id']
            
            if chain_id not in chains:
                chains[chain_id] = Chain.from_dict(chain_data)
        
        return list(chains.values())
    
    @staticmethod
    def extract_hotels(data: Dict[str, Any]) -> List[Hotel]:
        """Extract hotels from raw data."""
        hotels = []
        
        for _, hotel_data in data.items():
            try:
                hotel = Hotel.from_raw_dict(hotel_data)
                hotels.append(hotel)
            except KeyError as e:
                logger.error(f"Missing key in hotel data: {e}")
                # Continue processing other hotels
        
        return hotels


class DataMigrator:
    """Handles the migration of data to MySQL database."""
    
    def __init__(self, db_connector: DatabaseConnector):
        """Initialize with a database connector."""
        self.db_connector = db_connector
    
    def migrate_categories(self, categories: List[Category]) -> bool:
        """Migrate categories to the database."""
        if not self.db_connector.connection or not self.db_connector.connection.is_connected():
            logger.error("No active database connection")
            return False
        
        cursor = self.db_connector.connection.cursor()
        
        try:
            for category in categories:
                # Check if category already exists
                cursor.execute(
                    "SELECT COUNT(*) FROM Category WHERE category_id = %s",
                    (category.id,)
                )
                if cursor.fetchone()[0] == 0:
                    # Insert if it doesn't exist
                    cursor.execute(
                        "INSERT INTO Category (category_id, category_name) VALUES (%s, %s)",
                        (category.id, category.name)
                    )
            
            self.db_connector.connection.commit()
            logger.info(f"Migrated {len(categories)} categories")
            return True
        except Error as e:
            logger.error(f"Error migrating categories: {e}")
            self.db_connector.connection.rollback()
            return False
        finally:
            cursor.close()
    
    def migrate_chains(self, chains: List[Chain]) -> bool:
        """Migrate chains to the database."""
        if not self.db_connector.connection or not self.db_connector.connection.is_connected():
            logger.error("No active database connection")
            return False
        
        cursor = self.db_connector.connection.cursor()
        
        try:
            for chain in chains:
                # Check if chain already exists
                cursor.execute(
                    "SELECT COUNT(*) FROM Chain WHERE chain_id = %s",
                    (chain.id,)
                )
                if cursor.fetchone()[0] == 0:
                    # Insert if it doesn't exist
                    cursor.execute(
                        "INSERT INTO Chain (chain_id, chain_name) VALUES (%s, %s)",
                        (chain.id, chain.name)
                    )
            
            self.db_connector.connection.commit()
            logger.info(f"Migrated {len(chains)} chains")
            return True
        except Error as e:
            logger.error(f"Error migrating chains: {e}")
            self.db_connector.connection.rollback()
            return False
        finally:
            cursor.close()
    
    def migrate_hotels(self, hotels: List[Hotel]) -> bool:
        """Migrate hotels to the database."""
        if not self.db_connector.connection or not self.db_connector.connection.is_connected():
            logger.error("No active database connection")
            return False
        
        cursor = self.db_connector.connection.cursor()
        
        try:
            for hotel in hotels:
                # Check if hotel already exists
                cursor.execute(
                    "SELECT COUNT(*) FROM Hotel WHERE hotel_id = %s",
                    (hotel.id,)
                )
                if cursor.fetchone()[0] == 0:
                    # Insert if it doesn't exist
                    cursor.execute(
                        """
                        INSERT INTO Hotel (
                            hotel_id, hotel_name, category_id, chain_id, location
                        ) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            hotel.id,
                            hotel.name,
                            hotel.category_id,
                            hotel.chain_id,
                            hotel.location
                        )
                    )
            
            self.db_connector.connection.commit()
            logger.info(f"Migrated {len(hotels)} hotels")
            return True
        except Error as e:
            logger.error(f"Error migrating hotels: {e}")
            self.db_connector.connection.rollback()
            return False
        finally:
            cursor.close()
    
    def run_migration(self, json_data: str) -> bool:
        """Run the complete migration process."""
        try:
            # Parse JSON
            data = DataProcessor.parse_json(json_data)
            
            # Extract entities
            categories = DataProcessor.extract_categories(data)
            chains = DataProcessor.extract_chains(data)
            hotels = DataProcessor.extract_hotels(data)
            
            if not hotels:
                logger.warning("No hotels found to migrate")
                return False
            
            # Create database tables if they don't exist
            if not self.db_connector.create_tables():
                return False
            
            # Migrate data in the correct order (respecting foreign key constraints)
            if not self.migrate_categories(categories):
                return False
            
            if not self.migrate_chains(chains):
                return False
            
            if not self.migrate_hotels(hotels):
                return False
            
            logger.info("Migration completed successfully")
            return True
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False


def main():
    """Main function to demonstrate the migration process."""
    # Example JSON data
    json_data = """{"0":{"property_id":10000527,"category":{"id":"1","name":"Hotel"},"chain":{"id":"0","name":"Independent"},"location":{"coordinates":{"latitude":42.60803,"longitude":8.864105},"obfuscation_required":false},"name":"hotel 0"},"1":{"property_id":10000528,"category":{"id":"1","name":"Hotel"},"chain":{"id":"0","name":"Independent"},"location":{"coordinates":{"latitude":-7.835805,"longitude":110.368185},"obfuscation_required":false},"name":"hotel 1"}}"""
    
    # Database connection parameters (replace with your actual MySQL credentials)
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "secret",
        "database": "data_base"
    }
    
    # Initialize database connector
    db_connector = DatabaseConnector(**db_config)
    
    # Connect to the database
    if db_connector.connect():
        # Initialize and run the migration
        migrator = DataMigrator(db_connector)
        success = migrator.run_migration(json_data)
        
        # Disconnect from the database
        db_connector.disconnect()
        
        if success:
            print("Migration completed successfully")
        else:
            print("Migration failed")


if __name__ == "__main__":
    main()