import mysql.connector
from typing import Dict, List
import logging
from DataClasses import Category, Chain, Hotel


logger = logging.getLogger(__name__)

class DatabaseConnector:
    """
    Handles MySQL database operations
    """
    
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """
        Establish a connection to the database
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.db_config["host"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                database=self.db_config["database"]
            )
            self.cursor = self.connection.cursor()
            logger.info("Database connection established")
        except mysql.connector.Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise
    
    def disconnect(self):
        """
        Close the database connection
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")
    
    def create_tables(self):
        """
        Create the tables in the database, Chain is replaced by _Chain_
        because Chain is a reserved keyword in MySQL
        """
        try:
            # Create tables
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS Category (
                    ID VARCHAR(255) PRIMARY KEY,
                    Name VARCHAR(255) NOT NULL
                );
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS _Chain_ (
                    ID VARCHAR(255) PRIMARY KEY,
                    Name VARCHAR(255) NOT NULL
                );
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS Hotel (
                    ID INT PRIMARY KEY,
                    Name VARCHAR(255) NOT NULL,
                    Category_ID VARCHAR(255),
                    Chain_ID VARCHAR(255),
                    Location VARCHAR(255),
                    FOREIGN KEY (Category_ID) REFERENCES Category(ID),
                    FOREIGN KEY (Chain_ID) REFERENCES _Chain_(ID)
                );
            """)
            self.connection.commit()
            logger.info("Tables created successfully")
        except mysql.connector.Error as e:
            logger.error(f"Error creating tables: {e}")
            self.connection.rollback()
            raise
    
    # I rollback when it throws exception so that no partiol insertion is done. Also assumed that this is one batch.
    # So an error prevents teh batch to be uploaded

    def insert_categories(self, categories: List[Category]):
        """
        Insert categories into the database
        """
        try:
            for category in categories:

                try:
                    id_val = int(category.id)
                except ValueError:
                    logger.error(f"Conversion error from string to integer: {category.id}")
                    continue

                if id_val < 0:
                    logger.warning(f"Skipping category with negative id: {category.id}")
                    continue

                # skip existing records 
                query = "INSERT IGNORE INTO Category (ID, Name) VALUES (%s, %s)"
                self.cursor.execute(query, (category.id, category.name))
            self.connection.commit()
            logger.info(f"Inserted categories")
        except mysql.connector.Error as e:
            logger.error(f"Error inserting category with ID:{category.id}, NAME:{category.name}: {e}")
            self.connection.rollback()
            raise
    
    def insert_chains(self, chains: List[Chain]):
        """
        Insert chains into the database
        """
        try:
            for chain in chains:
                try:
                    id_val = int(chain.id)
                except ValueError:
                    logger.error(f"Conversion error from string to integer: {chain.id}")
                    continue

                if id_val < 0:
                    logger.warning(f"Skipping chain with negative id: {chain.id}")
                    continue
                #skip existing records
                query = "INSERT IGNORE INTO _Chain_ (ID, Name) VALUES (%s, %s)"
                self.cursor.execute(query, (chain.id, chain.name))
            self.connection.commit()
            logger.info(f"Inserted/updated chains")
        except mysql.connector.Error as e:
            logger.error(f"Error inserting chain with ID:{chain.id}, NAME:{chain.name}: {e}")
            self.connection.rollback()
            raise
    
    def insert_hotels(self, hotels: List[Hotel]):
        """
        Insert hotels into the database
        """
        try:
            
            for hotel in hotels:
                try:
                    chain_id = int(hotel.chain_id)
                    category_id = int(hotel.category_id)
                except ValueError:
                    logger.error(f"Conversion error from string to integer: {chain_id}")
                    logger.error(f"Conversion error from string to integer: {category_id}")
                    continue

                if (chain_id < 0) or (category_id < 0):
                    logger.warning(f"Skipping hotel with negative id: Chain ID:{chain_id} or Category ID:{category_id}")
                    continue

                query = """
                INSERT INTO Hotel (ID, Name, Category_ID, Chain_ID, Location) 
                VALUES (%s, %s, %s, %s, %s)
                """
                self.cursor.execute(query, (hotel.id, hotel.name, hotel.category_id, hotel.chain_id, hotel.location))
            self.connection.commit()
            logger.info(f"Inserted {len(hotels)} hotels")
        except mysql.connector.Error as e:
            logger.error(f"Error inserting hotel with ID {hotel.id}, NAME:{hotel.name}: {e}")
            self.connection.rollback()
            raise


