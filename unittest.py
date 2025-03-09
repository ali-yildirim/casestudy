# test_data_migration.py

from DataMigrationSystem import DataMigrationClass
from DataProcessor import DataProcessorClass
from DBManager import DatabaseConnector
from DataClasses import Hotel, Category, Chain
import os


VALID_DATA = "valid_data.json"


def test_object_creation(test_data):
    print("Testing Object Creation...")

    data_processor = DataProcessorClass()
    data = data_processor.process_json_file(test_data)
    # Test with Category object creation
    
    for item in data:
        category = Category.from_dict(item)
    assert category.id == "1", f"Expected id '1', got {category.id}"
    assert category.name == "Test_Category", f"Expected name 'Test_Category', got {category.name}"
    print("Category.from_dict test passed")

    # Test with Chain object creation

    for item in data:
        chain = Chain.from_dict(item)

    assert chain.id == "0", f"Expected id '0', got {chain.id}"
    assert chain.name == "Test_Chain", f"Expected name 'Test_Chain', got {chain.name}"
    print("Chain.from_dict test passed")

    # Test with Hotel object creation
    for item in data:
        hotel = Hotel.from_dict(item)
    
    assert hotel.id == 99999999, f"Expected id 99999999, got {hotel.id}"
    assert hotel.name == "hotel 249", f"Expected name 'hotel 249', got {hotel.name}"
    assert hotel.category_id == "1", f"Expected category_id '1', got {hotel.category_id}"
    assert hotel.chain_id == "0", f"Expected chain_id '0', got {hotel.chain_id}"
    assert isinstance(hotel.location, str), f"Location should be a string, got {type(hotel.location)}"
    print("Hotel.from_dict test passed")
    
def test_DataProcessorClass(test_data):

    print("Testing DataProcessorClass...")
    
    data_processor = DataProcessorClass()
    
    # Test processing json file from the source
    data = data_processor.process_json_file(test_data)
    
    assert len(data) == 1, f"Expected 1 hotel, got {len(data)}"
    assert data[0]["property_id"] == 99999999, f"Expected property_id 99999999, got {data[0]['property_id']}"
    print("DataProcessorClass.process_json_file test passed")

    # test field:chain extraction
    chains = data_processor.extract_chains(data)
    for chain in chains:
        assert chain.id == "0", f"Expected chain id 0, got {chain.id}"
        assert chain.name == "Test_Chain", f"Expected chain name 'Test_Chain', got {chain.name}"
    print("DataProcessorClass.extract_chains test passed")

    # test field:category extraction
    categories = data_processor.extract_categories(data)
    for category in categories:
        assert category.id == "1", f"Expected category id 1, got {category.id}"
        assert category.name == "Test_Category", f"Expected category name 'Test_Category', got {category.name}"

    print("DataProcessorClass.extract_categories test passed")

    # test field:hotel extraction
    hotels = data_processor.extract_hotels(data)
    for hotel in hotels:
        assert hotel.id == 99999999, f"Expected hotel id 99999999, got {hotel.id}"
        assert hotel.name == "hotel 249", f"Expected hotel name 'hotel 249', got {hotel.name}"
        assert hotel.category_id == "1", f"Expected category_id '1', got {hotel.category_id}"
        assert hotel.chain_id == "0", f"Expected chain_id '0', got {hotel.chain_id}"
        assert isinstance(hotel.location, str), f"Location should be a string, got {type(hotel.location)}"
    print("DataProcessorClass.extract_hotels test passed")


# check if a connection can be established and closed
def test_DatabaseConnector():

    print("Testing DatabaseConnector...")

    db_config = {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'port': os.environ.get('DB_PORT', '3307'),
        'database': os.environ.get('DB_NAME', 'mydb'),
        'user': os.environ.get('DB_USER', 'root'),
        'password': os.environ.get('DB_PASSWORD', 'secret')
                    }
    
    db_connector = DatabaseConnector(db_config)
    db_connector.connect()
    assert db_connector.connection.is_connected(), "Expected connection to be open"
    db_connector.disconnect()
    assert not db_connector.connection.is_connected(), "Expected connection to be closed"
    print("DatabaseConnector test passed")


# test for migrating data with insertion methods
def test_DataMigrationClass(test_data):
    print("Testing DataMigrationClass...")
    
    db_config = {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'port': os.environ.get('DB_PORT', '3307'),
        'database': os.environ.get('DB_NAME', 'mydb'),
        'user': os.environ.get('DB_USER', 'root'),
        'password': os.environ.get('DB_PASSWORD', 'secret')
                    }
    
    data_migration = DataMigrationClass(db_config)
    data_migration.migrate_data(test_data)  
    print("DataMigrationClass test passed")

    


def run_tests(test_data):
    test_object_creation(test_data)
    test_DataProcessorClass(test_data)
    test_DatabaseConnector()
    test_DataMigrationClass(test_data)
    print("All tests passed")

if __name__ == "__main__":
    run_tests(VALID_DATA)