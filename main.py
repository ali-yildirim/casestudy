from DataMigrationSystem import DataMigrationClass

import os

db_config = {
    'host': os.environ.get('DB_HOST', 'mysql'),
    'port': os.environ.get('DB_PORT', '3306'),
    'database': os.environ.get('DB_NAME', 'db'),
    'user': os.environ.get('DB_USER', 'root'),
    'password': os.environ.get('DB_PASSWORD', 'secret')
}


# Instanciate the data migration class, which is the system itself.
migration_system = DataMigrationClass(db_config)

# Migrate data from a JSON file
migration_system.migrate_data('example_hotel_data.json')

