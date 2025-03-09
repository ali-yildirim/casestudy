# Data Mapping and Transformation for Hotel Providers 

This project demonstrates a data migration pipeline using Python and MySQL. The pipeline fetches data from multiple providers in JSON format. Then, the JSON file is converted into python objects and uploaded to the database.


## Project Architecture

### Components

- **DataClasses**: Contains class definitons for the objects that are converted from JSON.
- **DataProcessor**: Responsible for object creation from JSON input (Reading, Parsing, Object Creation)
- **DBManager**: Responsible for DB connection
- **DataMigrationSystem**: Responsible for uploading the objects to the DB
- **main.py**: Main executable python script
- **MYSQL**: The relational database management system of choice used to store the processed data

The project uses requirements.txt files to manage the Python dependencies. The dependencies are installed within the Docker containers during the build process.




## Usage:

1. Build and run the Docker containers:

```
docker-compose up --build
```

This command will build the Docker images and start the container for MySQL service.

2. Unit Testing

```
docker-compose run app python unittest.py
```

This command will run the script necessary for unit testing.


## Design Decisions

### Assumptions

- Tables have fields stated in the case study file:

Category Table: 

• Category ID 

• Category Name 

Chain Table: 

• Chain ID 

• Chain Name 

Hotel Table: 

• Hotel ID 

• Hotel Name 

• Category ID 

• Chain ID 

• Location 

- The datatypes of the fields in the JSON file are kept as is. For example, property id is kept as 1, instead of "000000001" because it is integer.

- Category ID is more like a table number, rather than a unique identifier. Chain ID is also assumed that way, even though it is probably not. 

- `"obfuscation_required":false` means `"obfuscated_coordinates"` will be used, instead of `"coordinates"`.

- No negative ID 

### Python Dataclasses

`dataclass` is used in this project due to its ability to streamline the transition from data types to objects. Each class instance represents a database entity (Hotel, Category, Chain) with proper typing and structure.

### Rollback Mechanism

The input file can be treated as a batch. If the code throws an exception during insertion process to the DB, then the entire batch is prevented from being uploaded. This way, consistency can be ensured, and partial data insertion can be prevented.

### Docker Compose


## Implementation

JSON file is read and parsed. Afterwards, field names are used to create objects. Then, DBmanager connects to the database and insert the objects. When done, the connection is closed.

## Missing Parts

No unit test for invalid data.

