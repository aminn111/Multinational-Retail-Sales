import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:

    #Initialises the class with the default credentials file and also initialises the db engine.
    def __init__(self):
        self.engine = self.init_db_engine()

    #Defines a method for reading the credentials file and returns them as a dictionary. 
    def read_db_creds(self, creds_file):
        try:
            with open(creds_file, 'r') as file:
                creds = yaml.safe_load(file)
                return creds
        except FileNotFoundError:
            print(f"Error: File '{self.creds_file}' not found.")
            return None
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {e}")
            return None

    #Defines the method for initialising the db engine using sqlalchemy.
    def init_db_engine(self):
        creds = self.read_db_creds('db_creds.yaml')
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine.connect()
    
    
    #Defines the method for listing the table names from the database.
    def list_table_names(self):
        if self.engine:
            inspector = inspect(self.engine)
            tables_names = inspector.get_table_names()
            return tables_names
        else:
            print("Error: Engine not initialised")

    
    #Defines the method for uploading the cleaned pd df to the databsae. Takes in the df and table name to be created as an argument.
    def upload_to_db(self, df, table_name):
        creds = self.read_db_creds('sales_data_db_creds.yaml')
        self.engine = create_engine(f"{creds['DATABASE_TYPE']}+{creds['DBAPI']}://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}")
        df.to_sql(name=table_name, con=self.engine, index = True, if_exists='replace')
                
