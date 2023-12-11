import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
class DatabaseConnector:
    def __init__(self, creds_file='db_creds.yaml'):
        self.creds_file = creds_file
        self.engine = self.init_db_engine()


    def read_db_creds(self):
        try:
            with open(self.creds_file, 'r') as file:
                creds = yaml.safe_load(file)
                return creds
        except FileNotFoundError:
            print(f"Error: File '{self.creds_file}' not found.")
            return None
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {e}")
            return None

    def init_db_engine(self):
        creds = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine.connect()
    
    def list_table_names(self):
        if self.engine:
            inspector = inspect(self.engine)
            tables_names = inspector.get_table_names()
            return tables_names
        else:
            print("Error: Engine not initialised")

instance = DatabaseConnector()
print(instance.list_table_names())
