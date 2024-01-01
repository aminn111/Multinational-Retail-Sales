import pandas as pd
import sqlalchemy
import tabula
import requests
import boto3

class DataExtractor:

    def read_rds_table(self, table_name, engine):
        rds_df = pd.read_sql_table(table_name, engine)
        return rds_df
    

    def retrieve_pdf_data(self, link):
        pdf_df = tabula.read_pdf(link, pages = 'all', stream = True)
        card_details = pd.concat(pdf_df, ignore_index=True)
        return card_details
    
    def list_number_of_stores(self, endpoint, header):
        response = requests.get(endpoint, headers=header)

        if response.status_code == 200:
            data = response.json()
        else:
            print(f'Error, {response.status_code}, {response.text}')
        
        return data
    
    def retrieve_stores_data(self, base_url, header):
        
        all_data = []

        for i in range(1, 451):
            endpoint = f'{base_url}/{i}'
            response = requests.get(endpoint, headers=header)
            if response.status_code == 200:
                data = response.json()
                all_data.append(data)
            else:
                print(f'Error, {response.status_code}, {response.text}')

        store_details_df = pd.DataFrame(all_data)


        return store_details_df 
    
    def extract_from_s3(self, s3_address):
        s3_parts = s3_address.replace("s3://", "").split("/")
        bucket_name = s3_parts[0]
        object_key = "/".join(s3_parts[1:])

        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket = bucket_name, Key = object_key)

        try:
            df = pd.read_csv(response['Body'])
            print(f'CSV file downloaded successfully and loaded into pandas dataframe.')
            return df
        except Exception as e:
            print(f'Error downloading CSV file from S3: {e}')
            
    def extract_date_times(self, s3_address):
        s3_parts = s3_address.replace("s3://", "").split("/")
        bucket_name = s3_parts[0]
        object_key = "/".join(s3_parts[1:])

        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket = bucket_name, Key = object_key)

        try:
            df =pd.read_json(response['Body'])
            print(f'JSON file downloaded successfully and loaded into pandas dataframe.')
            return df
        except Exception as e:
            print(f'Error downloading JSON file from S3: {e}')



        
    


