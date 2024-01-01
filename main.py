from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd
from api_key import api_header

'''db_connection = DatabaseConnector()
table_names = db_connection.list_table_names()
engine = db_connection.init_db_engine()

data_extractor = DataExtractor()
legacy_store_details_df = data_extractor.read_rds_table(table_names[0], engine)
legacy_users_df = data_extractor.read_rds_table(table_names[1], engine)
orders_table_df = data_extractor.read_rds_table(table_names[2], engine)

user_data_cleaner = DataCleaning()
legacy_users = user_data_cleaner.clean_user_data(legacy_users_df, 'last_name')

legacy_users.to_csv('cleaned_legacy_users_df.csv', index=False)

db_upload = DatabaseConnector()
db_upload.upload_to_db(legacy_users, 'dim_users')

card_details = DataExtractor()
card_details_df = card_details.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
card_details_cleaner = DataCleaning()
card_details_cleaner.clean_card_data(card_details_df, 'card_number')

card_details_upload = DatabaseConnector()
card_details_upload.upload_to_db(card_details_df, 'dim_card_details')'''

number_of_stores_extractor = DataExtractor()
number_of_stores = number_of_stores_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', api_header)

store_details_extractor = DataExtractor()
store_details_df = store_details_extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details', api_header)

store_clean = DataCleaning()
store_clean.clean_store_details(store_details_df, 'continent')

upload_store = DatabaseConnector()
upload_store.upload_to_db(store_details_df, 'dim_store_details')

'''extract_product_details = DataExtractor()
product_details_df = extract_product_details.extract_from_s3('s3://data-handling-public/products.csv')

clean_product = DataCleaning()
clean_product.clean_product_details(product_details_df)

upload_product = DatabaseConnector()
upload_product.upload_to_db(product_details_df, 'dim_products')


db_connection = DatabaseConnector()
table_names = db_connection.list_table_names()
engine = db_connection.init_db_engine()

data_extractor = DataExtractor()
orders_table_df = data_extractor.read_rds_table(table_names[2], engine)

clean_orders = DataCleaning()
clean_orders.clean_orders_data(orders_table_df, ['first_name', 'last_name', '1'])


upload_orders = DatabaseConnector()
upload_orders.upload_to_db(orders_table_df, 'orders_table')

#date_times = DataExtractor()
#date_times.extract_date_times('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

url_parts = url.replace("https://", "").split(".")
bucket_name = f'{url_parts[0]}'
print(bucket_name)'''

