import pandas as pd

class DataCleaning:

    def clean_user_data(self, user_data, column):
        user_data.dropna(axis = 1, thresh = (1 - 10/100)*len(user_data), inplace = True)
        user_data.replace('NULL', pd.NA, inplace=True)
        user_data.dropna(inplace=True)
        user_data = user_data[~user_data[column].str.contains('\d')]
        return user_data
    
    def clean_card_data(self, card_data, column):
        card_data.dropna(axis = 1, thresh = (1 - 10/100)*len(card_data), inplace = True)
        card_data.replace('NULL', pd.NA, inplace=True)
        card_data.dropna(inplace=True)
        card_data = card_data[card_data[column].notna() & card_data[column].astype(str).str.isdigit()]
        return card_data
    
    def clean_store_details(self, store_data, column):
        store_data.dropna(axis = 1, thresh = (1-10/100)*len(store_data), inplace = True)
        store_data.replace('NULL', pd.NA, inplace=True)
        store_data.dropna(inplace=True)
        store_data = store_data[~store_data[column].str.contains('\\d')]
        store_data.loc[:, column].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'}, inplace=True)
        return store_data
    
    #def convert_product_weights(self, products_df):
        #Write code to convert ml to g, and all weight units to kg representing values as a decimal float


    def clean_product_details(self, product_data):
        product_data.dropna(axis = 0, thresh = 10, inplace = True)
        return product_data
    
    def clean_orders_data(self, orders_data, list_of_columns):
        orders_data.drop(columns = list_of_columns, inplace = True)
        return orders_data

