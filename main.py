

import pandas as pd
from datetime import datetime,timedelta
from ibapi.contracts import HistoricData,request_contract_data 
import urllib3
urllib3.disable_warnings()

from db.db import Db
from db.helper import historic_stock_data_date, insert_historic

from google.cloud import storage

def upload_blob(bucket_name, source_file_name, destination_blob_name):
   
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )





def insert_new_data_to_historic(db, stocks_data_df):
    data_time_delta = datetime.today() - timedelta(days=7)
    print(data_time_delta.date())

    symbols = stocks_data_df["Symbol"].unique()
       
    data_db_df = historic_stock_data_date(db,data_time_delta.date() )

    print(data_db_df[data_db_df["Symbol"]=="ZS"])


    for symbol in symbols:
        data_db_symbol = data_db_df[data_db_df["Symbol"]==symbol]
        if data_db_symbol.empty != True:
            last_entry = data_db_symbol["DateTime"].max()

            print(symbol,last_entry)
        
            last_entry =datetime(last_entry.year, last_entry.month, last_entry.day)
       
            stocks_data_db_symbol = stocks_data_df[(stocks_data_df["Symbol"]==symbol) & (stocks_data_df["DateTime"]> last_entry) ]
            insert_historic(db,stocks_data_db_symbol )                
           

    data_db_df = historic_stock_data_date(db,data_time_delta.date() )
    print(data_db_df[data_db_df["Symbol"]=="ZS"])


def insert_old_data_to_historic(db, symbols, min_date):
  
    symbols_fill = []
    symbols_fill_min_date = {}
    for symbol in symbols:
        min_date_symbol = db.get_min_date_symbols("historic", symbol)
        if min_date_symbol == None:
            min_date_symbol = (datetime.today() + + timedelta(days=1)).date()
        if min_date < min_date_symbol:
            print(symbol, " ",min_date_symbol)
            symbols_fill.append(symbol)
            symbols_fill_min_date[symbol]= min_date_symbol
   
    contract_data = request_contract_data(symbols_fill)    
    stocks_data_df = HistoricData(contract_data,symbols_fill,"3y")

    for symbol in symbols_fill:
        stocks_data_new = stocks_data_df[(stocks_data_df["DateTime"]< pd.Timestamp(symbols_fill_min_date[symbol])) & (stocks_data_df["Symbol"]==symbol)&( stocks_data_df["DateTime"] >= pd.Timestamp(min_date) )]
        insert_historic(db,stocks_data_new )
        print(stocks_data_new)

    data_db_df = historic_stock_data_date(db,min_date )
    print(data_db_df[data_db_df["Symbol"]=="F"])
    print(data_db_df[data_db_df["Symbol"]=="AAPL"])


def main():

    try:
        ndx_df = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100')[3]
        spx_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]

        spx_df["Symbol"].replace('.', ' ', inplace=True)
       
        print(spx_df)

        db = Db()
        
        symbols_db =db.get_unique_values("symbol","historic")
        symbols_db = [symbol[0] for symbol in symbols_db]

        symbols = ndx_df['Ticker'].to_list()

     
        for symbol in symbols_db:
            if not symbol in symbols:
                symbols.append(symbol)

        for index, stock_data in spx_df.iterrows():
             if not stock_data["Symbol"] in symbols:
                symbols.append(stock_data["Symbol"])
   
        print(symbols)
        
        contract_data = request_contract_data(symbols)
        stocks_data_df = HistoricData(contract_data,symbols)
        stocks_data_df.to_csv('idxdata.csv')
        #stocks_data_df = pd.read_csv('ndxdata.csv')
        #stocks_data_df["DateTime"] = pd.to_datetime( stocks_data_df["DateTime"])
        #init_stock_prices_db(db)
        insert_new_data_to_historic(db, stocks_data_df)       
        min_date = db.get_min_max("historic","date")[0]
        print("min date ", min_date)
        insert_old_data_to_historic(db,symbols, min_date)
        db.close()

     
    except Exception as e:
        print(e)  
    

if __name__ == "__main__":
    main()