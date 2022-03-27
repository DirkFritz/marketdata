
import requests
import pandas as pd
from datetime import datetime,timedelta

import urllib3
urllib3.disable_warnings()

from db import Db


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

def FindConId(contracts_data):
    
    for contract_data in contracts_data: 
        for contract in contract_data["contracts"]:
            if contract["exchange"]=="NASDAQ":           
                return contract["conid"]


def HistoricData(contract_data,symbols):

    stocks_data_df =  pd.DataFrame(columns=['Symbol','DateTime', 'Open', 'Close','High','Low','Volume'])


    for symbol in symbols:

        print(symbol)
        conid = FindConId(contract_data[symbol])
        print(contract_data[symbol])
        print(conid)
     
     
        r = requests.get(f" https://localhost:5000/v1/api/hmds/history?conid={conid}&period=2d&bar=1d",verify=False)

      
        while r.status_code != 200 and r.status_code != 400:
            print(r.status_code)
            #print(r.text)
         
            r = requests.get(f" https://localhost:5000/v1/api/hmds/history?conid={conid}&period=2d&bar=1d",verify=False)
            
     
        if r.status_code == 200:
            historic_data = r.json()

      
            print(historic_data)

            for prices in historic_data["data"]:
                time = datetime.utcfromtimestamp(prices['t'] / 1e3)
                time = time.replace(hour=0, minute=00)
                
                stocks_data_df = pd.concat([stocks_data_df, pd.DataFrame([[symbol,time ,prices['o'],prices['c'],prices['h'],prices['l'],prices['v']]],columns=['Symbol','DateTime', 'Open','Close','High','Low','Volume'])])

    return stocks_data_df



def RequestReautehntication():
   
    r = requests.get(f"https://localhost:5000/v1/api/sso/validate",verify=False)
    print(r.json())

    
    r = requests.post(f"https://localhost:5000/v1/api/iserver/reauthenticate",verify=False)
    print(r.json())

 
    r = requests.get(f"https://localhost:5000/v1/api/iserver/accounts",verify=False)
    print(r.json())

   

def RequestContractData(symbols):
    r = requests.get(f"https://127.0.0.1:5000/v1/api/trsrv/stocks?symbols={symbols}",verify=False)
    print("Request Contract Data Status", r.status_code)
    return r.json()


def init_stock_prices_db(db):

    df = pd.read_csv("ndxdata.csv")
    df["DateTime"] = pd.to_datetime( df["DateTime"])
    db.drop_table()
    db.create_table()

    insert_historic(db, df)
    
   

def historic_stock_data(db, date):
        data_db =  db.select_historic_date(date)
        data_db_df =  pd.DataFrame(columns=['Symbol','DateTime', 'Open', 'Close','High','Low','Volume'])
        data_db_df = pd.concat([data_db_df, pd.DataFrame(data_db,columns=['Symbol','DateTime', 'Open','Close','High','Low','Volume'])])
        return data_db_df


def insert_historic(db, stocks_data):
    for index, stock_data in stocks_data.iterrows():
        symbol = stock_data['Symbol']
        date =  stock_data['DateTime'].date()
        open =  stock_data['Open']
        close =  stock_data['Close']
        high =  stock_data['High']
        low =  stock_data['Low']
        volume =  stock_data['Volume']
        db.insert_historic(symbol,date, open, close, high, low,volume)

def insert_new_data_to_historic(db, stocks_data_df):
    data_time_delta = datetime.today() - timedelta(days=7)
    print(data_time_delta.date())

    symbols = stocks_data_df["Symbol"].unique()
       
    data_db_df = historic_stock_data(db,data_time_delta.date() )

    print(data_db_df[data_db_df["Symbol"]=="ZS"])


    for symbol in symbols:
        data_db_symbol = data_db_df[data_db_df["Symbol"]==symbol]
        last_entry = data_db_symbol["DateTime"].max()
        last_entry =datetime(last_entry.year, last_entry.month, last_entry.day)
       
        stocks_data_db_symbol = stocks_data_df[(stocks_data_df["Symbol"]==symbol) & (stocks_data_df["DateTime"]> last_entry) ]
        insert_historic(db,stocks_data_db_symbol )                
           

    data_db_df = historic_stock_data(db,data_time_delta.date() )
    print(data_db_df[data_db_df["Symbol"]=="ZS"])


def main():

    try:
        ndx_df = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100')[3]
 
        symbols =""
        for symbol in ndx_df['Ticker']:
            symbols = symbols + "," + symbol

        contract_data = RequestContractData(symbols)
        stocks_data_df = HistoricData(contract_data,ndx_df['Ticker'])
        #stocks_data_df.to_csv('ndxdata.csv')
        #stocks_data_df = pd.read_csv('ndxdata.csv')
        #stocks_data_df["DateTime"] = pd.to_datetime( stocks_data_df["DateTime"])
        db = Db()
        #init_stock_prices_db(db)
        insert_new_data_to_historic(db, stocks_data_df)       

        db.close()

     
    except Exception as e:
        print(e)  
    

if __name__ == "__main__":
    main()