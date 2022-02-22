from time import sleep
import requests
import json
import pandas as pd
from datetime import datetime
import urllib3
urllib3.disable_warnings()
from ndxdata import NdxData


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

    stocks_data_df =  pd.DataFrame(columns=['Symbol','DateTime', 'Price'])


    for symbol in symbols:

        print(symbol)
        conid = FindConId(contract_data[symbol])
        print(contract_data[symbol])
        print(conid)
     
     
        r = requests.get(f" https://localhost:5000/v1/api/hmds/history?conid={conid}&period=6m&bar=1d",verify=False)

      
        while r.status_code != 200 and r.status_code != 400:
            print(r.status_code)
            #print(r.text)
         
            r = requests.get(f" https://localhost:5000/v1/api/hmds/history?conid={conid}&period=6m&bar=1d",verify=False)
            
     
        if r.status_code == 200:
            historic_data = r.json()

      
            #print(historic_data["symbol"])

            for prices in historic_data["data"]:
                time = datetime.fromtimestamp(prices['t'] / 1e3)
        
                stocks_data_df = pd.concat([stocks_data_df, pd.DataFrame([[symbol, time,prices['c']]],columns=['Symbol','DateTime', 'Price'])])

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


def main():

    try:
        ndx_df = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100')[3]
 
        symbols =""
        for symbol in ndx_df['Ticker']:
            symbols = symbols + "," + symbol

    
        contract_data = RequestContractData(symbols)
     
        stocks_data_df = HistoricData(contract_data,ndx_df['Ticker'])

        #print(stocks_data_df)

        stocks_data_df.to_csv('ndxdata.csv')

        upload_blob("lt-capital.de","ndxdata.csv","ndxdata.csv") 

        ndx_data = NdxData(
            "gs://lt-capital.de/nasdaq_screener.csv", "ndxdata.csv"
        )
        ndx_data.set_comparison_group(["AAPL", "GOOG", "GOOGL", "MSFT", "NVDA", "TSLA"])
        date1 = datetime.strptime("2021/11/15 15:30:00", "%Y/%m/%d %H:%M:%S")
        date2 = ndx_data.get_last_day()
       
        ndxgroups_df = ndx_data.set_compare_dates(date1, date2)     

        ndxgroups_df.to_csv("ndxgroups.csv") 
        upload_blob("lt-capital.de","ndxgroups.csv","ndxgroups.csv")

    except Exception as e:
        print(e)  
    

if __name__ == "__main__":
    main()