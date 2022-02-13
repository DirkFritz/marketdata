from time import sleep
import requests
import json
import pandas as pd
import datetime
import urllib3
urllib3.disable_warnings()





#r = requests.get("https://localhost:5000/v1/api/portfolio/accounts", verify=False)

#print(r.json())

#r = requests.get("https://127.0.0.1:5000/v1/api/trsrv/stocks?symbols=AAPL",verify=False)

#print(r.json())



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
     
        #r = requests.post(f"https://localhost:5000/v1/api/tickle",verify=False)
        #session_status = r.json()

        #r = requests.get(f"https://localhost:5000/v1/api/iserver/marketdata/history?conid={conid}&period=1y&bar=1d",verify=False)

       
        r = requests.get(f" https://localhost:5000/v1/api/hmds/history?conid={conid}&period=1y&bar=1d",verify=False)

      
        while r.status_code != 200:
            print(r.status_code)
            r = requests.get(f" https://localhost:5000/v1/api/hmds/history?conid={conid}&period=1y&bar=1d",verify=False)
            
     
        historic_data = r.json()

      
        #print(historic_data["symbol"])

        for prices in historic_data["data"]:
            time = datetime.datetime.fromtimestamp(prices['t'] / 1e3)
        
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
    return r.json()

def main():


    ndx_df = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100')[3]

 
    symbols =""
    for symbol in ndx_df['Ticker']:
        symbols = symbols + "," + symbol

    #print(symbols)
    
    contract_data = RequestContractData(symbols)
     
    stocks_data_df = HistoricData(contract_data,ndx_df['Ticker'])

    print(stocks_data_df)

    stocks_data_df.to_csv('ndxdata.csv')  
    

if __name__ == "__main__":
    main()