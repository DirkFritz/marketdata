
import pandas as pd
import requests
from datetime import datetime

def FindConId(contracts_data):
    
    for contract_data in contracts_data: 
        for contract in contract_data["contracts"]:
            if contract["exchange"]=="NASDAQ" or contract["exchange"]=="NYSE" :           
                return contract["conid"]


def HistoricData(contract_data,symbols, period ="7d"):

    stocks_data_df =  pd.DataFrame(columns=['Symbol','DateTime', 'Open', 'Close','High','Low','Volume'])


    for symbol in symbols:

        print(symbol)
        conid = FindConId(contract_data[symbol])
        print(contract_data[symbol])
        print(conid)
     
     
        r = requests.get(f" https://localhost:5000/v1/api/hmds/history?conid={conid}&period={period}&bar=1d",verify=False)

      
        while r.status_code != 200 and r.status_code != 400:
            print(r.status_code)
            #print(r.text)
         
            r = requests.get(f" https://localhost:5000/v1/api/hmds/history?conid={conid}&period={period}&bar=1d",verify=False)
            
     
        if r.status_code == 200:
            historic_data = r.json()

      
            #print(historic_data)

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


def request_contract_details(contract_data, symbols):

    details = pd.DataFrame(columns=["Symbol","Industry","Name", "Category"])
    for symbol in symbols:

        print(symbol)
        conid = FindConId(contract_data[symbol])
    
        r = requests.get(f"https://localhost:5000/v1/api/iserver/contract/{conid}/info",verify=False).json()
        print(r)
        details = pd.concat([details,  pd.DataFrame([[r["symbol"],r["industry"],r["company_name"],r["category"]]],columns=["Symbol","Industry","Name", "Category"])])

    return details
   

def request_contract_data(symbols):

    symbols_contract_req =""
    for symbol in symbols:
        symbols_contract_req = symbols_contract_req + "," + symbol
    print(symbols_contract_req)

    r = requests.get(f"https://127.0.0.1:5000/v1/api/trsrv/stocks?symbols={symbols_contract_req}",verify=False)
    print("Request Contract Data Status", r.status_code)
    return r.json()