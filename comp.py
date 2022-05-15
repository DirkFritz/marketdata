from statistics import quantiles

from numpy import NaN
from db.db import Db
import pandas as pd
import json
from ibapi.contracts import request_contract_details, request_contract_data


def companies_data(contract_detail, stock_data):    
        stock_data = stock_data.set_index("Symbol")
        stock_data[stock_data.columns[1]] = (
            stock_data[stock_data.columns[1]].replace("[\$,]", "", regex=True).astype(float)
        )
        stock_data["Outstanding Shares"] = stock_data["Market Cap"] / stock_data["Last Sale"]

        symbols = contract_detail["Symbol"].to_list()

        #gcis = pd.read_html("https://en.wikipedia.org/wiki/Global_Industry_Classification_Standard")[0]
    
        outstanding_shares = []
        sector = []
        industries = []
        for symbol in symbols:
            outstanding_shares.append(stock_data.loc[symbol]["Outstanding Shares"])
            sector.append(stock_data.loc[symbol]["Sector"])
            industries.append(stock_data.loc[symbol]["Industry"])
        
      
     
        contract_detail["Outstanding Shares"] = outstanding_shares
        contract_detail["Sector"] = sector
        contract_detail.fillna("", inplace = True)

     
def main():

    try:
        db = Db()

        #db.drop_table("companies")
        #db.create_table_companies()
        #symbols_db =db.get_unique_values("symbol","historic")
        #symbols_db = [symbol[0] for symbol in symbols_db]

        #contract_data = request_contract_data(symbols_db)  
        #contract_details =request_contract_details(contract_data,symbols_db)

        #print(contract_details)

        #contract_details.to_csv("details.csv")
        
        
        contract_details= pd.read_csv("details.csv")        
        stock_data =pd.read_csv("gs://lt-capital.de/nasdaq_screener_2.csv")

        companies_data(contract_details, stock_data)

        #for index, cd in contract_details.iterrows():
        #    db.insert_companies(cd["Symbol"], cd["Name"],cd["Outstanding Shares"],cd["Sector"], cd["Industry"], cd["Category"])

        print(db.select("companies"))
        #print(contract_details)

        #contract_detail.to_csv("details.csv")      
        db.close() 
        
     
    except Exception as e:
        print(e)  
    

if __name__ == "__main__":
    main()