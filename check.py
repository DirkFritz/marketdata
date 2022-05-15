
import urllib3
urllib3.disable_warnings()

from db.db import Db
from db.helper import historic_stock_data, historic_stock_data_date


def ckeckForStockSplit(db):
    data_db= historic_stock_data(db)

    data_db.sort_values(by='DateTime', inplace=True)

    symbols = data_db["Symbol"].unique()

    print(symbols)

    for symbol in symbols:
        close = data_db[data_db["Symbol"]==symbol]["Close"].to_list()
        open = data_db[data_db["Symbol"]==symbol]["Open"].to_list()
        date = data_db[data_db["Symbol"]==symbol]["DateTime"].to_list()

        i = 0

        #splits = db.select("splits")
      
        for i in range(len(close)-1):
            multiple = close[i] /open[i+1]
            if multiple > 1.9:                
                print(symbol, date[i+1],multiple, close[i], open[i+1])
                db.insert_splits(symbol, date[i+1],int(round(multiple)))
 
       
 
def main():

    try:
        db = Db()
        #db.drop_table("splits")
        #db.create_table_stock_splits()
        #ckeckForStockSplit(db)
        historic_stock_data_date(db, db.get_min_max("historic","date")[0])
        db.close() 

     
    except Exception as e:
        print(e)  
    

if __name__ == "__main__":
    main()


