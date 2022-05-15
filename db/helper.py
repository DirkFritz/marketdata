
import pandas as pd
from datetime import datetime


def perform_split(db, data_db):
    splits = pd.DataFrame(db.select("splits"),columns=["Symbol", "Date","Multiple"])
    symbols = splits["Symbol"].unique()

    print(splits)
    for symbol in symbols:
        splits_per_symbol = splits[splits["Symbol"]==symbol]
        for split in splits_per_symbol.iterrows():
            data_db.loc[(data_db["Symbol"]== symbol) &(data_db["DateTime"] < split[1]["Date"]),["Close","Open","High","Low"]]/= split[1]["Multiple"]
             



def historic_stock_data(db):
        data_db =  db.select("historic")
        data_db_df =  pd.DataFrame(columns=['Symbol','DateTime', 'Open', 'Close','High','Low','Volume'])
        data_db_df = pd.concat([data_db_df, pd.DataFrame(data_db,columns=['Symbol','DateTime', 'Open','Close','High','Low','Volume'])])

        perform_split(db, data_db_df)
        return data_db_df

def historic_stock_data_date(db, date):
        data_db =  db.select_date(date, "historic")
        data_db_df =  pd.DataFrame(columns=['Symbol','DateTime', 'Open', 'Close','High','Low','Volume'])
        data_db_df = pd.concat([data_db_df, pd.DataFrame(data_db,columns=['Symbol','DateTime', 'Open','Close','High','Low','Volume'])])
        
        perform_split(db, data_db_df)

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

def insert_companies(db, companies_data):
    for index, stock_data in companies_data.iterrows():
        symbol = stock_data['Symbol']
        #db.insert_historic(symbol,date, open, close, high, low,volume)

def init_stock_prices_db(db):

    df = pd.read_csv("ndxdata.csv")
    df["DateTime"] = pd.to_datetime( df["DateTime"])
    db.drop_table("historic")
    db.create_table_hisoric()

    insert_historic(db, df)
