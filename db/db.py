import mysql.connector
from mysql.connector import Error


class Db:

    def __init__(self):

        try:
           if self.connection.is_connected():
                db_Info = self.connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                self.cursor = self.connection.cursor()
        
        except Error as e:
            print("Error processing database", e)
       

    def close(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()
        print("Disconnected from MySQL")

    def create_table_historic(self):
        sql ='''CREATE TABLE historic(symbol CHAR(5) NOT NULL,date DATE, open FLOAT, close FLOAT, high FLOAT, low FLOAT, volume INT)'''
        self.cursor.execute(sql)

    def create_table_stock_splits(self):
        sql ='''CREATE TABLE splits(symbol CHAR(5) NOT NULL,date DATE, multiple INT)'''
        self.cursor.execute(sql)

    def create_table_companies(self):
        sql ='''CREATE TABLE companies(symbol CHAR(5) NOT NULL,name VARCHAR(300), shares BIGINT, sector VARCHAR(150), industry VARCHAR(150), category VARCHAR(150))'''
        self.cursor.execute(sql)

    def drop_table(self, table):
        self.cursor.execute(f"DROP TABLE IF EXISTS {table}")


    def insert_historic(self, symbol, date, open, close, high, low, volume):
        sql = "INSERT INTO stockprices.historic (symbol, date, open, close, high, low, volume) VALUES (%s, %s,%s, %s,%s, %s, %s)"
        val = (symbol, date, open, close, high, low, volume)
        self.cursor.execute(sql, val)   

    def insert_splits(self, symbol, date, multiple):
        sql = "INSERT INTO stockprices.splits (symbol, date, multiple) VALUES (%s, %s,%s)"
        val = (symbol, date, multiple)
        self.cursor.execute(sql, val)     

    def insert_companies(self, symbol, name, shares, sector, industry, category):
        sql = "INSERT INTO stockprices.companies (symbol, name, shares, sector, industry, category) VALUES (%s, %s,%s, %s,%s, %s)"
        val = (symbol, name, shares, sector, industry, category)
        print(val)
      
        self.cursor.execute(sql, val)     
       

    def select(self, table):
        self.cursor = self.connection.cursor()

        sql = f"SELECT COUNT(*) FROM {table}"
        self.cursor.execute(sql)
        print(self.cursor.fetchall())
        sql = f"SELECT * FROM {table}"
        self.cursor.execute(sql)
        
        return self.cursor.fetchall()

    def select_date(self, date, table):
        sql = f"SELECT * FROM {table} WHERE date >=\'{date}\'"
    
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def get_min_max(self,table, column):
        sql = f"SELECT MIN({column}) FROM {table}"
        self.cursor.execute(sql)
        min = self.cursor.fetchall()
        sql = f"SELECT MAX({column}) FROM {table}"
        self.cursor.execute(sql)
        max = self.cursor.fetchall()

        return [min[0][0], max[0][0]]

    def get_min_date_symbols(self, table, symbol):
        sql = f"SELECT MIN(date) FROM {table} WHERE symbol = \'{symbol}\'"
        self.cursor.execute(sql)
        min = self.cursor.fetchall()
        return min[0][0]  

    def get_unique_values(self, col, table):
        sql = f"SELECT DISTINCT({col}) FROM {table}"

        self.cursor.execute(sql)
        return self.cursor.fetchall()
    

    





