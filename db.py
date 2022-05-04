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

    def create_table(self):
        sql ='''CREATE TABLE historic(symbol CHAR(5) NOT NULL,date DATE, open FLOAT, close FLOAT, high FLOAT, low FLOAT, volume INT)'''
        self.cursor.execute(sql)

    def drop_table(self):
        self.cursor.execute("DROP TABLE IF EXISTS historic")


    def insert_historic(self, symbol, date, open, close, high, low, volume):
        sql = "INSERT INTO stockprices.historic (symbol, date, open, close, high, low, volume) VALUES (%s, %s,%s, %s,%s, %s, %s)"
        val = (symbol, date, open, close, high, low, volume)
        self.cursor.execute(sql, val)   
       

    def select_historic(self):
        self.cursor = self.connection.cursor()

        sql = "SELECT COUNT(*) FROM historic"
        self.cursor.execute(sql)
        print(self.cursor.fetchall())
        sql = "SELECT * FROM historic"
        self.cursor.execute(sql)
        
        return self.cursor.fetchall()

    def select_historic_date(self, date):
        sql = f"SELECT * FROM historic WHERE date >=\'{date}\'"
    
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
    

    





