import mysql.connector
from mysql.connector import Error


class Db:

    def __init__(self):

        try:
            self.connection = mysql.connector.connect(host='34.89.225.47',
                                         database='stockprices',
                                         user='root',
                                         password='ltcapital')
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

    





