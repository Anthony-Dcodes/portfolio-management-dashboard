import sqlite3
from sqlite3 import Error

import pandas as pd
import yfinance as yf

tickers = ["BTC"]
db = "ticker.db"


class DB_Handler:
    def __init__(self, db, tickers):
        self.db = db
        self.tickers = tickers
        self.con = self.create_connection(self.db)
        self.cur = self.con.cursor()

    def create_connection(self, db: str):
        try:
            con = sqlite3.connect(db)
            return con
        except Error as e:
            print(f"SQLite error: {e}")

    def drop_all_tables(self):
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cur.fetchall()
        for table in tables:
            self.cur.execute(f'DROP TABLE IF EXISTS "{table}";')
            print(f"Dropping table: {table} from db: {self.db}")

    def create_tables(self):
        try:
            for ticker in self.tickers:
                sql = f"""CREATE TABLE IF NOT EXISTS "{ticker}" (
                    Date TEXT, Open REAL, High REAL, Low REAL,
                    Close REAL, Volume INTEGER, Dividends REAL, Stock_Splits REAL
                );"""
                print(f"Successfully created table: {ticker}")
            self.con.commit()
        except Error as e:
            print(f"SQLite error: {e}")

    def insert_history(self):
        try:
            for ticker in self.tickers:
                yf_ticker = yf.Ticker(ticker)
                df = yf_ticker.history(period="max").reset_index()
                df["Date"] = pd.to_datetime(df["Date"])
                df["Date"] = df["Date"].dt.strftime("%Y%m%d")
                df.to_sql(ticker, self.con, index=False)
                self.con.commit()
                print(f"Successfully inserted history into table: {ticker}")

        except Error as e:
            print(f"SQLite failed on: {e}")


db_handler = DB_Handler(db, tickers)
db_handler.drop_all_tables()
db_handler.create_tables()
db_handler.insert_history()
