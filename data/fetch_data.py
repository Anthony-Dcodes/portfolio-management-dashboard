import sqlite3
from sqlite3 import Error

import pandas as pd
import yfinance as yf

tickers = ["BTC-USD"]
db = "ticker.db"


class DB_Handler:
    def __init__(self, db, tickers):
        self.db = db
        self.tickers = tickers
        self.con = self._create_connection(self.db)
        self.cur = self.con.cursor()

    def _create_connection(self, db: str):
        try:
            con = sqlite3.connect(db)
            return con
        except Error as e:
            print(f"SQLite error: {e}")

    def commit_and_close(self):
        self.con.commit()
        self.con.close()
        print("Successfully commited and closed connection")

    def drop_all_tables(self):
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cur.fetchall()
        for table in tables:
            table_name = table[0]
            self.cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
            print(f"Dropping table: {table_name} from db: {self.db}")
        print(f"Tables that should have been dropped: {tables}")

    def create_tables(self):
        try:
            for ticker in self.tickers:
                sql = f"""CREATE TABLE IF NOT EXISTS "{ticker}" (
                    Date TEXT, Open REAL, High REAL, Low REAL,
                    Close REAL, Volume INTEGER, Dividends REAL
                );"""
                self.cur.execute(sql)
                print(f"Successfully created table: {ticker}")
        except Error as e:
            print(f"SQLite error: {e}")

    def insert_history(self):
        try:
            for ticker in self.tickers:
                yf_ticker = yf.Ticker(ticker)
                last_date = self._retrieve_last_date_formated(ticker)
                if last_date:
                    df = yf_ticker.history(period="max", start=last_date).reset_index()
                else:
                    df = yf_ticker.history(period="max").reset_index()

                df["Date"] = pd.to_datetime(df["Date"])
                df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
                print(df["Date"].head())
                df.to_sql(ticker, self.con, index=False, if_exists="append")
                print(f"Successfully inserted history into table: {ticker}")

        except Error as e:
            print(f"SQLite failed on: {e}")

    def _retrieve_last_date_formated(self, ticker):
        last_date = self.cur.execute(
            f'SELECT Date FROM "{ticker}" ORDER BY Date DESC LIMIT 1;'
        ).fetchone()
        if last_date is None:
            print(f"Table: {ticker} is empty, will fetch all historical data")
            return None
        else:
            last_date = last_date[0]
            print(f"last_date: {last_date}")
            return last_date


db_handler = DB_Handler(db, tickers)
db_handler.drop_all_tables()
db_handler.create_tables()
db_handler.insert_history()
db_handler.commit_and_close()
