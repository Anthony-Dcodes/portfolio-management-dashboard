import logging
import sqlite3
from datetime import datetime as dt
from datetime import timedelta
from sqlite3 import Error

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


class DB_Handler:
    def __init__(self, db, tickers):
        self.db = db
        self.tickers = tickers
        self.con = self._create_connection(self.db)
        self.cur = self.con.cursor()
        logger.debug("DB_Handler initialized")

    def _create_connection(self, db: str):
        try:
            con = sqlite3.connect(db)
            logger.debug(f"Succesfully connected to db: {self.db}")
            return con
        except Error as e:
            logger.error(f"Connecting to db failed: {e}")

    def commit_and_close(self):
        self.con.commit()
        self.con.close()
        logger.debug("Successfully commited and closed connection")

    def drop_all_tables(self):
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cur.fetchall()
        for table in tables:
            table_name = table[0]
            self.cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
            logger.info(f"Dropping table: {table_name} from db: {self.db}")

    def create_tables(self):
        try:
            for ticker in self.tickers:
                sql = f"""CREATE TABLE IF NOT EXISTS "{ticker}" (
                    date, open, high, low,
                    close, volume, dividends, stock_splits
                );"""
                self.cur.execute(sql)
                logger.debug(f"Successfully created table: {ticker}")
        except Error as e:
            logger.error(f"SQLite error: {e}")

    def insert_history(self):
        try:
            for ticker in self.tickers:
                yf_ticker = yf.Ticker(ticker)
                last_date = self._retrieve_last_date(ticker)
                if last_date:
                    next_day = self._add_one_day(last_date)
                    logger.debug(
                        f"Loading history for ticker: {ticker}, from: {next_day} odwards"
                    )
                    df = yf_ticker.history(period="max", start=next_day).reset_index()
                else:
                    logger.debug(f"Loading max history for ticker: {ticker}")
                    df = yf_ticker.history(period="max").reset_index()

                df = self._format_df(df)
                df.to_sql(ticker, self.con, index=False, if_exists="append")
                logger.info(f"Successfully inserted history into table: {ticker}")

        except Error as e:
            logger.error(f"SQLite failed on: {e}")

    def _retrieve_last_date(self, ticker):
        last_date = self.cur.execute(
            f'SELECT Date FROM "{ticker}" ORDER BY Date DESC LIMIT 1;'
        ).fetchone()
        if last_date is None:
            logger.debug(f"Table: {ticker} is empty, will fetch all historical data")
            return None
        else:
            last_date = last_date[0]
            logger.debug(f"Retrieved last_date: {last_date} for ticker: {ticker}")
            return last_date

    def _format_df(self, df):
        new_df = df.copy()
        # Date formating
        new_df["Date"] = pd.to_datetime(new_df["Date"])
        new_df["Date"] = new_df["Date"].dt.strftime("%Y-%m-%d")
        # Rename columns:
        new_df = new_df.rename(
            columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Dividends": "dividends",
                "Stock Splits": "stock_splits",
            }
        )
        logger.debug("Successfully formated df to be inserted")
        return new_df

    def _add_one_day(self, date):
        datetime_date = dt.strptime(date, "%Y-%m-%d")
        next_day = datetime_date + timedelta(days=1)
        next_day_formated = next_day.strftime("%Y-%m-%d")
        return next_day_formated
