import sqlite3
from sqlite3 import Error
import yfinance as yf

tickers = ["BTC"]
db = "ticker.db"

class db_handler():
	def __init__(self, db, tickers):
		self.db = db
		self.tickers = tickers
		self.con = create_connection(self.db)
		self.cur = self.con.connect()

	def create_connection(self, db: str):
		try:
			con = sqlite3.connect(db)
			return con
		except Error as e:
			print(f"SQLite error: {e}")

	def create_tables(self):
		try:
			for ticker in self.tickers:
				self.cur.execute("CREATE TABLE IF NOT EXISTS ?(Date, Open, High, Low, Close, Volume, Dividends, Stock Splits)", ticker)
			self.con.commit()
		except Error as e:
			print(f"SQLite error: {e}")

	def insert_history(self):
		try:
			for ticker in self.tickers:
				self.cur.execute("")
