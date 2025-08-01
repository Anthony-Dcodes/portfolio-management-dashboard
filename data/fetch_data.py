import sqlite3

import yfinance as yf

con = sqlite3.connect("ticker.db")
cur = con.cursor()
