import matplotlib.pyplot as plt
import streamlit as st
import yfinance as yf

df = yf.Ticker("MSFT")
msft_df = df.history(period="max")
st.write(msft_df)
