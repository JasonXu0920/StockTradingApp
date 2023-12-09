import yfinance

df = yfinance.download('AAPL', start='2020-01-01', end='2020-10-02')
df.to_csv('AAPL.csv')