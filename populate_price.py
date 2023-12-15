import sqlite3
import config
import alpaca_trade_api as tradeapi
from alpaca_trade_api import TimeFrame, TimeFrameUnit

connection = sqlite3.connect('app.db')
connection.row_factory = sqlite3.Row
cursor = connection.cursor()

cursor.execute("""
    SELECT id, symbol, name FROM stock
""")

rows = cursor.fetchall()

symbols = [row['symbol'] for row in rows]
stock_dict = {}
for row in rows:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']



api = tradeapi.REST(config.API_KEY, config.API_SECRETE, base_url=config.BASE_URL)

chunk_size = 200
for i in range(0, len(symbols), chunk_size):
    symbol_chunk = symbols[i:i+chunk_size]
    barsets = api.get_bars(symbol_chunk, TimeFrame(1, TimeFrameUnit.Day), "2023-06-01", "2023-12-01", adjustment='raw').df

    for index, row in barsets.iterrows():
        print(f"Processing ${row['symbol']}")
        stock_id = stock_dict[row['symbol']]
        cursor.execute("""
                INSERT INTO stock_price (stock_id, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (stock_id, index.date(), row['open'], row['high'], row['low'], row['close'], row['volume']))
        
connection.commit()