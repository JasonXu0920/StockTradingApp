import sqlite3
import alpaca_trade_api as tradeapi
import os
from dotenv import load_dotenv

load_dotenv()

connection = sqlite3.connect("app.db")
cursor = connection.cursor()

api = tradeapi.REST(os.getenv('API_KEY'), os.getenv('API_SECRETE'), base_url='https://paper-api.alpaca.markets')
assets = api.list_assets()

for asset in assets:
    try:
        if asset.status == 'active':
            cursor.execute("INSERT INTO stock (symbol, company) VALUES (?, ?)", (asset.symbol, asset.name))
    except Exception as e:
        print(e)


connection.commit()