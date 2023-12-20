import sqlite3, config
import alpaca_trade_api as tradeapi
from  datetime import date
import smtplib, ssl

context = ssl.create_default_context()

connection = sqlite3.connect(config.DB)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()

cursor.execute("""
    SELECT id FROM strategy WHERE name = 'opening_range_breakout'
""")

strategy_id = cursor.fetchone()['id']

cursor.execute("""
    SELECT symbol, name
    FROM stock
    JOIN stock_strategy ON stock_strategy.stock_id = stock.id
    WHERE stock_strategy.strategy_id = ?
""", (strategy_id,))

stocks = cursor.fetchall()
symbols = [stock['symbol'] for stock in stocks]

api = tradeapi.REST(config.API_KEY, config.API_SECRETE, base_url=config.BASE_URL)

orders = api.list_orders(status='all', after='2023-06-30T13:30:00Z')
existing_orders_symbol = [order.symbol for order in orders]

current_date = date.today().isoformat()
start_minute_bar = f"{current_date} 09:30:00-04:00"
end_minute_bar = f"{current_date} 09:45:00-04:00"

messages = []
for symbol in symbols:
    minutes_bars = api.polygon.historic_agg_v2(symbol, 1, 'minute', _from='2023-06-30', to='2023-06-30').df
    opening_range_mask = (minutes_bars.index >= start_minute_bar) and (minutes_bars.index < end_minute_bar)
    opening_range_bars = minutes_bars.loc[opening_range_mask]
    opening_range_low = opening_range_bars['low'].min()
    opening_range_high = opening_range_bars['high'].max()
    opening_range = opening_range_high - opening_range_low

    after_opening_range_mask = minutes_bars.index >= end_minute_bar
    after_opening_range_bars = minutes_bars.loc[after_opening_range_mask]
    after_opening_range_breakout = after_opening_range_bars[after_opening_range_bars['close'] > opening_range_high]

    if not after_opening_range_breakout.empty:
        limit_price = after_opening_range_breakout.iloc[0]['close']
        
        if symbol not in existing_orders_symbol:
            messages.append(f"Placing order for {symbol} at {limit_price}\n\n")
            api.submit_order(
                symbol=symbol,
                side='buy',
                type='limit',
                qty='100',
                time_in_force='day',
                order_class='bracket',
                limit_price=limit_price,
                take_profit=dict(
                    limit_price=limit_price+opening_range,
                ),
                stop_loss=dict(
                    stop_price=limit_price-opening_range,
                )
            )
        else:
            print("Order already placed !!!")

with smtplib.SMTP_SSL(config.EMAIL_HOST, config.EMAIL_PORT, context=context) as server:
    server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)
    email_message = f"Subject: Trade Notifications for {current_date}."
    email_message = "\n\n".join(messages)
    server.sendmail(config.EMAIL_ADDRESS, config.EMAIL_ADDRESS, email_message)
