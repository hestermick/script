from enum import Enum
import time
import alpaca_trade_api as tradeapi
import asyncio
import os
import pandas as pd
import sys
import json
import curses
from alpaca_trade_api.rest import TimeFrame, URL
from alpaca_trade_api.rest_async import gather_with_concurrency, AsyncRest
from alpaca_trade_api.common import URL
from alpaca_trade_api.stream import Stream
from ta.trend import MACD
from ta.momentum import RSIIndicator
from tabulate import tabulate
from ta.trend import EMAIndicator
from collections import deque
from ta.momentum import RSIIndicator
import ta


NY = 'America/New_York'

# Read the configuration file
with open('config.json') as f:
    config = json.load(f)

# Initialize the dictionary
previous_data_dict = {}

# Iterate over the symbols in the configuration file
for stock in config["symbols"]["stock"]:
    symbol = stock["symbol"]
    # Initialize the dictionary entry for this symbol
    # You might want to adjust this part based on your needs
    previous_data_dict[symbol] = {
        'prices': deque(maxlen=26),  # Will keep only the last 14 prices
        'macd': None,
        'signal': None,
        'histogram': None,
        'rsi': None
    }
    
# Initialize an empty list to store trade data
trade_data_list = []    

# Extract the API keys from the configuration file
api_key = config['api_key']
api_secret = config['api_secret']

# Initialize the Alpaca API client
api = tradeapi.REST(api_key, api_secret, base_url='https://paper-api.alpaca.markets', api_version='v2')
rest = AsyncRest(key_id=api_key, secret_key=api_secret)

feed = "sip"  # change to "sip" if you have a paid account

async def trade_callback(t):
    #print('trade_callback function called')
    #print('Trade data:', t)

    # Get the symbol and price from the trade data
    symbol = t.symbol
    price = t.price

    # Get the previous data and calculations for this symbol
    previous_data = previous_data_dict.get(symbol, {
        'prices': deque(maxlen=15),  # Keep only the last 15 prices
        'macd': pd.Series(dtype=float),
        'signal': pd.Series(dtype=float),
        'histogram': pd.Series(dtype=float),
        'rsi': 0,
    })

    # Update the data and calculations
    previous_data['prices'].append(price)  # Add the new price
    if len(previous_data['prices']) > 14:  # Keep only the last 14 prices
        previous_data['prices'].popleft()

    previous_data['macd'], previous_data['signal'], previous_data['histogram'] = calculate_macd(previous_data['prices'])
    previous_data['rsi'] = calculate_rsi(previous_data['prices'], 14)

    previous_data_dict[symbol] = previous_data  # Update the previous data dict

    # Store a mapping from symbols to lines
    symbol_to_line_map = {symbol: i for i, symbol in enumerate(previous_data_dict.keys(), 1)}

    # Then in your trade_callback function:
    # Get the line number for this symbol
    line_number = symbol_to_line_map[symbol]

    # Move to the line
    sys.stdout.write(f"\033[{line_number};0f")

    # Erase the line
    sys.stdout.write("\033[K")

    histogram = previous_data['histogram'].iat[-1] if not previous_data['histogram'].empty else 0
    macd = previous_data['macd'].iat[-1] if not previous_data['macd'].empty else 0
    signal = previous_data['signal'].iat[-1] if not previous_data['signal'].empty else 0
    sys.stdout.write(f"{symbol}_Price_{price}_RSI_{previous_data['rsi']}_Histogram_{histogram}_MACD_{macd}_Signal_{signal}{'_' * 30}")
    sys.stdout.flush()

async def quote_callback(q):
    if config['print_quote_data']:
        print('Quote data:', q)

async def setup_stream():
    # Initiate Class Instance
    stream = Stream(api_key,
                    api_secret,
                    base_url=URL('https://paper-api.alpaca.markets'),
                    data_feed='sip')  # <- replace to 'sip' if you have PRO subscription

    # subscribing to event
    for stock in config['symbols']['stock']:
        symbol = stock['symbol']
        print(f'Subscribing to trades for {symbol}')  # This will print the symbol being subscribed to
        stream.subscribe_trades(trade_callback, symbol)
        stream.subscribe_quotes(quote_callback, symbol)

    await asyncio.ensure_future(stream._run_forever())

class DataType(str, Enum):
    Bars = "Bars"
    Trades = "Trades"
    Quotes = "Quotes"

def get_data_method(data_type: DataType):
    if data_type == DataType.Bars:
        return rest.get_bars_async
    elif data_type == DataType.Trades:
        return rest.get_trades_async
    elif data_type == DataType.Quotes:
        return rest.get_quotes_async
    else:
        raise Exception(f"Unsupoported data type: {data_type}")

async def gather_tasks(tasks, minor):
    if minor >= 8:
        return await asyncio.gather(*tasks, return_exceptions=True)
    else:
        return await gather_with_concurrency(500, *tasks)

def create_tasks(symbols, start, end, timeframe, data_type):
    tasks = []
    for symbol in symbols:
        limit = config['activity_levels'][timeframe]['limit']  # get the limit from the config file
        args = [symbol, start, end, timeframe, limit] if timeframe and data_type == DataType.Bars else \
            [symbol, start, end, timeframe]
        tasks.append(get_data_method(data_type)(*args))  # use the limit only for bars
    return tasks

def process_results(results, timeframe, data_type):
    bad_requests = 0
    for response in results:
        if isinstance(response, Exception):
            print(f"Got an error: {response}")
        elif not len(response[1]):
            bad_requests += 1
        else:
            sorted_data = response[1].sort_index(ascending=True)  # sort the DataFrame by date
            last_candle = sorted_data.iloc[-1]  # get the last row of the sorted DataFrame

            # Calculate RSI and MACD
            rsi = calculate_rsi(sorted_data['close'])
            macd_line, signal_line, histogram = calculate_macd(sorted_data['close'])

            # Add the data to the trade_data_list
            trade_data_list.append({
                "Timestamp": last_candle.name,
                "Symbol": response[0],
                "Activity Level": timeframe,
                "Activity Level Volume": last_candle['volume'] if data_type == DataType.Bars else None,
                "RSI": rsi[-1],  # the last value is the current RSI
                "MACD Line": macd_line[-1],  # the last value is the current MACD line
                "Signal Line": signal_line[-1],  # the last value is the current signal line
                "Histogram": histogram[-1]  # the last value is the current histogram
            })

async def get_historic_data_base(symbols, data_type: DataType, start, end, timeframe: str = None):
    """
    base function to use with all
    :param symbols:
    :param start:
    :param end:
    :param timeframe:
    :return:
    """
    major = sys.version_info.major
    minor = sys.version_info.minor
    if major < 3 or minor < 6:
        raise Exception('asyncio is not support in your python version')
    msg = f"Getting {data_type} data for {len(symbols)} symbols"
    msg += f", timeframe: {timeframe}" if timeframe else ""
    msg += f" between dates: start={start}, end={end}"
    #print(msg)

    tasks = create_tasks(symbols, start, end, timeframe, data_type)
    results = await gather_tasks(tasks, minor)
    process_results(results, timeframe, data_type)  # pass timeframe and data_type as arguments
    print(historic_data)


async def get_historic_bars(symbols, start, end, timeframe: TimeFrame):
    await get_historic_data_base(symbols, DataType.Bars, start, end, timeframe)


async def get_historic_trades(symbols, start, end, timeframe: str = None):
    await get_historic_data_base(symbols, DataType.Trades, start, end, timeframe)

async def get_historic_quotes(symbols, start, end, timeframe: str = None):
    await get_historic_data_base(symbols, DataType.Quotes, start, end, timeframe)

timeframes = {
    "1Min": (1, "Minute"),
    "3Min": (3, "Minute"),
    "5Min": (5, "Minute"),
    "15Min": (15, "Minute"),
    "1Hour": (1, "Hour"),
    "1Day": (1, "Day")
}

timeframe_units_to_minutes = {
    "Minute": 1,
    "Hour": 60,
    "Day": 1440
}

async def get_stock_bars(stock):
    symbol = stock['symbol']
    activity_level = stock['activity_level']
    timeframe = activity_level  # directly use the activity level as timeframe

    unit_count, unit = timeframes[activity_level]
    limit = config['activity_levels'][activity_level]['limit']  # get the limit from the config file
    minutes_per_unit = timeframe_units_to_minutes[unit]
    minutes_to_fetch = limit * unit_count * minutes_per_unit

    # Stock market isn't 24/7, so we should use the New York time
    current_time = pd.Timestamp.now(tz=NY)
    start_date = (current_time - pd.DateOffset(minutes=minutes_to_fetch)).date().isoformat()
    end_date = current_time.date().isoformat()

    await get_historic_bars([symbol], start_date, end_date, timeframe)
    await get_historic_trades([symbol], start_date, end_date, timeframe)
    await get_historic_quotes([symbol], start_date, end_date, timeframe)

    bars = await rest.get_bars_async(symbol, start_date, end_date, timeframe)
    symbol, data = bars  # unpack the tuple into the symbol and the DataFrame

    # Get the latest price
    latest_price = data['close'].iloc[-1]

    # Calculate RSI and MACD
    rsi = calculate_rsi(data['close'])
    macd_line, signal_line, histogram = calculate_macd(data['close'])

    # Trade based on MACD
    if stock['macd']['enable_trade_macd']:
        await trade_macd(symbol, data, stock['trade_amount']['buy'], stock['macd']['allow_short_selling'], 
                        stock['sell_at_loss'], stock['multiple_trades'])

    # Trade based on RSI
    if stock['trade_rsi']:
        await trade_rsi(symbol, data, stock['trade_amount']['buy'], stock['rsi_thresholds']['Buy'], 
                        stock['rsi_thresholds']['Sell'], stock['sell_at_loss'], stock['multiple_trades'])
    
    previous_data_dict[symbol] = {
        'prices': deque(list(data['close'][-14:])),  # Keep only the last 14 prices
        'macd': macd_line,
        'signal': signal_line,
        'histogram': histogram,
        'rsi': rsi
    }

    return latest_price

async def get_latest_data_for_symbol(api, symbol, timeframe):
    ################################################################I THINK YOU CAN DELETE THIS FUNCTION
    # Get the latest bars for the symbol
    loop = asyncio.get_event_loop()
    barset = await loop.run_in_executor(None, api.get_barset, symbol, timeframe, limit=1)

    # Get the latest bar
    latest_bar = barset[symbol][0]

    # Print the latest volume and close price
    #print(f"Symbol: {symbol}")
    #print(f"Latest volume: {latest_bar.v}")
    #print(f"Latest close price: {latest_bar.c}")
    #print("\n")

def calculate_ema(data, window):
    # Convert data to a list then pandas DataFrame
    data = pd.DataFrame(list(data))
    ema_indicator = ta.trend.EMAIndicator(close=data[0], window=window)
    return ema_indicator.ema_indicator()

def calculate_macd(data, window_slow=26, window_fast=12, window_sign=9, activity_level='Medium'):
    # Adjust the window parameters based on the activity level
    if activity_level == 'High':
        window_slow = int(window_slow / 2)
        window_fast = int(window_fast / 2)
        window_sign = int(window_sign / 2)
    elif activity_level == 'Low':
        window_slow = window_slow * 2
        window_fast = window_fast * 2
        window_sign = window_sign * 2

    # Calculate the Short-term Exponential Moving Average
    short_ema = calculate_ema(data, window_fast)

    # Calculate the Long-term Exponential Moving Average
    long_ema = calculate_ema(data, window_slow)

    # Calculate the Moving Average Convergence Divergence (MACD)
    macd = short_ema - long_ema

    # Calculate the signal line
    signal = calculate_ema(macd, window_sign)

    # Calculate the MACD histogram
    histogram = macd - signal

    return macd, signal, histogram

def calculate_rsi(data, window):
    data_series = pd.Series(list(data))  # Convert deque to pandas Series
    rsi_indicator = RSIIndicator(close=data_series, window=window, fillna=False)
    return rsi_indicator.rsi().iloc[-1]  # Get the last value

async def trade_macd(symbol, data, trade_amount, allow_short_selling, sell_at_loss, multiple_trades):
    macd_line, signal_line, histogram = calculate_macd(data['close'])
    if macd_line.iloc[-1] > 0 and (macd_line.iloc[-2] < 0 or multiple_trades):
        # MACD line crossed above the signal line, buy
        print(f"MACD - Buying {trade_amount} shares of {symbol}")  # This will print when buying due to MACD
        api.submit_order(symbol, trade_amount, 'buy', 'market', 'gtc')
    elif macd_line.iloc[-1] < 0 and (macd_line.iloc[-2] > 0 or (allow_short_selling and multiple_trades)):
        # MACD line crossed below the signal line, sell
        print(f"MACD - Selling {trade_amount} shares of {symbol}")  # This will print when selling due to MACD
        api.submit_order(symbol, trade_amount, 'sell', 'market', 'gtc')

async def trade_rsi(symbol, data, trade_amount, rsi_buy_threshold, rsi_sell_threshold, sell_at_loss, multiple_trades):
    rsi = calculate_rsi(data['close'])
    if rsi[-1] < rsi_buy_threshold and (rsi[-2] >= rsi_buy_threshold or multiple_trades):
        # RSI crossed below the buy threshold, buy
        print(f"RSI - Buying {trade_amount} shares of {symbol}")  # This will print when buying due to RSI
        api.submit_order(symbol, trade_amount, 'buy', 'market', 'gtc')
    elif rsi[-1] > rsi_sell_threshold and (rsi[-2] <= rsi_sell_threshold or (sell_at_loss and multiple_trades)):
        # RSI crossed above the sell threshold, sell
        print(f"RSI - Selling {trade_amount} shares of {symbol}")  # This will print when selling due to RSI
        api.submit_order(symbol, trade_amount, 'sell', 'market', 'gtc')

# Define the asynchronous place_order function
async def place_order(api, symbol, qty, side, order_type, time_in_force):
    try:
        order = await api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type=order_type,
            time_in_force=time_in_force
        )
        print(f"{side.capitalize()} order placed for {symbol} with quantity {qty}.")
        return order
    except Exception as e:
        print(f"Error placing {side} order for {symbol}: {e}")
        return None

def print_table():
    print(tabulate(trade_data_list, headers="keys", tablefmt="pretty"))

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

async def main(stocks):
    previous_data_dict = {}
    trade_data_list = []
    start = config["start"]
    end = config["end"]
    historic_data = await get_historic_data_base(stocks, api, start, end)

    # Initialize previous_data_dict for each symbol
    for stock in stocks:
        previous_data_dict[stock['symbol']] = {
            'prices': deque(maxlen=26),  # Will keep only the last 14 prices
            'macd': None,
            'signal': None,
            'histogram': None,
            'rsi': None
        }

    stream = await setup_stream()
    
    for symbol in symbols:
        # Get the historical data for this symbol
        data = historic_data[symbol]

        # Get the previous data for this symbol
        previous_data = previous_data_dict[symbol]

        # Populate the prices deque with the historical data
        for price in data['prices']:
            previous_data['prices'].append(price)

        # Calculate the MACD, signal, and histogram based on the historical data
        previous_data['macd'], previous_data['signal'], previous_data['histogram'] = calculate_macd(previous_data['prices'])

        # Update the previous data dict
        previous_data_dict[symbol] = previous_data    
    
    # Fetch historic data once at the start
    for stock in stocks:
        await get_stock_bars(stock)

    while True:
        print("Fetching data...")
        trade_data_list.clear()  # Clear the list at the beginning of each iteration

        await stream.run()

        # After all the data has been collected...
        print(tabulate(trade_data_list, headers="keys", tablefmt="grid"))

        # Sleep for a while before the next iteration
        await asyncio.sleep(20)  # Sleep for 20 seconds
        print(previous_data_dict)


if __name__ == '__main__':
    print('Starting main function')  # This will print when the main function starts
    start_time = time.time()
    stocks = config['symbols']['stock']  # get stocks from config file
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(stocks))
    finally:
        loop.close()
        print(f"took {time.time() - start_time} sec")

