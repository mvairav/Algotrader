# Use full path for safe logging
LOG_DIR = "/home/vairavan/projects/Algotrader/data/historical/logs"
DATA_DIR = "/home/vairavan/projects/Algotrader/data/historical/NIFTY/"

# Parameters for data collection 
strike_price_range = 500

# Number of days to go back for historical data
market_timezone_str = "Asia/Calcutta"
### Do not change anything below this line unless you know what you are doing ###

import os
import sys
import logging
import asyncio
import threading
thread_lock = threading.Lock()


ROOT_LOGGER = None
total_rows_received = 0
total_contracts = 0

# --- Add src directory to Python path ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path: sys.path.insert(0, project_root)


async def main():
    IBKR_CLIENT_ID = 18
    symbol = 'NIFTY50'
    exchange = 'NSE'
    currency = "INR"
    

    from src.data_management.ibkr_helper import IBKRHelper
    import time
    from rich.console import Console
    import datetime
    from ib_async import Index
    from tqdm import tqdm
    # Setup logging

    console= Console()

    ibkr = IBKRHelper(market_timezone_str=market_timezone_str, default_timeout=300)
    await ibkr.connect(port=IBKRHelper.TWS_PORT, client_id=IBKR_CLIENT_ID)

    console.print(f"[bold green]Getting options chain for symbol={symbol}, strike_range= +/- {strike_price_range}, expiry weeks = {1}.")  
    ROOT_LOGGER.info(f"Getting options chain for symbol={symbol}, strike_range= +/- {strike_price_range}, expiry weeks = {1}.")

    today = datetime.datetime.now(tz=ibkr.market_timezone).replace(hour=0,minute=0,second=0,microsecond=0)
    days_till_thursday = (3 - today.weekday()) % 7
    this_week_thursday =(today + datetime.timedelta(days=days_till_thursday))

    all_weekdays = [
                this_week_thursday - datetime.timedelta(days=6), #friday
                this_week_thursday - datetime.timedelta(days=3), #monday
                this_week_thursday - datetime.timedelta(days=2), #tuesday
                this_week_thursday - datetime.timedelta(days=1), #wednesday
                this_week_thursday #thursday
                ]

    all_contracts = await ibkr.get_option_chains_new(symbol=symbol,currency=currency,exchange=exchange,expiry_weeks=1)
    ROOT_LOGGER.info(f"Found {len(all_contracts)} contracts for NIFTY options")

    all_contracts = [c for c in all_contracts if c.lastTradeDateOrContractMonth == this_week_thursday.strftime("%Y%m%d")]
    
    console.print(f"[bold green]Filtered contracts for {this_week_thursday.strftime('%Y-%m-%d')}: {len(all_contracts)} contracts")
    ROOT_LOGGER.info(f"Filtered to {len(all_contracts)} contracts for today's date: {this_week_thursday.strftime('%Y-%m-%d')}")

    idx = Index(symbol=symbol,currency=currency,exchange=exchange)

    for i, day in enumerate(all_weekdays):
        ROOT_LOGGER.info(f"Processing finding open price for {symbol} on {day.date()}")

        index_history = await ibkr.get_historical_bars(contract=idx, start_date=day, end_date=day+datetime.timedelta(days=1,hours=10), bar_size="5 secs", return_bars=True)
        open_price = index_history[0].open if index_history else None
        if open_price is None:
            console.print(f"[bold red]No historical data found for {symbol} on {day.date()}. Skipping this day.")
            ROOT_LOGGER.warning(f"No historical data found for {symbol} on {day.date()}. Skipping this day.")
            continue

        ROOT_LOGGER.info(f"Open price: {open_price} on {day.date()}")

        ROOT_LOGGER.info(f"Filtering contracts for {symbol} with strike price range +/- {strike_price_range} from index open price {index_history[i].close} on {day.date()}")
        contracts = [c for c in all_contracts if (c.strike > (index_history[i].close-strike_price_range) ) and (c.strike < (index_history[i].close+strike_price_range))]
        ROOT_LOGGER.info(f"Found {len(contracts)} contracts for {symbol} with strike price range +/- {strike_price_range} from index open price {index_history[i].close} on {day.date()}")

        console.print(f"\n[bold blue]Collecting data for {day.date()} with {len(contracts)} contracts")
        ROOT_LOGGER.info(f"Collecting data for {day.date()} with {len(contracts)} contracts")

        for c in tqdm(contracts, total=len(contracts), desc=f"{i+1}/{len(contracts)}: Processing contracts for {day.date()}"):
            #TODO: Find if data already collected for this contract, on this day, for this bar size

            oneday_data_logger = ibkr.get_bardata_logger(contract=c, bar_size="1 day", data_dir=DATA_DIR)
            next_day = day + datetime.timedelta(days=1)
            hist_bars_oneday = await ibkr.get_historical_bars(c, day, next_day, bar_size="1 day", return_bars=True)
            ROOT_LOGGER.info(f"Retrieved {len(hist_bars_oneday)} daily bars for contract {c.localSymbol} on {day.date()}")

            if hist_bars_oneday is not None and len(hist_bars_oneday) > 0:
                oneday_data_logger.log_bars(hist_bars_oneday)
                secs_data_logger = ibkr.get_bardata_logger(contract=c, bar_size="5 secs", data_dir=DATA_DIR)
                hist_bars_secs = await ibkr.get_historical_bars(c, day, next_day, bar_size="5 secs", return_bars=True )
                if hist_bars_secs is not None and len(hist_bars_secs) > 0:
                    secs_data_logger.log_bars(hist_bars_secs)


if __name__ == "__main__":
    
    import asyncio
    from datetime import datetime
    
    from src.utils.logging_setup import setup_general_logging
    pid = os.getpid()
    log_filename = f"data_collector_{datetime.now().strftime('%Y%m%d_%H%M%f') }.log"
    setup_general_logging(
        log_dir=LOG_DIR,
        level=logging.DEBUG,
        log_to_console=False,
        log_to_file=True,
        log_filename=log_filename
    )
    ROOT_LOGGER = logging.getLogger(__name__)  # Get the root logger after setting up logging
    logging.getLogger("ib_async").setLevel(logging.INFO)  # Reduce verbosity for ib_insync

    try:
        # Run the main function using asyncio
        asyncio.run(main())

    except KeyboardInterrupt:
        ROOT_LOGGER.info("Program stopped by KeyboardInterrupt")
    finally:
        ROOT_LOGGER.info("Market data collector has exited")
