# Use full path for safe logging
LOG_DIR = "/home/vairavan/projects/Algotrader/data/logs"
DATA_DIR = "/home/vairavan/projects/Algotrader/data/historical/NIFTY/"

strike_price_range = 500
#expiry_months = 2
expiry_weeks = 1
# Number of days to go back for historical data
num_days_to_go_back = 60
market_timezone_str = "Asia/Calcutta"
bar_sizes = ["1 day", "5 secs"]
#bar_sizes = ["1 day", "1 hour", "5 mins", "1 min"] 


### Do not change anything below this line unless you know what you are doing ###

import os
import sys
import logging
import asyncio
import threading

import rich.progress
thread_lock = threading.Lock()


ROOT_LOGGER = None
total_rows_received = 0
total_contracts = 0

# --- Add src directory to Python path ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path: sys.path.insert(0, project_root)

from typing import List
import src.utils.helpers as myutil
from src.utils.objects import TaskInfo
from rich.progress import Progress, BarColumn,TimeElapsedColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn
from rich.console import Console
from rich.table import Column


async def submit_tasks(task_info:TaskInfo,ibkr):
    if task_info.req_count_for_contract >0 and task_info.req_count_for_contract % 3 == 0: #rate limit for same contract is 2 secs
        await asyncio.sleep(2)
    else:
        await asyncio.sleep(0.3)

    result =  await ibkr.get_historical_bars(task_info.contract, task_info.start, task_info.end,
                                              bar_size=task_info.bar_size, return_bars=task_info.return_bars)

    return result

async def wait_for_tasks(tasks_list):

    import time
    for task_info in tasks_list:
        
        start_time = time.perf_counter()
        await task_info.task
        end_time = time.perf_counter()
        ROOT_LOGGER.info(f"Task {task_info.task.get_name()} completed in {end_time - start_time} seconds.")

        hist_bars = task_info.task.result()
        
        if hist_bars is not None and len(hist_bars) > 0:
            ROOT_LOGGER.debug(f"Received {len(hist_bars)} rows for {task_info.localSymbol} for date range {task_info.start} to {task_info.end} for bar size {task_info.start.bar_size}.")
            task_info.data_logger.log_bars(hist_bars)
        elif hist_bars is not None: #Empty data
            ROOT_LOGGER.debug(f"No data received for {task_info.localSymbol} for date range {task_info.start} to {task_info.end} for bar size {task_info.start.bar_size}.")
        else:
            ROOT_LOGGER.debug(f"Error in fetching data for {task_info.localSymbol} for date range {task_info.start} to {task_info.end} for bar size {task_info.start.bar_size}.")


async def process_task_list(ibkr, task_list:List[TaskInfo]):
    global total_rows_received
    import time as time

    from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from src.utils.helpers import print_header_line

    console = Console()
    progress = Progress(
                            TextColumn("{task.description}"),
                            BarColumn(bar_width=40),
                            MofNCompleteColumn(),
                            TimeElapsedColumn(),
                            console=console
                        )
    task = progress.add_task("Requesting", total=len(task_list))
    with Live(console=console, refresh_per_second=10) as live:
        console.print("\n")
        for task_info in task_list:
        # Create a status panel with multi-line details
            status_panel = Panel(
                f"[bold cyan]Symbol:[/bold cyan] {task_info.contract.localSymbol}\n"
                f"[bold cyan]Date:[/bold cyan] {task_info.start.date()} to {task_info.end.date()}\n"
                f"[bold cyan]Bar Size:[/bold cyan] {task_info.bar_size}",
                title="[b]Current Task Details",
                border_style="cyan",
                width=40
            )
            live.update(Group(progress, status_panel))

            data_logger = ibkr.get_bardata_logger(contract=task_info.contract, bar_size=task_info.bar_size, data_dir=DATA_DIR)
            hist_bars = await ibkr.get_historical_bars(task_info.contract, task_info.start, task_info.end, bar_size=task_info.bar_size, return_bars=True )

            if hist_bars is not None and len(hist_bars) > 0:
                total_rows_received += len(hist_bars)
                data_logger.log_bars(hist_bars)
            elif hist_bars is not None: #Empty data
                ROOT_LOGGER.debug(f"No data received for {task_info.contract.localSymbol} for date range {task_info.start} to {task_info.end} for bar size {task_info.bar_size}.")
            else:
                ROOT_LOGGER.debug(f"Error in fetching data for {task_info.contract.localSymbol} for date range {task_info.start} to {task_info.end} for bar size {task_info.bar_size}.")
            
            progress.advance(task)

        #show a final status panel
        status_panel = Panel(
            "[green]All tasks completed![/green]",
            title="[b]Status",
            border_style="green"
        )
        live.update(Group(progress, status_panel))
        await asyncio.sleep(1)  # Allow time for the final status to be displayed
    
        

def get_dates_with_trade(contracts, bar_size_to_use, data_dir,bar_sizes_for_tasks):
    import pandas as pd
    from src.utils.helpers import get_contract_data_filepath
    import os 

    trades_list = []

    for contract in contracts:

        file_path = get_contract_data_filepath(contract,data_dir, bar_size_to_use)
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            if df.empty or len(df) <= 0: continue

            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            
            trades_list += [ 
                                TaskInfo.FromContract(contract,d.to_pydatetime(),d.to_pydatetime(),bar_size) 
                                for d in df[df["Close"].notna()]["Timestamp"].sort_values().unique() 
                                for bar_size in bar_sizes_for_tasks
                            ]

    return trades_list



async def main():
    IBKR_CLIENT_ID = 18
    symbol = 'NIFTY50'
    exchange = 'NSE'
    currency = "INR"
    pid = os.getpid()

    from src.data_management.ibkr_helper import IBKRHelper
    import time
    from dateutil.relativedelta import relativedelta
    
    console= Console()
    console.print(f"[bold green]Starting historical bar data collector at process id = {pid}")  # Log the process ID for debugging

    ROOT_LOGGER.info(f"Starting historical bar data collector at process id = {pid}")  # Log the process ID for debugging

    ibkr = IBKRHelper(market_timezone_str=market_timezone_str, default_timeout=300)
    await ibkr.connect(port=IBKRHelper.TWS_PORT, client_id=IBKR_CLIENT_ID)

    
    console.print(f"[bold green]Getting options chain for symbol={symbol}, strike_range= +/- {strike_price_range}, expiry weeks = {expiry_weeks}.")  # Log the process ID for debugging
    ROOT_LOGGER.info(f"Getting options chain for symbol={symbol}, strike_range= +/- {strike_price_range}, expiry weeks = {expiry_weeks}.")

    start_time = time.perf_counter()
    contracts = await ibkr.get_option_chains_new(symbol=symbol, exchange=exchange, currency=currency,
                                                 strike_price_range=strike_price_range,expiry_weeks=expiry_weeks) 
    end_time = time.perf_counter()

    console.print(f"[bold green]Fetched {len(contracts)} contracts in {end_time - start_time} seconds.")
    ROOT_LOGGER.info(f"{len(contracts)} contracts fetched from options chain fetched in {end_time - start_time} seconds.")

    hist_end_date = datetime.now(tz=ibkr.market_timezone).replace(hour=15, minute=30,second=0,microsecond=0,tzinfo=ibkr.market_timezone)
    hist_start_date = (hist_end_date - relativedelta(days=num_days_to_go_back)).replace(hour=9, minute=15, tzinfo=ibkr.market_timezone)
    
    console.print(f"[bold green]Processing {len(contracts)} contracts for date range {hist_start_date} to {hist_end_date}.")
    ROOT_LOGGER.info(f"Processing {len(contracts)} contracts for date range {hist_start_date} to {hist_end_date}.")


    # Do 1 day first
    console.print(f"[bold green]Fetching 1 day bars for {len(contracts)} contracts.")
    ROOT_LOGGER.info(f"Fetching 1 day bars for {len(contracts)} contracts.")

    start_time = time.perf_counter()
    from src.utils.objects import TaskInfo
    one_day_tasks = [ 
                TaskInfo.FromContract(c, hist_start_date, hist_end_date, "1 day") 
                for c in contracts
            ]
    await process_task_list(ibkr, one_day_tasks)
    end_time = time.perf_counter()

    console.print(f"[bold green]Fetched 1 day bars for {len(contracts)} contracts in {end_time - start_time} seconds.")
    ROOT_LOGGER.info(f"1 day bars fetched in {end_time - start_time} seconds.")

    new_bar_sizes = [b for b in bar_sizes if b != "1 day"]  # Exclude 1 day from the bar sizes for further processing

    if len(new_bar_sizes) == 0:
        console.print(f"[bold green]All contracts are up to date for 1 day bars. Exiting.")
        ROOT_LOGGER.info(f"All contracts are up to date for 1 day bars. Exiting.")
        ibkr.disconnect()
        return

    #Now get all days from one day files
    dates_with_trade = get_dates_with_trade(contracts,bar_size_to_use="1 day",data_dir=DATA_DIR,bar_sizes_for_tasks=new_bar_sizes)
    console.print(f"[bold green]Processing {len(dates_with_trade)} contracts.")
    ROOT_LOGGER.info(f"Processing {len(dates_with_trade)} contracts.")
    
    start_time = time.perf_counter()
    await process_task_list(ibkr, dates_with_trade)
    end_time = time.perf_counter()

    console.print(f"[bold green]Processed {len(dates_with_trade)} contracts in {end_time - start_time} seconds.")
    ROOT_LOGGER.info(f"Processed {len(dates_with_trade)} contracts in {end_time - start_time} seconds.")

    await ibkr.disconnect()

if __name__ == "__main__":
    
    import asyncio
    from datetime import datetime
    
    from src.utils.logging_setup import setup_general_logging
    pid = os.getpid()
    log_filename = f"data_collector_{datetime.now().strftime('%Y%m%d_%H%M%f') }.log"
    print(f"Starting market data collector at process id = {pid} and logs saved to {LOG_DIR}/{log_filename}")  # Log the process ID for debugging
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
