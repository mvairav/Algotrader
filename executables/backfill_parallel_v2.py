# Use full path for safe logging
LOG_DIR = "/home/vairavan/projects/Algotrader/data/historical_data/logs"
DATA_DIR = "/home/vairavan/projects/Algotrader/data/historical_data/NIFTY/"

# Parameters for data collection 
strike_price_range = 500
expiry_months = 2

# Number of days to go back for historical data
num_days_to_go_back = 60
market_timezone_str = "Asia/Calcutta"
bar_sizes = ["1 day"]
#bar_sizes = ["1 day", "1 hour", "5 mins", "1 min"] 

#This will stop collecting data for next bar size if no data for current bar size
skip_next_bar_sizes_if_no_data = False

#This will use 1 day dates for 1 day bars
use_1day_dates = True

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

import src.utils.helpers as myutil
from rich.progress import Progress, BarColumn,TimeElapsedColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn
from rich.console import Console
from rich.table import Column

def print_header_line(console,columns):
    from rich.text import Text  # For colored text

    # --- Create the header string using a loop ---
    header_parts = []
    total_width = 0

    for column in columns:
        if hasattr(column, "_table_column"):  # Check if the column has a table_column attribute.
            header = Text(column._table_column.header, style=column._table_column.header_style, justify="left")  # Create a Text object with the header style
            header_width = column._table_column.width 
            if  column._table_column.header == "Task":
                header_width += 2
            if column._table_column.header == "Curr Value":
                header_width -= 1
            total_width += header_width #Accumulate header width values

            #Calculate the amount of padding the headers should take
            header.pad_right(header_width - len(column._table_column.header))  # Add padding
            header_parts.append(header)
        else: #There is no table column attribute, and skip append
            print("skipping the non column elements like progress")
    console.print("-" * total_width)  
    console.print(*header_parts)  # Print the header with the specified style
    console.print("-" * total_width)  

async def wait_for_tasks(requested_contracts, progress, waiting_task, hist_end_date):
    global total_rows_received
    import time as time 

    progress.update(waiting_task, description="[bold yellow]Receiving...",total=len(requested_contracts), visible=True)
    for task, curr_contract, _start, _end,req_count_for_contract,len_date_range_iter, bar_size, data_logger in requested_contracts:
        progress.update(waiting_task, value=curr_contract.localSymbol)
        start_time = time.perf_counter()
        await task
        end_time = time.perf_counter()
        ROOT_LOGGER.info(f"Task {task.get_name()} completed in {end_time - start_time} seconds.")
        progress.update(waiting_task, advance=1)
        hist_bars = task.result()
        
        if hist_bars is not None and len(hist_bars) > 0:
            ROOT_LOGGER.debug(f"Received {len(hist_bars)} rows for {curr_contract.localSymbol} for date range {_start} to {_end} for bar size {bar_size}.")
            total_rows_received += len(hist_bars)
            data_logger.log_bars(hist_bars)
        elif hist_bars is not None: #Empty data
            ROOT_LOGGER.debug(f"No data received for {curr_contract.localSymbol} for date range {_start} to {_end} for bar size {bar_size}.")
        else:
            ROOT_LOGGER.debug(f"Error in fetching data for {curr_contract.localSymbol} for date range {_start} to {_end} for bar size {bar_size}.")

        if req_count_for_contract == len_date_range_iter-1:
            data_logger.log_emptyRow(end_date=hist_end_date)

        task = None #Release the task object and free up memory
    progress.update(waiting_task, description="[bold green]Waiting...",value="Completed")

async def process_contracts(ibkr,contracts, DATA_DIR):
    from datetime import datetime, time as dt_time
    import time as time

    import pandas as pd
    from dateutil.relativedelta import relativedelta

    # Initialize Rich Progress tracker
    console = Console()
    progress = Progress(TextColumn("{task.description}", table_column=Column(header="Task", width=30, header_style="bold cyan", justify="left")),
                        TextColumn("[cyan] {task.fields[value]} ", table_column=Column(header="Curr Status", width=25, header_style="bold cyan",justify="left")),
                        BarColumn(table_column=Column(header="Progress", width=45, header_style="bold cyan",justify="left"),bar_width=40),
                        MofNCompleteColumn(table_column=Column(header="Processing", width=15, header_style="bold cyan",justify="left")),
                        TimeElapsedColumn(table_column=Column(header="Elapsed", width=20, header_style="bold cyan",justify="left")),
                        #TimeRemainingColumn(table_column=Column(header="Remaining", width=20, header_style="bold cyan")),
                        console=console,
                        transient=False,
                        disable=False,
                        expand=False,
                        )

    hist_end_date = datetime.now(tz=ibkr.market_timezone).replace(hour=15, minute=30,second=0,microsecond=0,tzinfo=ibkr.market_timezone)

    hist_start_date = hist_end_date - relativedelta(days=num_days_to_go_back)
    hist_start_date = hist_start_date.replace(hour=9, minute=15, tzinfo=ibkr.market_timezone)

    # Define date parameters (modify as needed)
    date_range_freq = {"5 secs": 1,"30 secs":60,"1 min": 60,"5 mins": 60,"1 hour": 60,"1 day": 60}
    
    console.print(f"[bold green]Processing {len(contracts)} contracts for date range {hist_start_date} to {hist_end_date}.")
    ROOT_LOGGER.info(f"Processing {len(contracts)} contracts for date range {hist_start_date} to {hist_end_date}.")

    # ROOT_LOGGER.setLevel(logging.ERROR)  # Set the logging level to ERROR
    # logging.getLogger("data_connector.ibkr_helper").setLevel(logging.ERROR)  # Reduce verbosity for ibkr_helper
    # logging.getLogger("ib_async").setLevel(logging.FATAL)  # Reduce verbosity for ib_insync

    total_contracts = len(contracts)
    curr_contracts_count = 0
    total_required = len(contracts)* len(bar_sizes)
    total_requested = 0

    if not progress.disable:
        print_header_line(console,progress.columns)
    
    overall_task = None
    request_task = None
    waiting_task = None
    ibkr.rich_progress = None

    start_time = time.perf_counter()
    semapahores = {}
    for bar_size in bar_sizes:
        if "secs" in bar_size:
            semapahores["small_bar"] = asyncio.Semaphore(ibkr.concurrent_calls["small_bar"])
        else:
            semapahores["others"] = asyncio.Semaphore(ibkr.concurrent_calls["others"])
        

    async def concurrent_calls(curr_contract, start, end, bar_size, return_bars,iter_count,semaphore):
        nonlocal total_requested 
        if iter_count >0 and iter_count % 3 == 0: #rate limit for same contract in 2 secs
            await asyncio.sleep(2)
        else:
            await asyncio.sleep(0.3)
        async with semaphore:
            with thread_lock:
                progress.update(request_task, value=curr_contract.localSymbol, advance=1)
                total_requested += 1
                if total_requested == total_required:
                    progress.update(request_task, description="[bold green]Requesting", value="Completed")
            result =  await ibkr.get_historical_bars(curr_contract, start, end, bar_size=bar_size, return_bars=return_bars)

            return result


    with progress:

        requested_contracts = []
        overall_task = progress.add_task("[blue]Overall", total=2, value="Starting..",visible=False)
        request_task = progress.add_task("[bold yellow]Requesting", total=len(contracts), value="Contracts")
        ibkr.rich_progress = progress
        waiting_task = progress.add_task("[red]Receiving", value="", total=len(requested_contracts), visible=True)

        progress.update(request_task, completed=0)

        for curr_contract in contracts:
            progress.update(overall_task, value="requesting...")

            for bar_size in bar_sizes:
                semaphore = semapahores["small_bar"] if "secs" in bar_size else semapahores["others"]
                expiry = curr_contract.lastTradeDateOrContractMonth
                strike = curr_contract.strike
                right = curr_contract.right

                folder_name = f"{DATA_DIR}/{expiry}/{int(strike)}/"
                data_logger = ibkr.get_bardata_logger(curr_contract, bar_size=bar_size, data_dir=folder_name)

                df = pd.read_csv(data_logger.handlers[0].baseFilename)

                data_start_date = hist_start_date
                if df is not None and len(df) > 0:
                    last_date_on_file = datetime.fromisoformat(df.iloc[-1].Timestamp).astimezone(ibkr.market_timezone)
                    data_start_date = max(hist_start_date, last_date_on_file)

                date_intervals = []
                if use_1day_dates and "day" not in bar_size:
                    df = pd.read_csv(data_logger.handlers[0].baseFilename.replace(bar_size.replace(" ",""),"1day"))
                    if df is not None and len(df) > 0:
                        df["Timestamp"] = pd.to_datetime(df["Timestamp"])
                        df.sort_values(by="Timestamp", inplace=True)

                        date_intervals = [
                                            ( datetime.combine(date.date(), dt_time(9, 15), tzinfo=date.tzinfo),
                                            datetime.combine(date.date(), dt_time(15, 30), tzinfo=date.tzinfo) )
                                            for date in df[df["Timestamp"] > data_start_date]["Timestamp"]
                                        ]
                        ROOT_LOGGER.debug(f"Using 1 day dates from 1 day bars for {curr_contract.localSymbol} total dates = {len(date_intervals)}.")
                    else:
                        ROOT_LOGGER.debug(f"No data found for {curr_contract.localSymbol} for 1 day bars.\n\
                                          Using date range from {data_start_date} to {hist_end_date} for {bar_size} bars.")
                        date_intervals = myutil.get_date_intervals(data_start_date, hist_end_date, freq_in_days=date_range_freq[bar_size])

                else:
                    date_intervals = myutil.get_date_intervals(data_start_date, hist_end_date, freq_in_days=date_range_freq[bar_size])

                for iter_count, (_start, _end) in enumerate(date_intervals):

                    task = asyncio.create_task(
                                concurrent_calls(curr_contract,_start,_end,bar_size,True,iter_count,semaphore),
                                name=f"{curr_contract.localSymbol}_{iter_count}"
                            )
                    requested_contracts.append((task, curr_contract, _start, _end,iter_count,len(date_intervals), bar_size, data_logger))
                    if iter_count > 0:
                        total_required += 1
                        progress.update(request_task, total=total_required)

                    #progress.update(request_task, value=curr_contract.localSymbol, advance=1)
                    
                    # if len(requested_contracts) >= ibkr.max_calls["concurrent"]:
                    #     progress.update(contracts_task,value="waiting...")
                    #     progress.update(request_task,description="[red]Requesting")
                    #     #progress.console.print(f"\n[bold green]Waiting for {len(requested_contracts)} requests to complete...\n")

                    #     await wait_for_tasks(requested_contracts, progress, waiting_task, hist_end_date)
                    #     requested_contracts = []
                        
                    #     progress.update(contracts_task,value="requesting...")
                    #     progress.update(request_task,completed=0, value=curr_contract.localSymbol, description="[red]Requesting")
                    
                    await asyncio.sleep(0.05)
            curr_contracts_count += 1
            progress.update(overall_task, advance=1)
        
        progress.update(request_task, total=len(requested_contracts))

        if len(requested_contracts) > 0:
            progress.update(overall_task,value="waiting...")
            #progress.console.print(f"\n[bold green]Waiting for {len(requested_contracts)} tasks to complete...\n")
            await wait_for_tasks(requested_contracts, progress, waiting_task, hist_end_date)

            requested_contracts = []
        else:
            progress.remove_task(request_task)
            progress.remove_task(overall_task)
            progress.remove_task(waiting_task)
            #Delete 3 lines
            console.print(f"\n[bold green]All contracts up to date. No data to request for date range {hist_start_date} to {hist_end_date} for bar size {bar_sizes}.")
            
    end_time = time.perf_counter()

    #ROOT_LOGGER.setLevel(logging.INFO)  # Set the logging level to INFO
    ROOT_LOGGER.info(f"Processed {curr_contracts_count} contracts and stored {total_rows_received} rows in {(end_time - start_time)} seconds!\n")
    console.print(f"\n[bold green]Processed {curr_contracts_count} contracts and stored {total_rows_received} rows in {(end_time - start_time):.2f} seconds!\n")

async def main():
    IBKR_CLIENT_ID = 18
    symbol = 'NIFTY50'
    exchange = 'NSE'
    currency = "INR"
    pid = os.getpid()

    from src.data_management.ibkr_helper import IBKRHelper
    import time
    # Setup logging

    console= Console()

    loop = asyncio.get_event_loop()
    loop.slow_callback_duration = 1 #second
    console.print(f"[bold green]Starting historical bar data collector at process id = {pid}")  # Log the process ID for debugging

    ROOT_LOGGER.info(f"Starting historical bar data collector at process id = {pid}")  # Log the process ID for debugging

    ibkr = IBKRHelper(market_timezone_str=market_timezone_str, default_timeout=300)
    await ibkr.connect(port=IBKRHelper.TWS_PORT, client_id=IBKR_CLIENT_ID)

    console.print(f"[bold green]Getting options chain for symbol={symbol}, strike_range= +/- {strike_price_range}, expiry month = {expiry_months}.")  # Log the process ID for debugging
    ROOT_LOGGER.info(f"Getting options chain for symbol={symbol}, strike_range= +/- {strike_price_range}, expiry month = {expiry_months}.")
    start_time = time.perf_counter()
    contracts = await ibkr.get_option_chains(symbol, exchange, currency,strike_price_range,expiry_months) 
    end_time = time.perf_counter()

    console.print(f"[bold green]Fetched {len(contracts)} contracts in {end_time - start_time} seconds.")
    ROOT_LOGGER.info(f"Options chain fetched in {end_time - start_time} seconds.")

    await process_contracts(ibkr,contracts, DATA_DIR)

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
