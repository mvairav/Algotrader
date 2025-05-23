# executables/run_paper_trading.py
import argparse
import json
import asyncio
import logging
import os
import sys
from datetime import datetime
import pandas as pd # Needed for Timedelta

# --- Add src directory to Python path ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path: sys.path.insert(0, project_root)
# --- End Path Addition ---

# Import necessary components
from src.utils.helpers import load_json_config, get_value_from_dict, get_contract_data_filepath
from src.utils.logging_setup import setup_general_logging
from src.trading_account.trading_account import TradingAccount 
from src.strategies.base_strategy import BaseStrategy
from src.strategies.registry import get_strategy_class 
from src.trading_engines.paper_trader import PaperTrader 
from src.data_management.ibkr_helper import IBKRHelper 
from src.data_management.live_marketdata_collector import LiveMarketDataCollector 

# --- Root Logger. Configured in the main ---
ROOT_LOGGER = None

async def run_paper_trading(args,trade_filename):
    """Sets up and runs the PaperTrader engine."""
    ROOT_LOGGER.info(f"--- Initializing Paper Trading ---")
    ROOT_LOGGER.info(f"Config: {args.config}")

    # 1. Load Config
    config = load_json_config(args.config)
    if not config:
        ROOT_LOGGER.critical(f"Failed to load configuration from {args.config}. Exiting.")
        sys.exit(1)

    import json
    ROOT_LOGGER.info(f"Loaded configuration: \n{json.dumps(config, indent=4)}")  # Debug print to verify config loading

    # 3. Extract parameters
    try:
        trading_params = get_value_from_dict(config, 'trading', {}, ROOT_LOGGER, raise_error=True)
        strategy_params = get_value_from_dict(config, 'strategy_params', {}, ROOT_LOGGER, raise_error=True)
        exit_params = get_value_from_dict(config, 'exit_params', {}, ROOT_LOGGER, raise_error=True)
        papertrading_params = get_value_from_dict(config, 'papertrading_params', {}, ROOT_LOGGER, raise_error=True)
        ibkr_config = get_value_from_dict(config, 'ibkr', {}, ROOT_LOGGER, raise_error=True)
        ibkr_account_id = get_value_from_dict(ibkr_config, 'account_id', None, ROOT_LOGGER, raise_error=True)
    except KeyError as e: ROOT_LOGGER.critical(f"Config missing key: {e}. Exiting."); sys.exit(1)

    # 4. Instantiate Components
    ROOT_LOGGER.info("Instantiating components...")

    # IB Connection Helper
    available_funds = 0
    ibkr = IBKRHelper(market_timezone_str=ibkr_config['trading_timezone'])
    await ibkr.connect(ip_address=ibkr_config['host'], port=ibkr_config['port'], client_id=ibkr_config['client_id'],
                        force_new=True )
    
    if ibkr.is_connected():
        account_summary = await ibkr._ib_instance.accountSummaryAsync(ibkr_account_id)
        if account_summary is None or len(account_summary) == 0:
            ROOT_LOGGER.critical("No account summary data found. Exiting.")
            sys.exit(1)
        for a in account_summary: 
            if a.tag == "AvailableFunds": 
                available_funds = float(a.value)
                break
    else:
        ROOT_LOGGER.critical("Failed to connect to IBKR. Exiting.")
        sys.exit(1)

    ROOT_LOGGER.info(f"Connected to IBKR. Available funds: {available_funds}")

    # Find contract to trade
    #If thursday, get contracts for next week
    today = datetime.now(tz=ibkr.market_timezone)
    expiry_week = papertrading_params['expiry_weeks']
    expiry_week += 1 if today.weekday() == 3 else 0 # Thursday 

    contracts = await ibkr.get_option_chains_new(symbol=papertrading_params['index'], 
                               expiry_weeks=expiry_week, strike_price_range=papertrading_params['strike_range'])
    
    #Filter contracts if expiring today

    max_strike = max([c.strike for c in contracts])
    min_strike = min([c.strike for c in contracts])
    ce_contract = None
    pe_contract = None
    for c in contracts:
        if c.lastTradeDateOrContractMonth != today.strftime("%Y%m%d"):
            if c.right == "C" and c.strike == max_strike:
                ce_contract = c
            if c.right == "P" and c.strike == min_strike:
                pe_contract = c
    if not ce_contract or not pe_contract:
        ROOT_LOGGER.critical(f"Failed to find contracts for {papertrading_params['index']} with expiry week {expiry_week}. Exiting.")
        sys.exit(1)
    ROOT_LOGGER.info(f"Found contracts to trade: \nCE: {ce_contract}, \nPE: {pe_contract}")
    contracts = [ce_contract, pe_contract]

    # Use TradingAccount
    trading_account = TradingAccount(trading_params=trading_params,initial_capital=available_funds, account_id=ibkr_account_id)

    strategy_name = config.get('strategy_name')
    if not strategy_name: 
        ROOT_LOGGER.critical("Config missing 'strategy_name'. Exiting.")
        sys.exit("Config missing 'strategy_name'.")
    StrategyClass = get_strategy_class(strategy_name) # Use registry function
    if not StrategyClass: # Check if None was returned
        ROOT_LOGGER.critical(f"Strategy class '{strategy_name}' not found in registry or is invalid. Exiting.")
        sys.exit(f"Strategy class '{strategy_name}' not found in registry or is invalid.")
    ROOT_LOGGER.info(f"Using Strategy Class: {strategy_name}")
    # Strategy now only needs config and trading_account
    strategies= [
                 StrategyClass(strategy_params, exit_params, trading_params, trading_account)
                 for c in contracts
                ]

    # Live Data Handler (which uses BarAggregator internally)
    try:
        # Pass ib_connector instead of ib object, handler manages connection lifecycle?
        # Or connect here and pass ib object? Let's assume handler takes connector.
        live_data_handler = LiveMarketDataCollector(None, ibkr)
    except Exception as e: ROOT_LOGGER.exception(f"Failed to initialize LiveDataHandler: {e}"); sys.exit(1)


    # 5. Instantiate PaperTrader Engine
    ROOT_LOGGER.info("Initializing PaperTrader engine...")
    paper_trader = PaperTrader(papertrading_params=papertrading_params,  strategies=strategies, trading_account= trading_account,
                 ib_connector=ibkr, contracts=contracts)


    # 6. Run the Engine
    ROOT_LOGGER.info("Starting paper trading run...")
    try:
        await paper_trader.run(trade_filename) # Run the main async loop
        ROOT_LOGGER.info("Paper trading run finished normally.")
    except asyncio.CancelledError:
        ROOT_LOGGER.info("Paper trading run cancelled.")
    except Exception as e:
        ROOT_LOGGER.exception("An uncaught exception occurred during the paper trading run.")
    finally:
        ROOT_LOGGER.info("Ensuring disconnection...")
        del paper_trader # Ensure paper_trader is deleted to trigger cleanup
        ibkr.disconnect() # Disconnect from IBKR
        # Disconnect logic is inside paper_trader's run/stop methods

    ROOT_LOGGER.info("--- Paper Trading Process Finished ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Paper Trading Engine")
    # Connection Args
    parser.add_argument('--config', required=True, help="Path to configuration JSON files")
    parser.add_argument('--trade-dir', default=f'{os.path.join(project_root, "trades")}', help="Folder to save trade files")
    parser.add_argument('--log-dir', default=f'{os.path.join(project_root, "logs")}', help="Folder to save log files")
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help="Set logging level")
    args = parser.parse_args()

    # Setup Logging Level
    log_dir = args.log_dir if os.path.isabs(args.log_dir) else os.path.join(project_root, args.log_dir)
    file_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"papertrading_{file_timestamp}.log"

    trade_dir = args.trade_dir if os.path.isabs(args.trade_dir) else os.path.join(project_root, args.trade_dir)
    trade_filename = os.path.join(trade_dir,f"papertrading_{file_timestamp}.csv")
    setup_general_logging(
        log_dir=log_dir,
        level= getattr(logging, args.log_level.upper(), logging.INFO),
        log_to_console=True,
        log_to_file=True,
        log_filename=log_filename  # General log file for backtesting
    )
    ROOT_LOGGER = logging.getLogger(os.path.basename(__file__).replace('.py', ''))  # Use the script name as logger name
    ROOT_LOGGER.info(f"Started Backtesting Process with pid {os.getpid()} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    ROOT_LOGGER.info(f"arguments: {args}")

    # Run the main async function
    try:
        asyncio.run(run_paper_trading(args,trade_filename))
    except KeyboardInterrupt:
        ROOT_LOGGER.info("Paper Trading process interrupted by user (Ctrl+C).")
    except Exception as e:
        ROOT_LOGGER.exception("Unhandled error in main execution block.")
        sys.exit(1)
    finally:
        from time import sleep
        sleep(3)
    sys.exit(0) # Normal exit

