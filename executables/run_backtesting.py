# executables/run_backtest.py
import argparse
import json
import pandas as pd
from datetime import datetime
import logging
import os
import sys
import time # For timing execution

# --- Add src directory to Python path ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path: sys.path.insert(0, project_root)
# --- End Path Addition ---

# Import necessary components
from src.utils.helpers import load_json_config, get_value_from_dict, get_contract_data_filepath
from src.utils.logging_setup import setup_general_logging
from src.trading_account.trading_account import TradingAccount # Use renamed class
from src.strategies.base_strategy import BaseStrategy
from src.strategies.registry import get_strategy_class # Import the registry function
from src.trading_engines.backtester import Backtester # Use revised Backtester
#from src.data_management.historical_provider import HistoricalProvider

# --- Root Logger. Configured in the main ---
ROOT_LOGGER = None

def run_backtest(args):
    """Sets up and runs the Backtester engine."""
    ROOT_LOGGER.info(f"--- Initializing Backtest ---")
    ROOT_LOGGER.info(f"Config file: {args.config}")

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
        backtest_params = get_value_from_dict(config, 'backtest_params', {}, ROOT_LOGGER, raise_error=True)
        data_params = get_value_from_dict(config, 'data_source', {}, ROOT_LOGGER, raise_error=True)
        data_path_template = get_value_from_dict(data_params, 'path_template', None, ROOT_LOGGER, raise_error=True)
        contract_params = get_value_from_dict(config, 'contract', {}, ROOT_LOGGER, raise_error=True)
    except KeyError as e: ROOT_LOGGER.critical(f"Config missing key: {e}. Exiting."); sys.exit(1)

    # 4. Instantiate Components
    ROOT_LOGGER.info("Instantiating components...")
    # Use TradingAccount (renamed PortfolioManager)
    initial_capital = get_value_from_dict(backtest_params, 'initial_capital', 100000.0, ROOT_LOGGER)
    trading_account = TradingAccount(trading_params,initial_capital=initial_capital)
    ROOT_LOGGER.info(f"Trading Account initialized with capital: {initial_capital}")
 
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
    strategy = StrategyClass(strategy_params, exit_params, trading_params, trading_account)

    # 5. Get FINE-GRAINED Historical Data
    ROOT_LOGGER.info("Loading FINE-GRAINED historical data...")
    from ib_async import Contract  # Import here to avoid circular imports
    try:
        contract = Contract(**contract_params)
        ROOT_LOGGER.info(f"Created contract object from parameters: {contract}")
    except Exception as e:
        ROOT_LOGGER.critical(f"Failed to create contract from parameters: {contract_params}. Error: {e}. Exiting.")
        sys.exit(f"Failed to create contract from parameters: {contract_params}. Error: {e}.")
    data_barsize = get_value_from_dict(data_params, 'barsize', '5min', ROOT_LOGGER)
    data_filetype = get_value_from_dict(data_params, 'filetype', 'csv', ROOT_LOGGER)
    filename=get_contract_data_filepath(contract, data_barsize, data_filetype)
    data_path = data_path_template.format(symbol=contract.symbol,expiry=contract.lastTradeDateOrContractMonth, strike=contract.strike,filename=filename) 
    if not os.path.isabs(data_path):
        data_path = os.path.join(project_root, data_path)
        ROOT_LOGGER.info(f"Data path resolved to: {data_path}")
    if not os.path.exists(data_path):
        ROOT_LOGGER.critical(f"Historical data file not found: {data_path}. Exiting.")
        sys.exit(f"Historical data file not found: {data_path}.")
    ROOT_LOGGER.info(f"Loading historical data from: {data_path}")

    # 6. Instantiate Backtester Engine
    datatime_col = get_value_from_dict(data_params, 'datetime_col', 'Timestamp', ROOT_LOGGER)
    historical_data = pd.read_csv(data_path, parse_dates=[datatime_col])
    if historical_data.empty:
        ROOT_LOGGER.critical(f"Historical data file is empty or could not be loaded: {data_path}. Exiting.")
        sys.exit(f"Historical data file is empty or could not be loaded: {data_path}.")
    historical_data.set_index(datatime_col, inplace=True)  
    historical_data.sort_index(inplace=True)  # Ensure data is sorted by datetime
    ROOT_LOGGER.info("Initializing Backtester engine...")
    backtester = Backtester(
        backtest_params,
        contract,
        fine_historical_data=historical_data,
        strategy=strategy,
        trading_account=trading_account,
        trading_barsize=strategy.barsize,
        # Pass simulation params directly to engine
        commission_per_share=get_value_from_dict(trading_params,'commission_per_share', 0,ROOT_LOGGER),
        slippage_pct=get_value_from_dict(trading_params,'slippage_pct', 0,ROOT_LOGGER)
    )
     
    # 7. Run the Backtest
    ROOT_LOGGER.info("Starting backtest run...")
    start_run_time = time.time()
    try:
        final_account_state = backtester.run() # run() now returns TradingAccount
        ROOT_LOGGER.info("Backtest run completed successfully.")
        # Summary is printed by backtester.run() via _print_summary()
    except Exception as e:
         ROOT_LOGGER.exception("An uncaught exception occurred during the backtest run. Exiting.")
         sys.exit(1)
    finally:
         end_run_time = time.time()
         ROOT_LOGGER.info(f"Total backtest execution time: {end_run_time - start_run_time:.2f} seconds.")

    ROOT_LOGGER.info("--- Backtesting Process Finished ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Backtester Engine")
    # Arguments remain the same...
    parser.add_argument('--config', required=True, help="Path to strategy configuration JSON file")
    parser.add_argument('--log-dir', default=f'{os.path.join(project_root, "logs")}', help="Folder to save log files")
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help="Set logging level")
    args = parser.parse_args()

    # Setup Logging Level based on CLI arg

    log_dir = args.log_dir if os.path.isabs(args.log_dir) else os.path.join(project_root, args.log_dir)
    log_filename = f"backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

    run_backtest(args)
    sys.exit(0) # Normal exit

