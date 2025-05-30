# src/trading_engines/backtester.py
import pandas as pd
from datetime import datetime, date
import logging
import os
import sys
import math
import queue  # For data queue management
import uuid # For simulated execution IDs

# --- Add src directory to Python path ---
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path: sys.path.insert(0, project_root)
# --- End Path Addition ---

# Import necessary components
from .base_engine import BaseEngine # Import the corrected abstract base class
from src.trading_account.trading_account import TradingAccount # Use corrected TradingAccount
from src.strategies.base_strategy import BaseStrategy
from src.data_management.bar_aggregator import BarAggregator
from src.utils.helpers import get_value_from_dict
try: from ib_async import Contract, Execution, Fill, CommissionReport, Position
except ImportError:
    class Contract: pass; 
    class Execution: pass; 
    class Fill: pass; 
    class CommissionReport: pass;
    class Position: pass

class Backtester(BaseEngine):
    """
    Backtesting engine implementing the BaseEngine interface.
    Handles execution simulation directly via _place_order.
    Calls TradingAccount to process simulated fills (which also logs).

    No need multiple stratgies as in papertrader since this wont run concurrently.
    Can just reset the strategy state at the start of each backtest run.
    """
    def __init__(self, backtest_params:dict, strategy: BaseStrategy,trading_account: TradingAccount):
        """ Initializes the Backtester engine. """
        
        super().__init__( trading_account) # Initialize BaseEngine
        self.logger = logging.getLogger(self.__class__.__name__) # Specific logger

        if backtest_params is None:
            raise ValueError("Backtesting parameters must be provided.")
            self.logger.critical("Backtesting parameters must be provided.")
            return
        
        try:
            self.options_data_folder = get_value_from_dict(backtest_params, 'options_data_folder', None, self.logger, raise_error=True)
            self.index_data_file = get_value_from_dict(backtest_params, 'index_data_file', None, self.logger, raise_error=True)
            self.expiry_dates = get_value_from_dict(backtest_params, 'expiry_dates', [], self.logger, raise_error=True)
            self.strike_range = get_value_from_dict(backtest_params, 'strike_range', 200, self.logger, raise_error=True)
        except KeyError as e:
            self.logger.critical(f"Backtest parameters missing key: {e}. Exiting.")
            raise ValueError(f"Backtest parameters missing key: {e}")

        self.options_data_folder = self.options_data_folder.rstrip('/')  
        self.params = backtest_params.copy()
        self.strategy = strategy
        self.trading_account = trading_account

        self.slippage_pct = get_value_from_dict(self.params, "slippage_pct", 0.001, self.logger, raise_error=False)
        self.commission_per_share = get_value_from_dict(self.params, "commission_per_share", 0.005, self.logger, raise_error=False)
        self.load_previous_day_data = get_value_from_dict(self.params, 'load_previous_day_data', True, self.logger)

        #Need for BarAggregator
        barsize_str = self.strategy.barsize.replace("secs", "sec").replace("mins", "min").replace("hours", "hour").replace("days", "day")
        self.coarse_timedelta = pd.Timedelta(barsize_str)

        self.contract_id_counter = 0
        self.order_id_counter = 0
        self.data_queue = queue.Queue()
        self._running = False  


    def _get_contract_id(self):
        """ Generates a unique contract ID for each contract. """
        self.contract_id_counter += 1
        return self.contract_id_counter
    def _get_order_id(self):
        """ Generates a unique order ID for each order. """
        self.order_id_counter += 1
        return self.order_id_counter

    def find_contract(self, index_close: float, expiry: date, right: str) -> Contract:
        #Read folder and get all available contracts and then filter by date and right
        #folder structure = "/home/vairavan/Projects/AlgoTrading/src/data/historical/NIFTY/20250529/"

        #read dir to get all strike prices
        strike_prices = os.listdir(self.options_data_folder)
        if not strike_prices:
            self.logger.error(f"No strike prices found in {self.options_data_folder}.")
            raise ValueError("No strike prices found in options data folder.")
        
        self.logger.debug(f"Following strike prices found in {self.options_data_folder}: \n {strike_prices}")
        valid_strikes = []

        for strike in strike_prices:
            if right == "C" or right == "c":
                if float(strike) + self.strike_range >= index_close:
                    valid_strikes.append(strike)
            elif right == "P" or right == "p":        
                if float(strike) - self.strike_range <= index_close:
                    valid_strikes.append(strike)
                    
        if len(valid_strikes) > 0:
            strike = min(valid_strikes) if right.lower() == "c" else max(valid_strikes)
            contract = Contract(
                symbol="NIFTY",
                secType='OPT',
                currency='INR',
                exchange='NSE',
                lastTradeDateOrContractMonth=expiry.strftime('%Y%m%d'),
                strike=float(strike),
                right= right.upper(),  # Ensure right is uppercase
                localSymbol=f"NIFTY{expiry.strftime('%Y%m%d')}{strike}C"
            )
            contract.conId = self._get_contract_id()
            return contract
        
        return None  # No valid contract found for the given criteria

    def get_data(self, contract: Contract, date) -> pd.DataFrame:
        """
        Retrieves historical data for the given contract and date.
        This is a placeholder implementation that simulates data retrieval.

        Args:
            contract (Contract): The contract for which to retrieve data.
            date (date): The date for which to retrieve data.

        Returns:
            pd.DataFrame: Simulated historical data for the contract.
        """

        from src.utils.helpers import get_contract_data_filepath
        #go one level up from options_data_folder to get the top level folder
        top_level_folder = os.path.dirname(self.options_data_folder)

        self.logger.debug(f"Retrieving data for contract {contract.localSymbol} on date {date} from {top_level_folder}.")

        file_path = get_contract_data_filepath(contract, top_level_folder, "5 secs")
        df = pd.read_csv(file_path, index_col='Timestamp', parse_dates=True)
        df.dropna(inplace=True)  
        df = df[df.index.date == date]  
        return df



    def _start(self, contract:Contract, date:date):

    
        self._running = True
        self.logger.info(f"Paper Trading Engine started for contract {contract.localSymbol} at {date}.")
        
        while self._running:
            try:

                current_position = self.trading_account.get_position(contract)
                current_fine_bar = self.data_queue.get()

                timestamp = current_fine_bar.time
                current_price = current_fine_bar.close
                current_volume = current_fine_bar.volume

                #Aggregate the fine bar into a coarse bar before processing or skipping
                if current_price is not None and current_volume is not None:
                    new_bar_started, fine_update_info, completed_coarse_bar = self.aggregator.process_update(price=current_price, volume=current_volume, timestamp=timestamp)
                else:
                    self.logger.warning(f"Skipping aggregation for bar {timestamp}: missing 'close' or 'volume'.")
                    new_bar_started, fine_update_info, completed_coarse_bar = False, None, None

                # Check for EOD exit time and skip trading if needed
                if self.trading_account.is_eod_exit_time(timestamp):
                    self.logger.info(f"End of day exit time reached for {contract.localSymbol}.")
                    self._running = False
                    self.sell(contract, "sell signal", current_price, timestamp,current_price)
                    break

                # Check exit conditions
                if current_position and current_position.position > 0:
                    condn = self.strategy.check_pl_exit_conditions(current_position.avgCost,current_fine_bar)
                    if condn:
                        self.logger.info(f"Exit conditions met for {contract.localSymbol} at {timestamp}.")
                        self.sell(contract, condn, current_price, timestamp,current_price)


                # Check if the bar is complete and if we have a valid bar and then check for signals
                if new_bar_started and completed_coarse_bar is not None:
                    fill_price = completed_coarse_bar.close
                    self.logger.info(f"Checking signals for {contract.localSymbol} at bar {completed_coarse_bar.time} with close price {completed_coarse_bar.close}.")
                    if current_position and current_position.position > 0 :
                        if self.strategies[contract].check_sell_signal(completed_coarse_bar,contract.right):
                            self.sell(contract, "sell signal", current_price, timestamp,fill_price)
                    elif self.strategies[contract].check_buy_signal(completed_coarse_bar,contract.right):
                        self.buy(contract, "buy signal", current_price, timestamp,fill_price)             
                
            except Exception as e:
                self.logger.exception(f"Error during Backtesting loop: {e}")
                self.logger.error(f"Backtesting loop cancelled for contract {contract.localSymbol} for date: {date}.")
                self._running[contract] = False

                        
        self.logger.info(f"Paper Trading loop for {contract.localSymbol} stopped at {datetime.now(tz=self.ibkr.market_timezone)}.")


    def sell(self,contract: Contract, reason: str, current_price: float, current_timestamp: datetime, intended_fill_price: float=None):

        intended_fill_price = intended_fill_price if intended_fill_price is not None else current_price

        slippage = self.slippage_pct * intended_fill_price
        execution = Execution(
            execId=f"sim-{uuid.uuid4()}",
            time=current_timestamp,
            acctNumber=self.trading_account.account_id,
            exchange=getattr(contract, 'exchange', 'SMART'),
            side='SLD',
            shares=1,  # Default quantity, can be adjusted
            price=intended_fill_price - slippage,  # Apply slippage to intended fill price
            orderId=self._get_order_id(),  # Simulated order ID
            orderRef=reason
        )
        commission_report = CommissionReport(
            execId=execution.execId,
            commission=self.commission_per_share
        )
        fill = Fill (contract, execution, commission_report, current_timestamp)

        self.logger.debug(f"Placing simulated buy order for {contract.symbol} at {intended_fill_price:.2f} with reason: {reason}")
        self.trading_account.process_fill(fill)  

    def buy(self, contract: Contract, reason: str, current_price: float, current_timestamp: datetime, intended_fill_price: float=None):


        intended_fill_price = intended_fill_price if intended_fill_price is not None else current_price

        slippage = self.slippage_pct * intended_fill_price
        execution = Execution(
            execId=f"sim-{uuid.uuid4()}",
            time=current_timestamp,
            acctNumber=self.trading_account.account_id,
            exchange=getattr(contract, 'exchange', 'SMART'),
            side='BOT',
            shares=1,  # Default quantity, can be adjusted
            price=intended_fill_price + slippage,  # Apply slippage to intended fill price
            orderId=self._get_order_id(),  # Simulated order ID
            orderRef=reason
        )
        commission_report = CommissionReport(
            execId=execution.execId,
            commission=self.commission_per_share
        )
        fill = Fill (contract, execution, commission_report, current_timestamp)

        self.logger.debug(f"Placing simulated buy order for {contract.symbol} at {intended_fill_price:.2f} with reason: {reason}")
        self.trading_account.process_fill(fill)  
    


    def run(self):
        """
            Executes the main backtesting loop.
            Iterates through fine-grained historical data, simulates fills,
            checks exit conditions, and aggregates coarse bars.

            Class Flow Overview: 
            backtester -> 
                on all -> check exit conditions (PL/EOD)
                on new bar -> Strategy -> backtester (simulate fills) -> TradingAccount (process fills/logs).
            
        """
        try:
            start_time = datetime.now()

            #get index data
            import pandas as pd
            if not os.path.exists(self.params["index_data_file"]):
                self.logger.critical(f"Index data file {self.params['index_data_file']} does not exist. Exiting backtest.")
                raise FileNotFoundError(f"Index data file {self.params['index_data_file']} does not exist.")
            
            self.logger.info(f"Loading index data from {self.params['index_data_file']}...")
            self.index_df = pd.read_csv(self.params["index_data_file"], index_col='date', parse_dates=True)
            self.index_df.dropna(inplace=True) 
    
            if self.index_df.empty:
                self.logger.warning(f"No index data found in {self.params['index_data_file']}. Backtest may not function correctly.")
                return self.trading_account
            
            self.logger.info(f"Index data loaded from {self.params['index_data_file']} with {len(self.index_df)} records.")
            self.logger.info(f"Starting backtest for expiry dates: {self.expiry_dates}.")
            
            for expiry in self.expiry_dates:
                self.logger.info(f"Processing expiry date: {expiry}.")

                #7 days before expiry
                expiry_date = pd.to_datetime(expiry)
                start_date = expiry_date - pd.Timedelta(days=7); end_date = pd.to_datetime(datetime.now().date())
                all_dates = pd.date_range(start=start_date, end=end_date, freq='B') # Business days only
                self.logger.info(f"Processing expiry {expiry} for {len(all_dates)} dates:\n {all_dates}.")

                for date in all_dates:
                    #if date is monday, then yesterday is friday
                    yesterday = date - pd.Timedelta(days=1)
                    if date.weekday() == 0: 
                        yesterday = date - pd.Timedelta(days=3)  
                    
                    matching_rows = self.index_df[self.index_df.index.date == yesterday.date()]["close"].values
                    if len(matching_rows) == 0:
                        self.logger.warning(f"No index data found for {yesterday.date()}. Skipping date {date}.")
                        continue
                    index_close = matching_rows[0]

                    self.logger.info(f"Processing date {date} with index close {index_close}.")
                    ce_contract = self.find_contract(index_close,expiry_date, "c")
                    pe_contract = self.find_contract(index_close,expiry_date, "p")
                    self.logger.info(f"Found contracts: CE - {ce_contract.localSymbol if ce_contract else 'None'}, PE - {pe_contract.localSymbol if pe_contract else 'None'} for date {date}.")
                    
                    for contract in [ce_contract, pe_contract]:
                        #Fill data queue
                        self.aggregator = BarAggregator(timeframe_seconds=self.coarse_timedelta.total_seconds(), store_fine_data=False, store_coarse_data=True)
                        self.strategy._reset_data()  

                        if self.load_previous_day_data:
                            prev_day = date - pd.Timedelta(days=1)
                            fine_data = self.get_data(contract, prev_day) 
                            for fine_bar in fine_data.itertuples():
                                self.data_queue.put(fine_bar)
                        continue
                        fine_data = self.get_data(contract,date)
                        for fine_bar in fine_data.itertuples():
                            self.data_queue.put(fine_bar)

                        self._start(contract, date) 
                    
            end_time = datetime.now()
            self.logger.info(f"Backtest completed in {end_time - start_time}.")
            self.logger.info(f"Final account state: {self.trading_account.get_summary_stats()}")
            self._running = False
        except Exception as e:
            self.logger.exception(f"An error occurred during backtesting: {e}")
            self._running = False
            raise e
        
        return self.trading_account  # Return the final account state after backtest completion