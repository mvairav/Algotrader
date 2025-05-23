# src/trading_engines/backtester.py
import pandas as pd
from datetime import datetime, date
import logging
import os
import sys
import math
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
    """
    def __init__(self, backtest_params:dict, contract:Contract, fine_historical_data: pd.DataFrame, strategy: BaseStrategy,
                 trading_account: TradingAccount, trading_barsize: str,
                 commission_per_share: float = 0.0, slippage_pct: float = 0.0):
        """ Initializes the Backtester engine. """
        
        super().__init__(strategy, trading_account) # Initialize BaseEngine
        self.logger = logging.getLogger(self.__class__.__name__) # Specific logger
        if not isinstance(fine_historical_data, pd.DataFrame) or not isinstance(fine_historical_data.index, pd.DatetimeIndex): raise ValueError("Invalid fine_historical_data format.")
        if fine_historical_data.empty: raise ValueError("Cannot run backtest on empty historical data.")
        self.fine_data = fine_historical_data
        self.commission_per_share = float(commission_per_share)
        self.slippage_pct = abs(float(slippage_pct))
        self.coarse_timeframe_str = trading_barsize.replace("secs", "sec").replace("mins", "min").replace("hours", "hour").replace("days", "day")
        try:
             self.logger.info(f"Initializing BarAggregator with bar size: {trading_barsize}")
             coarse_timedelta = pd.Timedelta(self.coarse_timeframe_str)
             self.aggregator = BarAggregator(timeframe_seconds=coarse_timedelta.total_seconds(), store_fine_data=False, store_coarse_data=True)
        except Exception as e: 
            self.logger.exception(f"Failed to initialize BarAggregator: {e}"); raise

        self.backtest_params = backtest_params
        self.contract = contract

        start_date_str = get_value_from_dict(backtest_params, "start_date", self.fine_data.index.min().strftime('%Y-%m-%d') , self.logger)
        end_date_str = get_value_from_dict(backtest_params, "end_date", self.fine_data.index.max().strftime('%Y-%m-%d') , self.logger)

        try:
            self.start_date = pd.to_datetime(start_date_str).date()
            self.end_date = pd.to_datetime(end_date_str).date()
        except ValueError as e:
            self.logger.warning(f"Invalid date format in backtest parameters: {e}. Using min/max dates from data.")
            self.start_date = self.fine_data.index.min().date()
            self.end_date = self.fine_data.index.max().date()

        self.filtered_fine_data = self.fine_data[(self.fine_data.index.date >= self.start_date) & (self.fine_data.index.date <= self.end_date)]
        
        self.logger.info(f"Backtester initialized. bar size: {self.coarse_timeframe_str}. Commission: {self.commission_per_share:.4f}/share, Slippage: {self.slippage_pct*100:.4f}%")
        self.logger.info(f"Backtest period: {self.start_date} to {self.end_date}. Contract: {self.contract.symbol} ({self.contract.secType})")

    def _simulate_fill(self, order_dict):
        """ Internal helper to simulate a fill. Creates a Fill-like dictionary. """
        # (Code remains the same as backtester_engine_py_v5_base)
        try:
            
            action = get_value_from_dict(order_dict, 'action', None, self.logger,raise_error=True).upper()
            qty = get_value_from_dict(order_dict, 'quantity', None, self.logger,raise_error=True)
            contract_info = get_value_from_dict(order_dict, 'contract', None, self.logger,raise_error=True)
            order_params = get_value_from_dict(order_dict, 'params', None, self.logger,raise_error=True)

            intended_fill_price = get_value_from_dict(order_params, 'intended_fill_price', None, self.logger,raise_error=True)
            order_reason = get_value_from_dict(order_params, 'reason', None, self.logger,raise_error=True)
            timestamp = get_value_from_dict(order_params, 'timestamp', None, self.logger,raise_error=True)
        
            if action == 'BUY': 
                simulated_fill_price = intended_fill_price * (1 + self.slippage_pct)
            elif action == 'SELL': 
                simulated_fill_price = intended_fill_price * (1 - self.slippage_pct)
            else: 
                simulated_fill_price = intended_fill_price
            commission = self.commission_per_share * qty

            sim_exec_id = f"sim-{uuid.uuid4()}"
            sim_order_id = order_params.get('orderId', 0)
            execution_data = { 'execId': sim_exec_id, 'time': timestamp, 'acctNumber': self.trading_account.account_id,
                               'exchange': getattr(contract_info, 'exchange', 'SMART'), 'side': 'BOT' if action == 'BUY' else 'SLD',
                               'shares': float(qty), 'price': float(simulated_fill_price), 'permId': 0, 'clientId': 0,
                               'orderId': sim_order_id, 'liquidation': 0, 'cumQty': float(qty), 'avgPrice': float(simulated_fill_price),
                               'orderRef': order_reason, 'evRule': '', 'evMultiplier': 0.0, 'modelCode': '', 'lastLiquidity': 0,
                               'commission': float(commission) }
            commission_report_data = { 'execId': sim_exec_id, 'commission': float(commission),
                                       'currency': getattr(contract_info, 'currency', 'USD'),
                                       'realizedPNL': None, 'yield_': 0.0, 'yieldRedemptionDate': 0 }
            simulated_fill = { 'contract': contract_info, 'execution': execution_data,
                               'commissionReport': commission_report_data, 'time': timestamp }
            self.logger.debug(f"   Engine: Simulated Fill created for {action} {qty} {contract_info.symbol} @ {simulated_fill_price:.4f}")
            return simulated_fill
        except Exception as e: 
            self.logger.exception(f"Error during fill simulation: {e}. Order: {order_dict}")
            sys.exit(1)
            return None


    def _place_order(self, order_dict):
        """ Simulates the fill and calls TradingAccount.process_fill. """
        simulated_fill = self._simulate_fill(order_dict)
        if simulated_fill:
            # Process the fill using TradingAccount (which now also logs)
            self.trading_account.process_fill(fill=simulated_fill)
            # NO call to self._log_trade here anymore
            return simulated_fill # Return fill for confirmation if needed
        else:
            self.logger.error(f"Failed to simulate execution for order: {order_dict}")
            return None


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

        start_time = datetime.now()
        
        self.logger.info(f"Starting backtest run from {self.filtered_fine_data.index.date.min()} to {self.filtered_fine_data.index.date.max()}...")
        total_bars = len(self.filtered_fine_data)
        self.logger.info(f"Processing {total_bars} fine bars...")

        skip_trading_today = False; 
        current_processing_date = self.start_date;

        
        if self.filtered_fine_data.empty:
            self.logger.warning(f"No fine data available in the specified date range ({self.start_date} to {self.end_date}). Exiting backtest.")
            return self.trading_account
          
        for i, (timestamp, current_fine_bar) in enumerate(self.filtered_fine_data.iterrows()):

            if (i + 1) % (total_bars // 20 if total_bars >= 20 else 1) == 0:
                progress_pct = (i + 1) / total_bars * 100; current_value = self.trading_account.get_total_value()
                self.logger.info(f"  Progress: {i+1}/{total_bars} ({progress_pct:.0f}%) | {timestamp} | Value: ${current_value:,.2f}")

            if timestamp.date() > current_processing_date:
                self.logger.debug(f"--- Processing next day: {timestamp.date()} ---")
                current_processing_date = timestamp.date()
                skip_trading_today = False

            
            try:

                current_position = self.trading_account.get_position(self.contract)
                current_price = current_fine_bar.get('Close')
                current_volume = current_fine_bar.get('Volume')
                reason = None
                trade_cond = False

                #Aggregate the fine bar into a coarse bar
                if current_price is not None and current_volume is not None:
                    new_bar_started, fine_update_info, completed_coarse_bar = self.aggregator.process_update(price=current_price, volume=current_volume, timestamp=timestamp)
                else:
                    self.logger.warning(f"Skipping aggregation for bar {timestamp}: missing 'Close' or 'Volume'.")
                    new_bar_started, fine_update_info, completed_coarse_bar = False, None, None

                #Aggregate and then skip trading 
                if skip_trading_today:
                    self.logger.debug(f"Skipping trading for {timestamp} due to previous exit or EOD condition.")
                    continue

                #Check exit conditions first
                if current_position and current_position.position > 0 and not skip_trading_today:
                    if self.trading_account.is_eod_exit_time():
                        trade_cond = True
                        reason = "EOD"
                        fill_price = current_price
                    else:
                        trade_cond, reason, fill_price = self.strategy.check_pl_exit_conditions(current_fine_bar, current_position)
                        if not trade_cond: reason = None

                    if trade_cond and reason:
                        skip_trading_today = True
                        self.sell(self.contract, reason, current_price, timestamp,fill_price)
                        self.logger.info(f"   >>> Trading skipped for rest of day {current_processing_date} due to exit.")
                        continue
                    
                 
                if new_bar_started and completed_coarse_bar is not None:
                    fill_price = completed_coarse_bar.get('Close')
                    if current_position and current_position.position > 0 :
                        if self.strategy.check_sell_signal(completed_coarse_bar):
                            self.sell(self.contract, "sell signal", current_price, timestamp,fill_price)
                    elif self.strategy.check_buy_signal(completed_coarse_bar):
                        self.buy(self.contract, "buy signal", current_price, timestamp,fill_price)

            except Exception as e: 
                self.logger.exception(f"Error during backtest loop at fine bar index {i}, timestamp {timestamp}: {e}")
                raise

        end_time = datetime.now(); 
        duration = end_time - start_time
        self.logger.info(f"Backtest loop finished. Duration: {duration}")
        if not self.fine_data.empty:
             final_bar = self.fine_data.iloc[-1]
            #  final_market_data = {self.strategy.symbol: final_bar.to_dict()}
            #  final_value = self.trading_account.get_total_value(final_market_data)
             self.trading_account.update_equity_curve(final_bar.name)
        self._print_summary() # Call BaseEngine summary printer
        df = self.trading_account.get_all_trades_df()
        df.to_csv(os.path.join(project_root, 'backtest_trades.csv'), index=True)
        self.logger.info(f"Backtest completed. Total trades: {len(df)}, saved to {os.path.join(project_root, 'backtest_trades.csv')}. Final account value: ${self.trading_account.get_total_value():,.2f}")
        return self.trading_account


    def sell(self,contract: Contract, reason: str, current_price: float, current_timestamp: datetime, intended_fill_price: float=None):
        """
        Places a simulated sell order for the given contract with a reason and intended fill price.
        Args:
            contract (Contract): The contract to sell.
            reason (str): The reason for the sell order.
            current_price (float): The intended fill price for the order.
        """
        intended_fill_price = intended_fill_price if intended_fill_price is not None else current_price
            
        order_dict = {
            'action': 'SELL',
            'quantity': 1,  # Default quantity, can be adjusted
            'contract': contract,
            #Keep these separate to make it work with ib_Async order object
            'params': {'reason': reason, 'intended_fill_price': intended_fill_price, 'timestamp': current_timestamp}
        }
        self.logger.debug(f"Placing simulated sell order for {contract.symbol} at {intended_fill_price:.2f} with reason: {reason}")
    
        return self._place_order(order_dict)

    def buy(self, contract: Contract, reason: str, current_price: float, current_timestamp: datetime, intended_fill_price: float=None):
        """
        Places a simulated buy order for the given contract with a reason and intended fill price.
        Args:
            contract (Contract): The contract to buy.
            reason (str): The reason for the buy order.
            current_price (float): The intended fill price for the order.
        """
        intended_fill_price = intended_fill_price if intended_fill_price is not None else current_price
        order_dict = {
            'action': 'BUY',
            'quantity': 1,  # Default quantity, can be adjusted
            'contract': contract,
            #Keep these separate to make it work with ib_Async order object
            'params': {'reason': reason, 'intended_fill_price': intended_fill_price, 'timestamp': current_timestamp}
        }
        self.logger.debug(f"Placing simulated buy order for {contract.symbol} at {intended_fill_price:.2f} with reason: {reason}")
        return self._place_order(order_dict)