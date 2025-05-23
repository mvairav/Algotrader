# src/trading_engines/paper_trader.py
import asyncio
import logging
import pandas as pd
from datetime import datetime, date, time
import os 
import sys

# --- Add src directory to Python path ---
# (Ensure this path logic works for your structure)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path: sys.path.insert(0, project_root)
# --- End Path Addition ---

from ib_async.contract import Contract

from .base_engine import BaseEngine # Import the abstract base class
from src.trading_account.trading_account import TradingAccount
from src.strategies.base_strategy import BaseStrategy
from src.data_management.ibkr_helper import IBKRHelper 
from src.data_management.bar_aggregator import BarAggregator 
from src.utils.helpers import get_value_from_dict, BarData_to_RealTimeBar

class PaperTrader(BaseEngine):
    """
    Paper Trading engine implementing the BaseEngine interface.
    - Connects to IBKR paper account.
    - Handles live data aggregation via LiveDataHandler.
    - Calls strategy for decisions.
    - Places paper orders via _place_order.
    - Updates TradingAccount based on broker execution fills via callbacks.
    """

    def __init__(self, papertrading_params: dict,  strategies: list[BaseStrategy], trading_account: TradingAccount,
                 ib_connector: IBKRHelper,  contracts: list[Contract] ):
        """
        Initializes the Paper Trading engine.

        Args:
            strategy: Instantiated strategy object.
            trading_account: Instantiated TradingAccount object.
            ib_connector: Helper class instance for managing IB connection.
            live_data_handler: Helper class instance for managing live data and aggregation.
        """
        
        super().__init__( trading_account) # Initialize BaseEngine
         # Specific logger
        self.logger = logging.getLogger(self.__class__.__name__)

        if papertrading_params is None or contracts is None or ib_connector is None:
            raise ValueError("Papertrading parameters, contracts, and IB connector must be provided.")
            self.logger.critical("Papertrading parameters, contracts, and IB connector must be provided.")
            return
        
        self.load_previous_day_data = get_value_from_dict(papertrading_params, 'load_previous_day_data', True, self.logger)

        self.params = papertrading_params
        self.contracts = contracts.copy()
        self.ibkr = ib_connector
        self.trading_account = trading_account


        self.strategies = {}
        self.aggregator = {}
        self.contract_lock = {}
        for i,c in enumerate(self.contracts):
            try:
                self.strategies[c] = strategies[i]
                barsize_str = self.strategies[c].barsize.replace("secs", "sec").replace("mins", "min").replace("hours", "hour").replace("days", "day").replace(" ","")
                self.logger.info(f"Initializing BarAggregator with bar size: {barsize_str}")
                coarse_timedelta = pd.Timedelta(barsize_str)
                self.aggregator[c] = BarAggregator(timeframe_seconds=coarse_timedelta.total_seconds(), store_fine_data=False, store_coarse_data=True)

                self.contract_lock[c] = asyncio.Lock()
            except Exception as e: 
                self.logger.exception(f"Failed to initialize BarAggregator: {e}")
                raise 
        
        # Initialize data queue and callbacks for live data
        import functools
        self.data_queue = { c: asyncio.Queue() for c in self.contracts  }
        self.live_data_subscription = { c: None for c in self.contracts }
        self.callbacks = { c: functools.partial(self._on_bar_data, contract=c) for c in self.contracts }
        self._running = { c: False for c in self.contracts }
        
        # Define bookmarking for trades
        self._current_processing_date = None
        self._skip_trading_today = False
        self._active_exit_orders = {} 

        for c in self.contracts:
            self.logger.info(f"PaperTrader initialized for "
                         f"Strategy: {type(self.strategies[c]).__name__}, Account: {self.trading_account.account_id} "
                         f"Contracts: {c.localSymbol}")

    def __del__(self):
        """Destructor to ensure cleanup."""
        self.logger.info("PaperTrader instance is being deleted.")

    async def _connect_and_setup(self):
        """Connects to IB, sets up handlers, subscribes to data."""

        # Get the IB instance from the helper, dont like it but using it for now
        if not await self.ibkr.connect():
            self.logger.critical("Failed to connect to IBKR.")
            return False

        if self.load_previous_day_data:
            self.logger.info("Loading previous day data...")
            # Load data starting from previous day data till now to kick start the engine immediately
            today = datetime.now(tz=self.ibkr.market_timezone).replace(hour=0,minute=0,second=0,microsecond=0)
            if today.weekday() == 0: # Monday
                prev_day = today - pd.Timedelta(days=3)
            else:
                prev_day = today - pd.Timedelta(days=1)
            now = datetime.now(tz=self.ibkr.market_timezone) 

            for c in self.contracts:
                self.logger.info(f"Loading historical data for {c.localSymbol} from {prev_day} to {now}.")
                bars = await self.ibkr.get_historical_bars(contract=c,start_date=prev_day,end_date=now,bar_size=self.strategies[c].barsize, return_bars=True)
                if bars is not None:
                    self.logger.info(f"Loading {len(bars)} historical bars for {c.localSymbol}.")
                    for bar in bars:
                        self.strategies[c]._update_data_and_indicators(BarData_to_RealTimeBar(bar))
                    self.logger.info(f"Loaded {len(bars)} historical bars for {c.localSymbol}.")
                else:
                    self.logger.warning(f"No historical data found for {c.localSymbol} from {prev_day} to {now}.")
                    break

        # Set up the live data handler
        for c in self.contracts:
            self.logger.info(f"Setting up live data handler for {c.localSymbol}.")
            self.live_data_subscription[c] = await self.ibkr.subscribe_live_bars( c, self.callbacks[c], only_last_row=True )

        self.logger.info("PaperTrader setup complete.")
        return True

    def _on_bar_data(self, current_bar, contract):
        """Callback handler for bar data from IB."""
        
        if contract not in self.data_queue:
            self.logger.warning(f"Received data for unknown contract: {contract.localSymbol}")
            return
        total_execpts = 0
        not_stored = True
        while not_stored:
            try:
                #weirdly the data received is on UTC
                current_bar.time = current_bar.time.astimezone(self.ibkr.market_timezone)
                self.data_queue[contract].put_nowait(current_bar)
                not_stored = False
            except asyncio.QueueFull:
                total_execpts += 1
                if total_execpts > 5:
                    self.logger.warning(f"Data queue for {contract.localSymbol} is full. Skipping data.")
                    break
                self.logger.warning(f"Data queue for {contract.localSymbol} is full. Retry: {total_execpts}.")
                time.sleep(0.5)



    async def _on_exit_order(self, trade):
        """Callback for exit orders."""
        contract = trade.contract
        async with self.contract_lock[contract]:
            if trade.contract not in self._active_exit_orders:
                self.logger.info(f"Exit order callback received for a sold contract: {trade.contract.localSymbol}")
                return
            # This will be called when the exit order is filled
            order = trade.order
            fill = trade.fills[0] if trade.fills else None

            self.logger.info(f"Exit order filled: order-id:  {order.orderId}, reason: {fill.execution.orderRef} for {trade.contract.localSymbol} at {fill.execution.price}")
            self.trading_account.process_fill(fill)
            #self._log_trade(fill)
            self._active_exit_orders.pop(trade.contract,None)
           
        

    async def _start(self, contract:Contract):

        trade_start_time = datetime.now(tz=self.ibkr.market_timezone)
        last_execption_time = datetime.now(tz=self.ibkr.market_timezone)
        last_execption_count = 0 
    
        self._running[contract] = True
        self.logger.info(f"Paper Trading Engine started for contract {contract.localSymbol} at {trade_start_time}.")
        
        test_buy = True
        while self._running[contract]:
            try:

                current_position = self.trading_account.get_position(contract)
                current_fine_bar = await self.data_queue[contract].get()

                timestamp = current_fine_bar.time
                current_price = current_fine_bar.close
                current_volume = current_fine_bar.volume

                #Aggregate the fine bar into a coarse bar before processing or skipping
                if current_price is not None and current_volume is not None:
                    new_bar_started, fine_update_info, completed_coarse_bar = self.aggregator[contract].process_update(price=current_price, volume=current_volume, timestamp=timestamp)
                else:
                    self.logger.warning(f"Skipping aggregation for bar {timestamp}: missing 'close' or 'volume'.")
                    new_bar_started, fine_update_info, completed_coarse_bar = False, None, None

                # Check for EOD exit time and skip trading if needed
                if self.trading_account.is_eod_exit_time():
                    self.logger.info(f"End of day exit time reached for {contract.localSymbol}.")
                    self._running[contract] = False
                    break

                if self._skip_trading_today:
                    self.logger.info(f"Skipping trading today for {contract.localSymbol}.")
                    self._running[contract] = False
                    break

                # Check if the bar is complete and if we have a valid bar and then check for signals
                if new_bar_started and completed_coarse_bar is not None:
                    fill_price = completed_coarse_bar.close
                    self.logger.info(f"Checking signals for {contract.localSymbol} at bar {completed_coarse_bar.time} with close price {completed_coarse_bar.close}.")
                    if current_position and current_position.position > 0 :
                        if self.strategies[contract].check_sell_signal(completed_coarse_bar,contract.right):
                            await self.sell(contract, "sell signal", current_price, timestamp,fill_price)
                    elif self.strategies[contract].check_buy_signal(completed_coarse_bar,contract.right):
                        await self.buy(contract, "buy signal", current_price, timestamp,fill_price)

            except asyncio.CancelledError:
                self.logger.info("Paper Trading loop cancelled.")
                self._running[contract] = False
            except Exception as e:
                last_execption_count += 1
                diff = datetime.now(tz=self.ibkr.market_timezone) - last_execption_time 
                if diff.total_seconds() < 60:
                    if last_execption_count > 5:
                        self.logger.critical(f"Too many exceptions in a short time. Exiting loop for {contract.localSymbol}.")
                        self._running[contract] = False
                        break
                else:
                    last_execption_count = 0
                last_execption_time = datetime.now(tz=self.ibkr.market_timezone)
                self.logger.exception(f"Error during Paper Trading loop: {e}")
                self.logger.info(f"Retrying in 5 seconds...")
                await asyncio.sleep(5) 
                
        await asyncio.sleep(0.1)
        await self._stop(contract)

        self.logger.info(f"Paper Trading loop for {contract.localSymbol} stopped at {datetime.now(tz=self.ibkr.market_timezone)}.")


    async def _stop(self,contract):
        """Stops the engine loop."""
        self.logger.info("Stopping Paper Trading Engine...")
        if contract in self._running: self._running[contract] = False
        # Unsubscribe from live data
        if contract in self.live_data_subscription and self.live_data_subscription[contract]:
            self.ibkr.unsubscribe_live_bars(self.live_data_subscription[contract])
            self.live_data_subscription.pop(contract,None)
            self.logger.info(f"Unsubscribed from live data for {contract.localSymbol}.")
        if contract in self._active_exit_orders:
            self.logger.info(f"Selling active exit orders for {contract.localSymbol}.")
            await self.sell(contract, "code completed", 0, 0,0)



    async def buy(self, contract, reason, current_price, timestamp, fill_price):

        async with self.contract_lock[contract]:
            self.logger.info(f"Placing buy order for {contract.localSymbol} using bar from {timestamp} at {current_price:.2f} with reason: {reason}")

            quantity, exit_cond = self.trading_account.get_quantity(), self.strategies[contract].exit_params
            self._active_exit_orders[contract]=None
            trades, sell_order = await self.ibkr.place_custombracket_order(
                contract=contract,
                quantity=quantity,
                take_profit_percent=exit_cond['take_profit_percent'],
                stop_loss_percent=exit_cond['stop_loss_percent'],
                trailing_stop_percent=exit_cond['trailing_stop_percent'],
                exit_callback_func=self._on_exit_order,
                order_ref=reason
            )
            
            if sell_order:
                self.trading_account.process_fill(trades[0].fills[0],trigger_price=current_price,trigger_time=timestamp)
                self._active_exit_orders[contract] = sell_order
                #self._log_trade(trades[0].fills[0])
                slippage = trades[0].fills[0].execution.price - current_price
                latency = (trades[0].fills[0].execution.time - timestamp).total_seconds() 
                self.logger.info(f"Buy order and exit orders placed for {contract.localSymbol} with order ID: {trades[0].order.orderId} with slippage: {slippage:.2f} and latency: {latency} secs")
            elif trades:
                self.trading_account.process_fill(trades[0].fills[0], trigger_price=current_price,trigger_time=timestamp)
                self.logger.info(f"Only Buy order placed for {contract.localSymbol} with order ID: {trades[0].order.orderId}")
            else:
                del self._active_exit_orders[contract]
                self.logger.warning(f"Failed to place buy order for {contract.localSymbol}.")
            
        

    async def sell(self, contract, reason, current_price, timestamp, fill_price):
        async with self.contract_lock[contract]:
            if contract not in self._active_exit_orders:
                self.logger.info(f"Sell order does not exists for {contract.localSymbol}. Skipping sell.")
                return
            
            pos = self.trading_account.get_position(contract)
            if pos is None or pos.position <= 0:
                self.logger.warning(f"No position (in offline log) to sell for {contract.localSymbol}. But trying to sell anyway.")
            if contract in self._active_exit_orders and self._active_exit_orders[contract] is not None:
                self.logger.info(f"Placing sell order for {contract.localSymbol} at {current_price:.2f} with reason: {reason}")
                trade = await self.ibkr.place_market_order(contract=contract, ordertype="SELL", order_ref=reason,
                                                    quantity=self._active_exit_orders[contract].totalQuantity,
                                                    group_name=self._active_exit_orders[contract].ocaGroup 
                                                    ) 
                if not trade:
                    self.logger.warning(f"Failed to place sell order for {contract.localSymbol}.")
                    return
                slippage = trade.fills[0].execution.price - current_price
                if timestamp is None or timestamp is not datetime:
                    timestamp = datetime.now(tz=self.ibkr.market_timezone)
                latency = (trade.fills[0].execution.time - timestamp).total_seconds() 
                self.logger.info(f"Sell order placed for {contract.localSymbol} with order ID: {trade.order.orderId} with slippage: {slippage:.2f} and latency: {latency} secs")
                self._active_exit_orders.pop(contract,None)
                self.trading_account.process_fill(trade.fills[0],trigger_price=current_price,trigger_time=timestamp)
            else:
                self.logger.info(f"Sell order does not exist for {contract.localSymbol}. Skipping sell now.")
                return
            #self._log_trade(trade[0].fills[0])


        

    # --- Main Run Loop ---
    async def run(self,trade_filename):

        """Main execution loop for paper trading."""
        if not await self._connect_and_setup():
            return # Exit if connection failed

        runs = [None for _ in range(len(self.contracts))]
        for i,c in enumerate(self.contracts):
            self.logger.info(f"Starting paper trading for {c.localSymbol}...")
            # Start the data handler and trading loop for each contract
            runs[i] = self._start(c)

        # Run all tasks concurrently
        try:
            await asyncio.gather(*runs)
        except asyncio.CancelledError:
            self.logger.info("Paper Trading Engine run loop cancelled.")
        except Exception as e:
            self.logger.exception(f"Unexpected error in Paper Trading Engine: {e}")
        finally:
            await asyncio.sleep(3)
            # Ensure all tasks are stopped
            for i in range(len(self.contracts)):
                self._running[self.contracts[i].localSymbol] = False
                self.logger.info(f"Stopping paper trading for {self.contracts[i].localSymbol}...")

        self.logger.info("Paper Trading Engine run loop finished.")

        # Seprate out each statement in try except block to enssure one error 
        # does not stop stats and trades from being saved
        try:
            for c in self.contracts:
                await self._stop(c)
        except Exception as e:
            self.logger.exception(f"Error during Paper Trading Engine stop: {e}")

        try:
            self._print_summary() # Print summary at the end
        except Exception as e:
            self.logger.exception(f"Error during summary print: {e}")

        try:
            df = self.trading_account.get_all_trades_df()
            df.to_csv(trade_filename, index=True)
            self.logger.info(f"\n === Trades === \n {df} \n === Trades === \n")
        except Exception as e:
            self.logger.exception(f"Error saving trades to file: {e}")


        self.logger.info("Paper Trading Engine stopped.")

