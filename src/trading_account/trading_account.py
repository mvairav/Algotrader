# src/trading_account/trading_account.py
import pandas as pd
import logging
from collections import defaultdict
import datetime
import copy # Import copy for deep copying contract
from dataclasses import dataclass
import os 
import sys

# --- Add src directory to Python path ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path: sys.path.insert(0, project_root)
# --- End Path Addition ---

from src.utils.helpers import with_timezone

# Assume ib_async is installed and import necessary classes
from ib_async import Contract, Execution, Position, Fill, CommissionReport

# Configure logging (ensure this is configured globally elsewhere)

@dataclass
class ExecutionInfo:
    """
    Data class to hold execution information.
    """
    fill:Fill
    trigger_price:float = None
    trigger_time:datetime = None
    timezone:pd.Timestamp|datetime.datetime = None

    def __post_init__(self):
        if not isinstance(self.fill, Fill):
            raise ValueError("fill must be an instance of ib_async.Fill")
        if self.trigger_price is not None and not isinstance(self.trigger_price, (float, int)):
            raise ValueError("trigger_price must be a float or int")
        timestamp = None
        if self.timezone:
            timestamp = with_timezone(self.fill.time,self.timezone) 
        self.fill = Fill(
                contract=self.fill.contract,
                execution=self.fill.execution,
                commissionReport=self.fill.commissionReport,
                time=timestamp
            )
        if self.trigger_time:
            self.trigger_time = with_timezone(self.trigger_time,self.timezone)
        self.fill = copy.deepcopy(self.fill)  # Deep copy to avoid mutation
        #Check if time has timezone info


class TradingAccount:
    """
    Manages portfolio state (cash, positions) based on processed fills.
    Uses ib_async.Position internally to store position state.
    Uses the ib_async.Contract object directly as the key for the positions dictionary.
    Accepts an ib_async.Fill object (or compatible dict) as input for processing fills.
    Logs executions internally and calculates performance metrics.
    Provides method to retrieve all processed trades.

    Not thread-safe.
    """
    def __init__(self, trading_params, initial_capital=100000.0, account_id=None):
        """
        Initializes the Trading Account state manager.

        Args:
            initial_capital (float): Starting cash balance.
            account_id (str, optional): Identifier for the account.
        """
        self.logger = logging.getLogger(self.__class__.__name__) # Initialize logger for this instance

        if initial_capital <= 0:
             self.logger.warning(f"Initial capital is {initial_capital}. Setting to 100000.0.")
             initial_capital = 100000.0

        self.initial_capital = float(initial_capital)
        self.account_id = account_id if account_id else "SimAccount" # Default account ID

        # Set timezone for the instance
        from zoneinfo import ZoneInfo
        from src.utils.helpers import get_value_from_dict
        
        market_timezone_str = get_value_from_dict(trading_params, 'market_timezone', 'UTC', self.logger) 
        self.market_timezone = ZoneInfo(market_timezone_str)
        self.logger.info(f"Market timezone set to: {self.market_timezone}")

        try:
            self.quantity = int(get_value_from_dict(trading_params, 'order_quantity', 75, self.logger))
            self.market_open_time = datetime.datetime.strptime(get_value_from_dict(trading_params, 'market_open_time', '09:00:00', self.logger), '%H:%M:%S').time()
            self.market_close_time = datetime.datetime.strptime(get_value_from_dict(trading_params, 'market_close_time', '16:00:00', self.logger), '%H:%M:%S').time()
            self.eod_exit_time = datetime.datetime.strptime(get_value_from_dict(trading_params, 'eod_exit_time', '15:10:00', self.logger), '%H:%M:%S').time()
        except ValueError as e:
            self.logger.error(f"Invalid time format in trading parameters: {e}. Using defaults.")
            self.market_open_time = datetime.datetime.strptime( '09:00:00', '%H:%M:%S').time()
            self.market_close_time = datetime.datetime.strptime( '16:00:00', '%H:%M:%S').time()
            self.eod_exit_time = datetime.datetime.strptime('15:10:00', '%H:%M:%S').time()

        # --- Core State Variables ---
        self.cash = float(initial_capital)
        # Positions: Key = ib_async.Contract object, Value = ib_async.Position NamedTuple
        self.positions = {}
        # Execution Log: List of ExecutionInfo objects storing details of all fills
        self.execution_log:list[ExecutionInfo] = []
        # Closed Trades Log: List of dicts storing details of realized PNL events
        self.closed_trades_log = []



        self.logger.info(f"TradingAccount initialized with initial capital: ${self.initial_capital:,.2f}. Account ID: {self.account_id} and quantity: {self.quantity}")
        self.logger.info(f"Market time zone: {self.market_timezone}, Open Time: {self.market_open_time}, Close Time: {self.market_close_time}, EOD Exit Time: {self.eod_exit_time}")

    def get_quantity(self):
        """ Returns the order quantity. """
        return self.quantity
    
    
    # --- Position Access ---
    def get_position(self, contract: Contract):
        """ Retrieves the ib_async.Position object for a given contract. """
        if not isinstance(contract, Contract):
             # Attempt conversion for backtesting convenience if it's a dict
             if isinstance(contract, dict):
                  try:
                       contract = Contract(**contract) # Create Contract from dict
                  except Exception as e:
                       self.logger.error(f"Failed to convert dict to Contract in get_position: {e}")
                       return None
             else:
                  self.logger.error(f"get_position requires an ib_async.Contract object or dict. Received {type(contract)}")
                  return None
        # Use the contract object directly as the key
        # Note: Ensure Contract objects created from dicts are hashable/comparable if used as keys
        # It might be safer to always use a canonical key representation (like conId or tuple)
        # For now, assuming direct Contract object key usage works if objects are consistent.
        return self.positions.get(contract, None)

    def get_all_positions(self):
        """ Returns a dictionary of all currently open positions (Position objects). """
        return self.positions.copy() # Return shallow copy

    def get_current_market_time(self):
        """ Returns the current market time in the configured market timezone. """
        return datetime.datetime.now(self.market_timezone).time()
    
    def is_eod_exit_time(self):
        """ Checks if the current time is past eod trading time. """
        if self.market_open_time is None or self.market_close_time is None:
            self.logger.warning("Market open/close times not set. Cannot check EOD exit conditions.")
            return False

        now = datetime.datetime.now(self.market_timezone).time()
        if now >= self.eod_exit_time:
            self.logger.info(f"End of day for trading. Current time: {now}, Trading EOD time: {self.eod_exit_time}.")
            return True
        
        return False


    # --- Unified Fill Processing ---
    def process_fill(self, fill: Fill,trigger_price=None,trigger_time=None):
        """
        Processes a single trade fill, updating cash, positions, and logs.
        Accepts an ib_async.Fill object or a compatible dictionary/object
        containing an ib_async.Contract object.

        Args:
            fill (Fill or dict/object): Fill object containing contract, execution,
                                        and commissionReport details.
        """
        try:
            # --- Extract Data from Fill ---
            is_dict = isinstance(fill, dict)
            contract = getattr(fill, 'contract', None) if not is_dict else fill.get('contract')
            execution = getattr(fill, 'execution', None) if not is_dict else fill.get('execution')
            commission_report = getattr(fill, 'commissionReport', None) if not is_dict else fill.get('commissionReport')
            timestamp = pd.Timestamp(getattr(fill, 'time', None) if not is_dict else fill.get('time')) # Fill time
            if timestamp: timestamp = with_timezone(timestamp, self.market_timezone) 
            # --- Validate Extracted Objects ---
            # Ensure contract is a Contract object for consistent key usage
            if isinstance(contract, dict):
                 try:
                      contract = Contract(**contract) # Convert dict to Contract object
                 except Exception as e:
                      self.logger.error(f"Failed to convert contract dict to Contract object: {e}. Fill: {fill}")
                      return
            elif not isinstance(contract, Contract):
                 self.logger.error(f"Fill object missing a valid Contract object. Found type: {type(contract)}. Fill: {fill}")
                 return

            if not all([execution, timestamp]):
                self.logger.error(f"Fill object/dict missing execution or time. Fill: {fill}")
                return

            # Extract commission
            if commission_report is None:
                self.logger.warning("CommissionReport missing in Fill. Assuming commission from execution.")
                comm = float(getattr(execution, 'commission', 0.0) if not isinstance(execution, dict) else execution.get('commission', 0.0))
            else:
                comm = float(getattr(commission_report, 'commission', 0.0) if not isinstance(commission_report, dict) else commission_report.get('commission', 0.0))

            # --- Extract Execution Details ---
            is_exec_dict = isinstance(execution, dict)
            side_raw = getattr(execution, 'side', None) if not is_exec_dict else execution.get('side')
            qty = abs(float(getattr(execution, 'shares', 0.0) if not is_exec_dict else execution.get('shares', 0.0)))
            price = float(getattr(execution, 'price', 0.0) if not is_exec_dict else execution.get('price', 0.0))
            symbol = contract.symbol

            # --- Validate Execution Details ---
            if not all([timestamp, side_raw, qty > 0, price > 0]):
                 self.logger.warning(f"Invalid execution details. Time: {timestamp}, Side: {side_raw}, Qty: {qty}, Price: {price}. Skipping.")
                 return

            # --- Standardize Action ---
            if side_raw.upper() in ['BOT', 'BUY']: action = 'BUY'
            elif side_raw.upper() in ['SLD', 'SELL']: action = 'SELL'
            else: self.logger.warning(f"Invalid execution side/action: {side_raw}. Skipping."); return

            # Use the Contract object directly as the key
            key = contract
            self.logger.debug(f"Processing fill: {action} {qty} {symbol} @ {price:.2f} | Comm: {comm:.2f} | Time: {timestamp}")

            # --- Update Cash ---
            trade_value = qty * price
            self.cash -= comm
            if action == 'BUY': self.cash -= trade_value
            else: self.cash += trade_value # SELL

            # --- Update Positions (using ib_async.Position) ---
            current_pos = self.positions.get(key) # ib_async.Position object or None

            if action == 'BUY':
                if current_pos:
                    # Update existing Position
                    new_total_qty = current_pos.position + qty
                    new_total_cost = (current_pos.avgCost * current_pos.position) + trade_value
                    new_avg_cost = new_total_cost / new_total_qty if new_total_qty else 0
                    self.positions[key] = Position(account=self.account_id, contract=current_pos.contract, position=new_total_qty, avgCost=new_avg_cost)
                    self.logger.debug(f"Updated position {symbol}: Qty={new_total_qty}, AvgCost={new_avg_cost:.4f}")
                else:
                    # Create new Position
                    self.positions[key] = Position(account=self.account_id, contract=contract, position=qty, avgCost=price)
                    self.logger.debug(f"Opened new position {symbol}: Qty={qty}, AvgCost={price:.4f}")

            elif action == 'SELL':
                if current_pos:
                    # Close or reduce existing Position
                    sell_qty = min(qty, current_pos.position)
                    if sell_qty < qty: self.logger.warning(f"Attempted to sell {qty} {symbol}, only had {current_pos.position}. Selling {sell_qty}.")

                    # Log Realized PNL
                    avg_cost = current_pos.avgCost
                    realized_pnl_trade = (price - avg_cost) * sell_qty - comm # Net PnL for this fill
                    closed_trade_info = { 'timestamp': timestamp, 'symbol': symbol, 'action': 'SELL', 'quantity': sell_qty,
                                          'entry_price': avg_cost, 'exit_price': price, 'commission': comm,
                                          'pnl': realized_pnl_trade, 'contract': copy.deepcopy(contract) }
                    self.closed_trades_log.append(closed_trade_info)
                    self.logger.debug(f"Realized PNL for trade: {realized_pnl_trade:.2f}")

                    # Update or Remove Position
                    if sell_qty == current_pos.position:
                        self.logger.debug(f"Position closed for {symbol}.")
                        del self.positions[key]
                    else:
                        new_qty = current_pos.position - sell_qty
                        self.positions[key] = Position(account=self.account_id, contract=current_pos.contract, position=new_qty, avgCost=current_pos.avgCost)
                        self.logger.debug(f"Reduced position {symbol}: Remaining Qty={new_qty}")
                else:
                    self.logger.error(f"Attempted to sell {qty} {symbol} but no position held.")

            # Store a copy of the original fill object/dict
            #check timezone info on fill.time

            self.execution_log.append(ExecutionInfo(fill=fill, trigger_price=trigger_price,trigger_time=trigger_time,timezone=self.market_timezone))
            self.logger.debug(f"Fill logged by TradingAccount. Total executions: {len(self.execution_log)}")


        except Exception as e:
            self.logger.exception(f"Error processing fill: {e}")



    def get_total_value(self, current_market_data=None):
         """
         Calculates the total current value of the portfolio (cash + market value of positions).
         Uses provided market data for valuation if available, otherwise uses avgCost.

         Args:
            current_market_data (dict, optional): {Contract_object: price, ...} or {symbol: {'close': price}, ...}

         Returns:
             float: Total portfolio value.
         """
         position_market_value = 0.0
         for contract_key, pos in self.positions.items(): # pos is an ib_async.Position object
            symbol = pos.contract.symbol
            current_price = None
            if current_market_data:
                # Prioritize lookup by Contract object if possible
                if contract_key in current_market_data:
                    current_price = current_market_data[contract_key]
                elif symbol in current_market_data: # Fallback to symbol lookup
                     price_info = current_market_data[symbol]
                     current_price = price_info.get('close') if isinstance(price_info, dict) else price_info

            if current_price is not None:
                 try: position_market_value += pos.position * float(current_price)
                 except (ValueError, TypeError):
                      self.logger.warning(f"Invalid current price {current_price} for {symbol}. Using avgCost.")
                      position_market_value += pos.position * pos.avgCost # Fallback
            else:
                 self.logger.debug(f"No current market price for {symbol}. Using avgCost for value calculation.")
                 position_market_value += pos.position * pos.avgCost # Fallback

         total_value = self.cash + position_market_value
         return total_value

    def get_summary_stats(self):
        """ Calculates and returns basic performance statistics. """
        # (Code remains the same as previous version portfolio_manager_py_v9_ib_classes)
        self.logger.info("Calculating final summary statistics...")
        if not self.execution_log:
            self.logger.warning("No executions recorded. Cannot calculate summary statistics.")
            return {"initial_capital": self.initial_capital, "final_portfolio_value": self.initial_capital, "total_net_pnl": 0.0, "total_return_pct": 0.0}

        final_timestamp = getattr(self.execution_log[-1].fill, 'time', None) if not isinstance(self.execution_log[-1].fill, dict) else self.execution_log[-1].fill.get('time')

        final_value = self.get_total_value()
        total_return_pct = ((final_value / self.initial_capital) - 1) * 100 if self.initial_capital else 0
        total_net_pnl = final_value - self.initial_capital

        num_executions = len(self.execution_log)
        total_commission_paid = 0.0
        for exec_info in self.execution_log:
             fill_log_entry = exec_info.fill
             is_dict = isinstance(fill_log_entry, dict)
             comm_report = getattr(fill_log_entry, 'commissionReport', None) if not is_dict else fill_log_entry.get('commissionReport')
             exec_data = getattr(fill_log_entry, 'execution', None) if not is_dict else fill_log_entry.get('execution')
             if comm_report: total_commission_paid += float(getattr(comm_report, 'commission', 0.0) if not isinstance(comm_report, dict) else comm_report.get('commission', 0.0))
             elif exec_data: total_commission_paid += float(getattr(exec_data, 'commission', 0.0) if not isinstance(exec_data, dict) else exec_data.get('commission', 0.0))

        num_closed_trades = len(self.closed_trades_log)
        gross_profit = sum(t['pnl'] for t in self.closed_trades_log if t['pnl'] > 0)
        gross_loss = sum(t['pnl'] for t in self.closed_trades_log if t['pnl'] < 0)
        net_profit_closed = sum(t['pnl'] for t in self.closed_trades_log)

        num_winning_trades = sum(1 for t in self.closed_trades_log if t['pnl'] > 0)
        num_losing_trades = sum(1 for t in self.closed_trades_log if t['pnl'] < 0)
        win_rate = (num_winning_trades / num_closed_trades * 100) if num_closed_trades > 0 else 0
        avg_win = (gross_profit / num_winning_trades) if num_winning_trades > 0 else 0
        avg_loss = (gross_loss / num_losing_trades) if num_losing_trades > 0 else 0
        profit_factor = abs(gross_profit / gross_loss) if gross_loss != 0 else float('inf')

        avg_slippage = self.get_avg_slippage()
        avg_latency = self.get_avg_latency()

        stats = { "initial_capital": self.initial_capital, "final_portfolio_value": final_value,
                  "total_net_pnl": total_net_pnl, "total_return_pct": total_return_pct,
                  "total_executions": num_executions,
                  "total_closed_trades": num_closed_trades, "total_commission_paid": total_commission_paid,
                  "net_profit_closed_trades": net_profit_closed, "num_winning_trades": num_winning_trades,
                  "num_losing_trades": num_losing_trades, "win_rate_pct": win_rate,
                  "average_winning_trade": avg_win, "average_losing_trade": avg_loss,
                  "profit_factor": profit_factor, "gross_profit": gross_profit, "gross_loss": gross_loss,
                   "average_slippage": avg_slippage, "average_latency": avg_latency,}
        return stats

    def get_avg_latency(self):
        """
        Calculates the average latency of fills in milliseconds.
        Returns:
            float: Average latency in milliseconds.
        """
        if not self.execution_log:
            self.logger.warning("No execution log entries found. Cannot calculate average latency.")
            return 0.0

        latencies = []
        for exec_info in self.execution_log:
            if exec_info.trigger_time and exec_info.fill.time:
                latency = (exec_info.fill.time - exec_info.trigger_time).total_seconds() * 1000.0
                latencies.append(latency)
        
        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            self.logger.debug(f"Average latency calculated: {avg_latency:.2f} ms")
            return avg_latency
        else:
            self.logger.warning("No valid latencies found. Cannot calculate average latency.")
            return 0.0

    def get_total_commission(self):
        """
        Calculates the total commission paid from all fills.
        Returns:
            float: Total commission paid.
        """
        total_commission = 0.0
        for exec_info in self.execution_log:
            fill_log_entry = exec_info.fill
            is_dict = isinstance(fill_log_entry, dict)
            comm_report = getattr(fill_log_entry, 'commissionReport', None) if not is_dict else fill_log_entry.get('commissionReport')
            exec_data = getattr(fill_log_entry, 'execution', None) if not is_dict else fill_log_entry.get('execution')
            if comm_report: total_commission += float(getattr(comm_report, 'commission', 0.0) if not isinstance(comm_report, dict) else comm_report.get('commission', 0.0))
            elif exec_data: total_commission += float(getattr(exec_data, 'commission', 0.0) if not isinstance(exec_data, dict) else exec_data.get('commission', 0.0))
        
        self.logger.debug(f"Total commission calculated: {total_commission:.2f}")
        return total_commission
    
    def get_avg_slippage(self):
        """
        Calculates the average slippage from all fills.
        Returns:
            float: Average slippage in percentage.
        """
        if not self.execution_log:
            self.logger.warning("No execution log entries found. Cannot calculate average slippage.")
            return 0.0

        slippages = [
                        exec_info.fill.execution.price - exec_info.trigger_price
                        for exec_info in self.execution_log
                        if exec_info.trigger_price and exec_info.fill.execution.price
                    ]
        if slippages:
            avg_slippage = sum(slippages) / len(slippages)
            self.logger.debug(f"Average slippage calculated: {avg_slippage:.2f}%")
            return avg_slippage
        else:
            self.logger.warning("No valid slippages found. Cannot calculate average slippage.")
            return 0.0

    # --- NEW: Method to get trades as DataFrame ---
    def get_all_trades_df(self):
        """
        Processes the internal execution log and returns a DataFrame
        summarizing all trades (fills).

        Returns:
            pd.DataFrame: DataFrame containing trade details with columns like:
                          Timestamp, Action, Symbol, SecType, Right, Expiry, Strike,
                          Quantity, AvgPrice, Commission, PnL (from closed_trades_log), ExecId.
                          Returns empty DataFrame if no trades occurred.
        """
        if not self.execution_log:
            self.logger.info("Execution log is empty. Cannot generate trades DataFrame.")
            # Define columns for empty DataFrame
            cols = ['Timestamp', 'Action', 'Symbol', 'SecType', 'Right', 'Expiry', 'Strike',
                    'Quantity', 'AvgPrice', 'Commission', 'PnL', 'ExecId', 'OrderId', 'OrderRef']
            return pd.DataFrame(columns=cols).set_index('Timestamp')

        self.logger.info(f"Generating DataFrame from {len(self.execution_log)} execution log entries.")
        trade_data_list = []

        # Create a lookup for PnL from closed trades based on execution ID if possible
        # This is approximate as closed_trades_log might not have execId easily
        # A better approach might involve pairing executions later.
        # For now, we'll try a simple lookup based on timestamp and quantity for SELLs.
        closed_trade_pnl_lookup = {}
        for closed_trade in self.closed_trades_log:
            # Create a key (can be improved with execId if available on closed log)
            lookup_key = (pd.Timestamp(closed_trade['timestamp']), closed_trade['quantity'], closed_trade['exit_price'])
            closed_trade_pnl_lookup[lookup_key] = closed_trade['pnl']


        for exec_info in self.execution_log:
            fill_entry = exec_info.fill
            # Extract info robustly from Fill object or dict
            is_dict = isinstance(fill_entry, dict)
            contract = getattr(fill_entry, 'contract', None) if not is_dict else fill_entry.get('contract')
            execution = getattr(fill_entry, 'execution', None) if not is_dict else fill_entry.get('execution')
            commission_report = getattr(fill_entry, 'commissionReport', None) if not is_dict else fill_entry.get('commissionReport')
            timestamp = pd.Timestamp(getattr(fill_entry, 'time', None) if not is_dict else fill_entry.get('time'))

            if not contract or not execution: continue # Skip if essential parts missing

            # Ensure contract is a Contract object for attribute access
            if isinstance(contract, dict):
                 try: contract_obj = Contract(**contract)
                 except: contract_obj = None # Failed conversion
            else: contract_obj = contract
            if not contract_obj: continue # Skip if no valid contract object

            #Extract trigger price and time if available
            trigger_price = exec_info.trigger_price if exec_info.trigger_price else None
            trigger_time = exec_info.trigger_time if exec_info.trigger_time else None

            # Extract execution details
            is_exec_dict = isinstance(execution, dict)
            side_raw = getattr(execution, 'side', None) if not is_exec_dict else execution.get('side')
            action = 'BUY' if side_raw.upper() in ['BOT', 'BUY'] else ('SELL' if side_raw.upper() in ['SLD', 'SELL'] else 'UNKNOWN')
            qty = abs(float(getattr(execution, 'shares', 0.0) if not is_exec_dict else execution.get('shares', 0.0)))
            price = float(getattr(execution, 'price', 0.0) if not is_exec_dict else execution.get('price', 0.0))
            exec_id = getattr(execution, 'execId', None) if not is_exec_dict else execution.get('execId')
            order_id = getattr(execution, 'orderId', None) if not is_exec_dict else execution.get('orderId')
            order_ref = getattr(execution, 'orderRef', None) if not is_exec_dict else execution.get('orderRef')

            # Extract commission
            if commission_report:
                 comm = float(getattr(commission_report, 'commission', 0.0) if not isinstance(commission_report, dict) else commission_report.get('commission', 0.0))
            else: # Fallback to execution commission
                 comm = float(getattr(execution, 'commission', 0.0) if not is_exec_dict else execution.get('commission', 0.0))

            # Extract contract details
            symbol = getattr(contract_obj, 'localSymbol', None)
            secType = getattr(contract_obj, 'secType', None)
            right = getattr(contract_obj, 'right', None) if secType == 'OPT' else pd.NA
            expiry = getattr(contract_obj, 'lastTradeDateOrContractMonth', None) if secType in ['OPT', 'FUT', 'FOP'] else pd.NA
            strike = getattr(contract_obj, 'strike', None) if secType in ['OPT', 'FOP'] else pd.NA

            # Attempt to find PnL if this was a closing trade
            pnl = pd.NA
            if action == 'SELL':
                 lookup_key = (timestamp, qty, price)
                 pnl = closed_trade_pnl_lookup.get(lookup_key, pd.NA)


            trade_dict = {
                'Timestamp': timestamp,
                'Action': action,
                'Reason': order_ref,
                'OrderRef': order_ref,
                'TriggerPrice': trigger_price,
                'TriggerTime': trigger_time,
                'Symbol': symbol,
                'SecType': secType,
                'Right': right,
                'Expiry': expiry,
                'Strike': strike,
                'Quantity': qty,
                'AvgPrice': price,
                'Commission': comm,
                'PnL': pnl, # PnL from closed_trades_log if found for this exit
                'ExecId': exec_id,
                'OrderId': order_id
            }
            trade_data_list.append(trade_dict)

        if not trade_data_list:
             cols = ['Timestamp', 'Action','Reason', 'Symbol','OrderRef','TriggerPrice','TriggerTime', 'SecType', 'Right', 'Expiry', 'Strike',
                     'Quantity', 'AvgPrice', 'Commission', 'PnL', 'ExecId', 'OrderId']
             return pd.DataFrame(columns=cols).set_index('Timestamp')

        # Create DataFrame
        trades_df = pd.DataFrame(trade_data_list)
        # Convert relevant columns to appropriate types
        trades_df['Timestamp'] = pd.to_datetime(trades_df['Timestamp'])
        trades_df["TriggerTime"] = pd.to_datetime(trades_df["TriggerTime"], errors='coerce')

        trades_df = trades_df.set_index('Timestamp').sort_index()
        numeric_cols = ['Quantity', 'AvgPrice', 'Commission', 'PnL', 'Strike', 'TriggerPrice']
        for col in numeric_cols:
             if col in trades_df.columns:
                  trades_df[col] = pd.to_numeric(trades_df[col], errors='coerce')

        return trades_df

