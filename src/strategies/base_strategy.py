# src/strategies/base_strategy.py
from abc import ABC, abstractmethod
import pandas as pd
import logging
import datetime
from ib_async import RealTimeBar  # Assuming this is the correct import for fine-grained bar data

# Assume ib_async Contract is available
from ib_async import Contract
from src.utils.helpers import get_value_from_dict

# Configure logging
log = logging.getLogger(__name__) # Use module-level logger for base class

class BaseStrategy(ABC):
    """
    Abstract Base Class for trading strategies.
    - Defines interface for signal generation (run) and exit condition checks.
    - Defines interface for exposing optimizable parameters.
    - Assumes Backtester/Engine handles calling methods appropriately.
    """

    def __init__(self, strategy_params, exit_params, trading_account):
        """
        Initializes the base strategy.

        Args:
            parameters (dict): Full configuration dictionary loaded from JSON/config.
            trading_account (TradingAccount): The TradingAccount instance for state access.
        """
        # Initialize the base strategy
        
        self.logger = logging.getLogger(self.__class__.__name__) # Instance-specific logger
        self.strategy_params = strategy_params 
        self.exit_params = exit_params 
        
        self.stop_loss_pct = get_value_from_dict(exit_params, 'stop_loss_percent', 0.03, self.logger)
        self.take_profit_pct = get_value_from_dict(exit_params, 'take_profit_percent', 0.06, self.logger)
        self.trailing_stop_percent = get_value_from_dict(exit_params, 'trailing_stop_percent', 0.02, self.logger)
        self.barsize = get_value_from_dict(strategy_params, 'barsize', '5mins', self.logger)

        self._min_bars_needed = 0

        self.trading_account = trading_account # Reference to the account state manager
        # self.symbol = parameters.get('symbol')
        # if not self.symbol:
        #     self.logger.error("Strategy initialized without a 'symbol' in parameters.")
        #     raise ValueError("Strategy requires a 'symbol' in parameters.")

        # Minimum number of coarse bars needed before the strategy can reliably calculate signals.
        # Concrete strategies MUST override this value in their __init__.
        
        # Default contract (can be refined/overridden by specific strategies)
        # Assumes simple stock for now
        #self._default_contract = Contract(symbol=self.symbol, secType='STK', currency='USD', exchange='SMART')

        #self.logger.info(f"Initialized BaseStrategy for symbol: {self.symbol}")

    @abstractmethod
    def check_buy_signal(self, coarse_bar, right):
        """
        Generate a buy signal based on coarsed-grained bar data.
        Called by the Engine on every coarse bar.

        Args:
            coarse_bar (pd.Series or dict): The completed coarse bar data (OHLCV).
            right (str): The side of the market to check for a signal ('P' or 'C') for put or call.

        Returns:
            bool: True if a buy signal is generated, otherwise False.
        """
        pass

    @abstractmethod
    def check_sell_signal(self, coarse_bar, right):
        """
        Generate a sell signal based on coarsed-grained bar data.
        Called by the Engine on every coarse bar.

        Args:
            coarse_bar (pd.Series or dict): The completed coarse bar data (OHLCV).
            right (str): The side of the market to check for a signal ('P' or 'C') for put or call.

        Returns:
            bool: True if a sell signal is generated, otherwise False.
        """
        pass

    @abstractmethod
    def _reset_data(self):
        """
        Reset the strategy state. Called by the Engine at the start of each backtest or live trading session.
        This allows the strategy to clear any internal state and prepare for a new trading session.
        """
        pass

    def check_pl_exit_conditions(self, entry_price, fine_bar:RealTimeBar):
        """
        Check Profit/Loss based exit conditions (SL/TP) using fine-grained bar data.
        Called by the Engine on every fine bar if a position is held.

        Args:
            entry_price (float): The price at which the position was entered.
            fine_bar (RealTimeBar): The fine-grained bar data containing high/low prices.
        Returns:
            str: Reason for exit if conditions are met, otherwise None.
        """
        reason = None

        if self.stop_loss_pct is None and self.take_profit_pct is None: return reason

        bar_high = fine_bar.high
        bar_low = fine_bar.low
        timestamp = fine_bar.time

        if bar_high is None or bar_low is None:
            self.logger.warning(f"Fine bar at {timestamp} missing high/low. Cannot check P/L exit.")
            return reason

        if self.stop_loss_pct is not None:
            stop_loss_price = entry_price * (1 - self.stop_loss_pct)
            if bar_low <= stop_loss_price:
                reason = f"Stop loss"

        if self.take_profit_pct is not None and reason is None:  
            take_profit_price = entry_price * (1 + self.take_profit_pct)
            if bar_high >= take_profit_price:
                reason = f"Take profit"

        if reason is not None:            
            self.logger.debug(f" Exit condition -> {reason} at {timestamp}")

        return reason

    def get_quantity(self, order_type='BUY'):
        """
        Get the order quantity for the current strategy.

        Args:
            order_type (str): 'BUY' or 'SELL'.

        Returns:
            int: The order quantity.
        """
        return abs(int(get_value_from_dict(self.strategy_params, 'order_quantity', 1, self.logger)))

    @staticmethod
    @abstractmethod
    def get_optimizable_params():
        """
        Returns a dictionary defining the parameters that can be optimized
        and their suggested ranges or choices for use with Optuna.

        Example Return:
            {
                'macd_fast': {'type': 'int', 'low': 5, 'high': 20},
                'macd_slow': {'type': 'int', 'low': 20, 'high': 60},
                'stop_loss_pct': {'type': 'float', 'low': 0.01, 'high': 0.10, 'step': 0.005},
                'rsi_period': {'type': 'categorical', 'choices': [7, 14, 21]}
            }
        """
        pass


    # --- Helper Methods ---
    def create_order_dict(self, action, quantity, order_type='MKT', limit_price=None, contract=None, **kwargs):
        """
        Helper to create a standardized order dictionary for the executor.
        Uses the strategy's default contract if none is provided.

        Args:
            action (str): 'BUY' or 'SELL'.
            quantity (int): Number of shares (always positive).
            order_type (str): 'MKT', 'LMT', etc.
            limit_price (float, optional): Limit price for LMT orders.
            contract (Contract, optional): Specific contract to trade. Defaults to self._default_contract.
            **kwargs: Additional parameters for the order (e.g., params={'reason': 'Signal'}).

        Returns:
            dict: Standardized order dictionary.
        """
        target_contract = contract if contract is not None else self._default_contract
        if not isinstance(target_contract, Contract):
             self.logger.error(f"Invalid contract provided to create_order_dict: {target_contract}")
             # Attempt conversion from dict if possible? For now, require Contract object.
             # if isinstance(target_contract, dict): target_contract = Contract(**target_contract) else: return None
             return None # Or raise error

        order = {
            'action': action.upper(),
            'contract': target_contract, # Use the Contract object
            'quantity': abs(int(quantity)),
            'order_type': order_type.upper(),
            'limit_price': limit_price,
            'params': kwargs # Store extra params like {'reason': 'MACD Cross'}
        }
        self.logger.debug(f"Strategy created order dict: {order}")
        return order


