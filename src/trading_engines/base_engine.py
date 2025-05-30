# src/trading_engines/base_engine.py
from abc import ABC, abstractmethod
import logging
import pandas as pd
import time

# Assume TradingAccount and BaseStrategy are importable
from src.trading_account.trading_account import TradingAccount
from src.strategies.base_strategy import BaseStrategy
# Assume Fill object structure is known (from ib_async or simulation)
try:
    from ib_async import Fill
except ImportError:
    class Fill: pass # Dummy

class BaseEngine(ABC):
    """
    Abstract Base Class for trading engines (Backtester, PaperTrader, LiveTrader).
    Handles common setup (logger, dependencies), summary printing,
    and defines the core run loop and order placement interface.
    Execution logging is handled by the TradingAccount.
    """
    def __init__(self, trading_account: TradingAccount):
        """
        Initializes the Base Engine.

        Args:
            strategy: The instantiated strategy object.
            trading_account: The instantiated TradingAccount object.
        """
        self.logger = logging.getLogger(self.__class__.__name__) # Logger specific to the subclass instance

        if not isinstance(trading_account, TradingAccount):
             self.logger.critical("Engine initialized with invalid trading_account type.")
             raise TypeError("trading_account must be an instance of TradingAccount.")

        self.trading_account = trading_account
        # Removed self.execution_log - it lives in TradingAccount now

    @abstractmethod
    def run(self):
        """
        Main execution loop. Must be implemented by subclasses.
        Should handle data acquisition, strategy interaction, order placement,
        and portfolio updates via trading_account.process_fill.
        """
        pass

    def _place_order(self, order_dict, context_bar):
        """
        Handles the placement or simulation of an order triggered by the strategy.
        Implementations should call trading_account.process_fill with the
        resulting fill information (real or simulated).
        Must be implemented by subclasses.

        Args:
            order_dict (dict): The order details generated by the strategy.
            context_bar (dict or pd.Series): The bar data relevant to this order.

        Returns:
            object or None: Can return information about the placed/simulated order
                            (e.g., trade object, fill details) or None if failed.
        """
        pass

    # Removed _log_trade method - logging happens inside TradingAccount.process_fill

    def _print_summary(self):
        """
        Retrieves summary statistics from the TradingAccount and prints them.
        """
        # (Code remains the same as previous version base_engine_py_v1)
        self.logger.info("Retrieving and printing performance summary...")
        start_time = time.time()
        summary_stats = self.trading_account.get_summary_stats()
        end_time = time.time()
        self.logger.debug(f"get_summary_stats took {end_time - start_time:.4f} seconds.")

        self.logger.info("\n" + "="*30 + f" {self.__class__.__name__} Summary " + "="*30)
        if not summary_stats:
             self.logger.info("No summary statistics available (likely no trades).")
        else:
            for key, value in summary_stats.items():
                if isinstance(value, float):
                    if key in {"capital", "value", "pnl", "profit", "loss", "trade", "slippage"}: self.logger.info(f"{key.replace('_', ' ').title():<30}: ${value:,.2f}")
                    elif key in {"pct", "rate"} in key: self.logger.info(f"{key.replace('_', ' ').title():<30}: {value:.2f}%")
                    elif "factor" in key: self.logger.info(f"{key.replace('_', ' ').title():<30}: {value:.2f}")
                    elif "latency" in key: self.logger.info(f"{key.replace('_', ' ').title():<30}: {value:.2f} microseconds")
                    else: self.logger.info(f"{key.replace('_', ' ').title():<30}: {value:.4f}")
                else: self.logger.info(f"{key.replace('_', ' ').title():<30}: {value}")
        self.logger.info("="*(62 + len(self.__class__.__name__)) + "\n")

