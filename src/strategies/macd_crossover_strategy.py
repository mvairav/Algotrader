# src/strategies/macd_strategy.py
from .base_strategy import BaseStrategy 
from src.utils.helpers import get_value_from_dict
import pandas as pd
import numpy as np
from collections import deque
import logging
import datetime

# Assume ib_async Contract is available
from ib_async import Contract, Position, RealTimeBar

# Attempt to import talib
try: import talib
except ImportError:
    logging.error("TA-Lib not found. Please install TA-Lib: pip install TA-Lib")
    talib = None

class MACDCrossOverStrategy(BaseStrategy):
    """
    Simple MACD Crossover Strategy for Backtesting.
    - Implements run() for MACD signals on coarse bars.
    - Implements check_pl_exit_conditions() for SL/TP on fine bars.
    - Implements check_eod_exit_conditions() for EOD exit on fine bars.
    - Stores indicator history based on coarse bars.
    - Provides optimizable parameter definitions.
    """
    def __init__(self, strategy_params, exit_params, trading_params, trading_account, max_indicator_history=20):
        """ Initializes the MACD strategy. """
        # Initialize BaseStrategy (stores params, trading_account, SL/TP %, EOD time etc.)
        super().__init__(strategy_params, exit_params, trading_account)
        self.logger = logging.getLogger(self.__class__.__name__) # Instance-specific logger

        if talib is None: raise ImportError("TA-Lib required but not imported.")

        # Extract MACD indicator parameters from strategy_params section
        # Use defaults defined here if not present in config
        self.macd_fast = get_value_from_dict(strategy_params, 'macd_fast', 12, self.logger)
        self.macd_slow = get_value_from_dict(strategy_params, 'macd_slow', 26, self.logger)
        self.macd_signal = get_value_from_dict(strategy_params, 'macd_signal', 9, self.logger)
        
        # Extract trading parameters (order quantity)
        self.order_quantity = get_value_from_dict(trading_params, 'order_quantity', 1, self.logger)

        # --- Data Window & Indicator State (for COARSE bars) ---
        # Minimum COARSE bars needed for TA-Lib MACD calculation
        self._min_bars_needed = self.macd_slow + self.macd_signal + 5
        self._min_indicators_needed = 2

        self.max_window_size = self._min_bars_needed + 20
        self._close_data = deque(maxlen=self.max_window_size)

        # --- Indicator History Storage (for COARSE bars) ---
        self.max_indicator_history = max(max_indicator_history,self._min_indicators_needed) if max_indicator_history else self._min_indicators_needed
        self.indicators = {
             'timestamp': deque(maxlen=self.max_indicator_history) if self.max_indicator_history else [],
             'macd': deque(maxlen=self.max_indicator_history) if self.max_indicator_history else [],
             'signal': deque(maxlen=self.max_indicator_history) if self.max_indicator_history else [],
             'hist': deque(maxlen=self.max_indicator_history) if self.max_indicator_history else []
         }

        self.logger.info(f"Initialized MACDStrategy: Params=({self.macd_fast},{self.macd_slow},{self.macd_signal}), "
                         f"min bars needed={self._min_bars_needed}")


    def _update_data_and_indicators(self, coarse_bar:RealTimeBar):
        """ Updates COARSE bar data window and calculates/stores indicators. """

        current_close = coarse_bar.close
        timestamp = coarse_bar.time

        if current_close is None :
            self.logger.warning(f"Strategy: COARSE bar missing close price {current_close}. Cannot update data.")
            return

        self._close_data.append(current_close)
        self.logger.debug(f"Strategy: COARSE bar data updated at {timestamp} - total data points: {len(self._close_data)}")

        if len(self._close_data) >= self._min_bars_needed:
            #Compute for all available data again. Dont need it, but doing it for now
            close_prices = np.array(list(self._close_data), dtype=float) 
            try:
                macd_line, signal_line, hist = talib.MACD(close_prices, self.macd_fast, self.macd_slow, self.macd_signal)
                if pd.notna(macd_line[-1]) and pd.notna(signal_line[-1]) and pd.notna(hist[-1]):
                    self.indicators["timestamp"].append(timestamp)
                    self.indicators["macd"].append(macd_line[-1])
                    self.indicators["signal"].append(signal_line[-1])
                    self.indicators["hist"].append(hist[-1])
                    self.logger.debug(f"Strategy: MACD calculated at {timestamp} - MACD: {macd_line[-1]}, Signal: {signal_line[-1]}, Hist: {hist[-1]}")

                else: 
                    self.logger.warning(f"Strategy: MACD calculation returned NaN at {timestamp} not storing.")

            except Exception as e:
                self.logger.error(f"TA-Lib MACD calc error at {timestamp}: {e}")
                

    def _run(self, coarse_bar:RealTimeBar, check_buy_signal=True, check_sell_signal=True):
        """
        Generates MACD crossover signals based on COARSE bars.
        - buy signal (MACD crosses above signal line - Bullish cross): Returns "buy"
        - sell signal (MACD crosses below signal line - Bearish cross): Returns "sell"
        """
        timestamp = coarse_bar.time
        self._update_data_and_indicators(coarse_bar)

        if len(self._close_data) < self._min_bars_needed:
            self.logger.info(f"Strategy: Data not ready at {timestamp}. Need {self._min_bars_needed} have {len(self._close_data)}. Try starting with previous day's data")
            return None
        if len(self.indicators['timestamp']) < self._min_indicators_needed:
            self.logger.info(f"Strategy: Indicators not ready at {timestamp}. Need {self._min_indicators_needed} have {len(self.indicators['timestamp'])}. Try starting with previous day's data")
            return None

        # Check for Bullish Cross (MACD crosses above signal line)
        if check_buy_signal:
            self.logger.debug(f"Values for Bullish cross: "
                    f"{self.indicators['macd'][-2]} < {self.indicators['signal'][-2]} and {self.indicators['macd'][-1]} > {self.indicators['signal'][-1]}")
            
            if self.indicators['macd'][-2] < self.indicators['signal'][-2] and self.indicators['macd'][-1] > self.indicators['signal'][-1]:
                reason = "buy"  
                self.logger.info(f"!MACDStrategy: Bullish Cross (buy) triggered at {timestamp}")
                return reason

        # Check for Bearish Cross (MACD crosses below signal line)
        if check_sell_signal:
            self.logger.debug(f"Values for Bearish cross: "
                              f"{self.indicators['macd'][-2]} > {self.indicators['signal'][-2]} and {self.indicators['macd'][-1]} < {self.indicators['signal'][-1]}")
            
            if self.indicators['macd'][-2] > self.indicators['signal'][-2] and self.indicators['macd'][-1] < self.indicators['signal'][-1]:
                reason = "sell" 
                self.logger.info(f"!MACDStrategy: Bearish Cross (sell) triggered at {timestamp}")
                return reason

        return None
    
    
    def check_buy_signal(self, coarse_bar, right):

        if right == "P" or right == "C":
            reason = self._run(coarse_bar, check_buy_signal=True, check_sell_signal=False)
            return reason == "buy"
        else:
            self.logger.error(f"Invalid right value: {right}. Expected 'P' or 'C'.")
            return False
    
    def check_sell_signal(self, coarse_bar, right):
        if right == "P" or right == "C":
            reason = self._run(coarse_bar, check_buy_signal=False, check_sell_signal=True)
            return reason == "sell"
        else:
            self.logger.error(f"Invalid right value: {right}. Expected 'PE' or 'CE'.")
            return False


    def check_pl_exit_conditions(self, fine_bar: RealTimeBar, position: Position):
        """ Checks SL/TP based on FINE bar High/Low. """
        trade_cond = False
        reason = None
        exit_price = None
        default_return = [trade_cond, reason, exit_price]

        if position is None or position.position <= 0: return default_return
        if self.stop_loss_pct is None and self.take_profit_pct is None: return default_return

        entry_price = position.avgCost
        bar_high = fine_bar.high
        bar_low = fine_bar.low
        timestamp = fine_bar.time

        if bar_high is None or bar_low is None:
            self.logger.warning(f"Fine bar at {timestamp} missing high/low. Cannot check P/L exit.")
            return default_return

        if self.stop_loss_pct is not None:
            stop_loss_price = entry_price * (1 - self.stop_loss_pct)
            if bar_low <= stop_loss_price:
                exit_price = stop_loss_price
                reason = f"Stop loss"
                trade_cond = True

        if not trade_cond and self.take_profit_pct is not None:
            take_profit_price = entry_price * (1 + self.take_profit_pct)
            if bar_high >= take_profit_price:
                exit_price = take_profit_price
                reason = f"Take profit"
                trade_cond = True

        if trade_cond: 
            self.logger.debug(f"      Strategy: P/L Exit -> {reason} at {timestamp}")

        return [trade_cond, reason, exit_price]


    @staticmethod
    def get_optimizable_params():
        """ Defines parameters and ranges for optimization. """
        return {
            # Parameter Name: { Optuna Suggestion Type, Args... }
            'macd_fast': {'type': 'suggest_int', 'name': 'macd_fast', 'low': 5, 'high': 20},
            'macd_slow': {'type': 'suggest_int', 'name': 'macd_slow', 'low': 21, 'high': 60},
            'macd_signal': {'type': 'suggest_int', 'name': 'macd_signal', 'low': 5, 'high': 15},
            'stop_loss_pct': {'type': 'suggest_float', 'name': 'stop_loss_pct', 'low': 0.01, 'high': 0.10, 'step': 0.005},
            'take_profit_pct': {'type': 'suggest_float', 'name': 'take_profit_pct', 'low': 0.02, 'high': 0.20, 'step': 0.01},
            # Add other parameters defined in strategy_params if needed
        }

    def get_indicator_history(self, as_dataframe=True):
        """ Returns the stored indicator history. """
        # (Code remains the same as previous version macd_strategy_py_hist_v3)
        if not self.indicators['timestamp']:
             if as_dataframe: return pd.DataFrame(columns=['timestamp', 'macd', 'signal', 'hist']).set_index('timestamp')
             else: return self.indicators
        if as_dataframe:
            try:
                df = pd.DataFrame(self.indicators)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.set_index('timestamp')
                return df
            except Exception as e:
                self.logger.exception(f"Error converting indicator history to DataFrame: {e}")
                return pd.DataFrame(columns=['timestamp', 'macd', 'signal', 'hist']).set_index('timestamp')
        else: return self.indicators

