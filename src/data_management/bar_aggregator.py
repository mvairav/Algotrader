# src/data_management/bar_aggregator.py
import pandas as pd
import logging
from collections import deque # Import deque for capped storage options
from ib_async import  RealTimeBar
import dataclasses 

class BarAggregator:
    """
    Aggregates fine-grained data into coarse bars based on a specified timeframe.
    Stores all received fine-grained updates AND the history of generated coarse bars.

    NOTE: Storing all fine/coarse data can consume significant memory over time.
          Consider implementing data flushing or using capped storage if necessary.
    """
    def __init__(self, timeframe_seconds=300,
                 store_fine_data=True, max_fine_data_points=None,
                 store_coarse_data=True, max_coarse_data_points=None):
        """
        Initializes the aggregator.

        Args:
            timeframe_seconds (int): The number of seconds for the coarse bar interval.
            store_fine_data (bool): If True, store all incoming fine-grained updates.
            max_fine_data_points (int, optional): Max fine data points to store (uses deque).
            store_coarse_data (bool): If True, store the history of generated coarse bars.
            max_coarse_data_points (int, optional): Max coarse bars to store (uses deque).
        """
        self.logger = logging.getLogger(self.__class__.__name__) # Get logger for this module

        if timeframe_seconds <= 0:
            self.logger.error("timeframe_seconds must be positive.")
            raise ValueError("timeframe_seconds must be positive.")

        self.logger.info(f"Initializing BarAggregator with {timeframe_seconds} seconds timeframe.")
        # --- Timeframe Setup ---
        self.timeframe = pd.Timedelta(seconds=timeframe_seconds)
        self.current_bar = None # Holds the currently forming coarse bar {timestamp, o, h, l, c, v}
        self.last_update_price = None
        self.last_update_time = None

        # --- Fine Data Storage ---
        self.store_fine_data = store_fine_data
        self.max_fine_data_points = max_fine_data_points
        if self.store_fine_data:
            if self.max_fine_data_points is not None and self.max_fine_data_points > 0:
                self.logger.info(f"Storing most recent {self.max_fine_data_points} fine-grained data points.")
                self.fine_data_storage = deque(maxlen=self.max_fine_data_points)
            else:
                self.logger.info("Storing all incoming fine-grained data (list). Warning: Memory usage can grow.")
                self.fine_data_storage = []
        else:
             self.fine_data_storage = None
        self.total_fine_data_received = 0 
        # --- Coarse Bar Storage ---
        self.store_coarse_data = store_coarse_data
        self.max_coarse_data_points = max_coarse_data_points
        if self.store_coarse_data:
            if self.max_coarse_data_points is not None and self.max_coarse_data_points > 0:
                self.logger.info(f"Storing most recent {self.max_coarse_data_points} coarse bars.")
                self.coarse_bar_storage = deque(maxlen=self.max_coarse_data_points)
            else:
                self.logger.info("Storing all generated coarse bars (list). Warning: Memory usage can grow.")
                self.coarse_bar_storage = []
        else:
            self.coarse_bar_storage = None
        self.total_coarse_bars_generated = 0
        self.logger.info(f"BarAggregator initialized for {self.timeframe} timeframe. "
                 f"StoreFine={self.store_fine_data}, StoreCoarse={self.store_coarse_data}")

    def process_update(self, price, volume, timestamp):
        """
        Processes a fine-grained update, stores it if configured, aggregates it,
        and updates the coarse bar history storage.

        Args:
            price (float): The price of the update.
            volume (float): The volume of the update. Can be None if not available.
            timestamp (pd.Timestamp or compatible): The timestamp of the update.

        Returns:
            tuple: (new_bar_started, fine_update_data, completed_bar)
            new_bar_started (bool): True if a new coarse bar was started, False otherwise.
            fine_update_data (dict): Contains the latest price and timestamp.
            completed_bar (dict or None): The completed OHLCV bar if the interval ended.
        """
        # --- Input Validation and Timestamp Handling ---
        try:
            price = float(price)
            volume = float(volume) if volume is not None else 0.0
            self.total_fine_data_received += 1
        except (ValueError, TypeError) as e:
             self.logger.error(f"Invalid price ({price}) or volume ({volume}): {e}. Skipping update.")
             # Return last known state or default values if none exist yet
             return False, {'price': self.last_update_price, 'timestamp': self.last_update_time}, None


        if timestamp.tzinfo is None:
             self.logger.debug(f"Timestamp {timestamp} is timezone naive.")

        self.last_update_price = price
        self.last_update_time = timestamp

        # --- Store Fine-Grained Data ---
        if self.store_fine_data:
            fine_update_record = {'timestamp': timestamp, 'price': price, 'volume': volume}
            # Check if storage is initialized and is a list or deque
            if isinstance(self.fine_data_storage, (deque, list)):
                 self.fine_data_storage.append(fine_update_record)

        # --- Bar Aggregation Logic ---
        completed_bar = None
        new_bar_started = False

        if self.current_bar is None:
            # --- First update or start after a completion ---

            # Initialize the new bar dictionary
            self.current_bar = RealTimeBar(time=timestamp, open_=price, high=price,
                                           low=price, close=price, volume=volume)

            self.logger.debug(f"Bar time: {timestamp}, Started first bar at {timestamp}")
            new_bar_started = True # Flag that a new bar was created

        else:
            # --- Check if update belongs to the next interval ---
            # Calculate the expected start time of the next bar
            next_bar_start_time = self.current_bar.time + self.timeframe
            if timestamp >= next_bar_start_time:
                # --- Finalize the current bar ---
                # The bar stored in self.current_bar is now complete
                current_bar_start_time = next_bar_start_time
                completed_bar = RealTimeBar(time=next_bar_start_time, open_=self.current_bar.open_,
                                           high=self.current_bar.high, low=self.current_bar.low,
                                           close=self.current_bar.close, volume=self.current_bar.volume)
                self.logger.debug(f"Completed bar: {completed_bar}")

                # If the timestamp jumped multiple intervals, advance start time accordingly
                while timestamp >= current_bar_start_time + self.timeframe:
                    self.logger.debug(f"Gap detected. Jumping bar start time from {current_bar_start_time}")
                    # Note: We don't explicitly create/store bars for the gap here,
                    current_bar_start_time += self.timeframe

                self.current_bar = RealTimeBar(time=timestamp, open_=price, high=price,
                                           low=price, close=price, volume=volume)

                self.logger.debug(f"Started next bar at {current_bar_start_time}")
                new_bar_started = True # Flag that a new bar was created
            else:
                # --- Update the currently forming bar ---
                # Update high, low, close, and volume
                self.current_bar.high = max(self.current_bar.high, price)
                self.current_bar.low = min(self.current_bar.low, price)
                self.current_bar.close = price
                self.current_bar.volume += volume
                self.logger.debug(f"Updated current bar: {self.current_bar.time} at time {timestamp}")
                

        # --- Update Coarse Bar Storage ---
        # If storage is enabled and we have a valid current bar
        if self.store_coarse_data and self.current_bar is not None \
            and self.coarse_bar_storage is not None and new_bar_started:
            # Check if the storage object exists and is a list or deque
            if isinstance(self.coarse_bar_storage, (deque, list)):
                current_bar_copy = dataclasses.replace(self.current_bar) 

                # Append the newly started bar to the storage
                self.coarse_bar_storage.append(current_bar_copy)
                self.logger.debug(f"Appended new coarse bar to storage (Total: {len(self.coarse_bar_storage)})")


        # Prepare the immediate fine update info to return
        fine_update_data = {'price': self.last_update_price, 'timestamp': self.last_update_time}
        if new_bar_started:
            self.total_coarse_bars_generated += 1
            self.logger.debug(f"New coarse bar started. Total coarse bars generated: {self.total_coarse_bars_generated}")
        # Return the latest fine update info and the completed bar (if any)
        return new_bar_started, fine_update_data, completed_bar

    def get_fine_data(self, as_dataframe=True):
        """
        Returns the stored fine-grained data.

        Args:
            as_dataframe (bool): If True, converts the stored data to a pandas DataFrame.
                                 If False, returns the raw storage (list or deque).

        Returns:
            pd.DataFrame or list or deque or None: The stored fine data, or None if not stored.
        """
        if not self.store_fine_data or self.fine_data_storage is None:
            self.logger.warning("Fine data storage is disabled or empty.")
            return None

        if as_dataframe:
            # Handle empty storage case
            if not self.fine_data_storage:
                 return pd.DataFrame(columns=['timestamp', 'price', 'volume']).set_index('timestamp')
            try:
                # Convert deque/list to list first, then to DataFrame
                df = pd.DataFrame(list(self.fine_data_storage))
                # Ensure timestamp column exists before processing
                if 'timestamp' in df.columns:
                     df['timestamp'] = pd.to_datetime(df['timestamp']) # Convert to datetime objects
                     df = df.set_index('timestamp') # Set as index
                # Ensure columns exist before returning
                for col in ['price', 'volume']:
                    if col not in df.columns:
                        df[col] = pd.NA # Add missing columns with NA values
                return df[['price', 'volume']] # Return only relevant columns
            except Exception as e:
                 self.logger.exception(f"Error converting stored fine data to DataFrame: {e}")
                 # Return empty DataFrame on error
                 return pd.DataFrame(columns=['timestamp', 'price', 'volume']).set_index('timestamp')
        else:
            # Return the raw storage object (list or deque)
            return self.fine_data_storage

    def get_coarse_data(self, as_dataframe=True):
        """
        Returns the stored coarse bar history.

        Args:
            as_dataframe (bool): If True, converts the stored bars to a pandas DataFrame.
                                 If False, returns the raw storage (list or deque).

        Returns:
            pd.DataFrame or list or deque or None: The stored coarse bars, or None if not stored.
        """
        if not self.store_coarse_data or self.coarse_bar_storage is None:
            self.logger.warning("Coarse bar storage is disabled or empty.")
            return None

        if as_dataframe:
            # Handle empty storage case
            if not self.coarse_bar_storage:
                 return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).set_index('timestamp')
            try:
                coarse_bar_list = [
                    {
                        'timestamp': bar.time,
                        'open': bar.open_,
                        'high': bar.high,
                        'low': bar.low,
                        'close': bar.close,
                        'volume': bar.volume
                    }
                    for bar in self.coarse_bar_storage
                ]
                df = pd.DataFrame(coarse_bar_list) 
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True) 

                return df
            except Exception as e:
                 self.logger.exception(f"Error converting stored coarse bars to DataFrame: {e}")
                 # Return empty DataFrame on error
                 return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).set_index('timestamp')
        else:
            # Return the raw storage object (list or deque)
            return self.coarse_bar_storage


    @staticmethod
    def aggregate_dataframe(fine_data_df, timeframe_str='5min'):
         """
         Aggregates a DataFrame of fine data (with DatetimeIndex and OHLCV columns)
         to a coarser OHLCV DataFrame using pandas resample.

         Args:
             fine_data_df (pd.DataFrame): DataFrame with a DatetimeIndex and columns
                                          like 'open', 'high', 'low', 'close', 'volume'.
             timeframe_str (str): A pandas offset string (e.g., '1min', '5min', '1H', '1D').

         Returns:
             pd.DataFrame: DataFrame aggregated to the specified timeframe.
         """
         logger = logging.getLogger(__name__) # Get logger for this module
         # Validate input DataFrame index
         if not isinstance(fine_data_df.index, pd.DatetimeIndex):
              logger.error("Input DataFrame index must be DatetimeIndex for resampling.")
              raise ValueError("DataFrame index must be DatetimeIndex for resampling.")

         # Validate required columns (case-insensitive)
         required_cols = ['open', 'high', 'low', 'close', 'volume']
         df_cols_lower = fine_data_df.columns.str.lower()
         if not all(col in df_cols_lower for col in required_cols):
              logger.error(f"Input DataFrame missing required OHLCV columns. Found: {list(fine_data_df.columns)}")
              raise ValueError("Input DataFrame missing required OHLCV columns.")
         # Standardize column names to lowercase
         fine_data_df.columns = df_cols_lower

         logger.info(f"Aggregating DataFrame to {timeframe_str} timeframe...")
         # Define aggregation functions for resampling
         ohlc_dict = {
             'open': 'first', # First price in the interval
             'high': 'max',   # Highest price in the interval
             'low': 'min',    # Lowest price in the interval
             'close': 'last', # Last price in the interval
             # Sum volume, return NaN if all values in interval are NaN, otherwise sum available numbers
             'volume': lambda x: x.sum(min_count=1)
         }

         # Perform resampling
         # label='left': Timestamps represent the start of the interval.
         # closed='left': Interval is [start, end).
         coarse_df = fine_data_df.resample(timeframe_str, label='left', closed='left').agg(ohlc_dict)

         # Remove rows where no data existed in the original fine_data_df for that interval
         # (these rows will have NaN for all OHLC columns after aggregation)
         coarse_df.dropna(subset=['open', 'close'], how='all', inplace=True)

         # Optional: Fill NaN volumes (resulting from intervals with no trades) with 0
         if 'volume' in coarse_df.columns:
              coarse_df['volume'].fillna(0, inplace=True)

         logger.info(f"Aggregation complete. Result shape: {coarse_df.shape}")
         return coarse_df

