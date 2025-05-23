# src/ml_optimization/optimizer.py
import optuna
import logging
import json
import pandas as pd
import os
import sys
import copy # For deep copying config

# --- Add src directory to Python path ---
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path: sys.path.insert(0, project_root)
# --- End Path Addition ---

from src.utils.config_loader import load_json_config
from src.trading_account.trading_account import TradingAccount
from src.strategies.base_strategy import BaseStrategy
from src.strategies.registry import get_strategy_class # Import the registry function
from src.trading_engines.backtester import Backtester
from src.data_management.historical_provider import HistoricalProvider

# Configure logging (optimizer might have its own level)
log = logging.getLogger(__name__)
# Ensure root logger is configured by the executable running this

class StrategyOptimizer:
    """
    Uses Optuna to optimize strategy parameters by running backtests.
    """
    def __init__(self, base_config_path: str, fine_data_path_template: str, fine_data_timeframe: str):
        """
        Initializes the optimizer.

        Args:
            base_config_path: Path to the base JSON configuration file for the strategy.
            fine_data_path_template: Path template for the fine-grained historical data.
            fine_data_timeframe: Timeframe string for the fine-grained data (e.g., '1min').
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.base_config_path = base_config_path
        self.fine_data_path_template = fine_data_path_template
        self.fine_data_timeframe = fine_data_timeframe

        # Load base config once
        self.base_config = load_json_config(base_config_path)
        if not self.base_config:
            raise ValueError(f"Failed to load base configuration from {base_config_path}")

        # Load data once (assuming same data for all trials)
        self.logger.info("Loading fine-grained data for optimization...")
        provider = HistoricalProvider(
            path_template=self.fine_data_path_template,
            datetime_col=self.base_config.get('data_source', {}).get('datetime_col', 'timestamp'),
            date_format=self.base_config.get('data_source', {}).get('date_format', None)
        )
        # Use date range from config for loading data
        backtest_params = self.base_config.get('backtest_params', {})
        self.historical_data = provider.load_data(
            symbol=self.base_config['symbol'], # Assume symbol is in base config
            timeframe=self.fine_data_timeframe,
            start_date=backtest_params.get('start_date'),
            end_date=backtest_params.get('end_date')
        )
        if self.historical_data is None or self.historical_data.empty:
            raise ValueError("Failed to load historical data for optimization.")
        self.logger.info(f"Data loaded: {self.historical_data.shape[0]} rows.")

        # Get strategy class
        strategy_name = self.base_config.get('strategy_name')
        if not strategy_name: raise ValueError("Config missing 'strategy_name'.")
        self.StrategyClass = get_strategy_class(strategy_name) # Use registry function
        if not self.StrategyClass: # Check if None was returned
            raise ValueError(f"Strategy class '{strategy_name}' not found in registry or invalid.")

        # Get optimizable parameter definitions from the strategy class
        self.optimizable_params_def = self.StrategyClass.get_optimizable_params()
        if not self.optimizable_params_def:
             raise ValueError(f"Strategy class '{strategy_name}' does not define optimizable parameters.")
        self.logger.info(f"Optimizable parameters for {strategy_name}: {self.optimizable_params_def}")


    def _objective(self, trial: optuna.trial.Trial):
        """ Optuna objective function: runs one backtest trial. """
        self.logger.debug(f"--- Starting Optuna Trial {trial.number} ---")
        # 1. Suggest Parameters based on Strategy Definition
        trial_params = {}
        # Ensure slow > fast for MACD example
        is_macd_type = False
        if 'macd_fast' in self.optimizable_params_def and 'macd_slow' in self.optimizable_params_def:
             is_macd_type = True
             while True: # Loop until valid pair is generated
                  trial_params['macd_fast'] = trial.suggest_int(**self.optimizable_params_def['macd_fast'])
                  trial_params['macd_slow'] = trial.suggest_int(**self.optimizable_params_def['macd_slow'])
                  if trial_params['macd_slow'] > trial_params['macd_fast'] + 2: # Ensure min separation
                       break
                  self.logger.debug(f"Trial {trial.number}: Regenerating MACD params (slow <= fast).")
        # Suggest other parameters
        for name, definition in self.optimizable_params_def.items():
            if name in trial_params: continue # Skip already suggested params (like MACD)
            suggest_type = definition['type']
            suggest_args = {k: v for k, v in definition.items() if k != 'type'} # Get args for suggest function
            if hasattr(trial, suggest_type):
                 trial_params[name] = getattr(trial, suggest_type)(**suggest_args)
            else:
                 self.logger.warning(f"Optuna trial does not have method '{suggest_type}'. Skipping param '{name}'.")

        self.logger.info(f"Trial {trial.number} Parameters: {trial_params}")

        # 2. Create Config for this Trial
        trial_config = copy.deepcopy(self.base_config)
        # Update strategy_params section with trial values
        trial_config['strategy_params'].update(trial_params)

        # 3. Instantiate Components for Trial
        # Use a unique account ID or reset state if needed, initial capital from config
        trading_account = TradingAccount(initial_capital=trial_config['trading']['initial_capital'])
        # Strategy needs the modified config for this trial
        strategy = self.StrategyClass(trial_config, trading_account)

        # Backtester uses the trial's strategy and account
        backtester = Backtester(
            fine_historical_data=self.historical_data, # Use preloaded data
            strategy=strategy,
            trading_account=trading_account,
            trading_barsize=trial_config['coarse_timeframe'],
            commission_per_share=trial_config['trading']['commission_per_share'],
            slippage_pct=trial_config['trading']['slippage_pct']
        )

        # 4. Run Backtest
        try:
            final_account = backtester.run()
            # Suppress summary printing during optimization runs? Add flag to Backtester?
            # Or capture stdout if needed. For now, it will print.
            if final_account is None:
                 self.logger.warning(f"Trial {trial.number}: Backtest run failed. Returning very low score.")
                 return -float('inf') # Penalize failed runs

            # 5. Extract Performance Metric
            stats = final_account.get_summary_stats()
            # Choose metric to optimize (e.g., 'total_net_pnl', 'profit_factor', 'sharpe_ratio' - needs calculation)
            # Let's use Total Net PnL for this example
            metric_value = stats.get('total_net_pnl', -float('inf'))
            self.logger.info(f"Trial {trial.number} Result (Total Net PnL): {metric_value:.2f}")

            # Handle NaN or Inf values if metric calculation fails
            if not pd.notna(metric_value) or math.isinf(metric_value):
                 self.logger.warning(f"Trial {trial.number}: Invalid metric value ({metric_value}). Returning very low score.")
                 return -float('inf')

            return metric_value # Return the value for Optuna to maximize/minimize

        except Exception as e:
            self.logger.exception(f"Trial {trial.number}: Exception during backtest run: {e}")
            return -float('inf') # Penalize exceptions


    def run_optimization(self, n_trials=50, direction='maximize', study_name=None, storage=None):
        """
        Runs the Optuna optimization study.

        Args:
            n_trials: Number of trials to run.
            direction: 'maximize' or 'minimize' the objective function's return value.
            study_name: Optional name for the study (useful with storage).
            storage: Optional database URL for storing/resuming study (e.g., 'sqlite:///optimization.db').

        Returns:
            optuna.study.Study: The completed Optuna study object.
        """
        self.logger.info(f"Starting optimization study: {n_trials} trials, direction='{direction}'")
        # Create or load the study
        study = optuna.create_study(
            study_name=study_name,
            storage=storage,
            direction=direction,
            load_if_exists=True, # Resume if study exists in storage
            pruner=optuna.pruners.MedianPruner() # Example pruner
        )

        # Run the optimization
        study.optimize(
            self._objective,
            n_trials=n_trials,
            timeout=None, # Optional timeout in seconds
            n_jobs=1 # Use 1 for simplicity, can use >1 for parallel if objective is safe
        )

        # Print results
        self.logger.info("\n" + "="*30 + " Optimization Finished " + "="*30)
        self.logger.info(f"Study Name: {study.study_name}")
        self.logger.info(f"Number of finished trials: {len(study.trials)}")

        try:
            best_trial = study.best_trial
            self.logger.info("Best trial:")
            self.logger.info(f"  Value ({direction}d): {best_trial.value:.4f}")
            self.logger.info("  Params: ")
            for key, value in best_trial.params.items():
                self.logger.info(f"    {key}: {value}")
        except ValueError:
             self.logger.warning("No completed trials found in the study. Cannot determine best trial.")

        self.logger.info("="*80 + "\n")
        return study

