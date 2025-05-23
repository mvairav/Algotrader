# executables/run_optimize.py
import argparse
import logging
import os
import sys
import time

# --- Add src directory to Python path ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path: sys.path.insert(0, project_root)
# --- End Path Addition ---

from src.optimization.optimize import StrategyOptimizer

# --- Configure Root Logger ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(log_formatter)
root_logger = logging.getLogger()
root_logger.addHandler(log_handler)
root_logger.setLevel(logging.INFO) # Set default level
log = logging.getLogger(__name__) # Logger for this script

def run_optimization(args):
    """ Sets up and runs the StrategyOptimizer. """
    log.info("--- Initializing Strategy Optimization ---")
    log.info(f"Base Config: {args.config}")
    log.info(f"Fine Data Template: {args.fine_data}")
    log.info(f"Fine Timeframe: {args.fine_timeframe}")
    log.info(f"Number of Trials: {args.trials}")
    log.info(f"Direction: {args.direction}")
    if args.storage: log.info(f"Storage: {args.storage}")
    if args.study_name: log.info(f"Study Name: {args.study_name}")

    try:
        # 1. Instantiate Optimizer
        optimizer = StrategyOptimizer(
            base_config_path=args.config,
            fine_data_path_template=args.fine_data,
            fine_data_timeframe=args.fine_timeframe
        )

        # 2. Run Optimization
        start_opt_time = time.time()
        study = optimizer.run_optimization(
            n_trials=args.trials,
            direction=args.direction,
            study_name=args.study_name,
            storage=args.storage
        )
        end_opt_time = time.time()
        log.info(f"Total optimization time: {end_opt_time - start_opt_time:.2f} seconds.")

        # 3. Optionally save best params
        if args.output_config and study.best_trial:
             best_params = study.best_params
             log.info(f"Saving best parameters found to {args.output_config}")
             # Load base config, update strategy_params, save new config
             import json
             import copy
             from src.utils.config_loader import load_json_config # Assumes this exists
             base_config = load_json_config(args.config)
             if base_config:
                  optimized_config = copy.deepcopy(base_config)
                  optimized_config['strategy_params'].update(best_params)
                  # Add a note about optimization
                  optimized_config['description'] = optimized_config.get('description', '') + f" [Optimized: {datetime.datetime.now().isoformat()}]"
                  try:
                       with open(args.output_config, 'w') as f:
                            json.dump(optimized_config, f, indent=2)
                       log.info("Best parameters saved.")
                  except Exception as e:
                       log.error(f"Failed to save optimized config: {e}")
             else:
                  log.error("Failed to load base config for saving optimized version.")


    except ValueError as e:
         log.critical(f"Initialization Error: {e}")
         sys.exit(1)
    except Exception as e:
         log.exception("An uncaught exception occurred during optimization.")
         sys.exit(1)

    log.info("--- Optimization Process Finished ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Strategy Parameter Optimization")
    parser.add_argument('--config', required=True, help="Path to the BASE strategy configuration JSON file")
    parser.add_argument('--fine-data', required=True, help="Path template for FINE-GRAINED historical data used for backtesting (e.g., ./data/{symbol}_1min.csv)")
    parser.add_argument('--fine-timeframe', required=True, help="Timeframe string of the fine-grained data (e.g., '1min')")
    parser.add_argument('--trials', type=int, default=50, help="Number of optimization trials to run")
    parser.add_argument('--direction', default='maximize', choices=['maximize', 'minimize'], help="Direction to optimize the objective function")
    parser.add_argument('--storage', help="Database URL for Optuna storage (e.g., 'sqlite:///study.db') to enable resume")
    parser.add_argument('--study-name', help="Name for the Optuna study (required if using storage)")
    parser.add_argument('--output-config', help="Path to save the configuration file with the best parameters found")
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help="Set logging level")

    args = parser.parse_args()

    # Validate storage/study name combination
    if args.storage and not args.study_name:
        parser.error("--study-name is required when using --storage")

    # Setup Logging Level
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    root_logger.setLevel(log_level)

    run_optimization(args)
    sys.exit(0)

