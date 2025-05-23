import pytest
import json
import os
import sys
import pathlib

# Add project root to the Python path
project_root = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.helpers import load_json_config

# --- Fixtures ---

@pytest.fixture
def temp_config_dir(tmp_path):
    """Creates a temporary directory for config files."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    return config_dir

@pytest.fixture
def simple_config_file(temp_config_dir):
    """Creates a simple JSON config file."""
    config_data = {"key1": "value1", "key2": 123}
    config_path = temp_config_dir / "simple.json"
    with open(config_path, 'w') as f:
        json.dump(config_data, f)
    return str(config_path) # Return path as string

@pytest.fixture
def sub_config_file(temp_config_dir):
    """Creates a sub-config JSON file."""
    config_data = {"sub_key": "sub_value", "key2": 456} # Overlaps key2
    config_path = temp_config_dir / "sub_config.json"
    with open(config_path, 'w') as f:
        json.dump(config_data, f)
    return str(config_path) # Return path as string

@pytest.fixture
def main_config_file_with_sub(temp_config_dir, sub_config_file):
    """Creates a main config file referencing the sub-config."""
    config_data = {
        "main_key": "main_value",
        "sub_config_file": sub_config_file, # Reference the sub-config
        "key2": 789 # Override sub_config's key2
    }
    config_path = temp_config_dir / "main_with_sub.json"
    with open(config_path, 'w') as f:
        json.dump(config_data, f)
    return str(config_path) # Return path as string

@pytest.fixture
def main_config_only_sub(temp_config_dir, sub_config_file):
    """Creates a main config file referencing only the sub-config."""
    config_data = {
        "sub_config_file": sub_config_file
    }
    config_path = temp_config_dir / "main_only_sub.json"
    with open(config_path, 'w') as f:
        json.dump(config_data, f)
    return str(config_path)

# --- Test Cases ---

def test_load_simple_config(simple_config_file):
    """Tests loading a single, simple config file."""
    expected_config = {"key1": "value1", "key2": 123}
    loaded_config = load_json_config(simple_config_file)
    assert loaded_config == expected_config

def test_load_config_with_subconfig(main_config_file_with_sub):
    """Tests loading a main config that includes and overrides a sub-config."""
    expected_config = {
        "sub_key": "sub_value", # From sub_config
        "main_key": "main_value", # From main_config
        "key2": 789 # Overridden by main_config
    }
    loaded_config = load_json_config(main_config_file_with_sub)
    assert loaded_config == expected_config

def test_load_config_with_only_subconfig(main_config_only_sub):
    """Tests loading a main config that only references a sub-config."""
    expected_config = {
        "sub_key": "sub_value",
        "key2": 456
    }
    loaded_config = load_json_config(main_config_only_sub)
    assert loaded_config == expected_config

def test_load_nonexistent_main_config(temp_config_dir):
    """Tests loading a non-existent main config file."""
    non_existent_path = str(temp_config_dir / "non_existent.json")
    with pytest.raises(FileNotFoundError, match="Configuration file not found"):
        load_json_config(non_existent_path)

def test_load_nonexistent_sub_config(temp_config_dir):
    """Tests loading a main config referencing a non-existent sub-config."""
    non_existent_sub_path = str(temp_config_dir / "non_existent_sub.json")
    main_config_data = {
        "main_key": "value",
        "sub_config_file": non_existent_sub_path # Reference non-existent file
    }
    main_config_path = temp_config_dir / "main_bad_sub.json"
    with open(main_config_path, 'w') as f:
        json.dump(main_config_data, f)

    # Ensure the main config file path exists before loading
    assert os.path.exists(main_config_path)

    with pytest.raises(FileNotFoundError, match="Sub-configuration file not found"):
        load_json_config(str(main_config_path))

def test_load_empty_main_config(temp_config_dir):
    """Tests loading an empty main config file."""
    config_path = temp_config_dir / "empty.json"
    with open(config_path, 'w') as f:
        json.dump({}, f)
    loaded_config = load_json_config(str(config_path))
    assert loaded_config == {}

def test_load_config_with_non_json_content(temp_config_dir):
    """Tests loading a file that is not valid JSON."""
    config_path = temp_config_dir / "invalid.txt"
    with open(config_path, 'w') as f:
        f.write("this is not json")
    with pytest.raises(json.JSONDecodeError):
        load_json_config(str(config_path))

def test_load_actual_run_config():
    """Tests loading an actual run configuration file and its sub-configs."""
    run_config_path = project_root / "config/run_configs/backtest_macd_aapl.json"

    expected_config = {
        "symbol": "AAPL",
        "secType": "STK",
        "currency": "USD",
        "exchange": "SMART",
        "strategy_name": "MACDStrategy",
        "coarse_timeframe": "5min",
        "trading": {
            "order_quantity": 10,
            "commission_per_share": 0.005,
            "slippage_pct": 0.0005
        },
        "strategy_params": {
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.06,
            "eod_exit_time": "15:45:00"
        },
        "backtest_params": {
            "start_date": "2023-01-01",
            "end_date": "2023-12-31",
            "initial_capital": 100000.0
        },
        "data_source": {
            "provider": "csv",
            "path_template": "./data/historical/{symbol}_1min.csv",
            "datetime_col": "timestamp",
            "date_format": None
        },
        "run_description": "Backtest MACD Crossover on AAPL 1min data (5min signals)",
        "mode": "backtest",
        "log_level": "INFO"
    }

    assert run_config_path.exists(), f"Config file not found at {run_config_path}"

    loaded_config = load_json_config(str(run_config_path))
    assert loaded_config == expected_config