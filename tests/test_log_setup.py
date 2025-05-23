import pytest
import json
import os
import logging
import sys
import pathlib

# Add project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path: sys.path.insert(0, project_root)

from src.utils.logging_setup import setup_general_logging

# --- Fixtures ---

@pytest.fixture(autouse=True)
def reset_logging():
    """Reset logging configuration before and after each test."""
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    yield
    for handler in root_logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            handler.close()
        root_logger.removeHandler(handler)

@pytest.fixture
def temp_log_dir(tmp_path):
    """Creates a temporary directory for log files."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    return log_dir

@pytest.fixture
def simple_log_config_file(temp_log_dir):
    """Creates a simple JSON log config file."""
    config_data = {
        "version": 1,
        "formatters": {
            "simple": {"format": "%(levelname)s: %(message)s"}
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "simple"
            }
        },
        "root": {
            "level": "INFO",
            "handlers": ["console"]
        }
    }
    config_path = temp_log_dir / "simple_log.json"
    with open(config_path, 'w') as f:
        json.dump(config_data, f)
    return str(config_path)

@pytest.fixture
def file_log_config_file(temp_log_dir):
    """Creates a log config with file handler."""
    log_file = temp_log_dir / "app.log"
    config_data = {
        "version": 1,
        "formatters": {
            "detailed": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"}
        },
        "handlers": {
            "file": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "formatter": "detailed",
                "filename": str(log_file)
            }
        },
        "root": {
            "level": "DEBUG",
            "handlers": ["file"]
        }
    }
    config_path = temp_log_dir / "file_log.json"
    with open(config_path, 'w') as f:
        json.dump(config_data, f)
    return str(config_path), str(log_file)

@pytest.fixture
def combined_log_config_file(temp_log_dir):
    """Creates a config with both console and file handlers."""
    log_file = temp_log_dir / "combined.log"
    config_data = {
        "version": 1,
        "formatters": {
            "simple": {"format": "%(levelname)s: %(message)s"},
            "detailed": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"}
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "simple"
            },
            "file": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "formatter": "detailed",
                "filename": str(log_file)
            }
        },
        "root": {
            "level": "DEBUG",
            "handlers": ["console", "file"]
        }
    }
    config_path = temp_log_dir / "combined_log.json"
    with open(config_path, 'w') as f:
        json.dump(config_data, f)
    return str(config_path), str(log_file)

@pytest.fixture
def invalid_level_config_file(temp_log_dir):
    """Creates a config with invalid log level."""
    config_data = {
        "version": 1,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INVALID_LEVEL"
            }
        },
        "root": {
            "level": "DEBUG",
            "handlers": ["console"]
        }
    }
    config_path = temp_log_dir / "invalid_level.json"
    with open(config_path, 'w') as f:
        json.dump(config_data, f)
    return str(config_path)

# --- Test Cases ---

def test_setup_simple_logging(simple_log_config_file, caplog):
    """Tests setting up a simple console logger."""
    setup_general_logging(simple_log_config_file)
    logger = logging.getLogger()
    
    # Debug should be filtered out since root logger level is INFO
    logger.debug("This debug message should be filtered out.")
    logger.info("This info message should appear.")
    
    assert "This debug message should be filtered out." not in caplog.text
    assert "INFO: This info message should appear." in caplog.text
    assert logging.getLogger().level == logging.INFO

def test_setup_file_logging(file_log_config_file):
    """Tests setting up logging to a file."""
    config_path, log_file_path = file_log_config_file
    setup_general_logging(config_path)
    
    logger = logging.getLogger()
    test_message = "This should be written to the file."
    logger.info(test_message)
    
    # Force flush and close handlers
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()
            handler.close()
    
    # Check the log file exists and contains the message
    with open(log_file_path, 'r') as f:
        content = f.read()
    assert test_message in content
    assert os.path.exists(log_file_path)

def test_setup_combined_logging(combined_log_config_file, caplog):
    """Tests setting up combined console and file logging."""
    config_path, log_file_path = combined_log_config_file
    setup_general_logging(config_path)
    
    logger = logging.getLogger()
    debug_msg = "Debug message goes to file only."
    info_msg = "Info message goes to both console and file."
    
    logger.debug(debug_msg)
    logger.info(info_msg)
    
    # Force flush and close handlers
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()
            handler.close()
    
    # Check console output
    assert debug_msg not in caplog.text  # DEBUG filtered from console
    assert "INFO: " + info_msg in caplog.text
    
    # Check file output
    with open(log_file_path, 'r') as f:
        content = f.read()
    assert debug_msg in content
    assert info_msg in content

def test_setup_general_logging_nonexistent_file(temp_log_dir):
    """Tests loading a non-existent log config file."""
    non_existent_path = str(temp_log_dir / "non_existent.json")
    
    with pytest.raises(FileNotFoundError):
        setup_general_logging(non_existent_path)

def test_setup_general_logging_invalid_level(invalid_level_config_file):
    """Tests handling of invalid log level in config."""
    with pytest.raises(ValueError, match="Unknown level"):
        setup_general_logging(invalid_level_config_file)

def test_setup_general_logging_empty_config(temp_log_dir):
    """Tests loading an empty log config file."""
    config_path = temp_log_dir / "empty.json"
    with open(config_path, 'w') as f:
        json.dump({}, f)  # Empty JSON object
    
    with pytest.raises(ValueError):
        setup_general_logging(str(config_path))

def test_setup_general_logging_non_json_content(temp_log_dir):
    """Tests loading a file with non-JSON content."""
    config_path = temp_log_dir / "invalid.txt"
    with open(config_path, 'w') as f:
        f.write("this is not json")
    
    with pytest.raises(json.JSONDecodeError):
        setup_general_logging(str(config_path))

def test_setup_general_logging_creates_directory(temp_log_dir):
    """Tests if missing log directories are created."""
    new_subdir = temp_log_dir / "new_dir"
    log_file = new_subdir / "app.log"
    config_data = {
        "version": 1,
        "handlers": {
            "file": {
                "class": "logging.FileHandler",
                "filename": str(log_file),
                "level": "INFO"
            }
        },
        "root": {
            "handlers": ["file"],
            "level": "INFO"
        }
    }
    
    config_path = temp_log_dir / "create_dir_config.json"
    with open(config_path, 'w') as f:
        json.dump(config_data, f)
    
    # Verify directory doesn't exist yet
    assert not os.path.exists(new_subdir)
    
    setup_general_logging(str(config_path))
    logger = logging.getLogger()
    logger.info("Test message")
    
    # Force flush and close handlers
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()
            handler.close()
    
    # Directory and log file should now exist
    assert os.path.exists(new_subdir)
    assert os.path.exists(log_file)

def test_setup_general_logging_with_dict_config():
    """Tests setting up logging with a dictionary configuration."""
    config = {
        "version": 1,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "WARNING"
            }
        },
        "root": {
            "handlers": ["console"],
            "level": "WARNING"
        }
    }
    
    setup_general_logging(config=config)
    assert logging.getLogger().level == logging.WARNING
    assert len(logging.getLogger().handlers) == 1
    assert isinstance(logging.getLogger().handlers[0], logging.StreamHandler)
