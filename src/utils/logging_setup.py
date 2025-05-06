'''
All Gemini 2.5 Pro 
'''

import logging
import logging.config
import os


def setup_general_logging(
    log_dir: str,
    level=logging.INFO,
    log_to_console: bool = True,
    log_to_file: bool = True,
    log_filename: str = "general.log"
):
    """
    Sets up general message logging (DEBUG, INFO, WARNING, ERROR, CRITICAL).

    Configures the root logger to handle messages.

    Args:
        log_dir (str): Directory for the general log file.
        level: Minimum logging level to process (e.g., logging.DEBUG, logging.INFO).
        log_to_console (bool): If True, log messages to stdout.
        log_to_file (bool): If True, log messages to a rotating file (general.log).
    """
    if not log_to_console and not log_to_file:
        print("Warning: General logging is disabled (neither console nor file selected).")
        logging.getLogger().addHandler(logging.NullHandler()) # Add NullHandler to avoid "no handler" warnings
        return

    created_dir = False
    if log_to_file and log_dir:
        try:
            os.makedirs(log_dir, exist_ok=False)
            created_dir = True
        except OSError as e:
            created_dir = False
    
    handlers = {}
    root_handlers = []

    if log_to_console:
        handlers["console"] = {
            "class": "logging.StreamHandler",
            "level": level, # Use the overall level for console
            "formatter": "console_formatter",
            "stream": "ext://sys.stdout"
        }
        root_handlers.append("console")

    if log_to_file:

        handlers["general_file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": level, # Use the overall level for the file
            "formatter": "file_formatter",
            "filename": os.path.join(log_dir, log_filename),
            "maxBytes": 10485760, # 10MB
            "backupCount": 5,
            "encoding": "utf8"
        }
        root_handlers.append("general_file")

    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "console_formatter": {
                "format": "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S %z"
            },
            "file_formatter": {
                "format": "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S %z"
            }
        },
        "handlers": handlers,
        "root": {
            "level": level,
            "handlers": root_handlers
        },
        # Set specific loggers to only show warnings and errors
        "loggers": {
            "ib_async": {"level": "WARNING", "propagate": True},
            "asyncio": {"level": "WARNING", "propagate": True}
        }
    }

    try:
        logging.config.dictConfig(log_config)
        import json
        logging.getLogger(__file__).info(f"General logging configured successfully.")
        logging.getLogger(__file__).debug(f"Logging config: {json.dumps(log_config,indent=4)}")
    except Exception as e:
        logging.basicConfig(level=level)
        logging.getLogger(__file__).exception(f"Error configuring general logging: {e}. Falling back to basicConfig.")
    if created_dir:
        logging.getLogger(__file__).warning(f"General logging initialized with new directory. Log directory: {log_dir}. Created: {created_dir}")

