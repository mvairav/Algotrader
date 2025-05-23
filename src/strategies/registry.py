"""
Registry for Strategy Classes.

Maps strategy names (strings) to their corresponding class objects.
This avoids using globals() and makes strategy loading more explicit.
"""
import logging

# Import all available strategy classes
from .macd_strategy import MACDStrategy
from .macd_crossover_strategy import MACDCrossOverStrategy


log = logging.getLogger(__name__)

STRATEGY_REGISTRY = {
    "MACDStrategy": MACDStrategy,
    "MACDCrossOverStrategy": MACDCrossOverStrategy,  
}

def get_strategy_class(strategy_name: str):
    """
    Retrieves a strategy class from the registry by name.

    Args:
        strategy_name: The name of the strategy class.

    Returns:
        The strategy class object if found, otherwise None.
    """
    strategy_class = STRATEGY_REGISTRY.get(strategy_name)
    if strategy_class is None:
        log.error(f"Strategy class '{strategy_name}' not found in registry.")
    return strategy_class

log.info(f"Strategy Registry initialized with keys: {list(STRATEGY_REGISTRY.keys())}")
