from dataclasses import dataclass
from datetime import datetime
from ib_async import Contract
import asyncio

@dataclass
class TaskInfo:
    
    symbol: str
    expiry: str
    strike: int
    right: str
    start: datetime
    end: datetime
    bar_size: str

    req_count_for_contract: int = None
    len_date_range_iter: int = None
    
    data_logger: object = None
    return_bars: bool = True
    contract: object = None
    task: asyncio.Task = None

    @classmethod
    def FromContract(cls, c: Contract, start: datetime, end: datetime, bar_size: str):
        return cls(
            symbol=c.symbol,
            expiry=c.lastTradeDateOrContractMonth,
            strike=c.strike,
            right=c.right,
            start=start,
            end=end,
            bar_size=bar_size,
            contract=c
        )