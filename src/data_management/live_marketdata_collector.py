
import os
import logging
import datetime
import asyncio
import signal
from copy import deepcopy


class LiveMarketDataCollector:
    """
    Class to encapsulate the live market data collector functionality.
    This is not strictly necessary but helps in organizing the code.
    """

    def __init__(self,data_dir,ibkr):

        self.logger = logging.getLogger(__name__)  

        self.subscriptions_list = []
        self.contracts_list = []  
        self.data_dir = data_dir  
        self.shutdown_in_progress = False  
        self.ibkr = ibkr      


    async def shutdown(self, signal=None):
        """Handle graceful shutdown"""
        
        # Prevent multiple shutdown calls
        if self.shutdown_in_progress:
            self.logger.debug("Shutdown already in progress, ignoring duplicate call")
            return
        
        self.shutdown_in_progress = True
        
        if signal:
            self.logger.info(f"Received exit signal {signal.name}...")
        
        self.logger.info("Unsubscribing from market data...")
        for contract, _, _ in self.subscriptions_list:
            self.ibkr.unsubscribe_market_data(contract)

        self.logger.info("Disconnecting from IBKR...")
        self.ibkr.disconnect()
        
        self.logger.info("Shutdown complete.")

    async def start(self,contracts_list):
        self.subscriptions_list = []  # Reset subscriptions list for fresh start

        try:
            await self.ibkr.connect()  # Ensure connection to TWS or Gateway

            for i,contract in enumerate(contracts_list):
                symbol_filename = f"{contract.symbol}_{contract.lastTradeDateOrContractMonth}_{int(contract.strike)}_{contract.right}"
                data_logger = self.ibkr.get_marketdata_logger(symbol_filename,self.data_dir)
                market_data_subscription = await self.ibkr.subscribe_market_data(
                    contract,
                    callback=data_logger.log_Ticker
                )
                self.subscriptions_list.append((contract, deepcopy(market_data_subscription), deepcopy(data_logger)))
                self.logger.debug(f"Subscribed market data for {contract} to filename : {data_logger.handlers[0].baseFilename}")
                if i%25 == 0:
                    await asyncio.sleep(0.5)  # Avoid hitting IBKR rate limits, sleep for a second every 40 subscriptions
                

            self.logger.debug(f"Successfully subscribed to market data for {contract.localSymbol}")

        except Exception as e:
            # Handle initial connection failure
            self.logger.error(f"Failed to start market data subscription: {e}")
            return  # Exit the main function if we cannot start
        
        self.logger.info(f"Subscribed to market data for {len(self.subscriptions_list)} contracts.")
        return 

    async def keep_alive(self,sleep_interval=60):
        try:
            # Keep the program running until interrupted
            while not self.shutdown_in_progress:
                # Optionally check connection status
                if not self.ibkr.is_connected():
                    self.logger.warning("Connection to IBKR lost. Attempting to reconnect...")

                    self.start(self.contracts_list)

                else:
                    self.logger.info("IBKR connection is active.")
                await asyncio.sleep(sleep_interval) # sleep for a minute
        except asyncio.CancelledError:
            self.logger.info("Main task cancelled")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        finally:
            await self.shutdown()

    async def run(self,list_of_contracts):
        # start subscribing to market data
        self.contracts_list = list_of_contracts
        
        await self.start(self.contracts_list)
            
            # Setup signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig, 
                lambda sig=sig: asyncio.create_task(self.shutdown(sig))           
            )
        
        await self.keep_alive()


