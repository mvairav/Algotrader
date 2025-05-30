from ib_async import IB, Contract, MarketOrder, util, BarDataList, Ticker, Index, Option, Order, Trade, Fill, TimeCondition
import pandas as pd
import asyncio
import time
import logging
import datetime

from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo  
import os 
import threading

# Configure logger

class IBKRHelper:
    TWS_PORT = 7497  # Default TWS port
    IBGATEWAY_PORT = 4002  # Default IBGateway port

    def __init__(self,market_timezone_str="UTC",default_timeout=60,rich_progress=None):
        """
        Initialize the IBHelper class with connection state variables.
        
        Parameters:
        - market_timezone_str: String representation of the timezone (default: "UTC")
        - default_timeout: Default timeout for IB connection (default: 60 seconds)
        """
        self._ib_instance = None
        self.client_id = None
        self.port = None
        self.ip_address = None  
        self._last_used_timestamp = 0
        self.total_calls = {"concurrent": 0}
        self.max_calls = {"small_bar": 57, "others":30, "same_contract": 4}
        self.concurrent_calls = {"small_bar": 10, "others":30}
        self.semaphors = {
                         "small_bar": asyncio.Semaphore(1),
                         "others": asyncio.Semaphore(1),
                         "concurrent": asyncio.Semaphore(1),
                         }
        self._ibkr_reqid_lock = asyncio.Lock()
        self._criticial_section = asyncio.Lock()
        self._last_used_timestamp = {
            "small_bar": [],
            "others": [],
            "concurrent": []
        }
        self.call_interval_in_secs = {"small_bar": 600, "others": 60, "concurrent": 60, "same_contract":3}
        #self.wait_time_secs = 600
        self.soft_rate_limit= 25

        self.next_call_will_wait = False #Not reliable

        self.default_timeout = default_timeout
        self.rich_progress = rich_progress
        # Use the name of the module for the logger, which will be properly configured
        # when setup_general_logging() is called
        self.logger = logging.getLogger(__name__)
    
        # Set timezone for the instance
        try:
            self.market_timezone = ZoneInfo(market_timezone_str)
        except Exception as e:
            # Fallback to UTC if the timezone string is invalid
            self.logger.warning(f"Invalid timezone '{market_timezone_str}' provided, falling back to UTC. Error: {e}")
            self.market_timezone = ZoneInfo("UTC")

        from tzlocal import get_localzone
        self.local_timezone = get_localzone()

        #Create queues and events
        self._completed_trade_queues = {}
        self._active_exit_orders = {}
        self._trade_events = {}
        self.callback_for_ocagroup = {}

        # Test logging initialization
        self.logger.debug(f"IBKRHelper instance created with timezone: {self.market_timezone}," \
                          f" current time is {datetime.datetime.now(self.market_timezone)}, with timeout = {self.default_timeout} seconds")
        self._data_loggers = {}  

        self._current_subscriptions = {}
        self.last_error = []

        self.contract_disconnections = {}

    def __del__(self):
        """
        Destructor to clean up the IBKRHelper instance.
        """
        try:
            if self._ib_instance and self._ib_instance.isConnected():
                self.disconnect()
        except Exception as e:
            logging.getLogger(__name__).error(f"Error during cleanup: {e}")
        finally:
            self._ib_instance = None
            logging.getLogger(__name__).debug("IBKRHelper instance destroyed")
    
    async def connect(self, ip_address="127.0.0.1", port=IBGATEWAY_PORT, client_id=14, force_new=False):
        """
        Connect to Interactive Brokers TWS/Gateway.
        
        Parameters:
        - ip_address: IB Gateway/TWS IP address
        - port: IB Gateway/TWS port
        - client_id: Client ID for IB connection
        - force_new: Force a new connection even if one exists
        
        Returns:
        - True if connection successful, False otherwise
        """
        # Check if we already have a valid connection
        if self._ib_instance is not None and self._ib_instance.isConnected() and not force_new:
            self.logger.debug("Already connected to IB")
            return True
            
        # Disconnect existing connection if any
        if self._ib_instance is not None and self._ib_instance.isConnected():
            try:
                self._ib_instance.disconnect()
                self.logger.info("Disconnected existing IB connection")
            except Exception as e:
                self.logger.error(f"Error disconnecting: {e}")
        
        # Create new connection
        try:
            self._ib_instance = IB()
            if self.client_id is None:
                self.client_id = client_id
            if self.port is None:
                self.port = port
            if self.ip_address is None:
                self.ip_address = ip_address
            await self._ib_instance.connectAsync(self.ip_address, self.port, clientId=self.client_id)
            self.logger.info(f"Successfully connected to IB at {ip_address}:{port}")
            self._ib_instance.orderStatusEvent += self.handle_order_status
            self._ib_instance.execDetailsEvent += self.handle_exec_details
            self._ib_instance.commissionReportEvent += self.handle_commission_details
            self._ib_instance.errorEvent += self.handle_error_details
            return True
        except Exception as e:
            self.logger.fatal(f"Failed to connect to IB: {e}")
            self._ib_instance = None
            return False

    def is_connected(self):
        """
        Check if the IBKR instance is connected.
        
        Returns:
        - True if connected, False otherwise
        """
        if self._ib_instance is not None and self._ib_instance.isConnected():
            return True
        else:
            self.logger.debug("IBKR instance is not connected")
            return False

    async def get_req_id(self):
        """
        Get a unique request ID for IBKR API calls.
        
        Returns:
        - Unique request ID
        """
        async with self._ibkr_reqid_lock:
            if self._ib_instance is not None:
                return self._ib_instance.client.getReqId()
            else:
                self.logger.error("IBKR instance is not connected, cannot get request ID")
                return None

    async def get_trading_hours(self, contract,contract_tzone=None,cdetails=None):
        """
        Get the trading hours for a given contract.
        
        Parameters:
        - contract: The contract object
        
        Returns:
        - Trading hours as a string
        """
        if not self.is_connected():
            self.logger.error("Cannot get trading hours: Not connected to IB")
            return None
        
        try:
            if contract_tzone is None:
                cdetails = await self._ib_instance.reqContractDetailsAsync(contract)
                contract_tzone = ZoneInfo(cdetails[0].timeZoneId)
            if cdetails is None:
                cdetails = await self._ib_instance.reqContractDetailsAsync(contract)
            start_time, end_time = [datetime.datetime.strptime(d, "%Y%m%d:%H%M").replace(tzinfo=contract_tzone) for d in cdetails[0].tradingHours.split(";")[0].split("-")]
            self.logger.debug(f"Trading hours for {contract.localSymbol}: {start_time} - {end_time}")
            return start_time, end_time
        except Exception as e:
            self.logger.error(f"Error getting trading hours: {e}")
            return None, None

    async def is_market_open(self,contract):
        """
        Check if the markets are open based on the current time and IBKR instance's timezone.
        
        Returns:
        - True if markets are open, False otherwise
        """
        if not self.is_connected():
            self.logger.error("Cannot check market status: Not connected to IB")
            return False
        
        cdetails = await self._ib_instance.reqContractDetailsAsync(contract)
        contract_tzone = ZoneInfo(cdetails[0].timeZoneId)
        start_time, end_time = await self.get_trading_hours(contract,contract_tzone=contract_tzone)
        now = datetime.datetime.now(contract_tzone)
        
        if start_time < now and now < end_time:
            self.logger.debug("Markets are open")
            return True
        else:
            self.logger.debug("Markets are closed")
            return False
            

    def disconnect(self):
        """
        Disconnect from Interactive Brokers.
        
        Returns:
        - True if disconnection successful or no connection existed, False if error
        """
        if self._ib_instance is not None and self._ib_instance.isConnected():
            try:
                if hasattr(self._ib_instance, 'execDetailsEvent') and self.handle_exec_details in self._ib_instance.execDetailsEvent:
                    self._ib_instance.execDetailsEvent -= self.handle_exec_details
                if hasattr(self._ib_instance, 'orderStatusEvent') and self.handle_order_status in self._ib_instance.orderStatusEvent:
                    self._ib_instance.orderStatusEvent -= self.handle_order_status
                if hasattr(self._ib_instance, 'errorEvent') and self.handle_error_details in self._ib_instance.errorEvent:
                    self._ib_instance.errorEvent -= self.handle_error_details
                if hasattr(self._ib_instance, 'commissionReportEvent') and self.handle_commission_details in self._ib_instance.commissionReportEvent:
                    self._ib_instance.commissionReportEvent -= self.handle_commission_details

                self._ib_instance.disconnect()
                self.logger.info("Disconnected from IB")
                self._ib_instance = None
                return True
            except Exception as e:
                self.logger.error(f"Error disconnecting from IB: {e}")
                return False
        else:
            self.logger.info("No active IB connection to disconnect")
            self._ib_instance = None
            return True

    async def handle_order_status(self, trade: Trade):
        try:
        
            # Check if this status update is for our initial market buy order
            if trade.orderStatus.status == 'Filled':
                self.logger.info(f"!IBKR On Status Change >> Order {trade.order.orderId} ref: {trade.order.orderRef} for contract {trade.contract.localSymbol} "
                                f"FILLED at {trade.orderStatus.avgFillPrice} for {trade.orderStatus.filled} shares.")
                if trade.order.ocaGroup in self._active_exit_orders:
                    self._active_exit_orders[trade.order.ocaGroup].discard(trade.order.orderId)   
                    if len(self._active_exit_orders[trade.order.ocaGroup]) == 0:
                        del self._active_exit_orders[trade.order.ocaGroup]
                        self.logger.info(f"All exit orders in group {trade.order.ocaGroup} completed.")

            elif trade.orderStatus.status in ['Cancelled', 'ApiCancelled', 'Inactive', 'Rejected']:

                self.logger.info(f"!IBKR On Status Change >> Order {trade.order.orderId} ref: {trade.order.orderRef} for contract {trade.contract.localSymbol} "
                                    f" {trade.orderStatus.status} with status {trade.orderStatus.status}.")
                if trade.order.ocaGroup in self._active_exit_orders:
                    self._active_exit_orders[trade.order.ocaGroup].discard(trade.order.orderId)   
                    if len(self._active_exit_orders[trade.order.ocaGroup]) == 0:
                        del self._active_exit_orders[trade.order.ocaGroup]
                        self.logger.info(f"All exit orders in group {trade.order.ocaGroup} completed.")

            elif trade.orderStatus.status == 'Submitted' or trade.orderStatus.status == 'PreSubmitted':
                
                self.logger.info(f"!IBKR On Status Change >> Order {trade.order.orderId} ref: {trade.order.orderRef} for contract {trade.contract.localSymbol} "
                                f"Submitted with status {trade.orderStatus.status}.")
            else:
                self.logger.info(f"!IBKR On Status Change >> Order {trade.order.orderId} ref: {trade.order.orderRef} for contract {trade.contract.localSymbol} "
                                f"status: {trade.orderStatus.status}.")
            
            if trade.order.orderId in self._completed_trade_queues:
                await self._completed_trade_queues[trade.order.orderId].put((trade.orderStatus.status,trade))

                


        except Exception as e:
            self.logger.error(f"Error handling order status: {e}")
            await self._completed_trade_queues[trade.order.orderId].put((None,None))
            
            
            


    async def handle_exec_details(self,trade: Trade, fill: Fill):
        # This provides details about each fill (execution)
        try:
            self.logger.info(f"!IBKR On Execution >> Order {trade.order.orderId} ref: {trade.order.orderRef} for contract {trade.contract.localSymbol} "
                          f" FILLED at {fill.execution.avgPrice} for {fill.execution.shares} shares.")
        except Exception as e:
            self.logger.error(f"Error handling execution details: {e}")
        
    async def handle_commission_details(self,trade: Trade, fill:Fill, commission_report):

        try:

            if trade.order.orderId in self._completed_trade_queues:
                await self._completed_trade_queues[trade.order.orderId].put(("Commission",trade))

            self.logger.info(f"!IBKR On Commission >> Order {trade.order.orderId} ref: {trade.order.orderRef} for contract {trade.contract.localSymbol} "
                            f" Commission report: {commission_report.commission} {commission_report.currency} ")
            
            if trade.order.ocaGroup in self.callback_for_ocagroup:
                self.logger.info(f" Calling callback for OCA group {trade.order.ocaGroup} and contract {trade.contract.localSymbol}")
                #check if callback function is async
                if asyncio.iscoroutinefunction(self.callback_for_ocagroup[trade.order.ocaGroup]):
                    await self.callback_for_ocagroup[trade.order.ocaGroup](trade)
                else:
                    self.callback_for_ocagroup[trade.order.ocaGroup](trade)
                del self.callback_for_ocagroup[trade.order.ocaGroup]
        except Exception as e:
            self.logger.error(f"Error handling commission details: {e}")
            

    async def handle_error_details(self, reqId, errorCode, errorString, contract=None,selfcall=False):
        """Handles errors from IB."""
        # Catch informational messages (like connection confirmations) and error messages (data disconnections)
        # See https://interactivebrokers.github.io/tws-api/message_codes.html
        info_codes = {2103, 2104, 2105, 2106, 2107, 2108, 2158}
        if errorCode in info_codes:
            if not selfcall:
                self.logger.info(f"IB Info [{errorCode}]: {errorString}. reqId: {reqId}. Live subscriptions may be delayed or closed.")
                self.last_error.append((datetime.datetime.now(self.market_timezone),errorCode, errorString))

                #2103, 2104 and 2108 are related to market data subscriptions. 

                if errorCode in {2103, 2104, 2108}:
                    if errorCode == 2103: 
                        self.logger.warning(f"!IB Error: Live data subscription could have been closed. ReqId: {reqId}, Code: {errorCode}, Msg: {errorString}, Contract: {contract}.")
                        self.logger.info(f"Waiting for 2 secs before re-subscribing to live bars for contract {contract}...")
                        self.contract_disconnections[contract] = 2103
                        await asyncio.sleep(2)
                        if contract not in self.contract_disconnections:
                            self.logger.info(f"Contract has been reconnected")
                            return
                        '''
                            Wait for 2 secs and call back again to resubscribe. 
                            If reconnected within 2 secs, conract will be removed from contract_disconnections.
                            As GIL lock guarantees that only one thread can execute Python bytecode at a time.
                            Weird logic but will probably work as most disconnections get resolved within 2 secs.
                        '''
                        await self.handle_error_details(reqId, errorCode, errorString, contract=contract, selfcall=True)
                    elif errorCode == 2104:
                        self.logger.warning(f"!IB Error: Live data subscription could have been re-instated. ReqId: {reqId}, Code: {errorCode}, Msg: {errorString}, Contract: {contract}.")
                        if contract in self.contract_disconnections:
                            del self.contract_disconnections[contract]
                            return
                    elif errorCode == 2108:
                        return
                if contract in self.contract_disconnections:
                    for c,value in self._current_subscriptions.items():
                        if c != contract: continue
                        self.logger.info(f"Re-subscribing to live bars for contract {contract}...")
                        if value[0] == "live_bars":
                            #This will also unsubscribe and update the subscription 
                            await self.subscribe_live_bars(contract=c, callback=value[2], bar_size=value[1].barSize,
                                                        what_to_show=value[1].whatToShow, use_rth=value[1].useRTH, only_last_row=value[3])
                        elif value[0] == "live_marketdata":
                            #This will also unsubscribe and update the subscription 
                            await self.subscribe_market_data(contract=c, callback=value[2])
                        del self.contract_disconnections[contract]

        else:
            
            for ocaGroup in self._active_exit_orders:
                if reqId in self._active_exit_orders[ocaGroup]: 
                    self._active_exit_orders[ocaGroup].discard(reqId)
                    if len(self._active_exit_orders[ocaGroup]) == 0:
                        del self._active_exit_orders[ocaGroup]
                        self.logger.info(f"All exit orders in group {ocaGroup} completed.")
                    break
                else:
                    self.logger.error(f"IB Error! ReqId: {reqId}, Code: {errorCode}, Msg: {errorString}, Contract: {contract}")
    
    
    async def get_contract(self, data_params):
        """
        Get a contract object from IB based on provided parameters.
        
        Parameters:
        - data_params: Dictionary containing contract parameters
        
        Returns:
        - Contract object if successful, None otherwise
        """
        self.logger.debug(f"Getting contract with params: {data_params}")
        
        if not self._ib_instance or not self._ib_instance.isConnected():
            self.logger.error("Not connected to IB")
            return None

        try:
            # Create the contract
            contract = Contract(
                symbol=data_params['symbol'], 
                secType=data_params['secType'], 
                exchange=data_params['exchange'],
                currency=data_params['currency'], 
                lastTradeDateOrContractMonth=data_params['lastTradeDateOrContractMonth'],
                strike=data_params['strike'], 
                right=data_params['right'],
                multiplier=data_params['multiplier']
            )
            
            self.logger.debug(f"Created contract object: {contract}")
            
            # Qualify the contract
            self.logger.debug("Qualifying contract...")
            qualified_contracts = await self._ib_instance.qualifyContractsAsync(contract)
            
            if not qualified_contracts:
                self.logger.error("Contract qualification returned empty result")
                return None
                
            self.logger.info(f"Successfully retrieved contract: {qualified_contracts[0]}")
            return qualified_contracts[0]
        except KeyError as ke:
            self.logger.error(f"Missing required parameter in data_params: {ke}")
            return None
        except Exception as e:
            self.logger.error(f"Error getting contract: {e}")
            return None

    async def get_option_chains_new(self, symbol="NIFTY50", exchange="NSE",currency="INR", 
                                    strike_price_range=None, expiry_months=None, expiry_weeks=None):
        '''
        Retrieve option chains for a given symbol from Interactive Brokers.
        Parameters:
        - symbol: The symbol for which to retrieve option chains (default: "NIFTY50")
        - exchange: The exchange to query (default: "NSE")
        - currency: The currency of the contract (default: "INR")
        - strike_price_range: Range of strike prices to filter (default: None)
        - expiry_months: Number of months to filter for expiry dates (default: None)
        - expiry_weeks: Used only if months is None. Number of weeks to filter for expiry dates (default: None)
        Returns:
        - list of contracts if successful, None otherwise
        '''
        try:
            index = Index(symbol=symbol, exchange=exchange, currency=currency)
            self.logger.debug(f"Qualifying index contract for symbol={symbol}...")
            await self._ib_instance.qualifyContractsAsync(index)
        

            
            if strike_price_range is not None:
            
                self.logger.debug(f"Find last close price for symbol: {symbol}")

                from datetime import timedelta

                if not await self.is_market_open(index):
                    #Request historical data for past 5 days (just in case)
                    start_date = (datetime.datetime.now(tz=self.market_timezone)-timedelta(days=5)).replace(hour=9,minute=15,second=0,microsecond=0,tzinfo=self.market_timezone)
                    end_date = (datetime.datetime.now(tz=self.market_timezone)+timedelta(days=1)).replace(hour=16,minute=0,second=0,microsecond=0,tzinfo=self.market_timezone)  
                    bars = await self.get_historical_bars(index,start_date,end_date,bar_size="1 day",return_bars=True)
                    if bars is None or len(bars) == 0:
                        self.logger.error(f"No historical bars received for {index}")
                        return None
                    market_price = bars[-1].close
                else:
                    #Request todays data
                    start_date = datetime.datetime.now(tz=self.market_timezone).replace(hour=9,minute=15,second=0,microsecond=0)
                    end_date = datetime.datetime.now(tz=self.market_timezone).replace(hour=16,minute=0,second=0,microsecond=0)
                    bars = await self.get_historical_bars(index,start_date,end_date,bar_size="5 secs",return_bars=True)
                    if bars is None or len(bars) == 0:
                        self.logger.error(f"No historical bars received for {index}")
                        return None
                    market_price = bars[0].close

                    self.logger.info(f"Today's close price for symbol={symbol} at {bars[0].date} is {market_price}")
                

            option = Option(symbol, '', 0, '', exchange)

            self.logger.debug(f"Requesting all partial options for Option : {option}")
            #This retrieves all partial matches for the option chain
            contract_details = await self._ib_instance.reqContractDetailsAsync(option)

            self.logger.debug(f"Received {len(contract_details)} contract details for symbol={symbol}")

            contract_details = [    
                                    c 
                                    for c in contract_details if c.contract.exchange == exchange and c.contract.currency == currency 
                                    and c.contract.secType == 'OPT' and c.contract.symbol == symbol
                                ]

                 
            #The for loops are not optimized but the reqContractDetails is very fast compared to reqSecDef so ok for now
            valid_contracts_pos = { i for i in range(len(contract_details)) }
            
            if strike_price_range is None and expiry_months is None and expiry_weeks is None:
                self.logger.debug("No strike price range or expiry months specified, returning all valid chains.")
            
            if expiry_months is not None:
                self.logger.debug(f"Filtering by expiry months: {expiry_months}")
                end_date = datetime.datetime.now() + relativedelta(months=expiry_months)
                # Filter by expiry months only
                for i,c in enumerate(contract_details):
                    d = datetime.datetime.strptime(c.contract.lastTradeDateOrContractMonth,"%Y%m%d")
                    if (d.year, d.month) > (end_date.year, end_date.month):
                        valid_contracts_pos.discard(i)
            elif expiry_weeks is not None:
                self.logger.debug(f"Filtering by expiry weeks: {expiry_weeks}")
                end_date = datetime.datetime.now() + relativedelta(weeks=expiry_weeks)
                # Filter by expiry weeks only
                for i,c in enumerate(contract_details):
                    d = datetime.datetime.strptime(c.contract.lastTradeDateOrContractMonth,"%Y%m%d")
                    if (d.year, d.month, d.day) > (end_date.year, end_date.month, end_date.day):
                        valid_contracts_pos.discard(i)

            self.logger.debug(f"Valid contracts after expiry filter: {len(valid_contracts_pos)}")

            if strike_price_range is not None:
                self.logger.debug(f"Filtering by strike price range: +/- {strike_price_range}")
                # Filter by strike price range only
                for i,c in enumerate(contract_details):
                    s = c.contract.strike
                    if not (s-strike_price_range <= market_price <= s+strike_price_range):
                        valid_contracts_pos.discard(i)
            # else:
            #     self.logger.debug(f"Filtering by strike price range: +/- {strike_price_range} and expiry months: {expiry_months}")
            #     end_date = datetime.datetime.now() + relativedelta(months=expiry_months)
            #     for c in contract_details:
            #         s = c.contract.strike
            #         d = datetime.datetime.strptime(c.contract.lastTradeDateOrContractMonth,"%Y%m%d")
            #         if s-strike_price_range <= market_price <= s+strike_price_range \
            #             and (d.year, d.month) <= (end_date.year, end_date.month):
            #                 valid_contracts_pos.append(c.contract)

        except Exception as e:
            self.logger.error(f"Error retrieving option chains for {symbol}: {e}")
            return None

        self.logger.info(f"Retrieved {len(valid_contracts_pos)} from option chains for symbol={symbol}, strike_range= +/- {strike_price_range}," 
                         f"expiry month = {expiry_months} and expiry week = {expiry_weeks}.")
        valid_contracts = [contract_details[i].contract for i in valid_contracts_pos]
        
        if len(valid_contracts)>0: valid_contracts.sort(key=lambda x: x.conId)
        
        self.logger.info(f"Retrieved contracts: \n {valid_contracts}")
        return valid_contracts

    async def get_option_chains(self, symbol="NIFTY50", exchange="NSE",currency="INR", strike_price_range=None, expiry_months=None, expiry_weeks=None):
        '''
        Retrieve option chains for a given symbol from Interactive Brokers.
        Parameters:
        - symbol: The symbol for which to retrieve option chains (default: "NIFTY50")
        - exchange: The exchange to query (default: "NSE")
        - currency: The currency of the contract (default: "INR")
        - strike_price_range: Range of strike prices to filter from last days closing price (default: None)
        - expiry_months: Number of months to filter for expiry dates (default: None)
        - expiry_weeks: Used only if months is None. Number of weeks to filter for expiry dates (default: None)
        Returns:
        - list of contracts if successful, None otherwise
        '''
        try:
            index = Index(symbol=symbol, exchange=exchange, currency=currency)

            self.logger.debug(f"Qualifying index contract for symbol={symbol}...")
            await self._ib_instance.qualifyContractsAsync(index)
            
            self.logger.debug(f"Requesting chains for index : {index}")
            chains = await self._ib_instance.reqSecDefOptParamsAsync(index.symbol, '', index.secType, index.conId)
            
            from datetime import timedelta

            #Request historical data for past 5 days (just in case)
            start_date = (datetime.datetime.now()-timedelta(days=5)).replace(hour=9,minute=15,second=0,microsecond=0,tzinfo=self.market_timezone)
            end_date = (datetime.datetime.now()-timedelta(days=1)).replace(hour=16,minute=0,second=0,microsecond=0,tzinfo=self.market_timezone)  
            bars = await self.get_historical_bars(index,start_date,end_date,bar_size="1 day",return_bars=True)

            if bars is None or len(bars) == 0:
                self.logger.error(f"No historical bars received for {index}")
                return None
            market_price = bars[-1].close

            self.logger.debug(f"Calculating strike prices for symbol={symbol} at price={market_price}...")

            if strike_price_range is None:
                strike_prices = chains[0].strikes
            else:
                strike_prices = [i  for i in chains[0].strikes if i-strike_price_range <= market_price <= i+strike_price_range ]

            self.logger.debug(f"Calculating expirations for symbol={symbol} at price={market_price}...")


            if expiry_months is not None:
                end_date = datetime.datetime.now() + relativedelta(months=expiry_months)
                exp_as_dates = [datetime.datetime.strptime(i,"%Y%m%d") for i in chains[0].expirations]  
                expirations = [datetime.datetime.strftime(d,"%Y%m%d") for d in exp_as_dates if (d.year, d.month) <= (end_date.year, end_date.month)]
            elif expiry_weeks is not None:
                end_date = datetime.datetime.now() + relativedelta(weeks=expiry_weeks)
                exp_as_dates = [datetime.datetime.strptime(i,"%Y%m%d") for i in chains[0].expirations]  
                expirations = [datetime.datetime.strftime(d,"%Y%m%d") for d in exp_as_dates if (d.year, d.month, d.day) <= (end_date.year, end_date.month, end_date.day)]
            else:
                expirations = chains[0].expirations

            rights = ['P', 'C']

            contracts = [Option(symbol, expiration, strike, right, exchange, tradingClass=chains[0].tradingClass)
                            for right in rights
                            for expiration in expirations
                            for strike in strike_prices
                        ]
            self.logger.info(f"Total possible strikes prices = {len(strike_prices)}, expirations = {len(expirations)} and option chains = {len(contracts)}")
            self.logger.info(f"Finding valid options contract for symbol={symbol}...")
        
            from rich.progress import track
            for i in track(range(0, len(contracts), self.soft_rate_limit)):
                await self._ib_instance.qualifyContractsAsync(*contracts[i:i+self.soft_rate_limit])
                await asyncio.sleep(0.25)  # Sleep to avoid hitting IB's rate limits on qualifying contracts
            validated_contracts = []
            for contract in contracts:
                if contract.conId > 0:
                    validated_contracts.append(contract)
            contracts = validated_contracts
        except Exception as e:
            self.logger.error(f"Error retrieving option chains for {symbol}: {e}")
            return None

        self.logger.info(f"Retrieved {len(contracts)} from option chains for symbol={symbol}, strike_range= +/- {strike_price_range}, expiry month = {expiry_months}.")
        if len(contracts)>0: contracts.sort(key=lambda x: x.conId)

        return contracts

    async def rate_limit_wait(self,for_historical_bar_data=False, bar_size="30 secs"):
        """
        Check if the rate limit for IBKR API is reached.
        """
        if not self._ib_instance or not self._ib_instance.isConnected():
            self.logger.error("Not connected to IB")
            return False


        if for_historical_bar_data:

            alowed_calls = 0
            semaphor = None
            interval_seconds = 0

            if bar_size == "30 secs" or bar_size == "5 secs" :
                allowed_calls = self.max_calls["small_bar"]
                semaphor = self.semaphors["small_bar"]
                interval_seconds = self.call_interval_in_secs["small_bar"]
                last_used_timestamp = self._last_used_timestamp["small_bar"]
            else:
                allowed_calls = self.max_calls["others"]
                semaphor = self.semaphors["others"]
                interval_seconds = self.call_interval_in_secs["others"]
                last_used_timestamp = self._last_used_timestamp["others"]

            self.logger.debug(f"Rate limit check for bar_size={bar_size}, current_calls={len(last_used_timestamp)} allowed_calls={allowed_calls}")  
            try :
                await semaphor.acquire()
                self.logger.debug(f"Semaphore acquired for bar_size={bar_size}")

                if len(last_used_timestamp) > allowed_calls:
                        
                    curr_timestamp = datetime.datetime.now()
                    first_used = last_used_timestamp.pop(0)
                    last_used_timestamp.append(curr_timestamp)

                    time_diff = max((curr_timestamp - first_used).total_seconds(),0)
                    self.logger.debug(f"Current timestamp: {curr_timestamp}, first used timestamp: {first_used}," 
                                f"time_diff: {time_diff}, max_interval: {interval_seconds}, curr_calls: {len(last_used_timestamp)}, allowed_calls: {allowed_calls}")    
                    
                    if time_diff > interval_seconds:
                        return
                    
                    sleep_timer = int(interval_seconds - time_diff)
                    self.logger.warning(f"Reached Rate limit. Sleeping for {sleep_timer} seconds")
                    if self.rich_progress:
                        wait_task = self.rich_progress.add_task(description="[bold green]IBKR", total=sleep_timer, completed=0, value="Rate limit (waiting).")
                        for i in range(sleep_timer):
                            await asyncio.sleep(1)
                            self.rich_progress.update(wait_task,advance=1)
                        self.rich_progress.remove_task(wait_task)
                    else:
                        await asyncio.sleep(sleep_timer)

                else:
                    last_used_timestamp.append(datetime.datetime.now())
            finally:
                semaphor.release()
            

        return 

    ## === Historical Data Methods === ##

    async def get_historical_bars(self, contract, start_date, end_date, bar_size="5 mins",
                               whatToShow="TRADES", useRTH=True, buffer_days=0, return_bars=False, timeout=None):
        """
        Fetch historical bars from Interactive Brokers for a given contract.
        Limitations:
        - https://ibkrcampus.com/campus/ibkr-api-page/twsapi-doc/#historical-pacing-limitations
        - Making identical historical data requests within 15 seconds.
        - Making six or more historical data requests for the same Contract, Exchange and Tick Type within two seconds.
        - Making more than 60 requests within any ten minute period.
        Parameters:
        - contract: The contract object to fetch data for
        - start_date: Start date for historical data (datetime object will be converted to market timezone)
        - end_date: End date for historical data (datetime object will be converted to market timezone)
        - bar_size: Size of the bars (e.g., "5 mins", "1 day")
        - whatToShow: Type of data to retrieve (e.g., "TRADES")
        - useRTH: Use regular trading hours (True/False)
        - buffer_days: Extra days to add to the duration for buffer (to avoid missing data)
        - return_bars: If True, returns BarDataList object instead of DataFrame
        - timeout: Timeout for the request (default is None, which uses default_timeout)
        Returns:
        - DataFrame containing historical bars if successful, None otherwise
        """
        self.logger.debug("Fetching historical data...")
        
        # Ensure we have a connection - connect() handles all the checks internally
        if not await self.connect():
            self.logger.error("Failed to connect to IB")
            return None
        
       
        # Qualify the contract
        try:
            await self._ib_instance.qualifyContractsAsync(contract)
        except Exception as e:
            self.logger.error(f"Error qualifying contract: {e}")
            return None
        
        if end_date < start_date:
            self.logger.error("End date cannot be earlier than start date")
            return None

        # Change to market timezone 
        end_date = end_date.astimezone(self.market_timezone)  
        start_date = start_date.astimezone(self.market_timezone)  

        # Calculate duration in days
        duration_days = (end_date - start_date).days+1 # Add one for covering start and end date
        duration_str = f"{duration_days + buffer_days} D"  
        end_date_str = end_date.strftime("%Y%m%d %H:%M:%S")  + " " + self.market_timezone.key

        _timeout = self.default_timeout if timeout is None else timeout
        # Request historical data
        try:

            await self.rate_limit_wait(for_historical_bar_data=True, bar_size=bar_size)

            self.logger.debug(f"Requesting historical data for contract={contract}, end_date={end_date_str}, duration_str={duration_str}, bar_size={bar_size}, whatToShow={whatToShow}, useRTH={useRTH}")
            bars = await self._ib_instance.reqHistoricalDataAsync(
                contract,
                endDateTime=end_date_str,
                durationStr=duration_str,
                barSizeSetting=bar_size,
                whatToShow=whatToShow,
                useRTH=useRTH,
                timeout=_timeout
            )
            self.logger.debug(f"Received {len(bars) if bars else 0} historical bars for contract={contract}")   

            if not bars:
                self.logger.warning(f"No historical bars received for {contract}")
                return []  # Return empty list instead of pd.Dataframe (faster?)
                
            self.logger.debug("Historical data fetched successfully.")
    
            if return_bars:
                self.logger.debug("Returning BarDataList object")
                corrected_bars = []
                if "sec" in bar_size or "min" in bar_size or "hour" in bar_size: 
                    corrected_bars = [bar for bar in bars if bar.date >= start_date]
                else:
                    for bar in bars:
                        self.logger.debug(f"Bar date: {bar.date} type = {type(bar.date)}, Start date: {start_date.date()} type= {type(start_date.date())}")
                        if bar.date >= start_date.date():
                            # Convert date to datetime with market timezone market timezone
                            bar.date = datetime.datetime.combine(bar.date, datetime.datetime.min.time()).replace(tzinfo=self.market_timezone)
                            corrected_bars.append(bar)
                        else:
                            self.logger.debug(f"Skipping bar with date {bar.date} as it is before start date {start_date.date()}")
                return corrected_bars
            
            df = util.df(bars)
            # Convert 'date' column to datetime and set as index
            df['date'] = pd.to_datetime(df['date'])
            df.sort_values(by='date', inplace=True)

            if df["date"].dt.tz is None:
                df["date"] = df["date"].dt.tz_localize(self.market_timezone)
            else:
                df["date"] = df["date"].dt.tz_convert(self.market_timezone)

            #Since ibkr returns data starting from the end date behind to duration filter out the data
            #Return only greater than start_date
            self.logger.debug(f"dataset: \n{df}")
            self.logger.debug(f"Filtering data for date >= {start_date}")
            df = df[df['date'] >= start_date]

            #df.set_index('date', inplace=True)
            #df.sort_index(inplace=True)  # Ensure the DataFrame is sorted by date

            return df
        except Exception as e:
            self.logger.error(f"Error fetching historical data: {e}")
            return None


    ## === Streaming Data Methods === ##

    async def subscribe_live_bars(self, contract: Contract,
                             callback: callable, # callback for each bar received
                             bar_size: int = 5, # Note: Only 5 seconds is officially supported by IBKR API for RT Bars
                             what_to_show: str = 'TRADES',
                             only_last_row = True,
                             use_rth: bool = True) -> BarDataList:
        """
        Fetch streaming bars from Interactive Brokers for a given contract.
        Parameters:
        - contract: The contract object to fetch data for
        - callback: Optional callback function to process each bar received
        - bar_size: Size of the bars in seconds (default is 5 seconds)
        - what_to_show: Type of data to retrieve (e.g., "TRADES")
        - use_rth: Use regular trading hours (True/False)
        Returns:
        - bars_subscription  object if successful, None otherwise
        """

        # Define the internal handler that calls the user's callback
        # Not worried about latency for now
        def _internal_bar_update_handler(bars: BarDataList, has_new_bar: bool):
            if has_new_bar and bars:
                callback(bars[int(only_last_row)*(-1)])



        if bar_size != 5:
            self.logger.error("IBKR API currently only supports 5 seconds for streaming bars.")
            return None

        if not await self.connect():
            self.logger.error("Failed to connect to IB")
            return None
        
        """
        Ensure the contract is qualified before requesting streaming bars
        """
        try:

            contracts_list = await self._ib_instance.qualifyContractsAsync(contract)
            self.logger.debug(f"Qualifying contract for streaming bars: {contracts_list}")
            if len(contracts_list) >1:
                self.logger.warning(f"Multiple contracts qualified for {contract.localSymbol}. Using the first one.")
                contract = contracts_list[0]
        except Exception as e:
            self.logger.error(f"Error qualifying contract for streaming bars: {e}")
            return None

        try:
            # Check if already subscribed to streaming bars for this contract
            stored_previously = False
            for c in self._current_subscriptions:
                if self._current_subscriptions[c][0] == "live_bars" and c == contract:
                    self.logger.warning(f"Already subscribed to streaming bars for contract {c}. Unsubscribing first.")
                    self.unsubscribe_live_bars(self._current_subscriptions[c][1],_dont_delete=True)
                    stored_previously = True
                    break
            # Request streaming bars
            self.logger.debug(f"Requesting streaming bars for contract={contract.localSymbol}, bar_size={bar_size}, what_to_show={what_to_show}, use_rth={use_rth}")
            self._ib_instance.reqMarketDataType(1)  # Set to real-time data
            bars_subscription = self._ib_instance.reqRealTimeBars(
                contract,
                barSize=bar_size,  # Size of the bars in seconds
                whatToShow=what_to_show,
                useRTH=use_rth
            )
            # Register the callback to process each bar received
            bars_subscription.updateEvent += _internal_bar_update_handler
            # Store the subscription object for later use
            if stored_previously:
                self.logger.info(f"Streaming bars for contract {contract.localSymbol} resubsrcibed successfully.")
                for c in self._current_subscriptions:
                    if self._current_subscriptions[c][0] == "live_bars" and c == contract:
                        self._current_subscriptions[c][1] = bars_subscription
                        self._current_subscriptions[c][2] = callback
                        self._current_subscriptions[c][3] = only_last_row
                        break
            else:
                self.logger.info(f"Streaming bars for contract {contract.localSymbol} requested successfully.")
                self._current_subscriptions[contract] = ["live_bars", bars_subscription,callback, only_last_row]
            
            self.logger.debug(f"Current subscriptions for {contract.localSymbol}: {self._current_subscriptions[contract]}")
            
            return bars_subscription
        except Exception as e:
            """
            Handle exceptions that may occur when requesting streaming bars
            """
            self.logger.error(f"Error requesting streaming bars: {e}")
            return None

    def unsubscribe_live_bars(self, bars_subscription, _dont_delete=True):
        """
        Unsubscribe from streaming bars for a given subscription object.
        
        Parameters:
        - bars_subscription: The BarDataList object to unsubscribe from
        
        Returns:
        - True if successful, False otherwise
        """
        from ib_async import RealTimeBarList
        if not isinstance(bars_subscription, RealTimeBarList):
            self.logger.warning("No bars subscription provided to unsubscribe")
            return False
        
        try:
            self.is_connected()

            # Unsubscribe from the real-time bars
            self._ib_instance.cancelRealTimeBars(bars_subscription)
            
            # Remove the subscription from the current subscriptions
            if bars_subscription.contract in self._current_subscriptions and self._current_subscriptions[bars_subscription.contract][0] == "live_bars" and not _dont_delete:
                del self._current_subscriptions[bars_subscription.contract]
            self.logger.info("Successfully unsubscribed from streaming bars.")
            return True
        except Exception as e:
            
            self.logger.error(f"Error unsubscribing from streaming bars: {e}")
            return False

    async def subscribe_market_data(self, contract: Contract,
                                   callback: callable) -> Ticker:
        """
        Subscribe to market data for a given contract.
        Market data is not tick-by-tick but rather a snapshot of the market at intervals.
        Still its less than few seconds delay which is good for many trading strategies.

        Parameters:
        - contract: The contract object to subscribe to
        - callback: A callable function that will be called with market data updates
        
        Returns:
        - 
        
        """
        if not await self.connect():
            self.logger.error("Failed to connect to IB")
            return False

        if not self._ib_instance or not self._ib_instance.isConnected():
            self.logger.error("Not connected to IB")
            return False

        try:
            # Check if already subscribed to market data for this contract
            await self._ib_instance.qualifyContractsAsync(contract)

            for c in self._current_subscriptions:
                if self._current_subscriptions[c][0] == "market_data" and c == contract:
                    self.logger.warning(f"Already subscribed to market data for contract {c}. Unsubscribing first.")
                    self.unsubscribe_market_data(c,dont_delete=True)
                    break


            # 232: Daily WAP, 233: Real Time Volume, 293: Daily Trade Count
            ticktype_list = "232,233,293" 
            # Request market data
            self._ib_instance.reqMarketDataType(1)  # Set to real-time data
            market_data_subscription = self._ib_instance.reqMktData(
                contract,
                genericTickList=ticktype_list,
                snapshot=False,
                regulatorySnapshot=False
            )

            # Register the callback for market data updates
            market_data_subscription.updateEvent += lambda data: callback(data)

            # Store the subscription object for later use
            stored_previously = False
            for c in self._current_subscriptions:
                if self._current_subscriptions[c][0] == "market_data" and c == contract:
                    self._current_subscriptions[c][2] = market_data_subscription
                    self._current_subscriptions[c][3] = callback
                    stored_previously = True
                    break
            if not stored_previously:
                self._current_subscriptions[contract] = ["market_data",market_data_subscription, callback]            
            self.logger.debug("Market data subscription requested successfully.")
            return market_data_subscription
        except Exception as e:
            self.logger.error(f"Error subscribing to market data: {e}")
            return None

    def unsubscribe_market_data(self, contract, _dont_delete=True):
        """
        Unsubscribe from market data for a given contract.
        Parameters:
        - contract: The contract to unsubscribe from
        - _dont_delete: (only internal use) If True, do not delete the subscription from the current subscriptions list
        Returns:
        - True if successful, False otherwise
        """

        if not contract:
            self.logger.warning("No contract provided to unsubscribe")
            return False
        
        try:
            # Cancel the market data subscription
            self._ib_instance.cancelMktData(contract)
            # Remove the subscription from the current subscriptions
            if contract in self._current_subscriptions and self._current_subscriptions[contract][0] == "market_data" and not _dont_delete:
                del self._current_subscriptions[contract]
            self.logger.debug("Successfully unsubscribed from market data.")
            return True
        except Exception as e:
            self.logger.error(f"Error unsubscribing from market data: {e}")
            return False


    #TODO
    async def get_historical_tick_data(self,starttime,endtime,contract):
        """
        Fetch historical tick data from Interactive Brokers for a given contract.
        
        Note: This method is a placeholder as IB does not provide tick data in the same way as bars.
        
        Returns:
        - None, as this is not supported directly via the IB API
        """
        self.logger.warning("Historical tick data is not supported by IB API.")
        return None
    

    ## === Data Logging Methods === ##

    def get_bardata_logger(
        self,
        contract: Contract,
        bar_size: str,
        data_dir: str,
        header: str = "Timestamp,Open,High,Low,Close,Volume,WAP,Count"
    ) -> logging.Logger:
        """
        Gets or creates a dedicated logger for saving data for a specific contract from ibkr.

        Logs data to a rotating CSV file named '{contract_symbol}_ohlc.csv'.

        Args:
            contract (Contract): The contract object for which to create the logger.
            bar_size (str): The size of the bars (e.g., '5 mins').
            log_dir (str): The directory where the data log file will be stored.
            header (str): Custom CSV header for the log file if the file is new.

        Returns:
            logging.Logger: The configured logger instance for the contract's data.
        """
        from src.utils.helpers import get_contract_data_filepath

        # get logger name and filename
        filepath = get_contract_data_filepath(contract,data_dir, bar_size)
        logger_name = filepath.split("/")[-1].replace(".csv","")


        # Check cache first
        if logger_name in self._data_loggers:
            return self._data_loggers[logger_name]


        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        data_logger = logging.getLogger(logger_name)
        data_logger.setLevel(logging.INFO) # Data logging level - INFO is usually appropriate

        # Prevent data logs from going to the general file/console
        data_logger.propagate = False

        # Add handler only if it doesn't exist for this logger
        if not data_logger.handlers:
            # Formatter: Just message for CSV data, no need for timestamps in the log format itself
            formatter = logging.Formatter(
                fmt="%(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )

            # Rotating file handler for the CSV data
            handler = logging.handlers.RotatingFileHandler(
                filename=filepath,
                maxBytes=20971520, # 20MB for data files
                backupCount=10,    # Keep more backups for data
                encoding='utf-8'
            )
            handler.setFormatter(formatter)
            data_logger.addHandler(handler)

            # Add header if the file is new/empty
            if os.path.getsize(filepath) == 0:
                # Note: This might have race conditions in multi-process scenarios.
                # For simplicity here, we assume single process or accept potential duplicate headers.
                data_logger.info(header)

            self.logger.debug(f"Bar Data logger '{logger_name}' configured for file '{filepath}'") # Info message
        

        # Add DataFrame logging method directly to the logger instance
        def log_df(df):
            """
            Logs each row of a pandas DataFrame using the existing info method.
            
            Args:
                df: pandas DataFrame to log
            """
            if df.empty:
                return
                
            # Convert DataFrame rows to CSV strings and log each row
            #check if index is datetime
            if pd.api.types.is_datetime64_any_dtype(df.index):
                df.index = df.index.tz_convert(self.market_timezone)
            
                for row in df.itertuples():
                    csv_row = row.Index.isoformat(sep=' ', timespec='seconds') + ","
                    csv_row += ",".join(str(v) for v in [row.open,row.high,row.low,row.close,row.volume,row.average,row.barCount])
                    data_logger.info(csv_row)
            else:   
                for row in df.itertuples():
                    csv_row = ",".join(str(v) for v in [row.date,row.open,row.high,row.low,row.close,row.volume,row.average,row.barCount])
                    data_logger.info(csv_row)
                
            # for index, row in df.iterrows():
            #     if isinstance(index, pd.Timestamp):
            #         csv_row = index.astimezone(self.market_timezone).isoformat(sep=' ', timespec='microseconds') + ","
            #     else: #Not storing index if its not datetime
            #         csv_row = ""
            #     csv_row += ",".join(str(v) for v in [row["date"],row["open"],row["high"],row["low"],row["close"],row["volume"]])
            #     data_logger.info(csv_row)
        
        # Attach the method to the logger instance
        data_logger.log_df = log_df

        def log_bars(bar_list):
            """
            Logs a BarDataList object from ib_insync.
            Args:
                bar_list: The BarDataList object to log.
            """
            if not bar_list:
                self.logger.warning("Invalid bar_list object provided.")
                return
            for bar in bar_list:
                csv_row = ",".join(str(v) for v in [bar.date,bar.open,bar.high,bar.low,bar.close,bar.volume,bar.average,bar.barCount])
                data_logger.info(csv_row)

        data_logger.log_bars = log_bars
        

        def log_emptyRow(end_date):
            """
            Logs an empty row with the end date.
            """
            csv_row = end_date.astimezone(self.market_timezone).isoformat(sep=' ', timespec='seconds') + ","
            #Add NaN for all other columns
            csv_row += "NaN,NaN,NaN,NaN,NaN,NaN,NaN"
            data_logger.info(csv_row)
        data_logger.log_emptyRow = log_emptyRow

        def log_RealTimeBar(realtime_bar):
            """
            Logs a RealTimeBar object from ib_insync.
            RealTimeBar has an extra endTime variable compared to the Bar above, 
            so this method is specifically for logging RealTimeBar objects.
            Args:
                realtime_bar: The RealTimeBar object to log.
            """
            if not realtime_bar:
                self.logger.warning("Invalid realtime_bar object provided.")
                return
            csv_row = realtime_bar.time.astimezone(self.market_timezone).isoformat(sep=' ', timespec='microseconds')
            csv_row += f",{realtime_bar.open_}" \
                    f",{realtime_bar.high}" \
                    f",{realtime_bar.low}"  \
                    f",{realtime_bar.close}" \
                    f",{realtime_bar.volume}" \
                    f",{realtime_bar.wap}" \
                    f",{realtime_bar.count}"
            data_logger.info(csv_row)

        data_logger.log_RealTimeBar = log_RealTimeBar

        # Store in cache
        self._data_loggers[logger_name] = data_logger
        return data_logger

    def get_marketdata_logger(
        self,
        contract_symbol: str,
        data_dir: str,
        header: str = "Timestamp,Last,Volume"
    ) -> logging.Logger:
        """
        Gets or creates a dedicated logger for saving data for a specific contract from ibkr.

        Logs data to a rotating CSV file named '{contract_symbol}_ohlc.csv'.

        Args:
            contract_symbol (str): The symbol of the contract (e.g., 'AAPL', 'ESU4').
                                Used for filename and logger name.
            log_dir (str): The directory where the data log file will be stored.
            header (str): Custom CSV header for the log file if the file is new.

        Returns:
            logging.Logger: The configured logger instance for the contract's data.
        """
        # Sanitize symbol for logger name and filename if necessary (basic example)
        safe_symbol = "".join(c if c.isalnum() else '_' for c in contract_symbol)
        logger_name = f"data.{safe_symbol}_mktdata"
        filename = os.path.join(data_dir, f"{logger_name}.csv")

        # Check cache first
        if logger_name in self._data_loggers:
            return self._data_loggers[logger_name]

        os.makedirs(data_dir, exist_ok=True)

        data_logger = logging.getLogger(logger_name)
        data_logger.setLevel(logging.INFO) # Data logging level - INFO is usually appropriate

        # Prevent data logs from going to the general file/console
        data_logger.propagate = False

        # Add handler only if it doesn't exist for this logger
        if not data_logger.handlers:
            # Formatter: Just message for CSV data, no need for timestamps in the log format itself
            formatter = logging.Formatter(
                fmt="%(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )

            # Rotating file handler for the CSV data
            handler = logging.handlers.RotatingFileHandler(
                filename=filename,
                maxBytes=20971520, # 20MB for data files
                backupCount=10,    # Keep more backups for data
                encoding='utf-8'
            )
            handler.setFormatter(formatter)
            data_logger.addHandler(handler)

            # Add header if the file is new/empty
            if os.path.getsize(filename) == 0:
                # Note: This might have race conditions in multi-process scenarios.
                # For simplicity here, we assume single process or accept potential duplicate headers.
                data_logger.info(header)

            logging.getLogger(__name__).debug(f"Market Data logger '{logger_name}' configured for file '{filename}'") # Info message
        
        # Store in cache
        self._data_loggers[logger_name] = data_logger

        def log_Ticker(ticker):
            """
            Logs a ticker object from ib_async.
            
            Args:
                ticker: The ticker object to log.
            """
            if not ticker:
                self.logger.warning("Invalid ticker object provided.")
                return
            
            
            # Include timezone information
            csv_row = ticker.time.astimezone(self.market_timezone).isoformat(sep=' ', timespec='microseconds')
            csv_row += f",{ticker.last}" \
                    f",{ticker.volume}"  
            data_logger.info(csv_row)

        # Attach the method to the logger instance
        data_logger.log_Ticker = log_Ticker

        return data_logger


    ## === Order Management Methods === ##

    async def _place_order(self, contract:Contract, order:Order,wait_for_fill=False,wait_for_commission=False):

        try:
            if not self._ib_instance or not self._ib_instance.isConnected():
                self.logger.error("Not connected to IB")
                return None
            
            if order.orderId == 0: order.orderId = await self.get_req_id()
            async with self._criticial_section:
                self._completed_trade_queues[order.orderId] = asyncio.Queue()
                
            await self._ib_instance.qualifyContractsAsync(contract)
            trade = self._ib_instance.placeOrder(contract, order)
            self.logger.debug(f"Order placed successfully: {trade}")
            await asyncio.sleep(0.2)

            # Wait until submitted or pre-submitted
            self.logger.debug(f"Waiting for submit for order ID: {order.orderId}")
            status,completed_trade = await self._completed_trade_queues[order.orderId].get()
            self._completed_trade_queues[order.orderId].task_done()
            
            #Dont return need to delete the queue at the end of the function
            wait_for_fill = True if wait_for_commission else wait_for_fill
            if completed_trade is None:
                self.logger.error("Error in Order fill event. No completed trade received.")
                wait_for_commission = False
                wait_for_fill = False
            
            if status == "Submitted" or status == "PreSubmitted" or status == "Filled":
                self.logger.debug(f"order ID: {order.orderId} submitted successfully.")
            else:
                self.logger.error(f"Order id :{order.orderId} not submitted: {status}")
                wait_for_commission = False
                wait_for_fill = False
            
            if wait_for_fill:
                while status != "Filled":
                    self.logger.debug(f"Waiting for fill event for order ID: {order.orderId}")
                    status,completed_trade = await self._completed_trade_queues[order.orderId].get()
                    self._completed_trade_queues[order.orderId].task_done()
                    if status == "Filled":
                        self.logger.debug(f"Order filled successfully for order id: {order.orderId}")
                    else:
                        self.logger.error(f"Order id :{order.orderId} status: {status}")

            if wait_for_commission:
                while status != "Commission":
                    self.logger.debug(f"Waiting for commission event for order ID: {order.orderId}")
                    status,completed_trade = await self._completed_trade_queues[order.orderId].get()
                    self._completed_trade_queues[order.orderId].task_done()
                    if status == "Commission":
                        self.logger.debug(f"Commission event received for order ID: {order.orderId}")
                    else:
                        self.logger.error(f"Order id :{order.orderId} no commission event, status: {status}")
                        

            async with self._criticial_section:
                del self._completed_trade_queues[order.orderId]
                
            return completed_trade
        except Exception as e:

            self.logger.error(f"Error placing order for contract: {contract.localSymbol}. Error: {e}")
            async with self._criticial_section:
                if order.orderId in self._completed_trade_queues:
                    del self._completed_trade_queues[order.orderId]
            return None

    async def place_market_order(self, contract, ordertype, quantity,group_name=None, parent_id=None,order_ref=None):   

        if ordertype not in ['BUY', 'SELL']:
            self.logger.error("Invalid order type. Must be 'BUY' or 'SELL'.")
            return None

        order_ref = order_ref if order_ref else ordertype
        order = MarketOrder(ordertype, quantity, orderRef=order_ref)
        order.orderId = await self.get_req_id()
        if group_name is not None:
            order.ocaGroup = group_name
            order.ocaType = 1
        if parent_id is not None: order.parentId = parent_id
        trade = await self._place_order(contract, order,wait_for_commission=True)

        if trade is None:
            self.logger.error("Failed to place market order.")
            return None
        
        return trade


    async def place_custombracket_order(self,contract,quantity,take_profit_percent,stop_loss_percent,
                                         trailing_stop_percent,exit_callback_func,order_ref="Buy"):
        
        '''
        Places a custom bracket order with a market order, stop loss, take profit, trailing stop, and EOD order.
        Parameters:
        - contract: The contract object to place the order for
        - quantity: The number of shares/contracts to buy/sell
        - take_profit_percent: The percentage for the take profit order
        - stop_loss_percent: The percentage for the stop loss order
        - trailing_stop_percent: The percentage for the trailing stop order
        Returns:
        - A tuple containing the buy order, take profit order, stop loss order, trailing stop order, and EOD order placed.
        - A sell order for the bracket order - which will cancel all the orders in the bracket order.
        '''

        #Place a market order first
        
        if not await self.connect():
            self.logger.error("Failed to connect to IB")
            return None, None  
        
        try:
            buy_order_id = await self.get_req_id()
            buy_order = Order(orderId=buy_order_id, action='BUY', orderType='MKT', orderRef=order_ref, totalQuantity=quantity,tif='DAY' )
            trade = await self._place_order(contract, buy_order,wait_for_commission=True)

            if trade is None:
                self.logger.error("Failed to place buy order.")
                return None, None

            # Wait for the order to be filled
            # Use filled price and create bracket orders including stop loss, trailing stop, take profit and EOD

            avg_price = trade.orderStatus.avgFillPrice
            filled_quantity = trade.orderStatus.filled
        except Exception as e:
            self.logger.error(f"Error placing buy order: {e}")
            return None, None
        
        if filled_quantity == 0:
            self.logger.error("Buy order not filled. Cannot place exit orders.")
            return None, None
        
        try:

            contract_details = await self._ib_instance.reqContractDetailsAsync(contract)
            mintick = contract_details[0].minTick
            tp_amount = avg_price * (1 + take_profit_percent / 100)
            sl_amount = avg_price * (1 - stop_loss_percent / 100)

            #Confirm to the contract details
            tp_amount = round(tp_amount) if mintick < 1 else tp_amount - (tp_amount % mintick)
            sl_amount = round(sl_amount) if mintick < 1 else sl_amount - (sl_amount % mintick)

            # Create OCA group name
            pid = os.getpid()
            oca_group_name = f"OCA_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{await self.get_req_id()}_{pid}"
            self.logger.debug(f"OCA Group Name: {oca_group_name}")

            # Create Stop loss order 
            stop_loss_order_id = await self.get_req_id()
            self.logger.debug(f"Creating stop loss order id {stop_loss_order_id} for contract {contract.localSymbol} with amount: {sl_amount}")
            stop_loss_order = Order(orderId=stop_loss_order_id,action='SELL', orderType='STP', orderRef="StopLoss",
                                    totalQuantity=filled_quantity, auxPrice=sl_amount, 
                                    tif='GTC', ocaGroup=oca_group_name,ocaType=1)

            
            # Create Take profit order
            take_profit_order_id = await self.get_req_id()
            self.logger.debug(f"Creating take profit order id {take_profit_order_id} for contract {contract.localSymbol} with amount: {tp_amount}")
            take_profit_order = Order(orderId=take_profit_order_id,action='SELL', orderType='LMT', orderRef="TakeProfit",
                                    totalQuantity=filled_quantity, lmtPrice=tp_amount, 
                                    tif='GTC', ocaGroup=oca_group_name,ocaType=1)

            # Create Trailing stop order - doesnt accept MKT order as parent
            trailing_sl_order_id = await self.get_req_id()
            self.logger.debug(f"Creating trailing stop order id {trailing_sl_order_id} for contract {contract.localSymbol} with amount: {avg_price}")
            trailing_stop_order = Order(orderId=trailing_sl_order_id,action='SELL', orderType='TRAIL', orderRef="TrailingStop",
                                    totalQuantity=filled_quantity, trailingPercent=float(trailing_stop_percent), 
                                    tif='GTC', ocaGroup=oca_group_name,ocaType=1)

            #Create EOD order (using time condition)
            eod_order_id = await self.get_req_id()
            _, market_end_time = await self.get_trading_hours(contract)
            market_end_time = market_end_time.strftime("%Y%m%d %H:%M:%S")+ " " + market_end_time.tzinfo.key
            self.logger.debug(f"Creating EOD order id {eod_order_id} for contract {contract.localSymbol} at time: {market_end_time}")
            eod_order = Order(orderId=eod_order_id,action='SELL', orderType='MKT', orderRef="EOD",
                                    totalQuantity=filled_quantity, tif='DAY', ocaGroup=oca_group_name,ocaType=1)
            eod_order.conditions.append(TimeCondition(isMore=True, time=market_end_time, conjunction='a'))
            eod_order.conditionsIgnoreRth = True  
              
            async with self._criticial_section:
                self.callback_for_ocagroup[oca_group_name] = exit_callback_func
                self._active_exit_orders[oca_group_name] = {take_profit_order_id, stop_loss_order_id, trailing_sl_order_id, eod_order_id}
            
            stop_trade = await self._place_order(contract, stop_loss_order)
            take_profit_trade = await self._place_order(contract, take_profit_order)
            trailing_trade = await self._place_order(contract, trailing_stop_order)
            eod_trade = await self._place_order(contract, eod_order)

            sell_order = Order(action='SELL', orderType='MKT', 
                            totalQuantity=quantity, tif='DAY', ocaGroup=oca_group_name,ocaType=1)
            
            # Place the bracket orders
            if take_profit_trade is None or stop_trade is None or trailing_trade is None or eod_trade is None:
                self.logger.error("Failed to place one or more exit orders. Selling immediately.")
                #this will cancel all the orders in the OCA group and call on_exit_callback
                await self._place_order(contract, sell_order) 
                return [trade], None

            self.logger.debug("\nAll parts of the bracket order submitted successfully.")    

        except Exception as e:
            self.logger.error(f"Error placing exit orders, Selling immediately. Error: {e}")
            if oca_group_name and oca_group_name not in self._active_exit_orders:
                self.logger.debug(f"Error oocured before OCA group name {oca_group_name} added to active exit orders. Adding it before selling")
                async with self._criticial_section:
                    self.callback_for_ocagroup[oca_group_name] = exit_callback_func
                    self._active_exit_orders[oca_group_name] = {take_profit_order_id, stop_loss_order_id, trailing_sl_order_id, eod_order_id}
            sell_order = Order(action='SELL', orderType='MKT', 
                            totalQuantity=quantity, tif='DAY', ocaGroup=oca_group_name,ocaType=1)
            await self._place_order(contract, sell_order)
            return [trade], None
        
        return (trade, take_profit_trade, stop_trade, trailing_trade, eod_trade), sell_order