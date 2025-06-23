import websockets
import json
import asyncio
import logging
from typing import Optional, Callable, Dict, Any
from dataclasses import dataclass
import itertools
import time
import traceback

logger = logging.getLogger(__name__)


@dataclass
class DeribitCredentials:
    client_id: str
    client_secret: str
    test: bool = True


class DeribitWebsocketClient:
    # ...
    def __init__(self, credentials: Optional[DeribitCredentials] = None, is_test: bool = False):
        self.credentials = credentials
        self.req_ws: Optional[websockets.WebSocketClientProtocol] = None
        self.sub_ws: Optional[websockets.WebSocketClientProtocol] = None
        self.price_callback: Optional[Callable[[str, float], None]] = None
        self.running = False
        self.ws_url = self.TESTNET_WS_URL if is_test or (credentials and credentials.test) else self.MAINNET_WS_URL
        self.req_listener_task = None
        self.sub_listener_task = None
        self.pending_requests = {}  # id -> Future
        self._id_counter = itertools.count(100)
        self.last_subscribed_instrument = None
        self._req_ws_recv_lock = asyncio.Lock()  # Enforce single-consumer recv for req_ws
        self._sub_ws_recv_lock = asyncio.Lock()  # Enforce single-consumer recv for sub_ws
        self.subscribed_instruments = set()  # Track all currently subscribed instruments
        self.price_iv_cache = {}  # instrument_name -> {"mark_price": float, "iv": float}
        self._subscription_handlers = set()  # Set of handlers to notify about subscription confirmations

    def get_price_iv(self, instrument_name: str):
        """Return (mark_price, iv) for the instrument from cache, or (None, None) if not available."""
        entry = self.price_iv_cache.get(instrument_name)
        if entry:
            return entry.get("mark_price"), entry.get("iv")
        return None, None

    async def subscribe_to_instruments(self, instrument_names) -> bool:
        """
        Subscribe to a set of instrument tickers (options or futures).
        Avoid duplicate subscriptions using self.subscribed_instruments.

        Args:
            instrument_names: List of instrument names to subscribe to

        Returns:
            bool: True if all subscriptions were successful, False otherwise
        """
        if not self.sub_ws:
            logger.error("Cannot subscribe: WebSocket connection is not active")
            return False

        new_instruments = set(instrument_names) - self.subscribed_instruments
        if not new_instruments:
            logger.debug("No new instruments to subscribe to")
            return True

        channels = [f"ticker.{name}.100ms" for name in new_instruments]
        subscription_id = next(self._id_counter)
        subscribe_params = {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": subscription_id,
            "params": {"channels": channels}
        }

        logger.info(f"Sending subscription request for channels: {channels}")

        try:
            # Create a future to track the subscription response
            subscription_future = asyncio.get_running_loop().create_future()
            self.pending_requests[subscription_id] = subscription_future

            # Send the subscription request
            await self.sub_ws.send(json.dumps(subscribe_params))
            logger.debug(f"Sent subscription request: {subscribe_params}")

            # Wait for the subscription response with a timeout
            try:
                response = await asyncio.wait_for(subscription_future, timeout=10.0)
                logger.debug(f"Received subscription response: {response}")
            except asyncio.TimeoutError:
                logger.error(f"Timeout waiting for subscription response for {channels}")
                return False

            # Check if response is a dictionary with a result field
            if isinstance(response, dict) and "result" in response:
                result = response["result"]

                # Case 1: Result is a list of channels (e.g., ["ticker.BTC-PERPETUAL.100ms"])
                if isinstance(result, list) and all(isinstance(x, str) for x in result):
                    self.subscribed_instruments.update(new_instruments)
                    logger.info(f"Successfully subscribed to instruments: {new_instruments}")
                    # Notify handlers for each instrument
                    for instrument in new_instruments:
                        await self._notify_subscription_handlers(instrument)
                    return True

                # Case 2: Result is a dictionary with a channels field
                elif isinstance(result, dict) and "channels" in result:
                    channels = result["channels"]
                    if isinstance(channels, list):
                        self.subscribed_instruments.update(new_instruments)
                        logger.info(f"Successfully subscribed to instruments: {new_instruments}")
                        # Notify handlers for each instrument
                        for instrument in new_instruments:
                            await self._notify_subscription_handlers(instrument)
                        return True

            # Handle case where response is a list (shouldn't happen with current API but just in case)
            elif isinstance(response, list) and len(response) > 0 and isinstance(response[0], dict):
                if "result" in response[0]:
                    self.subscribed_instruments.update(new_instruments)
                    logger.info(f"Successfully subscribed to instruments: {new_instruments}")
                    # Notify handlers for each instrument
                    for instrument in new_instruments:
                        await self._notify_subscription_handlers(instrument)
                    return True

            logger.error(f"Unexpected subscription response: {response}")
            return False

        except Exception as e:
            logger.error(f"Error subscribing to instruments {new_instruments}: {e}", exc_info=True)
            return False
        finally:
            # Clean up the future
            self.pending_requests.pop(subscription_id, None)

    TESTNET_WS_URL = "wss://test.deribit.com/ws/api/v2"
    MAINNET_WS_URL = "wss://www.deribit.com/ws/api/v2"

    def _old_init(self, credentials: Optional[DeribitCredentials] = None):
        self.credentials = credentials
        self.req_ws: Optional[websockets.WebSocketClientProtocol] = None  # For request/response
        self.sub_ws: Optional[websockets.WebSocketClientProtocol] = None  # For subscriptions
        self.price_callback: Optional[Callable[[str, float], None]] = None
        self.running = False
        self.ws_url = self.TESTNET_WS_URL if credentials and credentials.test else self.MAINNET_WS_URL
        self.req_listener_task = None
        self.sub_listener_task = None
        self.pending_requests = {}  # id -> Future
        self._id_counter = itertools.count(100)
        self.last_subscribed_instrument = None
        self._req_ws_recv_lock = asyncio.Lock()  # Enforce single-consumer recv for req_ws
        self._sub_ws_recv_lock = asyncio.Lock()  # Enforce single-consumer recv for sub_ws

    async def connect(self):
        """Connect both request/response and subscription websockets"""
        self.req_ws = await websockets.connect(self.ws_url)
        self.sub_ws = await websockets.connect(self.ws_url)
        if self.credentials:
            await self._authenticate(self.req_ws)
            await self._authenticate(self.sub_ws)
        self.running = True
        if self.req_listener_task is None or self.req_listener_task.done():
            self.req_listener_task = asyncio.create_task(self.listen_req_ws())
            logger.info("Started req_ws listener task")
        if self.sub_listener_task is None or self.sub_listener_task.done():
            self.sub_listener_task = asyncio.create_task(self.listen_sub_ws())
            logger.info("Started sub_ws listener task")

    async def _authenticate(self, ws):
        """Authenticate a websocket connection if credentials are provided"""
        if not self.credentials:
            return
        auth_params = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self.credentials.client_id,
                "client_secret": self.credentials.client_secret
            }
        }
        await ws.send(json.dumps(auth_params))
        # Optionally wait for auth response, but Deribit allows unauthenticated public access

    async def send_request(self, method: str, params: dict) -> dict:
        """Send JSON-RPC request via req_ws and await response via req_ws listener"""
        req_id = next(self._id_counter)
        request = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params
        }
        fut = asyncio.get_running_loop().create_future()
        self.pending_requests[req_id] = fut
        await self.req_ws.send(json.dumps(request))
        try:
            response = await asyncio.wait_for(fut, timeout=10)
            return response
        finally:
            self.pending_requests.pop(req_id, None)

    async def subscribe_to_ticker(self, instrument_name: str):
        """Subscribe to ticker updates using sub_ws and remember for reconnects"""
        self.last_subscribed_instrument = instrument_name
        subscribe_params = {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 2,
            "params": {
                "channels": [f"ticker.{instrument_name}.100ms"]
            }
        }
        await asyncio.sleep(0.5)
        await self.sub_ws.send(json.dumps(subscribe_params))
        logger.info(f"Sent subscription request: {subscribe_params}")

    async def listen_req_ws(self):
        """Listen for responses to requests on req_ws"""
        backoff = 1
        while self.running:
            try:
                async with self._req_ws_recv_lock:
                    message = await self.req_ws.recv()
                data = json.loads(message)
                req_id = data.get("id")
                if req_id is not None and req_id in self.pending_requests:
                    fut = self.pending_requests[req_id]
                    if not fut.done():
                        fut.set_result(data)
                backoff = 1
            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK
            ) as e:
                logger.error(f"req_ws closed: {e}. Attempting to reconnect in {backoff}s...")
                await asyncio.sleep(backoff)
                await self.reconnect()
                backoff = min(backoff * 2, 60)
            except Exception as e:
                logger.error(f"Error in req_ws listener: {e}")
                await asyncio.sleep(backoff)
                await self.reconnect()
                backoff = min(backoff * 2, 60)

    async def listen_sub_ws(self):
        """Listen for subscription messages on sub_ws"""
        logger.info("listen_sub_ws() STARTED")
        backoff = 1
        while self.running:
            try:
                try:
                    async with self._sub_ws_recv_lock:
                        message = await asyncio.wait_for(self.sub_ws.recv(), timeout=10)
                    data = json.loads(message)
                    await self._handle_message(data)
                    backoff = 1
                except asyncio.TimeoutError:
                    logger.warning("No message received on sub_ws after 10 seconds!")
                    continue
            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK
            ) as e:
                logger.error(f"sub_ws closed: {e}. Attempting to reconnect in {backoff}s...")
                await asyncio.sleep(backoff)
                await self.reconnect()
                backoff = min(backoff * 2, 60)
            except Exception as e:
                logger.error(f"Error in sub_ws listener: {e}")
                await asyncio.sleep(backoff)
                await self.reconnect()
                backoff = min(backoff * 2, 60)

    async def reconnect(self):
        """Reconnect both websockets on connection loss and restore callbacks/subscriptions."""
        logger.info("Attempting to reconnect...")
        try:
            await self.connect()
            # Restore price callback if set
            if self.price_callback is not None:
                self.set_price_callback(self.price_callback)
            # Re-subscribe to all previous ticker channels if possible
            if hasattr(self, 'last_subscribed_instrument') and self.last_subscribed_instrument:
                await self.subscribe_to_ticker(self.last_subscribed_instrument)
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            await asyncio.sleep(5)  # Wait before retrying

    async def close(self):
        """Close both websocket connections"""
        self.running = False
        if self.req_ws:
            await self.req_ws.close()
        if self.sub_ws:
            await self.sub_ws.close()

    async def get_index_price(self, index_name: str = "btc_usd") -> Optional[float]:
        """Get the current index price for the given index.

        Args:
            index_name: Name of the index (e.g., 'btc_usd')

        Returns:
            float: The current index price, or None if the request fails
        """
        try:
            result = await self.send_request(
                "public/get_index_price",
                {"index_name": index_name}
            )
            if result and "result" in result and "index_price" in result["result"]:
                return float(result["result"]["index_price"])
            return None
        except Exception as e:
            logger.error(f"Error getting index price: {e}")
            return None

    async def get_instrument_mark_price_and_iv(self, instrument_name: str, force_refresh: bool = False) -> tuple:
        """
        Fetch mark price and implied volatility (mark_iv) for an instrument.
        First checks local cache before making a websocket request.

        Args:
            instrument_name: Name of the instrument to fetch data for
            force_refresh: If True, bypass cache and fetch fresh data

        Returns:
            tuple: (mark_price, iv) where iv is 0 for futures, or (None, 0.0) on error
        """
        # Check cache first if not forcing refresh
        if not force_refresh and instrument_name in self.price_iv_cache:
            cached = self.price_iv_cache[instrument_name]
            # logger.debug(f"Cache hit for {instrument_name}: {cached}")
            return cached["mark_price"], cached["iv"]

        try:
            if not self.req_ws:
                await self.connect()

            response = await self.send_request(
                "public/get_book_summary_by_instrument",
                {"instrument_name": instrument_name}
            )

            result = response.get("result")
            if isinstance(result, list) and result:
                mark_price = result[0].get("mark_price")
                mark_iv = result[0].get("mark_iv")

                if mark_iv is not None:
                    iv = mark_iv / 100 if mark_iv > 3 else mark_iv  # Deribit sometimes returns percent
                else:
                    iv = 0.0  # Futures or missing IV

                # Update cache
                self.price_iv_cache[instrument_name] = {
                    "mark_price": mark_price,
                    "iv": iv,
                    "timestamp": time.time()
                }

                logger.info(f"Fetched mark price for {instrument_name}: {mark_price}, IV: {iv}")
                return mark_price, iv

            logger.warning(f"No valid result for {instrument_name}: {result}")
            return None, 0.0

        except Exception as e:
            logger.error(f"Error in get_instrument_mark_price_and_iv: {e}\n{traceback.format_exc()}")
            await self.reconnect()
            return None, 0.0

    def set_price_callback(self, callback):
        """Set callback for price updates.

        The callback can be either a synchronous function or a coroutine function.
        """
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self.price_callback = callback

    async def _handle_message(self, message: Dict[str, Any]) -> None:
        """Handle incoming WebSocket messages.

        This method processes different types of WebSocket messages:
        1. List responses (subscription confirmations)
        2. Dictionary responses (pending requests, subscription updates)
        3. Error responses
        """
        try:
            # logger.debug(f"[WEBSOCKET] Received message: {message}")

            # Handle list responses (subscription confirmations)
            if isinstance(message, list) and message:
                for item in message:
                    if not isinstance(item, dict):
                        continue

                    # Check for subscription confirmation in different formats
                    if "result" in item and "channels" in item["result"]:
                        # Format: [{"result": {"channels": ["ticker.BTC-PERPETUAL.100ms"]}}]
                        channels = item["result"]["channels"]
                    elif "channels" in item:
                        # Format: [{"channels": ["ticker.BTC-PERPETUAL.100ms"]}]
                        channels = item["channels"]
                    else:
                        continue

                    if not isinstance(channels, list):
                        channels = [channels]

                    for channel in channels:
                        if not isinstance(channel, str):
                            continue

                        if channel.startswith("ticker."):
                            try:
                                instrument = channel.split('.')[1]
                                logger.info(
                                    "[WEBSOCKET] Processing subscription confirmation" +
                                    f" for ticker: {instrument}"
                                )
                                # Update our internal tracking
                                self.subscribed_instruments.add(instrument)
                                # Notify handlers
                                try:
                                    await self._notify_subscription_handlers(instrument)
                                except Exception as e:
                                    logger.error(
                                        "Error notifying subscription handlers" +
                                        f" for {instrument}: {e}",
                                        exc_info=True
                                    )
                            except Exception as e:
                                logger.error(f"Error processing subscription channel {channel}: {e}", exc_info=True)
                return

            # Handle dictionary responses
            if not isinstance(message, dict):
                logger.warning(f"[WEBSOCKET] Received unexpected message type: {type(message)}")
                return

            # Check if this is a response to a pending request
            if "id" in message and message.get("id") in self.pending_requests:
                request_id = message["id"]
                future = self.pending_requests.get(request_id)
                if future and not future.done():
                    if "error" in message:
                        error = message["error"]
                        logger.error(f"Error in WebSocket response (id={request_id}): {error}")
                        future.set_exception(Exception(f"WebSocket error: {error}"))
                    else:
                        future.set_result(message)
                return

            # Handle subscription confirmations
            if "result" in message:
                # Handle different formats of subscription confirmations
                channels = []
                result = message["result"]

                # Format 1: Direct list of channels
                if isinstance(result, list):
                    channels = result
                # Format 2: Dictionary with 'channels' key
                elif isinstance(result, dict) and "channels" in result:
                    channels = result["channels"]
                # Format 3: Dictionary with 'channel' key (single channel)
                elif isinstance(result, dict) and "channel" in result:
                    channels = [result["channel"]]

                # If we're in a subscription confirmation, also check the 'params' field
                if not channels and "method" in message and message["method"] == "subscription":
                    params = message.get("params", {})
                    if "channel" in params:
                        channels = [params["channel"]]

                processed_instruments = set()
                for channel in channels:
                    # Handle different channel formats
                    if isinstance(channel, str):
                        if channel.startswith("ticker."):
                            # Extract just the instrument name (e.g., "BTC-PERPETUAL" from "ticker.BTC-PERPETUAL.100ms")
                            parts = channel.split('.')
                            if len(parts) >= 2:
                                instrument = parts[1]
                                processed_instruments.add(instrument)
                    elif isinstance(channel, dict) and "channel" in channel:
                        # Handle case where channel is an object with a 'channel' property
                        channel_str = channel["channel"]
                        if channel_str.startswith("ticker."):
                            parts = channel_str.split('.')
                            if len(parts) >= 2:
                                instrument = parts[1]
                                processed_instruments.add(instrument)

                # Notify handlers for each unique instrument
                for instrument in processed_instruments:
                    logger.info(f"[WEBSOCKET] Subscription confirmed for {instrument}")
                    # Update our internal tracking
                    self.subscribed_instruments.add(instrument)
                    # Notify all subscription handlers
                    try:
                        logger.debug(f"[WEBSOCKET] Notifying handlers for {instrument}")
                        await self._notify_subscription_handlers(instrument)
                    except Exception as e:
                        logger.error(f"Error notifying handlers for {instrument}: {e}", exc_info=True)
                    else:
                        logger.debug(f"[WEBSOCKET] Successfully notified handlers for {instrument}")

            # Handle subscription updates
            elif "method" in message and message["method"] == "subscription":
                params = message.get("params", {})
                channel = params.get("channel", "")
                data = params.get("data", {})

                if not channel:
                    logger.warning("[WEBSOCKET] Received subscription message without channel")
                    return

                # logger.debug(f"[WEBSOCKET] Processing subscription update - Channel: {channel}")

                # Route to appropriate handler based on channel type
                if channel.startswith("ticker."):
                    await self._handle_ticker_update(channel, data)
                elif channel.startswith("book."):
                    await self._handle_order_book_update(channel, data)
                elif channel.startswith("trades."):
                    await self._handle_trade_update(channel, data)
                else:
                    logger.debug(f"[WEBSOCKET] Unhandled subscription channel: {channel}")

        except Exception as e:
            logger.error(f"[WEBSOCKET] Error handling WebSocket message: {e}", exc_info=True)

    async def _notify_subscription_handlers(self, instrument: str) -> bool:
        """
        Notify all registered subscription handlers about a new subscription.

        Args:
            instrument: The instrument that was subscribed to

        Returns:
            bool: True if there were handlers to notify, False otherwise
        """
        if not self._subscription_handlers:
            logger.debug(f"No subscription handlers registered to notify about {instrument}")
            return False

        logger.info(f"Notifying {len(self._subscription_handlers)} handlers about subscription to {instrument}")

        # Create a list to store all coroutines
        tasks = []

        # Create a task for each handler
        for handler in list(self._subscription_handlers):  # Create a copy to avoid modification during iteration
            logger.debug(f"Creating task for handler {handler}")
            try:
                # Call the handler and store the coroutine
                coro = handler(instrument)
                if asyncio.iscoroutine(coro):
                    tasks.append(coro)
                else:
                    logger.warning(f"Handler {handler} did not return a coroutine")
            except Exception as e:
                logger.error(f"Error creating task for handler {handler}: {e}", exc_info=True)

        # Wait for all handlers to complete with a timeout
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10.0)
                logger.debug(f"Successfully notified all handlers about {instrument}")
            except asyncio.TimeoutError:
                logger.warning(f"Timeout waiting for subscription handlers to complete for {instrument}")
            except Exception as e:
                logger.error(f"Error in subscription handlers for {instrument}: {e}", exc_info=True)
            return True

        return False

    async def _handle_sub_message(self, message: Dict[str, Any]) -> None:
        """Legacy handler for incoming subscription messages.

        This is kept for backward compatibility but most logic has been moved to _handle_message.
        """
        try:
            # logger.debug(f"[WEBSOCKET] Received legacy sub message: {message}")
            # Just forward to _handle_message for processing
            await self._handle_message(message)
        except Exception as e:
            logger.error(f"Error in _handle_sub_message: {e}", exc_info=True)

    def add_subscription_handler(self, handler: Callable[[str], None]) -> None:
        """Add a handler to be called when a subscription is confirmed.

        Args:
            handler: A coroutine function that takes an instrument name as argument
        """
        self._subscription_handlers.add(handler)

    def remove_subscription_handler(self, handler: Callable[[str], None]) -> None:
        """Remove a subscription handler."""
        self._subscription_handlers.discard(handler)

    async def _handle_ticker_update(self, channel: str, data: Dict[str, Any]) -> None:
        """Handle ticker subscription updates."""
        if not data:
            logger.debug("[WEBSOCKET] No data in ticker update")
            return

        try:
            instrument = channel.split(".")[1] if "." in channel else "unknown"

            # Extract price - prioritize mark_price, fall back to last_price
            mark_price = data.get("mark_price")
            if mark_price is None:
                mark_price = data.get("last_price")
                if mark_price is None:
                    logger.debug(f"[WEBSOCKET] No price data in ticker update for {instrument}")
                    return

            mark_price = float(mark_price)
            # logger.debug(f"[WEBSOCKET] Ticker update - {instrument}: {mark_price}")

            # Update price/IV cache
            mark_iv = data.get("mark_iv")
            iv = mark_iv / 100 if mark_iv and mark_iv > 3 else (mark_iv if mark_iv else 0.0)

            self.price_iv_cache[instrument] = {
                "mark_price": mark_price,
                "iv": iv,
                "timestamp": time.time()
            }

            # Dispatch to price callback if set
            if self.price_callback:
                try:
                    if asyncio.iscoroutinefunction(self.price_callback):
                        await self.price_callback(instrument, mark_price)
                    else:
                        self.price_callback(instrument, mark_price)
                except Exception as e:
                    logger.error(f"[WEBSOCKET] Error in price callback: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"[WEBSOCKET] Error processing ticker update: {e}", exc_info=True)

    async def _handle_order_book_update(self, channel: str, data: Dict[str, Any]) -> None:
        """Handle order book subscription updates."""
        if not self.price_callback:
            return

        try:
            instrument = channel.split(".")[1] if "." in channel else "unknown"
            if not data or "bids" not in data or "asks" not in data or not data["bids"] or not data["asks"]:
                logger.debug(f"[WEBSOCKET] Incomplete order book data for {instrument}")
                return

            bid = float(data["bids"][0][0])
            ask = float(data["asks"][0][0])
            mid_price = (bid + ask) / 2

            # logger.debug(f"[WEBSOCKET] Order book update - {instrument}: {mid_price} (bid: {bid}, ask: {ask})")

            # Update price cache (without IV for order book updates)
            if instrument in self.price_iv_cache:
                self.price_iv_cache[instrument]["mark_price"] = mid_price
                self.price_iv_cache[instrument]["timestamp"] = time.time()

            # Dispatch to price callback if set
            if self.price_callback:
                try:
                    if asyncio.iscoroutinefunction(self.price_callback):
                        await self.price_callback(instrument, mid_price)
                    else:
                        self.price_callback(instrument, mid_price)
                except Exception as e:
                    logger.error(f"[WEBSOCKET] Error in order book callback: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"[WEBSOCKET] Error processing order book update: {e}", exc_info=True)

    async def _handle_trade_update(self, channel: str, data: Dict[str, Any]) -> None:
        """Handle trade subscription updates."""
        if not self.price_callback:
            return

        try:
            instrument = channel.split(".")[1] if "." in channel else "unknown"
            trades = data if isinstance(data, list) else [data]
            if not trades or "price" not in trades[0]:
                logger.debug(f"[WEBSOCKET] No trade data in update for {instrument}")
                return

            price = float(trades[0]["price"])
            # logger.debug(f"[WEBSOCKET] Trade update - {instrument}: {price}")

            # Update price cache (without IV for trade updates)
            if instrument in self.price_iv_cache:
                self.price_iv_cache[instrument]["mark_price"] = price
                self.price_iv_cache[instrument]["timestamp"] = time.time()

            # Dispatch to price callback if set
            if self.price_callback:
                try:
                    if asyncio.iscoroutinefunction(self.price_callback):
                        await self.price_callback(instrument, price)
                    else:
                        self.price_callback(instrument, price)
                except Exception as e:
                    logger.error(f"[WEBSOCKET] Error in trade callback: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"[WEBSOCKET] Error processing trade update: {e}", exc_info=True)
