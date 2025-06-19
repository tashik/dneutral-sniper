import websockets
import json
import asyncio
import logging
from typing import Optional, Callable, Dict
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

    def get_price_iv(self, instrument_name: str):
        """Return (mark_price, iv) for the instrument from cache, or (None, None) if not available."""
        entry = self.price_iv_cache.get(instrument_name)
        if entry:
            return entry.get("mark_price"), entry.get("iv")
        return None, None

    async def subscribe_to_instruments(self, instrument_names):
        """
        Subscribe to a set of instrument tickers (options or futures).
        Avoid duplicate subscriptions using self.subscribed_instruments.
        """
        new_instruments = set(instrument_names) - self.subscribed_instruments
        if not new_instruments:
            return
        channels = [f"ticker.{name}.100ms" for name in new_instruments]
        subscribe_params = {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 42,
            "params": {"channels": channels}
        }
        await asyncio.sleep(0.5)
        await self.sub_ws.send(json.dumps(subscribe_params))
        logger.info(f"Subscribed to tickers: {channels}")
        self.subscribed_instruments.update(new_instruments)



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
            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
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
            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
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

    async def send_request(self, method: str, params: dict) -> dict:
        """Send JSON-RPC request and await response via listener"""
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
            logger.debug(f"Cache hit for {instrument_name}: {cached}")
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

    def set_price_callback(self, callback: Callable[[str, float], None]):
        """Set callback for price updates"""
        self.price_callback = callback

    async def _handle_message(self, message: Dict):
        """Handle incoming WebSocket messages"""
        if message.get("method") == "subscription":
            data = message.get("params", {}).get("data", {})
            if "ticker" in message.get("params", {}).get("channel", ""):
                instrument_name = data.get("instrument_name")
                mark_price = data.get("mark_price")
                mark_iv = data.get("mark_iv")
                # Update cache
                iv = None
                if mark_iv is not None:
                    iv = mark_iv / 100 if mark_iv > 3 else mark_iv
                # logger.info(f"[DEBUG] Ticker update: {instrument_name}, mark_price={mark_price}, mark_iv={mark_iv}, iv={iv}")
                self.price_iv_cache[instrument_name] = {"mark_price": mark_price, "iv": iv}
                if self.price_callback and mark_price is not None:
                    self.price_callback(instrument_name, mark_price)
