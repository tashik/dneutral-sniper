import websockets
import json
import asyncio
import logging
from typing import Optional, Callable, Dict
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class DeribitCredentials:
    client_id: str
    client_secret: str
    test: bool = True

class DeribitWebsocketClient:
    TESTNET_WS_URL = "wss://test.deribit.com/ws/api/v2"
    MAINNET_WS_URL = "wss://www.deribit.com/ws/api/v2"

    def __init__(self, credentials: Optional[DeribitCredentials] = None):
        self.credentials = credentials
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.price_callback: Optional[Callable[[str, float], None]] = None
        self.running = False
        self.ws_url = self.TESTNET_WS_URL if credentials and credentials.test else self.MAINNET_WS_URL

    async def connect(self):
        """Connect to Deribit WebSocket"""
        self.ws = await websockets.connect(self.ws_url)
        if self.credentials:
            await self._authenticate()
        self.running = True

    async def _authenticate(self):
        """Authenticate with API credentials"""
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
        await self.ws.send(json.dumps(auth_params))
        response = await self.ws.recv()
        logger.info(f"Authentication response: {response}")

    async def subscribe_to_ticker(self, instrument_name: str):
        """Subscribe to ticker updates for a specific instrument"""
        subscribe_params = {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 2,
            "params": {
                "channels": [f"ticker.{instrument_name}.raw"]
            }
        }
        await self.ws.send(json.dumps(subscribe_params))

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
                if self.price_callback and mark_price is not None:
                    self.price_callback(instrument_name, mark_price)

    async def listen(self):
        """Listen for WebSocket messages"""
        while self.running:
            try:
                message = await self.ws.recv()
                await self._handle_message(json.loads(message))
            except Exception as e:
                logger.error(f"Error in WebSocket listener: {e}")
                await self.reconnect()

    async def reconnect(self):
        """Reconnect to WebSocket on connection loss"""
        logger.info("Attempting to reconnect...")
        try:
            await self.connect()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            await asyncio.sleep(5)  # Wait before retrying

    async def close(self):
        """Close WebSocket connection"""
        self.running = False
        if self.ws:
            await self.ws.close()
