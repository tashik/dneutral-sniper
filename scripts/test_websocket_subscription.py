#!/usr/bin/env python3
"""
Test script to verify WebSocket subscription to Deribit.
"""
import asyncio
import json
import logging
import os
from typing import Dict, Any
import websockets

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
        # logging.FileHandler('websocket_test.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuration
TESTNET = False
INSTRUMENTS = ["BTC-PERPETUAL"]

class WebSocketTester:
    def __init__(self, testnet: bool = True):
        self.ws_url = "wss://test.deribit.com/ws/api/v2" if testnet else "wss://www.deribit.com/ws/api/v2"
        self.ws = None
        self.running = False
        self._id_counter = 0
        self.pending_requests = {}

    async def connect(self):
        """Connect to the WebSocket server."""
        logger.info(f"Connecting to {self.ws_url}...")
        self.ws = await websockets.connect(self.ws_url, ping_interval=10, ping_timeout=5)
        logger.info("Connected to WebSocket")
        self.running = True

    async def _next_id(self) -> int:
        """Get the next request ID."""
        self._id_counter += 1
        return self._id_counter

    async def subscribe(self, channels: list) -> bool:
        """Subscribe to the given channels."""
        if not self.ws:
            logger.error("WebSocket is not connected")
            return False

        request_id = await self._next_id()
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "public/subscribe",
            "params": {
                "channels": channels
            }
        }

        logger.info(f"Sending subscription request: {request}")
        await self.ws.send(json.dumps(request))
        return True

    async def listen(self):
        """Listen for incoming messages and print them."""
        if not self.ws:
            logger.error("WebSocket is not connected")
            return

        logger.info("Starting to listen for messages...")
        try:
            async for message in self.ws:
                try:
                    data = json.loads(message)
                    logger.info(f"Received message: {json.dumps(data, indent=2)}")

                    # Handle subscription confirmations
                    if "result" in data and isinstance(data["result"], list):
                        channels = data["result"]
                        logger.info(f"Successfully subscribed to channels: {channels}")

                    # Handle subscription updates
                    elif data.get("method") == "subscription":
                        channel = data.get("params", {}).get("channel", "unknown")
                        logger.info(f"Received update on channel: {channel}")

                    # Handle errors
                    elif "error" in data:
                        error = data["error"]
                        logger.error(f"Error in response: {error}")

                except json.JSONDecodeError:
                    logger.error(f"Failed to decode message: {message}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)

        except websockets.exceptions.ConnectionClosed as e:
            logger.error(f"WebSocket connection closed: {e}")
            self.running = False
        except Exception as e:
            logger.error(f"Error in WebSocket listener: {e}", exc_info=True)
            self.running = False

    async def run(self, instruments: list):
        """Run the WebSocket test."""
        try:
            await self.connect()

            # Subscribe to ticker channels
            channels = [f"ticker.{instrument}.100ms" for instrument in instruments]
            await self.subscribe(channels)

            # Start listening for messages
            await self.listen()

        except Exception as e:
            logger.error(f"Error in WebSocket test: {e}", exc_info=True)
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Clean up resources."""
        self.running = False
        if self.ws:
            await self.ws.close()
            logger.info("WebSocket connection closed")

async def main():
    """Main function."""
    tester = WebSocketTester(testnet=TESTNET)
    try:
        await tester.run(INSTRUMENTS)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        await tester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
