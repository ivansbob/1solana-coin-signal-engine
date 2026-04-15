"""Helius WebSocket streamer for real-time Solana data."""

import asyncio
import json
import logging
from typing import Any, Dict, Optional
import websockets
from websockets.exceptions import ConnectionClosedError, WebSocketException

logger = logging.getLogger(__name__)

# Raydium AMM V4 program ID
RAYDIUM_AMM_V4_PROGRAM_ID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"


class HeliusWsStreamer:
    """WebSocket streamer for real-time Helius data."""

    def __init__(
        self,
        api_key: str,
        program_ids: Optional[list[str]] = None,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 60.0,
        heartbeat_interval: float = 30.0,
    ):
        self.api_key = api_key
        self.ws_url = f"wss://atlas-mainnet.helius-rpc.com/?api-key={api_key}"
        self.program_ids = program_ids or [RAYDIUM_AMM_V4_PROGRAM_ID]
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self.heartbeat_interval = heartbeat_interval

        self.websocket: Optional[websockets.WebSocketServerProtocol] = None
        self.data_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self.running = False
        self.subscriptions: Dict[int, str] = {}  # subscription_id -> program_id

    async def _transform_account_notification(self, notification: Dict[str, Any]) -> Dict[str, Any]:
        """Transform WebSocket account notification to account_info format."""
        try:
            params = notification.get("params", {})
            result = params.get("result", {})

            # Extract account data
            account_data = result.get("account", {})
            pubkey = result.get("pubkey", "")

            # Transform to match get_account_info format from solana_rpc_client.py
            account_info = {
                "owner": account_data.get("owner", ""),
                "lamports": account_data.get("lamports", 0),
                "data": account_data.get("data", ["", "base64"]),
                "executable": account_data.get("executable", False),
                "rentEpoch": account_data.get("rentEpoch", 0),
                "space": account_data.get("space", 0),
            }

            return {
                "pubkey": pubkey,
                "account": account_info,
                "context": result.get("context", {}),
                "notification_type": "account",
                "program_id": params.get("subscription", ""),
            }

        except Exception as e:
            logger.error(f"Error transforming account notification: {e}")
            return {}

    async def _transform_program_notification(self, notification: Dict[str, Any]) -> Dict[str, Any]:
        """Transform WebSocket program notification to account_info format."""
        try:
            params = notification.get("params", {})
            result = params.get("result", {})

            # For program notifications, we get account updates
            account_data = result.get("account", {})
            pubkey = result.get("pubkey", "")

            # Transform to match get_account_info format
            account_info = {
                "owner": account_data.get("owner", ""),
                "lamports": account_data.get("lamports", 0),
                "data": account_data.get("data", ["", "base64"]),
                "executable": account_data.get("executable", False),
                "rentEpoch": account_data.get("rentEpoch", 0),
                "space": account_data.get("space", 0),
            }

            return {
                "pubkey": pubkey,
                "account": account_info,
                "context": result.get("context", {}),
                "notification_type": "program",
                "program_id": params.get("subscription", ""),
            }

        except Exception as e:
            logger.error(f"Error transforming program notification: {e}")
            return {}

    async def _handle_notification(self, notification: Dict[str, Any]) -> None:
        """Handle incoming WebSocket notification."""
        try:
            method = notification.get("method", "")

            if method == "accountNotification":
                transformed_data = await self._transform_account_notification(notification)
            elif method == "programNotification":
                transformed_data = await self._transform_program_notification(notification)
            else:
                logger.debug(f"Unhandled notification method: {method}")
                return

            if transformed_data:
                await self.data_queue.put(transformed_data)
                logger.debug(f"Queued notification for {transformed_data.get('pubkey', 'unknown')}")

        except Exception as e:
            logger.error(f"Error handling notification: {e}")

    async def _subscribe_to_programs(self) -> None:
        """Subscribe to program updates for all configured program IDs."""
        if not self.websocket:
            return

        for program_id in self.program_ids:
            try:
                subscription_request = {
                    "jsonrpc": "2.0",
                    "id": len(self.subscriptions) + 1,
                    "method": "programSubscribe",
                    "params": [
                        program_id,
                        {
                            "commitment": "confirmed",
                            "encoding": "jsonParsed",
                            "filters": []
                        }
                    ]
                }

                await self.websocket.send(json.dumps(subscription_request))
                logger.info(f"Subscribed to program: {program_id}")

                # Store subscription mapping (we'll get the actual subscription ID in response)
                self.subscriptions[len(self.subscriptions) + 1] = program_id

            except Exception as e:
                logger.error(f"Error subscribing to program {program_id}: {e}")

    async def _heartbeat(self) -> None:
        """Send periodic heartbeat to keep connection alive."""
        while self.running:
            try:
                if self.websocket and self.websocket.open:
                    # Send a ping to keep connection alive
                    await self.websocket.ping()
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                break

    async def _connect_and_listen(self) -> None:
        """Connect to WebSocket and listen for messages."""
        current_delay = self.reconnect_delay

        while self.running:
            try:
                async with websockets.connect(
                    self.ws_url,
                    extra_headers={"User-Agent": "scse/0.1"},
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as websocket:
                    self.websocket = websocket
                    logger.info("Connected to Helius WebSocket")

                    # Reset reconnect delay on successful connection
                    current_delay = self.reconnect_delay

                    # Subscribe to programs
                    await self._subscribe_to_programs()

                    # Start heartbeat task
                    heartbeat_task = asyncio.create_task(self._heartbeat())

                    try:
                        async for message in websocket:
                            try:
                                notification = json.loads(message)
                                await self._handle_notification(notification)
                            except json.JSONDecodeError as e:
                                logger.error(f"Failed to parse WebSocket message: {e}")
                    finally:
                        heartbeat_task.cancel()
                        try:
                            await heartbeat_task
                        except asyncio.CancelledError:
                            pass

            except (ConnectionClosedError, WebSocketException, OSError) as e:
                logger.warning(f"WebSocket connection error: {e}")
                if self.running:
                    logger.info(f"Reconnecting in {current_delay} seconds...")
                    await asyncio.sleep(current_delay)
                    current_delay = min(current_delay * 2, self.max_reconnect_delay)
            except Exception as e:
                logger.error(f"Unexpected WebSocket error: {e}")
                if self.running:
                    await asyncio.sleep(current_delay)
                    current_delay = min(current_delay * 2, self.max_reconnect_delay)

        self.websocket = None

    async def start_stream(self) -> None:
        """Start the WebSocket streaming task."""
        if self.running:
            logger.warning("Streamer is already running")
            return

        self.running = True
        logger.info("Starting Helius WebSocket streamer")

        try:
            await self._connect_and_listen()
        except Exception as e:
            logger.error(f"Streamer failed: {e}")
        finally:
            self.running = False
            logger.info("Helius WebSocket streamer stopped")

    async def stop_stream(self) -> None:
        """Stop the WebSocket streaming task."""
        logger.info("Stopping Helius WebSocket streamer")
        self.running = False

        if self.websocket and self.websocket.open:
            try:
                await self.websocket.close()
            except Exception as e:
                logger.error(f"Error closing WebSocket: {e}")

    async def get_next_update(self) -> Optional[Dict[str, Any]]:
        """Get the next account update from the queue."""
        try:
            return await asyncio.wait_for(self.data_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            return None

    def get_queue_size(self) -> int:
        """Get the current size of the data queue."""
        return self.data_queue.qsize()