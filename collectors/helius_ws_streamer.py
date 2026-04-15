"""Helius WebSocket streamer for real-time Solana data."""

import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional
import websockets
from websockets.exceptions import ConnectionClosedError, WebSocketException
import aiohttp

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
        blockhash_refresh_interval: float = 1.0,  # Refresh every 1 second for HFT
        blockhash_cache_ttl: float = 30.0,  # Blockhash valid for 30 seconds
        rpc_url: str = "https://api.mainnet-beta.solana.com",
    ):
        self.api_key = api_key
        self.ws_url = f"wss://atlas-mainnet.helius-rpc.com/?api-key={api_key}"
        self.rpc_url = rpc_url
        self.program_ids = program_ids or [RAYDIUM_AMM_V4_PROGRAM_ID]
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self.heartbeat_interval = heartbeat_interval
        self.blockhash_refresh_interval = blockhash_refresh_interval
        self.blockhash_cache_ttl = blockhash_cache_ttl

        self.websocket: Optional[Any] = None  # WebSocket connection
        self.data_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self.running = False
        self.subscriptions: Dict[int, str] = {}  # subscription_id -> program_id

        # Blockhash caching for HFT performance
        self._blockhash_cache: Optional[str] = None
        self._blockhash_timestamp: float = 0.0
        self._blockhash_lock = asyncio.Lock()
        self._http_session: Optional[aiohttp.ClientSession] = None

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

                    # Ensure HTTP session for blockhash fetching
                    await self._ensure_http_session()

                    # Start background tasks
                    heartbeat_task = asyncio.create_task(self._heartbeat())
                    blockhash_task = asyncio.create_task(self._keep_blockhash_fresh())

                    try:
                        async for message in websocket:
                            try:
                                notification = json.loads(message)
                                await self._handle_notification(notification)
                            except json.JSONDecodeError as e:
                                logger.error(f"Failed to parse WebSocket message: {e}")
                    finally:
                        # Cancel background tasks
                        heartbeat_task.cancel()
                        blockhash_task.cancel()

                        try:
                            await heartbeat_task
                        except asyncio.CancelledError:
                            pass

                        try:
                            await blockhash_task
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

        # Cleanup HTTP session
        await self._cleanup_http_session()

    async def get_next_update(self) -> Optional[Dict[str, Any]]:
        """Get the next account update from the queue."""
        try:
            return await asyncio.wait_for(self.data_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            return None

    def get_queue_size(self) -> int:
        """Get the current size of the data queue."""
        return self.data_queue.qsize()

    async def _keep_blockhash_fresh(self) -> None:
        """Background task to keep blockhash cache fresh for HFT performance."""
        logger.info("Starting blockhash refresh task")

        while self.running:
            try:
                await self._refresh_blockhash_cache()
                await asyncio.sleep(self.blockhash_refresh_interval)
            except Exception as e:
                logger.error(f"Blockhash refresh error: {e}")
                await asyncio.sleep(self.blockhash_refresh_interval)

        logger.info("Blockhash refresh task stopped")

    async def _refresh_blockhash_cache(self) -> None:
        """Fetch and cache the latest blockhash."""
        if not self._http_session:
            self._http_session = aiohttp.ClientSession()

        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getLatestBlockhash",
                "params": [{"commitment": "confirmed"}]
            }

            async with self._http_session.post(
                self.rpc_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=2.0)  # Fast timeout for HFT
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    blockhash = result.get("result", {}).get("value", {}).get("blockhash")

                    if blockhash:
                        async with self._blockhash_lock:
                            self._blockhash_cache = blockhash
                            self._blockhash_timestamp = time.time()
                            logger.debug(f"Cached fresh blockhash: {blockhash[:8]}...")
                    else:
                        logger.warning("No blockhash in RPC response")
                else:
                    logger.warning(f"Blockhash RPC error: {response.status}")

        except asyncio.TimeoutError:
            logger.debug("Blockhash RPC timeout (expected for HFT)")
        except Exception as e:
            logger.error(f"Blockhash RPC request error: {e}")

    def get_cached_blockhash(self) -> Optional[str]:
        """Get cached blockhash if still fresh.

        Returns:
            Blockhash string if cache is valid, None otherwise
        """
        current_time = time.time()

        if (self._blockhash_cache and
            current_time - self._blockhash_timestamp < self.blockhash_cache_ttl):
            return self._blockhash_cache

        return None

    async def _ensure_http_session(self) -> None:
        """Ensure HTTP session exists for blockhash fetching."""
        if not self._http_session:
            self._http_session = aiohttp.ClientSession()

    async def _cleanup_http_session(self) -> None:
        """Clean up HTTP session."""
        if self._http_session:
            await self._http_session.close()
            self._http_session = None