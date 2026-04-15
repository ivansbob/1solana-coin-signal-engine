#!/usr/bin/env python3
"""HFT Arbitrage Daemon - Continuous high-frequency arbitrage execution."""

import asyncio
import logging
import signal
import sys
import os
from typing import Any, Dict, List, Optional
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.helius_ws_streamer import HeliusWsStreamer
from collectors.light_arb_detector import AdvancedArbDetector
from trading.live_executor import LiveTrader
from src.ingest.jupiter_api_client import JupiterClient
from src.ingest.jito_priority_context import JitoPriorityContextAdapter
from src.ingest.tx_builder import JupiterTxBuilder
from solders.keypair import Keypair

# Configure logging for HFT
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/hft_daemon.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)


class HFTArbitrageDaemon:
    """High-frequency arbitrage daemon orchestrating all components."""

    def __init__(self):
        # Configuration from environment
        self.helius_api_key = os.environ.get("HELIUS_API_KEY")
        self.rpc_url = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

        if not self.helius_api_key:
            raise ValueError("HELIUS_API_KEY environment variable required")

        # Initialize components
        self.streamer: Optional[HeliusWsStreamer] = None
        self.arb_detector: Optional[AdvancedArbDetector] = None
        self.live_trader: Optional[LiveTrader] = None
        self.jupiter_client: Optional[JupiterClient] = None
        self.tx_builder: Optional[JupiterTxBuilder] = None

        # Control flags
        self.running = False
        self.shutdown_event = asyncio.Event()

        # Statistics
        self.stats = {
            "start_time": time.time(),
            "opportunities_found": 0,
            "trades_executed": 0,
            "errors": 0,
            "last_opportunity": None,
            "last_trade": None,
        }

    async def initialize_components(self) -> None:
        """Initialize all HFT components."""
        logger.info("Initializing HFT components...")

        # 1. Initialize Helius WebSocket streamer
        self.streamer = HeliusWsStreamer(
            api_key=self.helius_api_key,
            rpc_url=self.rpc_url,
            blockhash_refresh_interval=1.0,  # Refresh every 1 second
        )
        logger.info("Helius streamer initialized")

        # 2. Initialize Jupiter client
        self.jupiter_client = JupiterClient()
        logger.info("Jupiter client initialized")

        # 3. Initialize transaction builder
        self.tx_builder = JupiterTxBuilder()
        logger.info("Transaction builder initialized")

        # 4. Initialize arbitrage detector
        self.arb_detector = AdvancedArbDetector(
            helius_api_key=self.helius_api_key,
            rpc_url=self.rpc_url,
            jupiter_client=self.jupiter_client,
        )
        logger.info("Arbitrage detector initialized")

        # 5. Initialize live trader
        try:
            trader_keypair = Keypair.from_base58_string(
                os.environ.get("TRADER_PRIVATE_KEY", "")
            )
            self.live_trader = LiveTrader(
                payer_keypair=trader_keypair,
                jito_client=None,  # Will be initialized internally
                jito_adapter=JitoPriorityContextAdapter()
            )
            logger.info("Live trader initialized")
        except Exception as e:
            logger.error(f"Failed to initialize live trader: {e}")
            logger.warning("Running in simulation mode (no real trading)")
            self.live_trader = None

        logger.info("All HFT components initialized successfully")

    async def run_streaming_task(self) -> None:
        """Task 1: Run Helius WebSocket streaming."""
        logger.info("Starting WebSocket streaming task")
        try:
            await self.streamer.start_stream()
        except Exception as e:
            logger.error(f"Streaming task failed: {e}")
        finally:
            logger.info("WebSocket streaming task stopped")

    async def run_arbitrage_scanning_task(self) -> None:
        """Task 2: Continuous arbitrage opportunity scanning."""
        logger.info("Starting arbitrage scanning task")
        scan_interval = 0.05  # 50ms scan interval for HFT

        while self.running:
            try:
                # Get current market tokens (simplified - in production would be dynamic)
                # For demo, we'll scan a fixed set of high-volume tokens
                token_addresses = [
                    "So11111111111111111111111111111112",  # SOL
                    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                    # Add more tokens as needed
                ]

                # Get fresh market states from streamer (simplified)
                market_states = []  # Would be populated from streamer data

                # Create portfolio context (simplified)
                portfolio_ctx = {
                    "portfolio": {
                        "total_value_sol": 100.0,
                        "free_capital_sol": 100.0,
                        "open_positions": 0,
                    },
                    "positions": [],
                }

                # Create settings context
                settings = type('Settings', (), {
                    'LIVE_MAX_CONCURRENT_POSITIONS': 5,
                    'LIVE_CONTRACT_VERSION': 'hft_daemon_v1',
                })()

                # Scan for arbitrage opportunities
                opportunities = await self.arb_detector.scan_arb_opportunities(
                    token_addresses=token_addresses,
                    market_states=market_states,
                    portfolio_ctx=portfolio_ctx,
                    settings=settings,
                )

                if opportunities:
                    self.stats["opportunities_found"] += len(opportunities)
                    self.stats["last_opportunity"] = time.time()

                    logger.info(f"Found {len(opportunities)} arbitrage opportunities")

                    # Queue opportunities for execution
                    for opportunity in opportunities:
                        await self._queue_arbitrage_signal(opportunity)

                await asyncio.sleep(scan_interval)

            except Exception as e:
                logger.error(f"Arbitrage scanning error: {e}")
                self.stats["errors"] += 1
                await asyncio.sleep(scan_interval * 2)  # Longer delay on error

        logger.info("Arbitrage scanning task stopped")

    async def run_execution_task(self) -> None:
        """Task 3: Execute queued arbitrage signals."""
        logger.info("Starting execution task")

        while self.running:
            try:
                # In a full implementation, this would listen to an asyncio.Queue
                # for arbitrage signals from the scanning task

                # For now, we'll just wait and log status
                await asyncio.sleep(1.0)

                # Log periodic statistics
                await self._log_status()

            except Exception as e:
                logger.error(f"Execution task error: {e}")
                await asyncio.sleep(1.0)

        logger.info("Execution task stopped")

    async def _queue_arbitrage_signal(self, signal: Dict[str, Any]) -> None:
        """Queue an arbitrage signal for execution."""
        logger.info(f"Queuing arbitrage signal: {signal.get('token_address')} -> {signal.get('expected_net_profit', 0):.4f} SOL profit")

        # In production, this would put the signal in an asyncio.Queue
        # that the execution task reads from

        if self.live_trader:
            try:
                # Execute the arbitrage signal
                market_states = []  # Would be populated with current market data
                portfolio_ctx = {
                    "portfolio": {"total_value_sol": 100.0, "free_capital_sol": 100.0, "open_positions": 0},
                    "positions": [],
                }
                settings = type('Settings', (), {
                    'LIVE_MAX_CONCURRENT_POSITIONS': 5,
                    'LIVE_CONTRACT_VERSION': 'hft_daemon_v1',
                })()

                result = await self.live_trader.execute_entry(
                    signal=signal,
                    market_states=market_states,
                    state=portfolio_ctx,
                    settings=settings,
                )

                if result:
                    self.stats["trades_executed"] += 1
                    self.stats["last_trade"] = time.time()
                    logger.info(f"Arbitrage executed: {result}")

            except Exception as e:
                logger.error(f"Arbitrage execution failed: {e}")
                self.stats["errors"] += 1

    async def _log_status(self) -> None:
        """Log current daemon status."""
        uptime = time.time() - self.stats["start_time"]
        logger.info(
            f"HFT Status - Uptime: {uptime:.1f}s | "
            f"Opportunities: {self.stats['opportunities_found']} | "
            f"Trades: {self.stats['trades_executed']} | "
            f"Errors: {self.stats['errors']}"
        )

    async def run(self) -> None:
        """Main daemon execution loop."""
        logger.info("Starting HFT Arbitrage Daemon...")

        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            # Initialize all components
            await self.initialize_components()

            # Set running flag
            self.running = True

            # Create and run all tasks concurrently
            tasks = [
                asyncio.create_task(self.run_streaming_task(), name="streaming"),
                asyncio.create_task(self.run_arbitrage_scanning_task(), name="scanning"),
                asyncio.create_task(self.run_execution_task(), name="execution"),
            ]

            logger.info("All HFT tasks started - entering main loop")

            # Wait for shutdown signal
            await self.shutdown_event.wait()

            logger.info("Shutdown signal received, stopping tasks...")

        except Exception as e:
            logger.error(f"Fatal error in HFT daemon: {e}")
            raise
        finally:
            # Cleanup
            self.running = False

            # Stop streamer if running
            if self.streamer:
                await self.streamer.stop_stream()

            # Cancel all remaining tasks
            current_task = asyncio.current_task()
            all_tasks = [t for t in asyncio.all_tasks() if t is not current_task]

            for task in all_tasks:
                if not task.done():
                    task.cancel()

            # Wait for tasks to complete
            if all_tasks:
                await asyncio.gather(*all_tasks, return_exceptions=True)

            logger.info("HFT Arbitrage Daemon stopped")


async def main() -> int:
    """Main entry point."""
    try:
        daemon = HFTArbitrageDaemon()
        await daemon.run()
        return 0
    except KeyboardInterrupt:
        logger.info("Daemon interrupted by user")
        return 0
    except Exception as e:
        logger.error(f"Daemon failed: {e}")
        return 1


if __name__ == "__main__":
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)

    # Run the daemon
    exit_code = asyncio.run(main())
    sys.exit(exit_code)