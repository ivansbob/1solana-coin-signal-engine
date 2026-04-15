"""Jito Bundle Client for sending real transactions via Jito bundles."""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
import aiohttp
from solders.keypair import Keypair
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction
from solders.message import MessageV0
from solders.hash import Hash

from src.ingest.jito_priority_context import JitoPriorityContext

logger = logging.getLogger(__name__)

# Jito Bundle API endpoint
JITO_BUNDLE_ENDPOINT = "https://mainnet.block-engine.jito.wtf/api/v1/bundles"

# Official Jito Tip accounts (as of 2024)
JITO_TIP_ACCOUNTS = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",  # Jito tip account 1
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bLmis",  # Jito tip account 2
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLk",  # Jito tip account 3
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",  # Jito tip account 4
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",  # Jito tip account 5
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimqcQn2WvDFAFER",  # Jito tip account 6
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",  # Jito tip account 7
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",  # Jito tip account 8
]


class JitoBundleClient:
    """Client for sending transaction bundles via Jito."""

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        self.session = session
        self.timeout = timeout
        self.max_retries = max_retries
        self._session_owned = session is None

    async def __aenter__(self):
        if self._session_owned and self.session is None:
            self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session_owned and self.session:
            await self.session.close()

    def _select_tip_account(self) -> str:
        """Select a random tip account for load balancing."""
        import random
        return random.choice(JITO_TIP_ACCOUNTS)

    def _build_tip_instruction(
        self,
        payer_keypair: Keypair,
        tip_amount_lamports: int,
        tip_account: str,
    ) -> Any:
        """Build a SOL transfer instruction to tip Jito."""
        from solders.pubkey import Pubkey

        transfer_ix = transfer(
            TransferParams(
                from_pubkey=payer_keypair.pubkey(),
                to_pubkey=Pubkey.from_string(tip_account),
                lamports=tip_amount_lamports,
            )
        )
        return transfer_ix

    async def _get_recent_blockhash(self) -> Hash:
        """Get recent blockhash for transaction construction."""
        # This is a simplified version - in production you'd get this from RPC
        # For now, we'll use a placeholder - this should be replaced with actual RPC call
        import time
        # Placeholder blockhash - replace with actual RPC call
        return Hash.from_string("11111111111111111111111111111112")

    async def build_and_send_bundle(
        self,
        swap_instructions: List[Any],
        payer_keypair: Keypair,
        jito_context: JitoPriorityContext,
        recent_blockhash: Optional[Hash] = None,
    ) -> Dict[str, Any]:
        """Build and send a transaction bundle to Jito.

        Args:
            swap_instructions: List of swap instructions to include in bundle
            payer_keypair: Keypair for signing transactions
            jito_context: Jito priority context with tip information
            recent_blockhash: Recent blockhash (optional, will fetch if not provided)

        Returns:
            Dict containing bundle ID and status information
        """
        try:
            if recent_blockhash is None:
                recent_blockhash = await self._get_recent_blockhash()

            # Select tip account
            tip_account = self._select_tip_account()

            # Build tip instruction
            tip_instruction = self._build_tip_instruction(
                payer_keypair,
                jito_context.dynamic_tip_target_lamports,
                tip_account,
            )

            # Combine swap instructions with tip
            all_instructions = swap_instructions + [tip_instruction]

            # Build message
            message = MessageV0.try_compile(
                payer_keypair.pubkey(),
                all_instructions,
                [],
                recent_blockhash,
            )

            # Create versioned transaction
            transaction = VersionedTransaction(message, [payer_keypair])

            # Convert to bundle format
            bundle = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sendBundle",
                "params": [[transaction.to_bytes().hex()]],
            }

            # Send bundle
            return await self._send_bundle_request(bundle)

        except Exception as e:
            logger.error(f"Error building/sending bundle: {e}")
            return {
                "success": False,
                "error": str(e),
                "bundle_id": None,
            }

    async def _send_bundle_request(self, bundle: Dict[str, Any]) -> Dict[str, Any]:
        """Send bundle request to Jito API."""
        if not self.session:
            raise RuntimeError("Client session not available")

        for attempt in range(self.max_retries):
            try:
                async with self.session.post(
                    JITO_BUNDLE_ENDPOINT,
                    json=bundle,
                    headers={"Content-Type": "application/json"},
                    timeout=self.timeout,
                ) as response:
                    result = await response.json()

                    if response.status == 200:
                        bundle_id = result.get("result")
                        logger.info(f"Bundle sent successfully, ID: {bundle_id}")
                        return {
                            "success": True,
                            "bundle_id": bundle_id,
                            "status": "sent",
                        }
                    else:
                        error_msg = result.get("error", {}).get("message", "Unknown error")
                        logger.warning(f"Bundle send failed (attempt {attempt + 1}): {error_msg}")

                        if attempt == self.max_retries - 1:
                            return {
                                "success": False,
                                "error": error_msg,
                                "bundle_id": None,
                            }

            except asyncio.TimeoutError:
                logger.warning(f"Bundle send timeout (attempt {attempt + 1})")
                if attempt == self.max_retries - 1:
                    return {
                        "success": False,
                        "error": "Request timeout",
                        "bundle_id": None,
                    }

            except Exception as e:
                logger.error(f"Bundle send error (attempt {attempt + 1}): {e}")
                if attempt == self.max_retries - 1:
                    return {
                        "success": False,
                        "error": str(e),
                        "bundle_id": None,
                    }

            # Wait before retry
            await asyncio.sleep(0.5 * (2 ** attempt))

        return {
            "success": False,
            "error": "Max retries exceeded",
            "bundle_id": None,
        }

    async def get_bundle_statuses(self, bundle_ids: List[str]) -> Dict[str, Any]:
        """Get status of one or more bundles.

        Args:
            bundle_ids: List of bundle IDs to check

        Returns:
            Dict mapping bundle IDs to their status information
        """
        if not self.session:
            raise RuntimeError("Client session not available")

        if not bundle_ids:
            return {}

        try:
            status_request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBundleStatuses",
                "params": [bundle_ids],
            }

            async with self.session.post(
                JITO_BUNDLE_ENDPOINT,
                json=status_request,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            ) as response:
                result = await response.json()

                if response.status == 200 and "result" in result:
                    statuses = result["result"]
                    logger.debug(f"Retrieved bundle statuses for {len(bundle_ids)} bundles")
                    return statuses
                else:
                    error_msg = result.get("error", {}).get("message", "Unknown error")
                    logger.error(f"Failed to get bundle statuses: {error_msg}")
                    return {}

        except Exception as e:
            logger.error(f"Error getting bundle statuses: {e}")
            return {}

    async def wait_for_bundle_confirmation(
        self,
        bundle_id: str,
        max_wait_time: float = 60.0,
        check_interval: float = 2.0,
    ) -> Dict[str, Any]:
        """Wait for bundle confirmation.

        Args:
            bundle_id: Bundle ID to monitor
            max_wait_time: Maximum time to wait in seconds
            check_interval: How often to check status in seconds

        Returns:
            Dict with final bundle status
        """
        import time
        start_time = time.time()

        while time.time() - start_time < max_wait_time:
            try:
                statuses = await self.get_bundle_statuses([bundle_id])

                if bundle_id in statuses:
                    status_info = statuses[bundle_id]
                    confirmation_status = status_info.get("confirmation_status")

                    if confirmation_status in ["confirmed", "finalized", "failed"]:
                        logger.info(f"Bundle {bundle_id} reached final status: {confirmation_status}")
                        return {
                            "bundle_id": bundle_id,
                            "status": confirmation_status,
                            "details": status_info,
                        }

                await asyncio.sleep(check_interval)

            except Exception as e:
                logger.error(f"Error checking bundle status: {e}")
                await asyncio.sleep(check_interval)

        logger.warning(f"Bundle {bundle_id} confirmation timeout after {max_wait_time}s")
        return {
            "bundle_id": bundle_id,
            "status": "timeout",
            "details": {},
        }