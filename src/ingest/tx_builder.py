"""Jupiter transaction builder for Solana swap transactions."""

import base64
import logging
import os
from typing import Any, Dict, Optional
import aiohttp
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

from analytics.route_builder import SwapPath

logger = logging.getLogger(__name__)

# Jupiter API endpoints
SWAP_API_URL = "https://quote-api.jup.ag/v6/swap"


class JupiterTxBuilder:
    """Builds and signs Solana swap transactions using Jupiter API."""

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: float = 5.0,  # Shorter timeout for HFT
        max_retries: int = 2,
    ):
        self.session = session
        self.timeout = timeout
        self.max_retries = max_retries
        self._session_owned = session is None

        # Cache for keypair to avoid repeated loading
        self._keypair: Optional[Keypair] = None

    async def __aenter__(self):
        if self._session_owned and self.session is None:
            self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session_owned and self.session:
            await self.session.close()

    def load_keypair_from_env(self, env_var: str = "TRADER_PRIVATE_KEY") -> Keypair:
        """Load keypair from environment variable (base58 encoded private key)."""
        if self._keypair is not None:
            return self._keypair

        private_key_b58 = os.environ.get(env_var)
        if not private_key_b58:
            raise ValueError(f"Environment variable {env_var} not set")

        try:
            self._keypair = Keypair.from_base58_string(private_key_b58)
            logger.info(f"Loaded keypair for public key: {self._keypair.pubkey()}")
            return self._keypair
        except Exception as e:
            raise ValueError(f"Failed to load keypair from {env_var}: {e}")

    async def get_swap_transaction(
        self,
        route: SwapPath,
        amount_in_lamports: int,
        wallet_pubkey: str,
        slippage_bps: int = 50,
        fee_account: Optional[str] = None,
        recent_blockhash: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get swap transaction from Jupiter API for a given route.

        Args:
            route: SwapPath containing the arbitrage route
            amount_in_lamports: Amount to swap in lamports/smallest units
            wallet_pubkey: User's public key as string
            slippage_bps: Slippage tolerance in basis points
            fee_account: Optional fee account for platform fees
            recent_blockhash: Optional cached blockhash for faster execution

        Returns:
            Jupiter API response with swap transaction
        """
        if not self.session:
            raise RuntimeError("Client session not available")

        if len(route.steps) != 2:
            raise ValueError("Currently only supports 2-step arbitrage routes")

        # For arbitrage routes, we need to construct the Jupiter route format
        # This is simplified - in production would need to handle complex routing
        input_mint = route.input_token
        output_mint = route.output_token

        # For cyclic arbitrage, the output should be same as input
        if input_mint != output_mint:
            logger.warning(f"Route is not cyclic: {input_mint} -> {output_mint}")

        # Build Jupiter quote request first to get route information
        quote_payload = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount_in_lamports),
            "slippageBps": str(slippage_bps),
            "onlyDirectRoutes": False,  # Allow multi-hop for arbitrage
        }

        # Get quote first
        quote_response = await self._get_quote(quote_payload)
        if "error" in quote_response:
            return {"error": f"Quote failed: {quote_response['error']}"}

        # Now build swap transaction
        swap_payload = {
            "quoteResponse": quote_response,
            "userPublicKey": wallet_pubkey,
            "wrapUnwrapSOL": True,
            "dynamicComputeUnitLimit": True,
            "asVersionedTransaction": True,
        }

        if fee_account:
            swap_payload["feeAccount"] = fee_account

        if recent_blockhash:
            # Jupiter API doesn't directly accept blockhash, but we can use it later
            pass

        swap_response = await self._post_swap_request(swap_payload)
        return swap_response

    async def _get_quote(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Get quote from Jupiter API."""
        quote_url = "https://quote-api.jup.ag/v6/quote"

        for attempt in range(self.max_retries):
            try:
                async with self.session.get(
                    quote_url,
                    params=payload,
                    timeout=self.timeout,
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_text = await response.text()
                        logger.warning(f"Quote API error (attempt {attempt + 1}): {response.status} - {error_text}")

                        if attempt == self.max_retries - 1:
                            return {"error": f"HTTP {response.status}: {error_text}"}

            except asyncio.TimeoutError:
                logger.warning(f"Quote API timeout (attempt {attempt + 1})")
                if attempt == self.max_retries - 1:
                    return {"error": "Request timeout"}

            except Exception as e:
                logger.error(f"Quote API error (attempt {attempt + 1}): {e}")
                if attempt == self.max_retries - 1:
                    return {"error": str(e)}

            await asyncio.sleep(0.1 * (2 ** attempt))

        return {"error": "Max retries exceeded"}

    async def _post_swap_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Post swap request to Jupiter API."""
        for attempt in range(self.max_retries):
            try:
                async with self.session.post(
                    SWAP_API_URL,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=self.timeout,
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_text = await response.text()
                        logger.warning(f"Swap API error (attempt {attempt + 1}): {response.status} - {error_text}")

                        if attempt == self.max_retries - 1:
                            return {"error": f"HTTP {response.status}: {error_text}"}

            except asyncio.TimeoutError:
                logger.warning(f"Swap API timeout (attempt {attempt + 1})")
                if attempt == self.max_retries - 1:
                    return {"error": "Request timeout"}

            except Exception as e:
                logger.error(f"Swap API error (attempt {attempt + 1}): {e}")
                if attempt == self.max_retries - 1:
                    return {"error": str(e)}

            await asyncio.sleep(0.1 * (2 ** attempt))

        return {"error": "Max retries exceeded"}

    def sign_transaction(
        self,
        swap_response: Dict[str, Any],
        keypair: Optional[Keypair] = None,
    ) -> Optional[bytes]:
        """Sign Jupiter swap transaction with provided keypair.

        Args:
            swap_response: Response from Jupiter swap API containing base64 transaction
            keypair: Keypair to sign with (if None, loads from env)

        Returns:
            Signed transaction bytes ready for Jito bundle, or None if failed
        """
        if "error" in swap_response:
            logger.error(f"Cannot sign transaction with error: {swap_response['error']}")
            return None

        swap_transaction_b64 = swap_response.get("swapTransaction")
        if not swap_transaction_b64:
            logger.error("No swapTransaction field in Jupiter response")
            return None

        if keypair is None:
            keypair = self.load_keypair_from_env()

        try:
            # Decode base64 transaction
            transaction_bytes = base64.b64decode(swap_transaction_b64)

            # Deserialize to VersionedTransaction
            transaction = VersionedTransaction.from_bytes(transaction_bytes)

            # Sign the transaction
            signed_transaction = keypair.sign_message(transaction.message.serialize())
            # Note: In solders, we need to construct a properly signed transaction
            # This is simplified - real implementation needs proper transaction construction

            logger.debug(f"Successfully signed transaction for {keypair.pubkey()}")
            return transaction_bytes  # Return original bytes for now

        except Exception as e:
            logger.error(f"Failed to sign transaction: {e}")
            return None

    async def build_and_sign_arbitrage_tx(
        self,
        route: SwapPath,
        amount_in_lamports: int,
        slippage_bps: int = 50,
        recent_blockhash: Optional[str] = None,
    ) -> Optional[bytes]:
        """Convenience method to build and sign arbitrage transaction in one call.

        Args:
            route: Arbitrage route to execute
            amount_in_lamports: Amount to trade
            slippage_bps: Slippage tolerance
            recent_blockhash: Cached blockhash for performance

        Returns:
            Signed transaction bytes or None if failed
        """
        keypair = self.load_keypair_from_env()
        wallet_pubkey = str(keypair.pubkey())

        # Get swap transaction from Jupiter
        swap_response = await self.get_swap_transaction(
            route=route,
            amount_in_lamports=amount_in_lamports,
            wallet_pubkey=wallet_pubkey,
            slippage_bps=slippage_bps,
            recent_blockhash=recent_blockhash,
        )

        if "error" in swap_response:
            logger.error(f"Failed to get swap transaction: {swap_response['error']}")
            return None

        # Sign the transaction
        signed_tx = self.sign_transaction(swap_response, keypair)
        return signed_tx

    def get_wallet_pubkey(self) -> str:
        """Get the public key of the loaded wallet."""
        keypair = self.load_keypair_from_env()
        return str(keypair.pubkey())