from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import logging

from aptos_sdk.account import Account
from aptos_sdk.async_client import RestClient
from aptos_sdk.bcs import Serializer
from aptos_sdk.transactions import EntryFunction, TransactionArgument, TransactionPayload
import httpx

from ..config import get_settings
from ..models import PickRecord
from ..schemas import SettlementOutcome


logger = logging.getLogger(__name__)

SIDE_AI_CORRECT = 1
SIDE_VOID = 2
SIDE_AI_INCORRECT = 3


@dataclass(frozen=True)
class MovementCreateResult:
    pick_id: int
    tx_hash: str | None
    status: str


@dataclass(frozen=True)
class MovementSettlementResult:
    tx_hash: str
    status: str


@dataclass(frozen=True)
class MovementWalletPickStatusResult:
    movement_pick_id: int
    wallet_address: str
    pick_status: str
    staked_raw: int
    claimable_raw: int
    eligible_to_claim: bool


@dataclass(frozen=True)
class MovementSubmissionResult:
    tx_hash: str
    receipt: dict


class MovementClient:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._read_enabled = bool(
            self._settings.movement_settlement_module_address
            and self._settings.movement_node_url
        )
        self._enabled = bool(
            self._settings.movement_enabled
            and self._settings.movement_private_key
            and self._settings.movement_settlement_module_address
            and self._settings.movement_rol_metadata_address
        )
        self._rest_client: RestClient | None = None
        self._account: Account | None = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def read_enabled(self) -> bool:
        return self._read_enabled

    async def ensure_pick(self, pick: PickRecord) -> MovementCreateResult:
        if not self.enabled:
            return MovementCreateResult(pick_id=0, tx_hash=None, status='DISABLED')

        payload = EntryFunction.natural(
            f'{self._settings.movement_settlement_module_address}::rolley_settlement',
            'create_pick',
            [],
            [
                TransactionArgument(self._movement_external_id(pick).encode('utf-8'), Serializer.to_bytes),
                TransactionArgument((pick.market or '').encode('utf-8'), Serializer.to_bytes),
                TransactionArgument(self._metadata_uri(pick).encode('utf-8'), Serializer.to_bytes),
                TransactionArgument(self._as_timestamp(pick.kick_off_utc), Serializer.u64),
                TransactionArgument(self._as_timestamp(pick.created_at), Serializer.u64),
            ],
        )
        submission = await self._submit(payload)
        pick_id = self._pick_id_from_create_receipt(submission.receipt)
        if pick_id <= 0:
            raise RuntimeError(f'Movement create_pick succeeded but no authoritative pick id was found for {pick.id}')
        return MovementCreateResult(pick_id=pick_id, tx_hash=submission.tx_hash, status='CREATED')

    async def settle_pick(self, *, movement_pick_id: int, outcome: SettlementOutcome, settled_at: datetime | None) -> MovementSettlementResult:
        if not self.enabled:
            raise RuntimeError('Movement integration is disabled')

        close_payload = EntryFunction.natural(
            f'{self._settings.movement_settlement_module_address}::rolley_settlement',
            'close_pick',
            [],
            [TransactionArgument(movement_pick_id, Serializer.u64)],
        )
        try:
            await self._submit(close_payload)
        except Exception as error:
            logger.info('Movement close_pick ignored for pick_id=%s: %s', movement_pick_id, error)

        settle_payload = EntryFunction.natural(
            f'{self._settings.movement_settlement_module_address}::rolley_settlement',
            'settle_pick',
            [],
            [
                TransactionArgument(movement_pick_id, Serializer.u64),
                TransactionArgument(self._winning_side(outcome), Serializer.u8),
                TransactionArgument(self._as_timestamp(settled_at or datetime.now(timezone.utc)), Serializer.u64),
            ],
        )
        submission = await self._submit(settle_payload)
        return MovementSettlementResult(tx_hash=submission.tx_hash, status='SETTLED')

    async def get_wallet_pick_statuses(
        self,
        *,
        wallet_address: str,
        movement_pick_ids: list[int],
    ) -> list[MovementWalletPickStatusResult]:
        if not self.read_enabled or not movement_pick_ids:
            return []

        resource = await self._fetch_settlement_resource()
        picks = resource.get('picks') or []
        requested = {int(pick_id) for pick_id in movement_pick_ids if int(pick_id) > 0}
        wallet_lower = wallet_address.strip().lower()
        results: list[MovementWalletPickStatusResult] = []

        for pick in picks:
            pick_id = self._coerce_int(pick.get('pick_id'))
            if pick_id not in requested:
                continue
            staked_raw = 0
            stakes = pick.get('stakes') or []
            for stake in stakes:
                if str(stake.get('staker') or '').strip().lower() == wallet_lower:
                    staked_raw += self._coerce_int(stake.get('amount'))
            claimable_raw = self._claimable_from_pick(pick, wallet_lower)
            results.append(
                MovementWalletPickStatusResult(
                    movement_pick_id=pick_id,
                    wallet_address=wallet_address,
                    pick_status=self._pick_status_label(self._coerce_int(pick.get('status'))),
                    staked_raw=staked_raw,
                    claimable_raw=claimable_raw,
                    eligible_to_claim=claimable_raw > 0,
                )
            )

        return sorted(results, key=lambda item: item.movement_pick_id)

    async def _submit(self, payload: EntryFunction) -> MovementSubmissionResult:
        client = self._client()
        account = self._account_instance()
        signed_transaction = await client.create_bcs_signed_transaction(account, TransactionPayload(payload))
        pending = await client.submit_bcs_transaction(signed_transaction)
        tx_hash = str(pending)
        try:
            await client.wait_for_transaction(pending)
        except AssertionError:
            receipt = await self._fetch_transaction(tx_hash)
            raise RuntimeError(
                f"Movement transaction failed: {receipt.get('vm_status', 'unknown')} ({tx_hash})"
            )
        receipt = await self._fetch_transaction(tx_hash)
        if not bool(receipt.get('success')):
            raise RuntimeError(
                f"Movement transaction failed: {receipt.get('vm_status', 'unknown')} ({tx_hash})"
            )
        return MovementSubmissionResult(tx_hash=tx_hash, receipt=receipt)

    async def _fetch_transaction(self, tx_hash: str) -> dict:
        url = f"{self._settings.movement_node_url.rstrip('/')}/transactions/by_hash/{tx_hash}"
        timeout = httpx.Timeout(15.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            for _ in range(20):
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                if data.get('type') != 'pending_transaction' and 'success' in data:
                    return data
                await asyncio.sleep(0.5)
        raise RuntimeError(f'Movement transaction receipt was not ready for {tx_hash}')

    async def _fetch_settlement_resource(self) -> dict:
        resource_type = f"{self._settings.movement_settlement_module_address}::rolley_settlement::SettlementConfig"
        url = (
            f"{self._settings.movement_node_url.rstrip('/')}"
            f"/accounts/{self._settings.movement_settlement_module_address}/resource/{resource_type}"
        )
        timeout = httpx.Timeout(15.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            return data.get('data') or {}

    def _coerce_int(self, value: object) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _pick_status_label(self, status: int) -> str:
        if status == 1:
            return 'OPEN'
        if status == 2:
            return 'CLOSED'
        if status == 3:
            return 'SETTLED'
        return 'UNKNOWN'

    def _claimable_from_pick(self, pick: dict, wallet_lower: str) -> int:
        if self._coerce_int(pick.get('status')) != 3:
            return 0
        winning_side = self._coerce_int(pick.get('winning_side'))
        total_staked = self._coerce_int(pick.get('total_staked'))
        home_pool = self._coerce_int(pick.get('home_pool'))
        draw_pool = self._coerce_int(pick.get('draw_pool'))
        away_pool = self._coerce_int(pick.get('away_pool'))
        winner_pool = home_pool if winning_side == 1 else draw_pool if winning_side == 2 else away_pool
        if winner_pool <= 0 or total_staked <= 0:
            return 0

        claimable = 0
        for stake in pick.get('stakes') or []:
            if str(stake.get('staker') or '').strip().lower() != wallet_lower:
                continue
            if self._coerce_int(stake.get('side')) != winning_side:
                continue
            if bool(stake.get('claimed')):
                continue
            amount = self._coerce_int(stake.get('amount'))
            claimable += (amount * total_staked) // winner_pool
        return claimable

    def _pick_id_from_create_receipt(self, receipt: dict) -> int:
        resource_type = f"{self._settings.movement_settlement_module_address}::rolley_settlement::SettlementConfig"
        for change in receipt.get('changes', []):
            if change.get('type') != 'write_resource':
                continue
            data = change.get('data') or {}
            if data.get('type') != resource_type:
                continue
            resource_data = data.get('data') or {}
            next_pick_id = resource_data.get('next_pick_id')
            if next_pick_id is not None:
                return max(int(next_pick_id) - 1, 0)
            picks = resource_data.get('picks') or []
            if picks:
                last_pick = picks[-1]
                if isinstance(last_pick, dict) and last_pick.get('pick_id') is not None:
                    return int(last_pick['pick_id'])
        return 0

    def _client(self) -> RestClient:
        if self._rest_client is None:
            self._rest_client = RestClient(self._settings.movement_node_url)
        return self._rest_client

    def _account_instance(self) -> Account:
        if self._account is None:
            assert self._settings.movement_private_key
            self._account = Account.load_key(self._settings.movement_private_key)
        return self._account

    def _metadata_uri(self, pick: PickRecord) -> str:
        base = self._settings.movement_pick_metadata_base_url.rstrip('/')
        return f'{base}/{pick.id}'

    def _movement_external_id(self, pick: PickRecord) -> str:
        return f'{pick.pick_date.isoformat()}::{pick.sport}::{pick.external_match_id}'

    def _as_timestamp(self, value: datetime) -> int:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp())

    def _winning_side(self, outcome: SettlementOutcome) -> int:
        if outcome == SettlementOutcome.WIN:
            return SIDE_AI_CORRECT
        if outcome == SettlementOutcome.VOID:
            return SIDE_VOID
        return SIDE_AI_INCORRECT
