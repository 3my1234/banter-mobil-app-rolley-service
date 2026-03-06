from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging

from aptos_sdk.account import Account
from aptos_sdk.async_client import RestClient
from aptos_sdk.bcs import Serializer
from aptos_sdk.transactions import EntryFunction, TransactionArgument, TransactionPayload

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


class MovementClient:
    def __init__(self) -> None:
        self._settings = get_settings()
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
        previous_count = await self._pick_count()
        tx_hash = await self._submit(payload)
        # Movement testnet view state can lag briefly after a confirmed transaction.
        # In the current single-admin writer model, the next on-chain pick id is deterministic.
        pick_id = previous_count + 1
        return MovementCreateResult(pick_id=pick_id, tx_hash=tx_hash, status='CREATED')

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
        tx_hash = await self._submit(settle_payload)
        return MovementSettlementResult(tx_hash=tx_hash, status='SETTLED')

    async def _pick_count(self) -> int:
        client = self._client()
        result = await client.view(
            f'{self._settings.movement_settlement_module_address}::rolley_settlement::pick_count',
            [],
            [],
        )
        if not result:
            return 0
        return int(result[0])

    async def _submit(self, payload: EntryFunction) -> str:
        client = self._client()
        account = self._account_instance()
        signed_transaction = await client.create_bcs_signed_transaction(account, TransactionPayload(payload))
        pending = await client.submit_bcs_transaction(signed_transaction)
        await client.wait_for_transaction(pending)
        return str(pending)

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
