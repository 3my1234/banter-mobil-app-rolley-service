# Rolley Settlement

`rolley_settlement` is the first on-chain settlement contract for Rolley picks.

This v1 package is intentionally simple and operational:
- admin creates picks
- users stake `ROL`
- stakes are transferred into the treasury wallet
- admin closes and settles picks
- treasury/admin executes winner payouts with `claim_payout`

## Important v1 limitation

This is a **real on-chain implementation**, but it is still a centralized v1:
- custody sits in the treasury/admin wallet, not an autonomous escrow account
- `claim_payout` must be called by the treasury signer
- this matches the current Banter/Rolley model where admin also settles outcomes

The next iteration can move custody to a resource account or object-owned escrow.

## Package layout

- package: `movement/rolley_settlement`
- dependency: local `movement/rolley_token`

## Main entry functions

- `create_pick`
- `stake_on_pick`
- `close_pick`
- `settle_pick`
- `claim_payout`

## Build

```bash
cd /root/banter-mobil-app-rolley-service

movement move build \
  --package-dir movement/rolley_settlement \
  --named-addresses \
    rolley_settlement=<YOUR_MOVEMENT_ADDRESS>,rolley_token=<YOUR_MOVEMENT_ADDRESS>
```

## Publish

```bash
movement move publish \
  --package-dir movement/rolley_settlement \
  --named-addresses \
    rolley_settlement=<YOUR_MOVEMENT_ADDRESS>,rolley_token=<YOUR_MOVEMENT_ADDRESS>
```

## Example flow

Create a pick:

```bash
movement move run \
  --function-id <YOUR_MOVEMENT_ADDRESS>::rolley_settlement::create_pick \
  --args \
    hex:534f434345522d373430383834 \
    hex:544f54414c5f474f414c53 \
    hex:68747470733a2f2f73706f727462616e7465722e6f6e6c696e652f726f6c6c6579 \
    u64:1774000000 \
    u64:1773900000 \
  --assume-yes
```

Stake on a pick:

```bash
movement move run \
  --function-id <YOUR_MOVEMENT_ADDRESS>::rolley_settlement::stake_on_pick \
  --args u64:1 u8:1 u64:100000000 u64:1773900100 \
  --assume-yes
```

Close and settle:

```bash
movement move run \
  --function-id <YOUR_MOVEMENT_ADDRESS>::rolley_settlement::close_pick \
  --args u64:1 \
  --assume-yes

movement move run \
  --function-id <YOUR_MOVEMENT_ADDRESS>::rolley_settlement::settle_pick \
  --args u64:1 u8:1 u64:1773986400 \
  --assume-yes
```

Pay a winner:

```bash
movement move run \
  --function-id <YOUR_MOVEMENT_ADDRESS>::rolley_settlement::claim_payout \
  --args u64:1 address:<WINNER_ADDRESS> \
  --assume-yes
```

## Side encoding

- `1` = home / selection A
- `2` = draw / middle outcome
- `3` = away / selection B
