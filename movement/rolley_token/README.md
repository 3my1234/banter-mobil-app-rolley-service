# Rolley Token

Initial Movement token scaffold for `ROL`.

Current scope:
- publish a Movement/Aptos-compatible fungible asset package
- create `ROL` metadata object
- treasury/admin-controlled minting
- one-time initial mint of the full 1B supply

Not included yet:
- settlement pools
- staking logic
- airdrop claim contract
- backend signing/integration

## Token assumptions

- Symbol: `ROL`
- Name: `Rolley Token`
- Decimals: `8`
- Initial supply: `1,000,000,000 ROL`
- Initial supply raw units: `100000000000000000`

## Publish flow

Initialize Movement CLI first:

```bash
movement init
movement account list
movement account fund-with-faucet --account default
```

Build:

```bash
cd /root/banter-mobil-app-rolley-service
movement move build \
  --package-dir movement/rolley_token \
  --named-addresses rolley_token=<YOUR_MOVEMENT_ADDRESS>
```

Publish:

```bash
movement move publish \
  --package-dir movement/rolley_token \
  --named-addresses rolley_token=<YOUR_MOVEMENT_ADDRESS>
```

After publish, mint the initial treasury supply:

```bash
movement move run \
  --function-id <YOUR_MOVEMENT_ADDRESS>::rolley_token::mint_initial_supply \
  --args address:<TREASURY_ADDRESS> \
  --assume-yes
```

View helpers:

```bash
movement move view \
  --function-id <YOUR_MOVEMENT_ADDRESS>::rolley_token::metadata_address

movement move view \
  --function-id <YOUR_MOVEMENT_ADDRESS>::rolley_token::treasury_address

movement move view \
  --function-id <YOUR_MOVEMENT_ADDRESS>::rolley_token::initial_supply_minted
```

## Notes

- `mint_initial_supply` can only run once.
- `mint_to` is treasury/admin-only.
- `burn_from_treasury` currently burns from the admin primary store. Keep the treasury signer aligned with the configured treasury address.
- Next package should be `movement/rolley_settlement`.
