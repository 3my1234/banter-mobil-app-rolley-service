module rolley_token::rolley_token {
    use std::error;
    use std::option;
    use std::signer;
    use std::string::utf8;

    use aptos_framework::fungible_asset::{Self, BurnRef, Metadata, MintRef, TransferRef};
    use aptos_framework::object::{Self, Object};
    use aptos_framework::primary_fungible_store;

    const ENOT_ADMIN: u64 = 1;
    const EINITIAL_SUPPLY_ALREADY_MINTED: u64 = 2;

    const ASSET_SYMBOL: vector<u8> = b"ROL";
    const ASSET_NAME: vector<u8> = b"Rolley Token";
    const ICON_URI: vector<u8> = b"https://sportbanter.online/assets/rolley-token.png";
    const PROJECT_URI: vector<u8> = b"https://sportbanter.online";
    const DECIMALS: u8 = 8;

    /// 1,000,000,000 ROL with 8 decimals.
    const INITIAL_SUPPLY: u64 = 100000000000000000;

    struct ManagedFungibleAsset has key {
        mint_ref: MintRef,
        transfer_ref: TransferRef,
        burn_ref: BurnRef,
    }

    struct TreasuryState has key {
        treasury: address,
        initial_supply_minted: bool,
    }

    fun init_module(admin: &signer) {
        let constructor_ref = &object::create_named_object(admin, ASSET_SYMBOL);
        primary_fungible_store::create_primary_store_enabled_fungible_asset(
            constructor_ref,
            option::none(),
            utf8(ASSET_NAME),
            utf8(ASSET_SYMBOL),
            DECIMALS,
            utf8(ICON_URI),
            utf8(PROJECT_URI),
        );

        let mint_ref = fungible_asset::generate_mint_ref(constructor_ref);
        let burn_ref = fungible_asset::generate_burn_ref(constructor_ref);
        let transfer_ref = fungible_asset::generate_transfer_ref(constructor_ref);
        let metadata_signer = object::generate_signer(constructor_ref);
        move_to(
            &metadata_signer,
            ManagedFungibleAsset {
                mint_ref,
                transfer_ref,
                burn_ref,
            }
        );

        move_to(
            admin,
            TreasuryState {
                treasury: signer::address_of(admin),
                initial_supply_minted: false,
            }
        );
    }

    #[view]
    public fun metadata_address(): address {
        object::create_object_address(&@rolley_token, ASSET_SYMBOL)
    }

    #[view]
    public fun treasury_address(): address acquires TreasuryState {
        borrow_global<TreasuryState>(@rolley_token).treasury
    }

    #[view]
    public fun initial_supply_minted(): bool acquires TreasuryState {
        borrow_global<TreasuryState>(@rolley_token).initial_supply_minted
    }

    #[view]
    public fun get_metadata(): Object<Metadata> {
        object::address_to_object<Metadata>(metadata_address())
    }

    public entry fun mint_initial_supply(admin: &signer, treasury: address) acquires ManagedFungibleAsset, TreasuryState {
        assert_admin(admin);

        assert!(
            !borrow_global<TreasuryState>(@rolley_token).initial_supply_minted,
            error::already_exists(EINITIAL_SUPPLY_ALREADY_MINTED)
        );

        let asset = get_metadata();
        let treasury_store = primary_fungible_store::ensure_primary_store_exists(treasury, asset);
        let asset_address = metadata_address();
        {
            let managed_asset = borrow_global<ManagedFungibleAsset>(asset_address);
            let minted = fungible_asset::mint(&managed_asset.mint_ref, INITIAL_SUPPLY);
            fungible_asset::deposit_with_ref(&managed_asset.transfer_ref, treasury_store, minted);
        };

        let state = borrow_global_mut<TreasuryState>(@rolley_token);
        state.treasury = treasury;
        state.initial_supply_minted = true;
    }

    public entry fun mint_to(admin: &signer, to: address, amount: u64) acquires ManagedFungibleAsset, TreasuryState {
        assert_admin(admin);

        let asset = get_metadata();
        let recipient_store = primary_fungible_store::ensure_primary_store_exists(to, asset);
        let asset_address = metadata_address();
        let managed_asset = borrow_global<ManagedFungibleAsset>(asset_address);
        let minted = fungible_asset::mint(&managed_asset.mint_ref, amount);
        fungible_asset::deposit_with_ref(&managed_asset.transfer_ref, recipient_store, minted);
    }

    public entry fun burn_from_treasury(admin: &signer, amount: u64) acquires ManagedFungibleAsset, TreasuryState {
        assert_admin(admin);

        let asset = get_metadata();
        let asset_address = metadata_address();
        let managed_asset = borrow_global<ManagedFungibleAsset>(asset_address);
        let withdrawn = primary_fungible_store::withdraw(admin, asset, amount);
        fungible_asset::burn(&managed_asset.burn_ref, withdrawn);
    }

    public entry fun rotate_treasury(admin: &signer, new_treasury: address) acquires TreasuryState {
        assert_admin(admin);
        let state = borrow_global_mut<TreasuryState>(@rolley_token);
        state.treasury = new_treasury;
    }

    fun assert_admin(admin: &signer) acquires TreasuryState {
        let state = borrow_global<TreasuryState>(@rolley_token);
        assert!(
            signer::address_of(admin) == state.treasury,
            error::permission_denied(ENOT_ADMIN)
        );
    }
}
