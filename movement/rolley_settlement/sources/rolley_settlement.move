module rolley_settlement::rolley_settlement {
    use std::error;
    use std::signer;
    use std::vector;

    use aptos_framework::primary_fungible_store;
    use rolley_token::rolley_token;

    const ENOT_ADMIN: u64 = 1;
    const EPICK_NOT_FOUND: u64 = 2;
    const EPICK_NOT_OPEN: u64 = 3;
    const EINVALID_SIDE: u64 = 4;
    const EINVALID_AMOUNT: u64 = 5;
    const EPICK_NOT_SETTLED: u64 = 6;
    const ENOTHING_TO_CLAIM: u64 = 7;
    const ENOT_TREASURY: u64 = 8;
    const EALREADY_SETTLED: u64 = 9;

    const STATUS_OPEN: u8 = 1;
    const STATUS_CLOSED: u8 = 2;
    const STATUS_SETTLED: u8 = 3;

    const SIDE_HOME: u8 = 1;
    const SIDE_DRAW: u8 = 2;
    const SIDE_AWAY: u8 = 3;

    struct Stake has copy, drop, store {
        staker: address,
        side: u8,
        amount: u64,
        claimed: bool,
        created_at: u64,
    }

    struct Pick has copy, drop, store {
        pick_id: u64,
        external_match_id: vector<u8>,
        market: vector<u8>,
        metadata_uri: vector<u8>,
        closing_ts: u64,
        created_at: u64,
        settled_at: u64,
        status: u8,
        winning_side: u8,
        total_staked: u64,
        home_pool: u64,
        draw_pool: u64,
        away_pool: u64,
        stakes: vector<Stake>,
    }

    struct SettlementConfig has key {
        admin: address,
        treasury: address,
        next_pick_id: u64,
        picks: vector<Pick>,
    }

    fun init_module(admin: &signer) {
        move_to(
            admin,
            SettlementConfig {
                admin: signer::address_of(admin),
                treasury: signer::address_of(admin),
                next_pick_id: 1,
                picks: vector::empty<Pick>(),
            }
        );
    }

    #[view]
    public fun admin_address(): address acquires SettlementConfig {
        borrow_global<SettlementConfig>(@rolley_settlement).admin
    }

    #[view]
    public fun treasury_address(): address acquires SettlementConfig {
        borrow_global<SettlementConfig>(@rolley_settlement).treasury
    }

    #[view]
    public fun pick_count(): u64 acquires SettlementConfig {
        vector::length(&borrow_global<SettlementConfig>(@rolley_settlement).picks)
    }

    #[view]
    public fun claimable_amount(staker: address, pick_id: u64): u64 acquires SettlementConfig {
        let config = borrow_global<SettlementConfig>(@rolley_settlement);
        let pick_index = find_pick_index(&config.picks, pick_id);
        let pick = vector::borrow(&config.picks, pick_index);
        preview_claimable(pick, staker)
    }

    public entry fun create_pick(
        admin: &signer,
        external_match_id: vector<u8>,
        market: vector<u8>,
        metadata_uri: vector<u8>,
        closing_ts: u64,
        created_at: u64,
    ) acquires SettlementConfig {
        assert_admin(admin);

        let config = borrow_global_mut<SettlementConfig>(@rolley_settlement);
        let pick_id = config.next_pick_id;
        config.next_pick_id = pick_id + 1;

        vector::push_back(
            &mut config.picks,
            Pick {
                pick_id,
                external_match_id,
                market,
                metadata_uri,
                closing_ts,
                created_at,
                settled_at: 0,
                status: STATUS_OPEN,
                winning_side: 0,
                total_staked: 0,
                home_pool: 0,
                draw_pool: 0,
                away_pool: 0,
                stakes: vector::empty<Stake>(),
            }
        );
    }

    public entry fun stake_on_pick(
        staker: &signer,
        pick_id: u64,
        side: u8,
        amount: u64,
        created_at: u64,
    ) acquires SettlementConfig {
        assert_valid_side(side);
        assert!(amount > 0, error::invalid_argument(EINVALID_AMOUNT));

        let pick_index = {
            let config = borrow_global<SettlementConfig>(@rolley_settlement);
            let idx = find_pick_index(&config.picks, pick_id);
            let pick = vector::borrow(&config.picks, idx);
            assert!(pick.status == STATUS_OPEN, error::invalid_state(EPICK_NOT_OPEN));
            let treasury = config.treasury;
            let asset = rolley_token::get_metadata();
            primary_fungible_store::transfer(staker, asset, treasury, amount);
            idx
        };

        let config_mut = borrow_global_mut<SettlementConfig>(@rolley_settlement);
        let pick_mut = vector::borrow_mut(&mut config_mut.picks, pick_index);
        pick_mut.total_staked = pick_mut.total_staked + amount;
        if (side == SIDE_HOME) {
            pick_mut.home_pool = pick_mut.home_pool + amount;
        } else if (side == SIDE_DRAW) {
            pick_mut.draw_pool = pick_mut.draw_pool + amount;
        } else {
            pick_mut.away_pool = pick_mut.away_pool + amount;
        };
        vector::push_back(
            &mut pick_mut.stakes,
            Stake {
                staker: signer::address_of(staker),
                side,
                amount,
                claimed: false,
                created_at,
            }
        );
    }

    public entry fun close_pick(admin: &signer, pick_id: u64) acquires SettlementConfig {
        assert_admin(admin);
        let config = borrow_global_mut<SettlementConfig>(@rolley_settlement);
        let pick_index = find_pick_index(&config.picks, pick_id);
        let pick = vector::borrow_mut(&mut config.picks, pick_index);
        assert!(pick.status == STATUS_OPEN, error::invalid_state(EPICK_NOT_OPEN));
        pick.status = STATUS_CLOSED;
    }

    public entry fun settle_pick(
        admin: &signer,
        pick_id: u64,
        winning_side: u8,
        settled_at: u64,
    ) acquires SettlementConfig {
        assert_admin(admin);
        assert_valid_side(winning_side);

        let config = borrow_global_mut<SettlementConfig>(@rolley_settlement);
        let pick_index = find_pick_index(&config.picks, pick_id);
        let pick = vector::borrow_mut(&mut config.picks, pick_index);
        assert!(pick.status != STATUS_SETTLED, error::invalid_state(EALREADY_SETTLED));
        pick.status = STATUS_SETTLED;
        pick.winning_side = winning_side;
        pick.settled_at = settled_at;
    }

    public entry fun claim_payout(
        treasury_signer: &signer,
        pick_id: u64,
        staker: address,
    ) acquires SettlementConfig {
        assert_treasury(treasury_signer);

        let payout = {
            let config = borrow_global_mut<SettlementConfig>(@rolley_settlement);
            let pick_index = find_pick_index(&config.picks, pick_id);
            let pick = vector::borrow_mut(&mut config.picks, pick_index);
            collect_claimable(pick, staker)
        };

        assert!(payout > 0, error::invalid_state(ENOTHING_TO_CLAIM));

        let asset = rolley_token::get_metadata();
        primary_fungible_store::transfer(treasury_signer, asset, staker, payout);
    }

    fun pool_for_side(pick: &Pick, side: u8): u64 {
        if (side == SIDE_HOME) {
            pick.home_pool
        } else if (side == SIDE_DRAW) {
            pick.draw_pool
        } else {
            pick.away_pool
        }
    }

    fun preview_claimable(pick: &Pick, staker: address): u64 {
        assert!(pick.status == STATUS_SETTLED, error::invalid_state(EPICK_NOT_SETTLED));

        let winner_pool = pool_for_side(pick, pick.winning_side);
        if (winner_pool == 0) {
            return 0
        };

        let total = 0;
        let i = 0;
        let total_mut = total;
        let i_mut = i;
        while (i_mut < vector::length(&pick.stakes)) {
            let stake = vector::borrow(&pick.stakes, i_mut);
            if (stake.staker == staker && stake.side == pick.winning_side && !stake.claimed) {
                total_mut = total_mut + ((stake.amount * pick.total_staked) / winner_pool);
            };
            i_mut = i_mut + 1;
        };
        total_mut
    }

    fun collect_claimable(pick: &mut Pick, staker: address): u64 {
        assert!(pick.status == STATUS_SETTLED, error::invalid_state(EPICK_NOT_SETTLED));

        let winner_pool = pool_for_side(pick, pick.winning_side);
        if (winner_pool == 0) {
            return 0
        };

        let total = 0;
        let i = 0;
        let total_mut = total;
        let i_mut = i;
        while (i_mut < vector::length(&pick.stakes)) {
            let stake = vector::borrow_mut(&mut pick.stakes, i_mut);
            if (stake.staker == staker && stake.side == pick.winning_side && !stake.claimed) {
                total_mut = total_mut + ((stake.amount * pick.total_staked) / winner_pool);
                stake.claimed = true;
            };
            i_mut = i_mut + 1;
        };
        total_mut
    }

    fun find_pick_index(picks: &vector<Pick>, pick_id: u64): u64 {
        let i = 0;
        let i_mut = i;
        while (i_mut < vector::length(picks)) {
            let pick = vector::borrow(picks, i_mut);
            if (pick.pick_id == pick_id) {
                return i_mut
            };
            i_mut = i_mut + 1;
        };
        abort error::not_found(EPICK_NOT_FOUND)
    }

    fun assert_valid_side(side: u8) {
        assert!(
            side == SIDE_HOME || side == SIDE_DRAW || side == SIDE_AWAY,
            error::invalid_argument(EINVALID_SIDE)
        );
    }

    fun assert_admin(admin: &signer) acquires SettlementConfig {
        let config = borrow_global<SettlementConfig>(@rolley_settlement);
        assert!(signer::address_of(admin) == config.admin, error::permission_denied(ENOT_ADMIN));
    }

    fun assert_treasury(treasury_signer: &signer) acquires SettlementConfig {
        let config = borrow_global<SettlementConfig>(@rolley_settlement);
        assert!(
            signer::address_of(treasury_signer) == config.treasury,
            error::permission_denied(ENOT_TREASURY)
        );
    }
}
