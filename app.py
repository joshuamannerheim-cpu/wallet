import json
import hmac
import os
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import psycopg
import requests
from flask import Flask, jsonify, render_template_string, request


app = Flask(__name__)

VERSION = "4.14.3-candidate-pipeline"
SCREENING_VERSION = "4.2.2"
INDEPENDENT_REPEAT_SECONDS = 6 * 60 * 60
SOL_MINT = "So11111111111111111111111111111111111111112"
STABLE_SYMBOLS = {
    "USDC", "USDT", "USD1", "PYUSD", "USDS", "USDE", "DAI", "FDUSD",
}
EXCLUDED_SIGNAL_SYMBOLS = STABLE_SYMBOLS | {
    "SOL", "WSOL", "JITOSOL", "MSOL", "BSOL", "BNSOL", "JUPSOL", "INF",
}
STABLE_MINTS = {
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB",  # USD1
}
BIRDEYE_BASE = "https://public-api.birdeye.so"
BIRDEYE_RPS = min(max(float(os.getenv("BIRDEYE_RPS", "7")), 1.0), 10.0)
DISCOVERY_MAX_TOKENS = 50
DISCOVERY_PAGE_SIZE = 10
DISCOVERY_WALLETS_PER_TOKEN = 5
PIPELINE_MAX_SECONDS = min(max(int(os.getenv("PIPELINE_MAX_SECONDS", "75")), 15), 90)
HELIUS_HISTORY_LIMIT = 100
HELIUS_WEBHOOKS_URL = "https://mainnet.helius-rpc.com/v0/webhooks"
ROBINHOOD_CHAIN_ID = 4663
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{address}"
DEXSCREENER_BOOSTS_TOP_URL = "https://api.dexscreener.com/token-boosts/top/v1"
DEX_WALLET_DISCOVERY_MAX_TOKENS = min(
    max(int(os.getenv("DEX_WALLET_DISCOVERY_MAX_TOKENS", "20")), 1), 50
)
DEX_WALLET_CANDIDATES_PER_TOKEN = min(
    max(int(os.getenv("DEX_WALLET_CANDIDATES_PER_TOKEN", "15")), 1), 30
)
DEX_WALLET_HISTORY_LIMIT = min(
    max(int(os.getenv("DEX_WALLET_HISTORY_LIMIT", "100")), 25), 100
)
DEX_WALLET_MIN_LIQUIDITY_USD = max(
    float(os.getenv("DEX_WALLET_MIN_LIQUIDITY_USD", "10000")), 0
)
DEX_WALLET_MIN_VOLUME_H24_USD = max(
    float(os.getenv("DEX_WALLET_MIN_VOLUME_H24_USD", "5000")), 0
)
DEX_WALLET_MIN_PAIR_AGE_HOURS = min(
    max(float(os.getenv("DEX_WALLET_MIN_PAIR_AGE_HOURS", "0.25")), 0), 168
)
DEX_WALLET_EARLY_SCORE_THRESHOLD = min(
    max(float(os.getenv("DEX_WALLET_EARLY_SCORE_THRESHOLD", "65")), 0), 100
)
DEX_WALLET_PROBATION_MIN_DAYS = min(
    max(int(os.getenv("DEX_WALLET_PROBATION_MIN_DAYS", "7")), 1), 30
)
DEX_WALLET_PROBATION_MIN_TRADES = min(
    max(int(os.getenv("DEX_WALLET_PROBATION_MIN_TRADES", "5")), 1), 50
)
ROBINHOOD_BLOCKSCOUT_URL = "https://robinhoodchain.blockscout.com/api/v2"
EVM_CHAIN_CONFIG = {
    "robinhood": {
        "chain_id": ROBINHOOD_CHAIN_ID,
        "label": "Robinhood Chain",
        "dex_chain_ids": {"robinhood", str(ROBINHOOD_CHAIN_ID)},
        "blockscout_url": ROBINHOOD_BLOCKSCOUT_URL,
        "explorer_url": "https://robinhoodchain.blockscout.com/token/{address}",
    },
    "base": {
        "chain_id": 8453,
        "label": "Base",
        "dex_chain_ids": {"base", "8453"},
        "blockscout_url": "https://base.blockscout.com/api/v2",
        "explorer_url": "https://base.blockscout.com/token/{address}",
    },
}
SUPPORTED_EVM_CHAINS = tuple(EVM_CHAIN_CONFIG)
EVM_REFRESH_MAX_SECONDS = min(
    max(int(os.getenv("EVM_REFRESH_MAX_SECONDS", "55")), 15), 75
)
EVM_FETCH_WORKERS = min(max(int(os.getenv("EVM_FETCH_WORKERS", "4")), 1), 4)
EVM_PROVIDER_TIMEOUT_SECONDS = min(
    max(int(os.getenv("EVM_PROVIDER_TIMEOUT_SECONDS", "7")), 3), 10
)
EVM_ALERT_STATUSES = {
    value.strip().upper()
    for value in os.getenv(
        "EVM_ALERT_STATUSES", "EVM_MOMENTUM,EVM_HIGH_MOMENTUM,EVM_RISK,EVM_DISTRIBUTION,EVM_CONFIRMED_BREAKOUT,EVM_CONFIRMED_BREAKDOWN"
    ).split(",")
    if value.strip()
}
EVM_MIN_MOMENTUM_LIQUIDITY_USD = max(
    float(os.getenv("EVM_MIN_MOMENTUM_LIQUIDITY_USD", "25000")), 0
)
EVM_MIN_MOMENTUM_H1_VOLUME_USD = max(
    float(os.getenv("EVM_MIN_MOMENTUM_H1_VOLUME_USD", "1000")), 0
)
EVM_MIN_MOMENTUM_H1_TRANSACTIONS = max(
    int(os.getenv("EVM_MIN_MOMENTUM_H1_TRANSACTIONS", "10")), 1
)
EVM_ANOMALY_PRICE_RATIO = max(
    float(os.getenv("EVM_ANOMALY_PRICE_RATIO", "5")), 2.0
)
EVM_ANOMALY_LIQUIDITY_RATIO = max(
    float(os.getenv("EVM_ANOMALY_LIQUIDITY_RATIO", "4")), 2.0
)
PUBLIC_BASE_URL = os.getenv(
    "PUBLIC_BASE_URL", "https://wallet-production-3b7b.up.railway.app"
).rstrip("/")
HELIUS_TARGET_WEBHOOK_URL = os.getenv(
    "HELIUS_TARGET_WEBHOOK_URL", f"{PUBLIC_BASE_URL}/helius-webhook"
)
HELIUS_AUTO_SYNC = os.getenv("HELIUS_AUTO_SYNC", "true").lower() in {
    "1", "true", "yes", "on",
}
HELIUS_MIN_SYNC_WALLETS = min(
    max(int(os.getenv("HELIUS_MIN_SYNC_WALLETS", "1")), 1), 100
)
HELIUS_MAX_REMOVAL_FRACTION = min(
    max(float(os.getenv("HELIUS_MAX_REMOVAL_FRACTION", "0.50")), 0.0), 1.0
)
SIGNAL_WINDOW_MINUTES = min(max(int(os.getenv("SIGNAL_WINDOW_MINUTES", "60")), 15), 360)
SOLANA_CONSENSUS_WINDOWS_HOURS = (1, 6, 24)
SOLANA_METADATA_REFRESH_HOURS = min(
    max(int(os.getenv("SOLANA_METADATA_REFRESH_HOURS", "24")), 1), 168
)
SOLANA_METADATA_PER_REFRESH = min(
    max(int(os.getenv("SOLANA_METADATA_PER_REFRESH", "3")), 0), 10
)
EVM_VISIBLE_STATE_CONFIRMATIONS = 2
EVM_ALERT_CONFIRMATIONS = 3
TELEGRAM_ALERT_STATUSES = {
    value.strip().upper()
    for value in os.getenv(
        "TELEGRAM_ALERT_STATUSES", "BUILDING,PAPER_CONFIRMED,INVALIDATED"
    ).split(",")
    if value.strip()
}
TELEGRAM_TIMEOUT_SECONDS = min(
    max(int(os.getenv("TELEGRAM_TIMEOUT_SECONDS", "10")), 3), 30
)
WATCH_WEIGHT = 1.0
ASYMMETRIC_WEIGHT = 0.35
CONFIDENCE_MULTIPLIERS = {"HIGH": 1.0, "MEDIUM": 0.75, "LOW": 0.5}
HARD_RELATIONSHIP_STRENGTHS = {"high", "moderate"}
DEFAULT_TOKEN_WATCHLIST = (
    ("base", "0x4200000000000000000000000000000000000006", "ETH", "Ether / WETH", "benchmark", "evm_monitoring_ready"),
    ("robinhood", "0x232CDFc415D10b673845D83Dc02ba2eaBe7e30d1", "IF", "What IF", "portfolio", "evm_monitoring_ready"),
    ("robinhood", "0xe934e36A439C94017B64a3FecE66AF12099aBF50", "STONKBROKER", "StonkBroker", "portfolio", "evm_monitoring_ready"),
    ("robinhood", "0x020bfC650A365f8BB26819deAAbF3E21291018b4", "CASHCAT", "Cash Cat", "portfolio", "evm_monitoring_ready"),
    ("robinhood", "0xfd181632e1F2335DaB74535E6dD29082d3191bb2", "RFLX", "RFLIX", "portfolio", "evm_monitoring_ready"),
    ("robinhood", "0xeC45C6C413b498Cf5aCF5a1a889F1a95cA9b6bB3", "PORTLY", "PORTLY", "existing_test_case", "evm_monitoring_ready"),
    ("robinhood", "0x298348d5b2e45C774E3ee4f1a0924071DfbDC8C7", "SWAPPY", "Swappy", "research_test_case", "evm_monitoring_ready"),
    ("base", "0xA4A2E2ca3fBfE21aed83471D28b6f65A233C6e00", "TIBBIR", "Ribbita by Virtuals", "research_test_case", "evm_monitoring_ready"),
    ("robinhood", "0x5f62c57e5c537887117eef828b7e3ad41c009feb", "GOOD", "Good In The Hood", "research_watchlist", "evm_monitoring_ready"),
    ("base", "0x0F61Edbfe6Cd86024C0f210c0695B08df55fdfc9", "BSTONK", "BaseStonk", "research_watchlist", "evm_monitoring_ready"),
)
BASE58_ALPHABET = (
    "123456789ABCDEFGHJKLMNPQRSTUVWXYZ"
    "abcdefghijkmnopqrstuvwxyz"
)

_rate_lock = threading.Lock()
_next_birdeye_request = 0.0
_diagnostic_lock = threading.Lock()
_diagnostics = {"birdeye_requests": 0, "helius_requests": 0,
                "helius_syncs": 0, "helius_sync_failures": 0,
                "dexscreener_requests": 0, "blockscout_requests": 0,
                "evm_refreshes": 0, "evm_refresh_failures": 0,
                "evm_anomalies": 0, "evm_canonical_pair_misses": 0,
                "evm_provider_unavailable": 0, "evm_provider_recoveries": 0,
                "telegram_requests": 0, "telegram_deliveries": 0,
                "telegram_failures": 0, "retries": 0, "rate_limits": 0,
                "timeouts": 0, "upstream_errors": 0,
                "dex_wallet_discovery_runs": 0,
                "dex_wallet_candidates_found": 0,
                "probation_wallet_events": 0}


def diagnostic_increment(name):
    with _diagnostic_lock:
        _diagnostics[name] = _diagnostics.get(name, 0) + 1


def throttle_birdeye():
    """Process-wide Premium limiter; defaults to 7 RPS and never exceeds 10 RPS."""
    global _next_birdeye_request
    interval = 1.0 / BIRDEYE_RPS
    with _rate_lock:
        now = time.monotonic()
        wait = max(0.0, _next_birdeye_request - now)
        _next_birdeye_request = max(now, _next_birdeye_request) + interval
    if wait:
        time.sleep(wait)


# =========================================================
# CONFIGURATION / DATABASE
# =========================================================

def birdeye_headers():
    return {
        "accept": "application/json",
        "X-API-KEY": os.getenv("BIRDEYE_API_KEY", ""),
        "x-chain": "solana",
    }


def db():
    return psycopg.connect(os.environ["DATABASE_URL"])


def upstream_request(method, url, *, headers=None, params=None, json_body=None,
                     timeout=30, retries=2, provider="birdeye"):
    """Bounded retry/backoff for 429s, 5xx responses and network timeouts."""
    last_error = None
    for attempt in range(retries + 1):
        if provider == "birdeye":
            throttle_birdeye()
        diagnostic_increment(f"{provider}_requests")
        try:
            response = requests.request(method, url, headers=headers, params=params,
                                        json=json_body, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            diagnostic_increment("timeouts")
            if attempt >= retries:
                raise
        else:
            if response.status_code == 429:
                diagnostic_increment("rate_limits")
            if response.status_code not in {429, 500, 502, 503, 504} or attempt >= retries:
                if response.status_code >= 500:
                    diagnostic_increment("upstream_errors")
                return response
        diagnostic_increment("retries")
        time.sleep(min(0.75 * (2 ** attempt) + random.uniform(0.05, 0.25), 5.0))
    raise last_error


def birdeye_get(path, params=None, retry_429=True):
    return upstream_request("GET", f"{BIRDEYE_BASE}{path}",
                            headers=birdeye_headers(), params=params, timeout=20,
                            retries=2 if retry_429 else 0)


def birdeye_post(path, body=None, retry_429=True):
    return upstream_request("POST", f"{BIRDEYE_BASE}{path}",
                            headers={**birdeye_headers(), "content-type": "application/json"},
                            json_body=body or {}, timeout=25,
                            retries=2 if retry_429 else 0)


def helius_get_address_transactions(address, limit=HELIUS_HISTORY_LIMIT):
    """Fetch a bounded parsed history sample for a wallet, token, or account."""
    return upstream_request(
        "GET", f"https://api-mainnet.helius-rpc.com/v0/addresses/{address}/transactions",
        params={
            "api-key": os.getenv("HELIUS_API_KEY", ""),
            "token-accounts": "balanceChanged",
            "sort-order": "desc",
            "limit": min(max(int(limit), 1), 100),
        },
        timeout=25, retries=1, provider="helius",
    )


def helius_get_transactions(wallet):
    """Fetch a bounded, parsed history sample for heuristic screening."""
    return helius_get_address_transactions(wallet, HELIUS_HISTORY_LIMIT)


def initialise_database():
    """Create the V4 schema and safely add any missing compatible columns."""
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS candidate_wallets (
                    wallet TEXT PRIMARY KEY,
                    tokens_found INTEGER NOT NULL DEFAULT 0,
                    realized_pnl_30d DOUBLE PRECISION,
                    total_pnl_30d DOUBLE PRECISION,
                    win_rate_30d DOUBLE PRECISION,
                    trades_30d INTEGER,
                    total_invested_30d DOUBLE PRECISION,
                    score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    score_status TEXT NOT NULL DEFAULT 'unscored',
                    early_entry_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    repeat_early_entries INTEGER NOT NULL DEFAULT 0,
                    discovery_tier TEXT NOT NULL DEFAULT 'CANDIDATE',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_scored TIMESTAMPTZ
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallet_token_hits (
                    wallet TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    token_symbol TEXT,
                    token_name TEXT,
                    token_realized_pnl DOUBLE PRECISION,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (wallet, token_address)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS discovery_runs (
                    id BIGSERIAL PRIMARY KEY,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMPTZ,
                    tokens_examined INTEGER NOT NULL DEFAULT 0,
                    wallets_found INTEGER NOT NULL DEFAULT 0,
                    api_429s INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'running'
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallet_screenings (
                    wallet TEXT PRIMARY KEY,
                    screening_status TEXT NOT NULL,
                    risk_score INTEGER NOT NULL DEFAULT 0,
                    risk_flags TEXT NOT NULL DEFAULT '[]',
                    transactions_sampled INTEGER NOT NULL DEFAULT 0,
                    unique_tokens_sampled INTEGER NOT NULL DEFAULT 0,
                    unique_counterparties INTEGER NOT NULL DEFAULT 0,
                    largest_funder TEXT,
                    largest_funder_share DOUBLE PRECISION,
                    funding_transfer_count INTEGER NOT NULL DEFAULT 0,
                    total_native_funding_lamports DOUBLE PRECISION NOT NULL DEFAULT 0,
                    transaction_types TEXT NOT NULL DEFAULT '{}',
                    screening_version TEXT NOT NULL DEFAULT '4.2',
                    failed_transaction_rate DOUBLE PRECISION,
                    median_interval_seconds DOUBLE PRECISION,
                    screened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    details TEXT NOT NULL DEFAULT '{}'
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS discovery_observations (
                    run_id BIGINT NOT NULL REFERENCES discovery_runs(id),
                    wallet TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    token_symbol TEXT,
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (run_id, wallet, token_address)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallet_token_validations (
                    wallet TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    token_symbol TEXT,
                    realized_pnl DOUBLE PRECISION,
                    total_pnl DOUBLE PRECISION,
                    invested DOUBLE PRECISION,
                    trades INTEGER,
                    win_rate DOUBLE PRECISION,
                    validated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (wallet, token_address)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallet_clusters (
                    wallet TEXT PRIMARY KEY,
                    cluster_id TEXT NOT NULL,
                    classification TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    signal_weight DOUBLE PRECISION NOT NULL,
                    relationship_basis TEXT NOT NULL DEFAULT 'independent',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallet_activity (
                    signature TEXT NOT NULL,
                    wallet TEXT NOT NULL,
                    cluster_id TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    token_symbol TEXT,
                    side TEXT NOT NULL,
                    token_amount DOUBLE PRECISION,
                    estimated_usd_value DOUBLE PRECISION,
                    occurred_at TIMESTAMPTZ NOT NULL,
                    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    raw_summary TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (signature, wallet, token_address, side)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dex_wallet_discovery_runs (
                    id BIGSERIAL PRIMARY KEY,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMPTZ,
                    tokens_examined INTEGER NOT NULL DEFAULT 0,
                    candidates_found INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'running',
                    details TEXT NOT NULL DEFAULT '{}'
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallet_discovery_cohorts (
                    wallet TEXT PRIMARY KEY,
                    source TEXT NOT NULL DEFAULT 'dexscreener',
                    cohort_status TEXT NOT NULL DEFAULT 'CANDIDATE',
                    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    admitted_at TIMESTAMPTZ,
                    eligible_signal_at TIMESTAMPTZ,
                    forward_trades INTEGER NOT NULL DEFAULT 0,
                    forward_tokens INTEGER NOT NULL DEFAULT 0,
                    last_forward_activity_at TIMESTAMPTZ,
                    rejection_reason TEXT,
                    details TEXT NOT NULL DEFAULT '{}',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallet_discovery_sources (
                    wallet TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    pair_address TEXT,
                    source TEXT NOT NULL DEFAULT 'dexscreener',
                    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    observed_entry_rank INTEGER,
                    observed_entry_delay_seconds DOUBLE PRECISION,
                    early_entry_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    liquidity_usd DOUBLE PRECISION,
                    volume_h24_usd DOUBLE PRECISION,
                    pair_age_hours DOUBLE PRECISION,
                    details TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (wallet, token_address)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallet_probation_activity (
                    signature TEXT NOT NULL,
                    wallet TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    token_symbol TEXT,
                    side TEXT NOT NULL,
                    token_amount DOUBLE PRECISION,
                    occurred_at TIMESTAMPTZ NOT NULL,
                    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    raw_summary TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (signature, wallet, token_address, side)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS paper_signals (
                    token_address TEXT PRIMARY KEY,
                    token_symbol TEXT,
                    status TEXT NOT NULL,
                    buy_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    sell_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    independent_buy_clusters INTEGER NOT NULL DEFAULT 0,
                    independent_sell_clusters INTEGER NOT NULL DEFAULT 0,
                    contributing_wallets TEXT NOT NULL DEFAULT '[]',
                    contributing_clusters TEXT NOT NULL DEFAULT '[]',
                    first_activity_at TIMESTAMPTZ,
                    last_activity_at TIMESTAMPTZ,
                    safety_status TEXT NOT NULL DEFAULT 'unverified',
                    actionable BOOLEAN NOT NULL DEFAULT FALSE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS solana_token_metadata (
                    token_address TEXT PRIMARY KEY,
                    token_symbol TEXT,
                    token_name TEXT,
                    safety_status TEXT NOT NULL DEFAULT 'unverified',
                    safety_details TEXT NOT NULL DEFAULT '{}',
                    metadata_provider TEXT NOT NULL DEFAULT 'birdeye',
                    last_attempted_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS paper_signal_history (
                    id BIGSERIAL PRIMARY KEY,
                    token_address TEXT NOT NULL,
                    token_symbol TEXT,
                    status TEXT NOT NULL,
                    buy_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    sell_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    independent_buy_clusters INTEGER NOT NULL DEFAULT 0,
                    independent_sell_clusters INTEGER NOT NULL DEFAULT 0,
                    details TEXT NOT NULL DEFAULT '{}',
                    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS notification_deliveries (
                    id BIGSERIAL PRIMARY KEY,
                    event_key TEXT NOT NULL UNIQUE,
                    notification_type TEXT NOT NULL,
                    channel TEXT NOT NULL DEFAULT 'telegram',
                    token_address TEXT,
                    token_symbol TEXT,
                    signal_status TEXT,
                    message TEXT NOT NULL,
                    delivery_status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    response_code INTEGER,
                    response_summary TEXT NOT NULL DEFAULT '{}',
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    delivered_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS helius_sync_history (
                    id BIGSERIAL PRIMARY KEY,
                    webhook_id TEXT,
                    dry_run BOOLEAN NOT NULL,
                    sync_status TEXT NOT NULL,
                    desired_wallet_count INTEGER NOT NULL DEFAULT 0,
                    current_wallet_count INTEGER NOT NULL DEFAULT 0,
                    addresses_added TEXT NOT NULL DEFAULT '[]',
                    addresses_removed TEXT NOT NULL DEFAULT '[]',
                    response_code INTEGER,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS token_watchlist (
                    chain TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    token_symbol TEXT NOT NULL,
                    token_name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    monitoring_status TEXT NOT NULL,
                    canonical_pair_address TEXT,
                    canonical_pair_dex_id TEXT,
                    pair_locked_at TIMESTAMPTZ,
                    active BOOLEAN NOT NULL DEFAULT TRUE,
                    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (chain, token_address)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS evm_token_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    chain TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    token_symbol TEXT NOT NULL,
                    price_usd DOUBLE PRECISION,
                    liquidity_usd DOUBLE PRECISION,
                    market_cap_usd DOUBLE PRECISION,
                    fdv_usd DOUBLE PRECISION,
                    volume_h1_usd DOUBLE PRECISION,
                    volume_h24_usd DOUBLE PRECISION,
                    buys_h1 INTEGER,
                    sells_h1 INTEGER,
                    buys_h24 INTEGER,
                    sells_h24 INTEGER,
                    price_change_h1_pct DOUBLE PRECISION,
                    price_change_h24_pct DOUBLE PRECISION,
                    holder_count BIGINT,
                    pair_address TEXT,
                    dex_id TEXT,
                    data_quality TEXT NOT NULL DEFAULT 'partial',
                    provider_errors TEXT NOT NULL DEFAULT '[]',
                    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
                    anomaly_reasons TEXT NOT NULL DEFAULT '[]',
                    is_provider_unavailable BOOLEAN NOT NULL DEFAULT FALSE,
                    availability_reasons TEXT NOT NULL DEFAULT '[]',
                    pair_selection TEXT,
                    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS evm_token_signals (
                    chain TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    token_symbol TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'EVM_OBSERVE',
                    momentum_score INTEGER NOT NULL DEFAULT 0,
                    risk_score INTEGER NOT NULL DEFAULT 0,
                    reasons TEXT NOT NULL DEFAULT '[]',
                    holder_change_pct DOUBLE PRECISION,
                    liquidity_change_pct DOUBLE PRECISION,
                    volume_liquidity_ratio DOUBLE PRECISION,
                    buy_sell_ratio DOUBLE PRECISION,
                    liquidity_tier TEXT NOT NULL DEFAULT 'UNKNOWN',
                    structure_state TEXT NOT NULL DEFAULT 'COLLECTING',
                    structure_confidence INTEGER NOT NULL DEFAULT 0,
                    structure_details TEXT NOT NULL DEFAULT '{}',
                    horizon_metrics TEXT NOT NULL DEFAULT '{}',
                    wallet_quality TEXT NOT NULL DEFAULT '{}',
                    anomaly_details TEXT NOT NULL DEFAULT '{}',
                    availability_details TEXT NOT NULL DEFAULT '{}',
                    last_trusted_status TEXT,
                    is_benchmark BOOLEAN NOT NULL DEFAULT FALSE,
                    latest_snapshot_id BIGINT REFERENCES evm_token_snapshots(id),
                    data_quality TEXT NOT NULL DEFAULT 'partial',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (chain, token_address)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS evm_signal_history (
                    id BIGSERIAL PRIMARY KEY,
                    chain TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    token_symbol TEXT NOT NULL,
                    previous_status TEXT,
                    status TEXT NOT NULL,
                    momentum_score INTEGER NOT NULL DEFAULT 0,
                    risk_score INTEGER NOT NULL DEFAULT 0,
                    details TEXT NOT NULL DEFAULT '{}',
                    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS evm_refresh_runs (
                    id BIGSERIAL PRIMARY KEY,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMPTZ,
                    tokens_selected INTEGER NOT NULL DEFAULT 0,
                    snapshots_created INTEGER NOT NULL DEFAULT 0,
                    transitions_created INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'running',
                    details TEXT NOT NULL DEFAULT '{}'
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS wallet_activity_token_time_idx ON wallet_activity (token_address, occurred_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS wallet_activity_wallet_time_idx ON wallet_activity (wallet, occurred_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS dex_wallet_run_time_idx ON dex_wallet_discovery_runs (started_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS wallet_cohort_status_idx ON wallet_discovery_cohorts (cohort_status, updated_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS wallet_probation_time_idx ON wallet_probation_activity (occurred_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS wallet_probation_wallet_time_idx ON wallet_probation_activity (wallet, occurred_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS notification_delivery_status_idx ON notification_deliveries (delivery_status, created_at)")
            cur.execute("CREATE INDEX IF NOT EXISTS helius_sync_created_idx ON helius_sync_history (created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS evm_snapshot_token_time_idx ON evm_token_snapshots (chain, token_address, captured_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS evm_signal_status_idx ON evm_token_signals (status, updated_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS evm_history_time_idx ON evm_signal_history (recorded_at DESC)")

            # These are idempotent and preserve databases created by earlier V4 builds.
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS total_pnl_30d DOUBLE PRECISION")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS total_invested_30d DOUBLE PRECISION")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS score_status TEXT NOT NULL DEFAULT 'unscored'")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS last_scored TIMESTAMPTZ")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS screening_status TEXT NOT NULL DEFAULT 'unscreened'")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS screening_risk_score INTEGER")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS last_screened TIMESTAMPTZ")
            cur.execute("ALTER TABLE wallet_screenings ADD COLUMN IF NOT EXISTS funding_transfer_count INTEGER NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE wallet_screenings ADD COLUMN IF NOT EXISTS total_native_funding_lamports DOUBLE PRECISION NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE wallet_screenings ADD COLUMN IF NOT EXISTS transaction_types TEXT NOT NULL DEFAULT '{}'")
            cur.execute("ALTER TABLE wallet_screenings ADD COLUMN IF NOT EXISTS screening_version TEXT NOT NULL DEFAULT '4.2'")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS validation_status TEXT NOT NULL DEFAULT 'unvalidated'")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS last_validated TIMESTAMPTZ")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS early_entry_score DOUBLE PRECISION NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS repeat_early_entries INTEGER NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS discovery_tier TEXT NOT NULL DEFAULT 'CANDIDATE'")
            cur.execute("ALTER TABLE wallet_discovery_sources ADD COLUMN IF NOT EXISTS observed_entry_rank INTEGER")
            cur.execute("ALTER TABLE wallet_discovery_sources ADD COLUMN IF NOT EXISTS observed_entry_delay_seconds DOUBLE PRECISION")
            cur.execute("ALTER TABLE wallet_discovery_sources ADD COLUMN IF NOT EXISTS early_entry_score DOUBLE PRECISION NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE wallet_discovery_sources ADD COLUMN IF NOT EXISTS liquidity_usd DOUBLE PRECISION")
            cur.execute("ALTER TABLE wallet_discovery_sources ADD COLUMN IF NOT EXISTS volume_h24_usd DOUBLE PRECISION")
            cur.execute("ALTER TABLE wallet_discovery_sources ADD COLUMN IF NOT EXISTS pair_age_hours DOUBLE PRECISION")
            cur.execute("ALTER TABLE discovery_observations ADD COLUMN IF NOT EXISTS token_symbol TEXT")
            cur.execute("ALTER TABLE token_watchlist ADD COLUMN IF NOT EXISTS canonical_pair_address TEXT")
            cur.execute("ALTER TABLE token_watchlist ADD COLUMN IF NOT EXISTS canonical_pair_dex_id TEXT")
            cur.execute("ALTER TABLE token_watchlist ADD COLUMN IF NOT EXISTS pair_locked_at TIMESTAMPTZ")
            cur.execute("ALTER TABLE evm_token_snapshots ADD COLUMN IF NOT EXISTS is_anomaly BOOLEAN NOT NULL DEFAULT FALSE")
            cur.execute("ALTER TABLE evm_token_snapshots ADD COLUMN IF NOT EXISTS anomaly_reasons TEXT NOT NULL DEFAULT '[]'")
            cur.execute("ALTER TABLE evm_token_snapshots ADD COLUMN IF NOT EXISTS is_provider_unavailable BOOLEAN NOT NULL DEFAULT FALSE")
            cur.execute("ALTER TABLE evm_token_snapshots ADD COLUMN IF NOT EXISTS availability_reasons TEXT NOT NULL DEFAULT '[]'")
            cur.execute("ALTER TABLE evm_token_snapshots ADD COLUMN IF NOT EXISTS pair_selection TEXT")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS liquidity_tier TEXT NOT NULL DEFAULT 'UNKNOWN'")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS structure_state TEXT NOT NULL DEFAULT 'COLLECTING'")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS structure_confidence INTEGER NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS structure_details TEXT NOT NULL DEFAULT '{}'")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS horizon_metrics TEXT NOT NULL DEFAULT '{}'")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS wallet_quality TEXT NOT NULL DEFAULT '{}'")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS anomaly_details TEXT NOT NULL DEFAULT '{}'")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS availability_details TEXT NOT NULL DEFAULT '{}'")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS last_trusted_status TEXT")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS is_benchmark BOOLEAN NOT NULL DEFAULT FALSE")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS pending_status TEXT")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS pending_status_count INTEGER NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS raw_status TEXT")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS raw_status_count INTEGER NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE evm_token_signals ADD COLUMN IF NOT EXISTS alert_confirmation_count INTEGER NOT NULL DEFAULT 0")
            cur.execute("CREATE INDEX IF NOT EXISTS solana_metadata_updated_idx ON solana_token_metadata (updated_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS evm_snapshot_integrity_idx ON evm_token_snapshots (chain, token_address, is_anomaly, captured_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS evm_snapshot_availability_idx ON evm_token_snapshots (chain, token_address, is_provider_unavailable, captured_at DESC)")
            for chain, address, symbol, name, source, monitoring_status in DEFAULT_TOKEN_WATCHLIST:
                cur.execute("""
                    INSERT INTO token_watchlist (
                        chain, token_address, token_symbol, token_name,
                        source, monitoring_status, active, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW())
                    ON CONFLICT (chain, token_address) DO UPDATE SET
                        token_symbol = EXCLUDED.token_symbol,
                        token_name = EXCLUDED.token_name,
                        source = EXCLUDED.source,
                        monitoring_status = CASE
                            WHEN token_watchlist.monitoring_status IN (
                                'live_evm_monitoring', 'partial_evm_monitoring',
                                'data_anomaly_quarantined',
                                'provider_temporarily_unavailable'
                            ) THEN token_watchlist.monitoring_status
                            ELSE EXCLUDED.monitoring_status
                        END,
                        updated_at = NOW()
                """, (chain, address, symbol, name, source, monitoring_status))

            # The original native marker was metadata-only and never entered
            # the EVM refresh. Base WETH makes ETH a real benchmark with the
            # a real, non-actionable benchmark with the same evidence cadence.
            cur.execute("""
                UPDATE token_watchlist
                SET active = FALSE,
                    monitoring_status = 'replaced_by_base_weth_benchmark',
                    updated_at = NOW()
                WHERE token_address = 'native:ETH'
            """)

            # Reclassify the V4.10.1/2 timeout-only observations. Their market
            # values were already excluded from trends, so this is a semantic
            # repair rather than a change to trusted evidence.
            cur.execute("""
                UPDATE evm_token_snapshots
                SET is_anomaly = FALSE, anomaly_reasons = '[]',
                    is_provider_unavailable = TRUE,
                    availability_reasons = provider_errors,
                    data_quality = 'provider_unavailable'
                WHERE is_anomaly = TRUE
                    AND anomaly_reasons = '["market_data_unavailable_preserving_last_trusted"]'
            """)
            cur.execute("""
                UPDATE evm_token_signals signal
                SET last_trusted_status = COALESCE(
                    signal.last_trusted_status,
                    (
                        SELECT history.previous_status
                        FROM evm_signal_history history
                        WHERE history.chain = signal.chain
                            AND LOWER(history.token_address) = LOWER(signal.token_address)
                            AND history.status = 'EVM_DATA_ANOMALY'
                            AND history.previous_status IS NOT NULL
                        ORDER BY history.recorded_at DESC LIMIT 1
                    ),
                    CASE WHEN signal.status NOT IN (
                        'EVM_DATA_ANOMALY', 'EVM_PROVIDER_UNAVAILABLE'
                    ) THEN signal.status END
                )
            """)
            cur.execute("""
                UPDATE evm_token_signals
                SET status = 'EVM_PROVIDER_UNAVAILABLE', momentum_score = 0,
                    risk_score = 0,
                    reasons = '["market_provider_temporarily_unavailable"]',
                    liquidity_tier = 'LAST_TRUSTED',
                    structure_state = 'DATA_STALE', structure_confidence = 0,
                    data_quality = 'provider_unavailable',
                    availability_details = anomaly_details,
                    anomaly_details = '{}', updated_at = NOW()
                WHERE status = 'EVM_DATA_ANOMALY'
                    AND reasons = '["market_data_unavailable_preserving_last_trusted"]'
            """)

            # Lock existing tokens to the historically active pair. This favours
            # repeated observations with real trading over a new zero-activity
            # pool that merely reports a larger liquidity number.
            cur.execute("""
                WITH pair_stats AS (
                    SELECT chain, token_address, pair_address,
                        COUNT(*) FILTER (
                            WHERE COALESCE(volume_h1_usd, 0) > 0
                                OR COALESCE(buys_h1, 0) + COALESCE(sells_h1, 0) > 0
                        ) AS active_samples,
                        COUNT(*) AS sample_count,
                        MAX(captured_at) AS latest_sample
                    FROM evm_token_snapshots
                    WHERE pair_address IS NOT NULL AND is_anomaly = FALSE
                    GROUP BY chain, token_address, pair_address
                ), ranked AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY chain, token_address
                        ORDER BY active_samples DESC, sample_count DESC,
                            latest_sample DESC
                    ) AS pair_rank
                    FROM pair_stats
                    WHERE active_samples > 0
                )
                UPDATE token_watchlist watchlist
                SET canonical_pair_address = ranked.pair_address,
                    pair_locked_at = COALESCE(watchlist.pair_locked_at, NOW())
                FROM ranked
                WHERE ranked.pair_rank = 1
                    AND watchlist.chain = ranked.chain
                    AND LOWER(watchlist.token_address) = LOWER(ranked.token_address)
                    AND watchlist.canonical_pair_address IS NULL
            """)
        conn.commit()


# =========================================================
# VALIDATION / RESPONSE PARSING
# =========================================================

def is_valid_solana_address(value):
    """Strictly accept base58 strings that decode to exactly 32 bytes."""
    if not isinstance(value, str) or not 32 <= len(value) <= 44:
        return False
    if not all(char in BASE58_ALPHABET for char in value):
        return False

    try:
        number = 0
        for char in value:
            number = number * 58 + BASE58_ALPHABET.index(char)

        decoded = (
            b"" if number == 0
            else number.to_bytes((number.bit_length() + 7) // 8, byteorder="big")
        )
        leading_zeros = len(value) - len(value.lstrip("1"))
        return len((b"\x00" * leading_zeros) + decoded) == 32
    except Exception:
        return False


def wallet_address_from_trader(item):
    """Extract only wallet-specific fields, never a generic token address field."""
    if not isinstance(item, dict):
        return None
    for key in ("owner", "wallet", "wallet_address", "walletAddress"):
        value = item.get(key)
        if is_valid_solana_address(value):
            return value
    return None


def find_list(obj):
    """Recursively locate a useful list in a variably wrapped Birdeye response."""
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            return obj
        return []

    if isinstance(obj, dict):
        for key in ("items", "tokens", "traders", "list"):
            if key in obj:
                result = find_list(obj[key])
                if result:
                    return result
        for value in obj.values():
            result = find_list(value)
            if result:
                return result
    return []


def get_nested_number(obj, candidate_paths):
    for path in candidate_paths:
        current = obj
        try:
            for key in path:
                current = current[key]
            if isinstance(current, (int, float)) and not isinstance(current, bool):
                return float(current)
        except (KeyError, TypeError):
            continue
    return None


def parse_pnl_summary(payload):
    """Preserve the working V4 parser, including Birdeye's summary wrapper."""
    root = payload.get("data", payload) if isinstance(payload, dict) else payload
    if not isinstance(root, dict):
        return {"metrics": {}, "debug": {"error": "data_not_dictionary"}}

    summary = root.get("summary", root)
    if not isinstance(summary, dict):
        return {
            "metrics": {},
            "debug": {
                "error": "summary_not_dictionary",
                "root_keys": list(root.keys()),
            },
        }

    realized = get_nested_number(summary, [
        ("pnl", "realized_profit_usd"),
        ("pnl", "realized_profit"),
        ("realized_profit_usd",),
        ("realized_pnl",),
        ("realizedPnl",),
    ])
    total_pnl = get_nested_number(summary, [
        ("pnl", "total_usd"),
        ("pnl", "total_pnl_usd"),
        ("total_pnl_usd",),
        ("total_pnl",),
        ("totalPnl",),
    ])
    win_rate = get_nested_number(summary, [
        ("counts", "win_rate"),
        ("win_rate",),
        ("winRate",),
    ])
    trades = get_nested_number(summary, [
        ("counts", "total_trade"),
        ("counts", "total_trades"),
        ("total_trade",),
        ("total_trades",),
        ("trade_count",),
    ])
    invested = get_nested_number(summary, [
        ("cashflow_usd", "total_invested"),
        ("cashflow", "total_invested"),
        ("total_invested",),
        ("total_invested_usd",),
    ])

    return {
        "metrics": {
            "realized_pnl": realized,
            "total_pnl": total_pnl,
            "win_rate": win_rate,
            "trades": int(trades) if trades is not None else None,
            "invested": invested,
        },
        "debug": {
            "root_keys": list(root.keys()),
            "summary_keys": list(summary.keys()),
        },
    }


# =========================================================
# SCORING
# =========================================================

def calculate_score(realized_pnl, total_pnl, win_rate, trades, tokens_found, invested=None):
    """Return a bounded research score, risk flags, and capital efficiency."""
    score = 0
    flags = []

    score += min(max(tokens_found or 0, 0) * 8, 24)

    if win_rate is not None:
        wr = win_rate * 100 if win_rate <= 1 else win_rate
        if wr >= 70:
            score += 25
        elif wr >= 60:
            score += 20
        elif wr >= 55:
            score += 15
        elif wr >= 50:
            score += 8
        elif wr < 40:
            score -= 5

    if realized_pnl is not None:
        if realized_pnl >= 250000:
            score += 25
        elif realized_pnl >= 100000:
            score += 22
        elif realized_pnl >= 50000:
            score += 18
        elif realized_pnl >= 10000:
            score += 12
        elif realized_pnl > 0:
            score += 5
        else:
            score -= 15

    if trades is not None:
        if 30 <= trades <= 300:
            score += 16
        elif 10 <= trades < 30:
            score += 8
        elif 300 < trades <= 1000:
            score += 5
            flags.append("high_trade_frequency")
        elif 1000 < trades <= 5000:
            score -= 10
            flags.append("probable_bot")
        elif trades > 5000:
            score -= 30
            flags.append("extreme_hft_bot_risk")
        elif trades < 10:
            score -= 5
            flags.append("insufficient_history")

    if total_pnl is not None and total_pnl < 0:
        score -= 20
        flags.append("negative_total_pnl")

    efficiency = None
    if realized_pnl is not None and invested is not None and invested > 0:
        efficiency = realized_pnl / invested
        if efficiency >= 0.50:
            score += 15
        elif efficiency >= 0.20:
            score += 10
        elif efficiency >= 0.05:
            score += 5
        elif efficiency < 0.01:
            score -= 10
            flags.append("low_capital_efficiency")

    return {
        "score": max(0, min(round(score, 1), 100)),
        "flags": flags,
        "capital_efficiency": efficiency,
    }


def persist_score(wallet, metrics, score, score_status):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO candidate_wallets (
                    wallet, tokens_found, realized_pnl_30d, total_pnl_30d,
                    win_rate_30d, trades_30d, total_invested_30d,
                    score, score_status, last_scored, updated_at
                ) VALUES (%s, 0, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (wallet) DO UPDATE SET
                    realized_pnl_30d = EXCLUDED.realized_pnl_30d,
                    total_pnl_30d = EXCLUDED.total_pnl_30d,
                    win_rate_30d = EXCLUDED.win_rate_30d,
                    trades_30d = EXCLUDED.trades_30d,
                    total_invested_30d = EXCLUDED.total_invested_30d,
                    score = EXCLUDED.score,
                    score_status = EXCLUDED.score_status,
                    last_scored = NOW(),
                    updated_at = NOW()
            """, (
                wallet,
                metrics.get("realized_pnl"),
                metrics.get("total_pnl"),
                metrics.get("win_rate"),
                metrics.get("trades"),
                metrics.get("invested"),
                score,
                score_status,
            ))
        conn.commit()


def analyse_wallet_history(wallet, transactions, candidate):
    """Build explainable risk signals from a bounded Helius history sample."""
    flags = []
    risk_score = 0
    timestamps = []
    token_mints = set()
    counterparties = set()
    funders = {}
    funding_transfer_count = 0
    transaction_types = {}
    failed = 0

    for tx in transactions:
        if not isinstance(tx, dict):
            continue
        timestamp = tx.get("timestamp")
        if isinstance(timestamp, (int, float)):
            timestamps.append(int(timestamp))
        if tx.get("transactionError") or tx.get("error"):
            failed += 1
        transaction_type = tx.get("type") or "UNKNOWN"
        if isinstance(transaction_type, str):
            transaction_types[transaction_type] = transaction_types.get(transaction_type, 0) + 1

        for transfer in tx.get("tokenTransfers") or []:
            if not isinstance(transfer, dict):
                continue
            mint = transfer.get("mint")
            if is_valid_solana_address(mint):
                token_mints.add(mint)
            for field in ("fromUserAccount", "toUserAccount"):
                address = transfer.get(field)
                if is_valid_solana_address(address) and address != wallet:
                    counterparties.add(address)

        for transfer in tx.get("nativeTransfers") or []:
            if not isinstance(transfer, dict):
                continue
            sender = transfer.get("fromUserAccount")
            recipient = transfer.get("toUserAccount")
            amount = transfer.get("amount")
            if is_valid_solana_address(sender) and sender != wallet:
                counterparties.add(sender)
            if is_valid_solana_address(recipient) and recipient != wallet:
                counterparties.add(recipient)
            # Only plain TRANSFER transactions count as funding candidates.
            # Native movements inside swaps commonly represent proceeds,
            # routing, refunds, rent, or wrapped-SOL operations.
            if (
                transaction_type == "TRANSFER"
                and
                recipient == wallet
                and is_valid_solana_address(sender)
                and isinstance(amount, (int, float))
                and amount > 0
            ):
                funders[sender] = funders.get(sender, 0) + float(amount)
                funding_transfer_count += 1

    timestamps = sorted(set(timestamps), reverse=True)
    intervals = [
        timestamps[index] - timestamps[index + 1]
        for index in range(len(timestamps) - 1)
        if timestamps[index] >= timestamps[index + 1]
    ]
    median_interval = None
    if intervals:
        ordered = sorted(intervals)
        middle = len(ordered) // 2
        median_interval = (
            float(ordered[middle]) if len(ordered) % 2
            else (ordered[middle - 1] + ordered[middle]) / 2
        )

    total_funding = sum(funders.values())
    largest_funder = None
    largest_funder_share = None
    if funders and total_funding > 0:
        largest_funder, largest_amount = max(funders.items(), key=lambda item: item[1])
        largest_funder_share = largest_amount / total_funding

    transaction_count = len(transactions)
    failed_rate = failed / transaction_count if transaction_count else None

    if transaction_count < 20:
        flags.append("limited_onchain_sample")
        risk_score += 15
    if candidate["tokens_found"] < 2:
        flags.append("single_token_evidence")
        risk_score += 15
    if candidate["trades"] is not None and candidate["trades"] < 30:
        flags.append("limited_trade_history")
        risk_score += 10
    if candidate["realized_pnl"] is not None and candidate["realized_pnl"] < 1000:
        flags.append("low_absolute_profit")
        risk_score += 10
    if candidate["win_rate"] is not None:
        win_rate = candidate["win_rate"] / 100 if candidate["win_rate"] > 1 else candidate["win_rate"]
        if win_rate >= 0.999:
            flags.append("perfect_win_rate_requires_validation")
            risk_score += 15
        elif win_rate < 0.30:
            flags.append("very_low_win_rate")
            risk_score += 20
    if median_interval is not None and median_interval < 10:
        flags.append("bursty_automated_activity")
        risk_score += 25
    elif median_interval is not None and median_interval < 30:
        flags.append("rapid_activity")
        risk_score += 10
    if failed_rate is not None and failed_rate >= 0.20:
        flags.append("high_failed_transaction_rate")
        risk_score += 15
    if (
        funding_transfer_count >= 3
        and largest_funder_share is not None
        and largest_funder_share >= 0.90
        and total_funding >= 100000000
    ):
        flags.append("concentrated_funding_source")
        risk_score += 15
    if len(token_mints) <= 1 and transaction_count >= 20:
        flags.append("concentrated_token_activity")
        risk_score += 10

    dominant_type = None
    dominant_type_share = None
    if transaction_types and transaction_count:
        dominant_type, dominant_count = max(transaction_types.items(), key=lambda item: item[1])
        dominant_type_share = dominant_count / transaction_count
        service_like_types = {"TRANSFER", "TOKEN_MINT", "COMPRESSED_NFT_MINT", "UNKNOWN"}
        if dominant_type in service_like_types and dominant_type_share >= 0.80:
            flags.append("service_like_activity")
            risk_score += 15

    return {
        "risk_score": min(risk_score, 100),
        "risk_flags": flags,
        "transactions_sampled": transaction_count,
        "unique_tokens_sampled": len(token_mints),
        "unique_counterparties": len(counterparties),
        "largest_funder": largest_funder,
        "largest_funder_share": largest_funder_share,
        "funding_transfer_count": funding_transfer_count,
        "total_native_funding_lamports": total_funding,
        "transaction_types": transaction_types,
        "dominant_transaction_type": dominant_type,
        "dominant_transaction_type_share": dominant_type_share,
        "failed_transaction_rate": failed_rate,
        "median_interval_seconds": median_interval,
        "history_is_bounded": True,
        "screening_version": SCREENING_VERSION,
        "sampled_counterparties": sorted(counterparties),
        "sampled_funders": sorted(funders),
        "classification": (
            "high_risk" if risk_score >= 60
            else "review" if risk_score >= 30
            else "provisional_pass"
        ),
    }


def persist_screening(wallet, screening, status="screened"):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wallet_screenings (
                    wallet, screening_status, risk_score, risk_flags,
                    transactions_sampled, unique_tokens_sampled,
                    unique_counterparties, largest_funder, largest_funder_share,
                    funding_transfer_count, total_native_funding_lamports,
                    transaction_types, screening_version,
                    failed_transaction_rate, median_interval_seconds, screened_at, details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (wallet) DO UPDATE SET
                    screening_status = EXCLUDED.screening_status,
                    risk_score = EXCLUDED.risk_score,
                    risk_flags = EXCLUDED.risk_flags,
                    transactions_sampled = EXCLUDED.transactions_sampled,
                    unique_tokens_sampled = EXCLUDED.unique_tokens_sampled,
                    unique_counterparties = EXCLUDED.unique_counterparties,
                    largest_funder = EXCLUDED.largest_funder,
                    largest_funder_share = EXCLUDED.largest_funder_share,
                    funding_transfer_count = EXCLUDED.funding_transfer_count,
                    total_native_funding_lamports = EXCLUDED.total_native_funding_lamports,
                    transaction_types = EXCLUDED.transaction_types,
                    screening_version = EXCLUDED.screening_version,
                    failed_transaction_rate = EXCLUDED.failed_transaction_rate,
                    median_interval_seconds = EXCLUDED.median_interval_seconds,
                    screened_at = NOW(), details = EXCLUDED.details
            """, (
                wallet, status, screening["risk_score"],
                json.dumps(screening["risk_flags"]), screening["transactions_sampled"],
                screening["unique_tokens_sampled"], screening["unique_counterparties"],
                screening["largest_funder"], screening["largest_funder_share"],
                screening["funding_transfer_count"], screening["total_native_funding_lamports"],
                json.dumps(screening["transaction_types"]), SCREENING_VERSION,
                screening["failed_transaction_rate"], screening["median_interval_seconds"],
                json.dumps(screening),
            ))
            cur.execute("""
                UPDATE candidate_wallets SET screening_status = %s,
                    screening_risk_score = %s, last_screened = NOW(), updated_at = NOW()
                WHERE wallet = %s
            """, (status, screening["risk_score"], wallet))
        conn.commit()


def compact_screening(screening):
    """Keep raw address evidence in PostgreSQL, not in ordinary API responses."""
    return {
        key: value for key, value in screening.items()
        if key not in {"sampled_counterparties", "sampled_funders"}
    }


def parse_token_pnl_details(payload):
    """Normalize Birdeye PnL Details token rows across wrapper variations."""
    root = payload.get("data", payload) if isinstance(payload, dict) else payload
    entries = []

    def collect(obj, inherited_address=None):
        if isinstance(obj, list):
            for item in obj:
                collect(item)
            return
        if not isinstance(obj, dict):
            return

        address = (
            obj.get("token_address") or obj.get("tokenAddress")
            or obj.get("address") or inherited_address
        )
        looks_like_token = address and any(
            key in obj for key in ("pnl", "counts", "cashflow_usd", "realized_pnl")
        )
        if looks_like_token and is_valid_solana_address(address):
            entries.append((address, obj))
            return

        for key, value in obj.items():
            child_address = key if is_valid_solana_address(key) else None
            collect(value, child_address)

    collect(root)
    normalized = []
    seen = set()
    for token_address, item in entries:
        if token_address in seen:
            continue
        seen.add(token_address)
        trades = get_nested_number(item, [
            ("counts", "total_trade"), ("counts", "total_trades"),
            ("total_trade",), ("trades",),
        ])
        normalized.append({
            "token_address": token_address,
            "token_symbol": item.get("symbol") or item.get("token_symbol"),
            "realized_pnl": get_nested_number(item, [
                ("pnl", "realized_profit_usd"), ("pnl", "realized_profit"),
                ("realized_pnl",),
            ]),
            "total_pnl": get_nested_number(item, [
                ("pnl", "total_usd"), ("pnl", "total_pnl_usd"), ("total_pnl",),
            ]),
            "invested": get_nested_number(item, [
                ("cashflow_usd", "total_invested"), ("total_invested",),
            ]),
            "trades": int(trades) if trades is not None else None,
            "win_rate": get_nested_number(item, [
                ("counts", "win_rate"), ("win_rate",),
            ]),
        })
    return normalized


def summarize_token_validation(rows):
    strategy_rows = [
        row for row in rows
        if row["token_address"] != SOL_MINT
        and row["token_address"] not in STABLE_MINTS
        and (row.get("token_symbol") or "").upper() not in STABLE_SYMBOLS
    ]
    realized = [row for row in strategy_rows if row["realized_pnl"] is not None]
    profitable = [row for row in realized if row["realized_pnl"] > 0]
    losing = [row for row in realized if row["realized_pnl"] < 0]
    material = [
        row for row in realized
        if abs(row["realized_pnl"]) >= 100 or (row["invested"] or 0) >= 500
    ]
    material_profitable = [row for row in material if row["realized_pnl"] > 0]
    material_gains = [row for row in material if row["realized_pnl"] >= 100]
    material_losses = [row for row in material if row["realized_pnl"] <= -100]
    profit_sum = sum(row["realized_pnl"] for row in material_gains)
    loss_sum = abs(sum(row["realized_pnl"] for row in material_losses))
    profit_factor = profit_sum / loss_sum if loss_sum > 0 else None
    return {
        "tokens_returned": len(rows),
        "strategy_tokens": len(strategy_rows),
        "excluded_base_or_stable_tokens": len(rows) - len(strategy_rows),
        "tokens_with_realized_pnl": len(realized),
        "profitable_tokens": len(profitable),
        "losing_tokens": len(losing),
        "material_tokens": len(material),
        "material_profitable_tokens": len(material_profitable),
        "token_profit_rate": len(profitable) / len(realized) if realized else None,
        "material_token_profit_rate": (
            len(material_profitable) / len(material) if material else None
        ),
        "material_profit_factor": profit_factor,
        "material_profit_factor_status": (
            "calculated" if loss_sum > 0
            else "no_material_losses" if profit_sum > 0
            else "insufficient_material_outcomes"
        ),
        "perfect_win_rate_supported": bool(realized) and not losing,
        "validation_strength": (
            "strong" if len(material) >= 5
            else "moderate" if len(material) >= 3
            else "limited"
        ),
    }


def persist_token_validation(wallet, rows):
    with db() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute("""
                    INSERT INTO wallet_token_validations (
                        wallet, token_address, token_symbol, realized_pnl,
                        total_pnl, invested, trades, win_rate, validated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (wallet, token_address) DO UPDATE SET
                        token_symbol = EXCLUDED.token_symbol,
                        realized_pnl = EXCLUDED.realized_pnl,
                        total_pnl = EXCLUDED.total_pnl,
                        invested = EXCLUDED.invested,
                        trades = EXCLUDED.trades,
                        win_rate = EXCLUDED.win_rate,
                        validated_at = NOW()
                """, (
                    wallet, row["token_address"], row["token_symbol"],
                    row["realized_pnl"], row["total_pnl"], row["invested"],
                    row["trades"], row["win_rate"],
                ))
            cur.execute("""
                UPDATE candidate_wallets SET validation_status = %s,
                    last_validated = NOW(), updated_at = NOW() WHERE wallet = %s
            """, ("validated" if rows else "parse_incomplete", wallet))
        conn.commit()


# =========================================================
# HOME / HEALTH
# =========================================================

@app.get("/")
def home():
    return jsonify({
        "service": "Solana Smart Wallet + Multi-chain EVM Evidence Monitor",
        "status": "online",
        "version": VERSION,
        "mode": "relationship-aware Solana consensus plus EVM contract paper tracking",
        "actionable": False,
    })


@app.get("/health")
def health():
    checks = {
        "database": False,
        "helius_key": bool(os.getenv("HELIUS_API_KEY")),
        "birdeye_key": bool(os.getenv("BIRDEYE_API_KEY")),
    }
    try:
        initialise_database()
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                checks["database"] = cur.fetchone()[0] == 1
    except Exception:
        pass

    healthy = all(checks.values())
    return jsonify({
        "status": "healthy" if healthy else "configuration_required",
        "checks": checks, "version": VERSION, "birdeye_rps": BIRDEYE_RPS,
    }), 200 if healthy else 503


@app.get("/diagnostics")
def diagnostics():
    """Secret-free operational counters for this application process."""
    with _diagnostic_lock:
        counters = dict(_diagnostics)
    return jsonify({"success": True, "version": VERSION, "premium_mode": True,
                    "paper_mode": True, "signals_actionable": False,
                    "birdeye_rps": BIRDEYE_RPS,
                    "discovery_max_tokens": DISCOVERY_MAX_TOKENS,
                    "pipeline_max_seconds": PIPELINE_MAX_SECONDS,
                    "signal_window_minutes": SIGNAL_WINDOW_MINUTES,
                    "solana_consensus_windows_hours": list(SOLANA_CONSENSUS_WINDOWS_HOURS),
                    "solana_metadata_refresh_hours": SOLANA_METADATA_REFRESH_HOURS,
                    "solana_metadata_per_refresh": SOLANA_METADATA_PER_REFRESH,
                    "dex_wallet_discovery": {
                        "enabled": True,
                        "maximum_tokens_per_run": DEX_WALLET_DISCOVERY_MAX_TOKENS,
                        "candidates_per_token": DEX_WALLET_CANDIDATES_PER_TOKEN,
                        "helius_history_limit": DEX_WALLET_HISTORY_LIMIT,
                        "minimum_pair_liquidity_usd": DEX_WALLET_MIN_LIQUIDITY_USD,
                        "minimum_pair_volume_h24_usd": DEX_WALLET_MIN_VOLUME_H24_USD,
                        "minimum_pair_age_hours": DEX_WALLET_MIN_PAIR_AGE_HOURS,
                        "minimum_probation_days": DEX_WALLET_PROBATION_MIN_DAYS,
                        "minimum_forward_trades": DEX_WALLET_PROBATION_MIN_TRADES,
                        "candidate_and_probation_consensus_weight": 0,
                        "source_tokens_permanently_excluded": True,
                        "manual_promotion_required": True,
                    },
                    "helius_webhook_configured": bool(os.getenv("HELIUS_WEBHOOK_SECRET")),
                    "helius_webhook_sync_configured": bool(os.getenv("HELIUS_API_KEY")),
                    "helius_auto_sync": HELIUS_AUTO_SYNC,
                    "helius_target_webhook_url_configured": bool(HELIUS_TARGET_WEBHOOK_URL),
                    "admin_key_configured": bool(os.getenv("ADMIN_API_KEY")),
                    "telegram_configured": bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID")),
                    "telegram_alert_statuses": sorted(TELEGRAM_ALERT_STATUSES),
                    "evm_chain_ids": {
                        chain: config["chain_id"] for chain, config in EVM_CHAIN_CONFIG.items()
                    },
                    "evm_supported_chains": list(SUPPORTED_EVM_CHAINS),
                    "evm_refresh_max_seconds": EVM_REFRESH_MAX_SECONDS,
                    "evm_fetch_workers": EVM_FETCH_WORKERS,
                    "evm_provider_timeout_seconds": EVM_PROVIDER_TIMEOUT_SECONDS,
                    "evm_alert_statuses": sorted(EVM_ALERT_STATUSES),
                    "evm_min_momentum_liquidity_usd": EVM_MIN_MOMENTUM_LIQUIDITY_USD,
                    "evm_min_momentum_h1_volume_usd": EVM_MIN_MOMENTUM_H1_VOLUME_USD,
                    "evm_min_momentum_h1_transactions": EVM_MIN_MOMENTUM_H1_TRANSACTIONS,
                    "evm_visible_state_confirmations": EVM_VISIBLE_STATE_CONFIRMATIONS,
                    "evm_alert_confirmations": EVM_ALERT_CONFIRMATIONS,
                    "evm_data_integrity": {
                        "canonical_pair_locking": True,
                        "anomalies_excluded_from_trends": True,
                        "anomalies_excluded_from_telegram": True,
                        "provider_outages_separate_from_anomalies": True,
                        "provider_outages_excluded_from_telegram": True,
                        "last_trusted_snapshot_preserved": True,
                        "price_ratio_threshold": EVM_ANOMALY_PRICE_RATIO,
                        "liquidity_ratio_threshold": EVM_ANOMALY_LIQUIDITY_RATIO,
                    },
                    "counters": counters})


# =========================================================
# DISCOVERY
# =========================================================

def observed_early_entry_score(entry_delay_seconds, entry_rank):
    """Score bounded observed entry evidence without claiming first-ever entry."""
    if entry_delay_seconds is None or entry_delay_seconds < 0:
        time_score = 20
    elif entry_delay_seconds <= 15 * 60:
        time_score = 95
    elif entry_delay_seconds <= 60 * 60:
        time_score = 85
    elif entry_delay_seconds <= 6 * 60 * 60:
        time_score = 70
    elif entry_delay_seconds <= 24 * 60 * 60:
        time_score = 50
    elif entry_delay_seconds <= 72 * 60 * 60:
        time_score = 30
    else:
        time_score = 15
    rank_bonus = 10 if entry_rank <= 3 else 5 if entry_rank <= 10 else 0
    return min(time_score + rank_bonus, 100)


def extract_dex_wallet_candidates(token_address, transactions, limit, pair_created_ms=None):
    """Rank recent on-chain buyers without treating discovery as proof of skill."""
    observations = {}
    if not isinstance(transactions, list):
        return []
    for transaction in transactions:
        if not isinstance(transaction, dict):
            continue
        if str(transaction.get("type") or "").upper() != "SWAP":
            continue
        signature = transaction.get("signature")
        timestamp = transaction.get("timestamp")
        if not isinstance(signature, str) or not isinstance(timestamp, (int, float)):
            continue
        for transfer in transaction.get("tokenTransfers") or []:
            if not isinstance(transfer, dict) or transfer.get("mint") != token_address:
                continue
            wallet = transfer.get("toUserAccount")
            if not is_valid_solana_address(wallet):
                continue
            try:
                amount = float(transfer.get("tokenAmount"))
            except (TypeError, ValueError):
                continue
            if amount <= 0 or transfer.get("fromUserAccount") == wallet:
                continue
            record = observations.setdefault(wallet, {
                "wallet": wallet, "buy_transactions": set(),
                "first_observed_at": timestamp, "last_observed_at": timestamp,
                "token_amount": 0.0,
            })
            record["buy_transactions"].add(signature)
            record["first_observed_at"] = min(record["first_observed_at"], timestamp)
            record["last_observed_at"] = max(record["last_observed_at"], timestamp)
            record["token_amount"] += amount
    ranked = []
    for record in observations.values():
        ranked.append({
            "wallet": record["wallet"],
            "buy_transactions": len(record["buy_transactions"]),
            "first_observed_at": datetime.fromtimestamp(
                record["first_observed_at"], timezone.utc
            ),
            "last_observed_at": datetime.fromtimestamp(
                record["last_observed_at"], timezone.utc
            ),
            "token_amount": record["token_amount"],
        })
    ranked.sort(key=lambda item: (
        item["first_observed_at"], -item["buy_transactions"], item["wallet"]
    ))
    pair_created_at = (
        datetime.fromtimestamp(pair_created_ms / 1000, timezone.utc)
        if isinstance(pair_created_ms, (int, float)) and pair_created_ms > 0 else None
    )
    for entry_rank, item in enumerate(ranked, start=1):
        delay = (
            max((item["first_observed_at"] - pair_created_at).total_seconds(), 0)
            if pair_created_at else None
        )
        item["observed_entry_rank"] = entry_rank
        item["observed_entry_delay_seconds"] = delay
        item["early_entry_score"] = observed_early_entry_score(delay, entry_rank)
    return ranked[:limit]


def refresh_discovery_wallet_evidence(cur, wallet):
    """Aggregate repeat early-entry evidence while keeping admission manual."""
    cur.execute("""
        SELECT COUNT(*),
            COUNT(*) FILTER (WHERE early_entry_score >= %s),
            COALESCE(AVG(early_entry_score), 0)
        FROM wallet_discovery_sources WHERE wallet = %s
    """, (DEX_WALLET_EARLY_SCORE_THRESHOLD, wallet))
    tokens_found, repeat_early_entries, average_early_score = cur.fetchone()
    if repeat_early_entries >= 3 and average_early_score >= DEX_WALLET_EARLY_SCORE_THRESHOLD:
        discovery_tier = "PROVEN"
    elif repeat_early_entries >= 2:
        discovery_tier = "PROBATION"
    else:
        discovery_tier = "CANDIDATE"
    cur.execute("""
        UPDATE candidate_wallets SET tokens_found = %s,
            early_entry_score = %s, repeat_early_entries = %s,
            discovery_tier = %s, updated_at = NOW() WHERE wallet = %s
    """, (
        tokens_found, round(float(average_early_score), 2),
        repeat_early_entries, discovery_tier, wallet,
    ))
    cur.execute("""
        UPDATE wallet_discovery_cohorts SET details =
            COALESCE(details, '{}')::jsonb || %s::jsonb,
            updated_at = NOW() WHERE wallet = %s
    """, (json.dumps({
        "discovery_tier": discovery_tier,
        "repeat_early_entries": repeat_early_entries,
        "average_early_entry_score": round(float(average_early_score), 2),
        "automatic_signal_admission": False,
    }), wallet))
    return discovery_tier


def select_solana_discovery_pair(payload, token_address):
    """Choose the most liquid Solana pair for bounded on-chain sampling."""
    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    eligible = []
    for pair in pairs if isinstance(pairs, list) else []:
        if not isinstance(pair, dict) or pair.get("chainId") != "solana":
            continue
        base_address = (pair.get("baseToken") or {}).get("address")
        quote_address = (pair.get("quoteToken") or {}).get("address")
        if token_address not in {base_address, quote_address}:
            continue
        pair_address = pair.get("pairAddress")
        if not is_valid_solana_address(pair_address):
            continue
        try:
            liquidity = float((pair.get("liquidity") or {}).get("usd") or 0)
        except (TypeError, ValueError):
            liquidity = 0.0
        eligible.append((liquidity, pair))
    return max(eligible, key=lambda item: item[0])[1] if eligible else None


def candidate_probation_assessment(wallet):
    """Apply the existing evidence classifier to one discovery candidate."""
    validation_summaries = load_validation_summaries()
    repeat_evidence = load_repeat_evidence()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.wallet, c.tokens_found, c.realized_pnl_30d,
                    c.total_pnl_30d, c.win_rate_30d, c.trades_30d,
                    c.total_invested_30d, c.score, c.score_status,
                    c.screening_status, c.screening_risk_score,
                    c.validation_status, COALESCE(ws.risk_flags, '[]')
                FROM candidate_wallets c
                LEFT JOIN wallet_screenings ws ON ws.wallet = c.wallet
                    AND ws.screening_version = %s
                WHERE c.wallet = %s
            """, (SCREENING_VERSION, wallet))
            row = cur.fetchone()
    if not row:
        return {"ready": False, "classification": "UNKNOWN", "reasons": ["candidate_not_found"]}
    try:
        risk_flags = json.loads(row[12]) if row[12] else []
    except (TypeError, ValueError):
        risk_flags = []
    candidate = {
        "wallet": row[0], "tokens_found": row[1], "realized_pnl": row[2],
        "total_pnl": row[3], "win_rate": row[4], "trades": row[5],
        "invested": row[6], "score": row[7], "score_status": row[8],
        "screening_status": row[9], "validation_status": row[11],
    }
    classification = classify_candidate(
        candidate, {"risk_score": row[10], "risk_flags": risk_flags},
        validation_summaries.get(wallet), repeat_evidence.get(wallet, {}),
    )
    hard_risks = {"service_like_activity", "bursty_automated_activity"}
    probation_requirements = {
        "scored_at_least_30": row[8] == "scored" and (row[7] or 0) >= 30,
        "screened_low_risk": row[9] == "screened" and row[10] is not None and row[10] <= 25,
        "no_hard_service_or_bot_risk": not bool(set(risk_flags) & hard_risks),
        "meaningful_trade_history": row[5] is not None and row[5] >= 30,
        "positive_realized_pnl": row[2] is not None and row[2] > 0,
    }
    classification["probation_requirements"] = probation_requirements
    classification["signal_ready"] = classification["classification"] in {"WATCH", "ASYMMETRIC"}
    classification["ready"] = all(probation_requirements.values())
    return classification


def load_dex_wallet_pipeline_status():
    """Summarise the wide discovery funnel without granting signal weight."""
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS all_candidates,
                    COUNT(*) FILTER (WHERE cohort.wallet IS NOT NULL) AS discovered,
                    COUNT(*) FILTER (
                        WHERE cohort.wallet IS NOT NULL
                            AND (candidate.last_scored IS NULL
                                OR candidate.score_status = 'parse_incomplete')
                    ) AS awaiting_score,
                    COUNT(*) FILTER (
                        WHERE cohort.wallet IS NOT NULL
                            AND candidate.score_status = 'scored'
                    ) AS scored,
                    COUNT(*) FILTER (
                        WHERE cohort.wallet IS NOT NULL
                            AND candidate.score_status = 'scored'
                            AND candidate.score >= 30
                    ) AS performance_pass,
                    COUNT(*) FILTER (
                        WHERE cohort.wallet IS NOT NULL
                            AND candidate.screening_status = 'screened'
                    ) AS screened,
                    COUNT(*) FILTER (
                        WHERE cohort.wallet IS NOT NULL
                            AND candidate.screening_status = 'screened'
                            AND candidate.screening_risk_score IS NOT NULL
                            AND candidate.screening_risk_score <= 25
                    ) AS low_risk,
                    COUNT(*) FILTER (
                        WHERE cohort.wallet IS NOT NULL
                            AND candidate.validation_status = 'validated'
                    ) AS validated,
                    COUNT(*) FILTER (
                        WHERE cohort.wallet IS NOT NULL
                            AND cohort.cohort_status = 'CANDIDATE'
                            AND candidate.score_status = 'scored'
                            AND candidate.score >= 30
                            AND candidate.screening_status = 'screened'
                            AND candidate.screening_risk_score IS NOT NULL
                            AND candidate.screening_risk_score <= 25
                            AND candidate.trades_30d >= 30
                            AND candidate.realized_pnl_30d > 0
                            AND COALESCE(screening.risk_flags, '[]')
                                NOT LIKE '%%service_like_activity%%'
                            AND COALESCE(screening.risk_flags, '[]')
                                NOT LIKE '%%bursty_automated_activity%%'
                    ) AS probation_ready,
                    COUNT(*) FILTER (
                        WHERE cohort.wallet IS NOT NULL
                            AND cohort.cohort_status = 'PROBATION'
                    ) AS probation,
                    COUNT(*) FILTER (
                        WHERE cohort.wallet IS NOT NULL
                            AND cohort.cohort_status = 'VALIDATED'
                    ) AS consensus_validated
                FROM candidate_wallets candidate
                LEFT JOIN wallet_discovery_cohorts cohort
                    ON cohort.wallet = candidate.wallet
                    AND cohort.source = 'dexscreener'
                LEFT JOIN wallet_screenings screening
                    ON screening.wallet = candidate.wallet
                    AND screening.screening_version = %s
            """, (SCREENING_VERSION,))
            row = cur.fetchone()
            cur.execute("""
                SELECT candidate.wallet, candidate.score,
                    candidate.screening_risk_score,
                    candidate.realized_pnl_30d, candidate.trades_30d,
                    candidate.validation_status, candidate.repeat_early_entries,
                    candidate.early_entry_score
                FROM candidate_wallets candidate
                JOIN wallet_discovery_cohorts cohort
                    ON cohort.wallet = candidate.wallet
                    AND cohort.source = 'dexscreener'
                    AND cohort.cohort_status = 'CANDIDATE'
                LEFT JOIN wallet_screenings screening
                    ON screening.wallet = candidate.wallet
                    AND screening.screening_version = %s
                WHERE candidate.score_status = 'scored'
                    AND candidate.score >= 30
                    AND candidate.screening_status = 'screened'
                    AND candidate.screening_risk_score IS NOT NULL
                    AND candidate.screening_risk_score <= 25
                    AND candidate.trades_30d >= 30
                    AND candidate.realized_pnl_30d > 0
                    AND COALESCE(screening.risk_flags, '[]')
                        NOT LIKE '%%service_like_activity%%'
                    AND COALESCE(screening.risk_flags, '[]')
                        NOT LIKE '%%bursty_automated_activity%%'
                ORDER BY candidate.repeat_early_entries DESC,
                    candidate.early_entry_score DESC, candidate.score DESC,
                    candidate.screening_risk_score ASC
                LIMIT 25
            """, (SCREENING_VERSION,))
            ready_rows = cur.fetchall()
    names = (
        "all_candidates", "discovered", "awaiting_score", "scored",
        "performance_pass", "screened", "low_risk", "validated",
        "probation_ready", "probation", "consensus_validated",
    )
    counts = dict(zip(names, row or (0,) * len(names)))
    return {
        "counts": counts,
        "probation_ready_wallets": [{
            "wallet": item[0], "performance_score": item[1],
            "screening_risk_score": item[2],
            "realized_pnl_30d": item[3], "trades_30d": item[4],
            "validation_status": item[5], "repeat_early_entries": item[6],
            "average_early_entry_score": item[7],
            "consensus_weight": 0,
        } for item in ready_rows],
        "policy": {
            "batch_limits": {"score": 5, "screen": 5, "validate": 2},
            "manual_probation_admission": True,
            "automatic_consensus_admission": False,
            "candidate_and_probation_consensus_weight": 0,
        },
    }


@app.get("/dex-wallet-pipeline-status")
def dex_wallet_pipeline_status():
    initialise_database()
    payload = load_dex_wallet_pipeline_status()
    return jsonify({
        "success": True, "version": VERSION, "paper_mode": True,
        **payload,
    })


@app.post("/discover-dex-wallets")
def discover_dex_wallets():
    """Use DexScreener for token discovery and Helius for wallet attribution."""
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    initialise_database()
    body = request.get_json(silent=True) or {}
    try:
        token_limit = int(body.get("tokens", DEX_WALLET_DISCOVERY_MAX_TOKENS))
    except (TypeError, ValueError):
        token_limit = DEX_WALLET_DISCOVERY_MAX_TOKENS
    token_limit = min(max(token_limit, 1), DEX_WALLET_DISCOVERY_MAX_TOKENS)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO dex_wallet_discovery_runs (status) VALUES ('running') RETURNING id")
            run_id = cur.fetchone()[0]
        conn.commit()
    diagnostic_increment("dex_wallet_discovery_runs")
    try:
        response = upstream_request(
            "GET", DEXSCREENER_BOOSTS_TOP_URL, timeout=15, retries=1,
            provider="dexscreener",
        )
    except (requests.Timeout, requests.ConnectionError) as exc:
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE dex_wallet_discovery_runs SET completed_at = NOW(),
                        status = 'provider_unavailable', details = %s WHERE id = %s
                """, (json.dumps({"error": type(exc).__name__}), run_id))
            conn.commit()
        return jsonify({"success": False, "run_id": run_id, "error": "DexScreener unavailable"}), 503
    try:
        boost_items = response.json() if response.status_code == 200 else []
    except ValueError:
        boost_items = []
    tokens = []
    seen = set()
    for item in boost_items if isinstance(boost_items, list) else []:
        token_address = item.get("tokenAddress") if isinstance(item, dict) else None
        if item.get("chainId") != "solana" or not is_valid_solana_address(token_address):
            continue
        if token_address in seen or token_address in STABLE_MINTS or token_address == SOL_MINT:
            continue
        seen.add(token_address)
        tokens.append(item)
        if len(tokens) >= token_limit:
            break
    if not tokens:
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE dex_wallet_discovery_runs SET completed_at = NOW(),
                        status = 'no_solana_tokens', details = %s WHERE id = %s
                """, (json.dumps({"dexscreener_status": response.status_code}), run_id))
            conn.commit()
        return jsonify({"success": False, "run_id": run_id, "error": "No usable boosted Solana tokens"}), 502

    unique_wallets = set()
    results = []
    for token in tokens:
        token_address = token["tokenAddress"]
        try:
            pair_response = upstream_request(
                "GET", DEXSCREENER_TOKEN_URL.format(address=token_address),
                timeout=15, retries=1, provider="dexscreener",
            )
            pair_payload = pair_response.json() if pair_response.status_code == 200 else {}
            pair = select_solana_discovery_pair(pair_payload, token_address)
            if not pair:
                results.append({
                    "token_address": token_address, "status": "pair_unavailable",
                    "candidates": 0,
                })
                continue
            try:
                pair_liquidity = float((pair.get("liquidity") or {}).get("usd") or 0)
                pair_volume_h24 = float((pair.get("volume") or {}).get("h24") or 0)
                pair_created_ms = float(pair.get("pairCreatedAt") or 0)
            except (TypeError, ValueError):
                pair_liquidity, pair_volume_h24, pair_created_ms = 0.0, 0.0, 0.0
            pair_age_hours = (
                (time.time() - pair_created_ms / 1000) / 3600
                if pair_created_ms > 0 else None
            )
            quality_reasons = []
            if pair_liquidity < DEX_WALLET_MIN_LIQUIDITY_USD:
                quality_reasons.append("insufficient_liquidity")
            if pair_volume_h24 < DEX_WALLET_MIN_VOLUME_H24_USD:
                quality_reasons.append("insufficient_24h_volume")
            if pair_age_hours is None or pair_age_hours < DEX_WALLET_MIN_PAIR_AGE_HOURS:
                quality_reasons.append("pair_too_new_or_age_unknown")
            if quality_reasons:
                results.append({
                    "token_address": token_address, "status": "quality_filtered",
                    "pair_address": pair.get("pairAddress"),
                    "liquidity_usd": pair_liquidity,
                    "volume_h24_usd": pair_volume_h24,
                    "pair_age_hours": round(pair_age_hours, 2) if pair_age_hours is not None else None,
                    "reasons": quality_reasons, "candidates": 0,
                })
                continue
            history_response = helius_get_address_transactions(
                pair["pairAddress"], DEX_WALLET_HISTORY_LIMIT
            )
            history = history_response.json() if history_response.status_code == 200 else []
        except (requests.Timeout, requests.ConnectionError, ValueError) as exc:
            results.append({
                "token_address": token_address, "status": "helius_unavailable",
                "error": type(exc).__name__, "candidates": 0,
            })
            continue
        candidates = extract_dex_wallet_candidates(
            token_address, history, DEX_WALLET_CANDIDATES_PER_TOKEN,
            pair_created_ms,
        )
        pair_token = (
            pair.get("baseToken")
            if (pair.get("baseToken") or {}).get("address") == token_address
            else pair.get("quoteToken")
        ) or {}
        with db() as conn:
            with conn.cursor() as cur:
                for candidate in candidates:
                    wallet = candidate["wallet"]
                    unique_wallets.add(wallet)
                    cur.execute("""
                        INSERT INTO candidate_wallets (wallet, tokens_found)
                        VALUES (%s, 1) ON CONFLICT (wallet) DO NOTHING
                    """, (wallet,))
                    cur.execute("""
                        INSERT INTO wallet_token_hits (
                            wallet, token_address, token_symbol, token_name
                        ) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (wallet, token_address) DO UPDATE SET
                            token_symbol = COALESCE(EXCLUDED.token_symbol, wallet_token_hits.token_symbol),
                            token_name = COALESCE(EXCLUDED.token_name, wallet_token_hits.token_name)
                    """, (
                        wallet, token_address, pair_token.get("symbol"),
                        pair_token.get("name") or token.get("description"),
                    ))
                    cur.execute("""
                        UPDATE candidate_wallets SET tokens_found = (
                            SELECT COUNT(*) FROM wallet_token_hits WHERE wallet = %s
                        ), updated_at = NOW() WHERE wallet = %s
                    """, (wallet, wallet))
                    cur.execute("""
                        INSERT INTO wallet_discovery_cohorts (
                            wallet, cohort_status, eligible_signal_at, details
                        ) SELECT %s,
                            CASE WHEN EXISTS (
                                SELECT 1 FROM wallet_clusters WHERE wallet = %s
                            ) THEN 'VALIDATED' ELSE 'CANDIDATE' END,
                            CASE WHEN EXISTS (
                                SELECT 1 FROM wallet_clusters WHERE wallet = %s
                            ) THEN NOW() ELSE NULL END,
                            %s
                        ON CONFLICT (wallet) DO UPDATE SET
                            details = EXCLUDED.details, updated_at = NOW()
                    """, (
                        wallet, wallet, wallet, json.dumps({
                            "latest_run_id": run_id,
                            "discovery_is_not_performance_proof": True,
                        }),
                    ))
                    cur.execute("""
                        INSERT INTO wallet_discovery_sources (
                            wallet, token_address, pair_address,
                            observed_entry_rank, observed_entry_delay_seconds,
                            early_entry_score, liquidity_usd, volume_h24_usd,
                            pair_age_hours, details
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (wallet, token_address) DO UPDATE SET
                            pair_address = EXCLUDED.pair_address,
                            observed_entry_rank = LEAST(
                                wallet_discovery_sources.observed_entry_rank,
                                EXCLUDED.observed_entry_rank
                            ),
                            observed_entry_delay_seconds = LEAST(
                                wallet_discovery_sources.observed_entry_delay_seconds,
                                EXCLUDED.observed_entry_delay_seconds
                            ),
                            early_entry_score = GREATEST(
                                wallet_discovery_sources.early_entry_score,
                                EXCLUDED.early_entry_score
                            ),
                            liquidity_usd = EXCLUDED.liquidity_usd,
                            volume_h24_usd = EXCLUDED.volume_h24_usd,
                            pair_age_hours = EXCLUDED.pair_age_hours,
                            details = EXCLUDED.details
                    """, (
                        wallet, token_address, pair.get("pairAddress"),
                        candidate["observed_entry_rank"],
                        candidate["observed_entry_delay_seconds"],
                        candidate["early_entry_score"], pair_liquidity,
                        pair_volume_h24, pair_age_hours,
                        json.dumps({
                            "run_id": run_id,
                            "buy_transactions_in_sample": candidate["buy_transactions"],
                            "first_observed_at": candidate["first_observed_at"].isoformat(),
                            "last_observed_at": candidate["last_observed_at"].isoformat(),
                            "observed_entry_not_first_ever_entry": True,
                        }),
                    ))
                    refresh_discovery_wallet_evidence(cur, wallet)
            conn.commit()
        results.append({
            "token_address": token_address, "status": "completed",
            "pair_address": pair.get("pairAddress"), "candidates": len(candidates),
        })
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE dex_wallet_discovery_runs SET completed_at = NOW(),
                    tokens_examined = %s, candidates_found = %s,
                    status = 'completed', details = %s WHERE id = %s
            """, (
                len(tokens), len(unique_wallets),
                json.dumps({"results": results}), run_id,
            ))
        conn.commit()
    for _ in unique_wallets:
        diagnostic_increment("dex_wallet_candidates_found")
    return jsonify({
        "success": True, "version": VERSION, "run_id": run_id,
        "tokens_examined": len(tokens), "unique_candidates_found": len(unique_wallets),
        "results": results, "cohort_status": "CANDIDATE",
        "next_gate": "existing scoring, Helius screening, and token validation",
        "policy": {
            "paper_only": True,
            "discovery_does_not_prove_success": True,
            "source_tokens_permanently_excluded": True,
            "automatic_signal_admission": False,
            "wide_funnel": True,
            "early_entry_threshold": DEX_WALLET_EARLY_SCORE_THRESHOLD,
        },
    })


@app.get("/dex-wallet-leaderboard")
def dex_wallet_leaderboard():
    """Expose the wide funnel and repeat early-entry evidence for review."""
    initialise_database()
    try:
        limit = min(max(int(request.args.get("limit", 50)), 1), 200)
    except (TypeError, ValueError):
        limit = 50
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT discovery_tier, COUNT(*)
                FROM candidate_wallets GROUP BY discovery_tier
            """)
            tier_counts = {row[0]: row[1] for row in cur.fetchall()}
            cur.execute("""
                SELECT wallet, discovery_tier, tokens_found,
                    repeat_early_entries, early_entry_score, score,
                    score_status, screening_status, screening_risk_score,
                    created_at, updated_at
                FROM candidate_wallets
                ORDER BY repeat_early_entries DESC, early_entry_score DESC,
                    tokens_found DESC, updated_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    return jsonify({
        "success": True, "version": VERSION, "paper_mode": True,
        "automatic_signal_admission": False,
        "early_entry_threshold": DEX_WALLET_EARLY_SCORE_THRESHOLD,
        "tier_counts": tier_counts,
        "wallets": [{
            "wallet": row[0], "discovery_tier": row[1],
            "tokens_found": row[2], "repeat_early_entries": row[3],
            "average_early_entry_score": row[4],
            "performance_score": row[5], "score_status": row[6],
            "screening_status": row[7], "screening_risk_score": row[8],
            "discovered_at": row[9], "updated_at": row[10],
        } for row in rows],
    })


@app.post("/dex-wallet-cohorts/<wallet>/probation")
def admit_dex_wallet_probation(wallet):
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    if not is_valid_solana_address(wallet):
        return jsonify({"success": False, "error": "Invalid Solana wallet"}), 400
    initialise_database()
    assessment = candidate_probation_assessment(wallet)
    if not assessment.get("ready"):
        return jsonify({
            "success": False, "error": "Candidate has not passed the zero-weight probation gates",
            "assessment": assessment,
        }), 409
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE wallet_discovery_cohorts SET cohort_status = 'PROBATION',
                    admitted_at = NOW(), eligible_signal_at = NOW(),
                    forward_trades = 0, forward_tokens = 0,
                    last_forward_activity_at = NULL, updated_at = NOW()
                WHERE wallet = %s AND source = 'dexscreener'
                    AND cohort_status = 'CANDIDATE'
            """, (wallet,))
            changed = cur.rowcount
        conn.commit()
    if not changed:
        return jsonify({"success": False, "error": "Candidate is not awaiting probation"}), 409
    sync_result = synchronise_helius_webhook(dry_run=False, force=False) if HELIUS_AUTO_SYNC else None
    return jsonify({
        "success": True, "wallet": wallet, "cohort_status": "PROBATION",
        "assessment": assessment, "helius_sync": sync_result,
        "consensus_weight": 0, "paper_only": True,
    })


@app.post("/dex-wallet-cohorts/<wallet>/validate")
def validate_dex_wallet_cohort(wallet):
    """Manual promotion after a minimum future-only probation sample."""
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    initialise_database()
    body = request.get_json(silent=True) or {}
    if body.get("manual_review_approved") is not True:
        return jsonify({"success": False, "error": "manual_review_approved=true is required"}), 400
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cohort_status, admitted_at, forward_trades, forward_tokens
                FROM wallet_discovery_cohorts WHERE wallet = %s
            """, (wallet,))
            row = cur.fetchone()
    if not row or row[0] != "PROBATION":
        return jsonify({"success": False, "error": "Wallet is not in probation"}), 409
    probation_age = datetime.now(timezone.utc) - row[1] if row[1] else timedelta(0)
    assessment = candidate_probation_assessment(wallet)
    requirements = {
        "minimum_days": probation_age >= timedelta(days=DEX_WALLET_PROBATION_MIN_DAYS),
        "minimum_forward_trades": row[2] >= DEX_WALLET_PROBATION_MIN_TRADES,
        "multiple_forward_tokens": row[3] >= 2,
        "full_signal_evidence_gates": bool(assessment.get("signal_ready")),
    }
    if not all(requirements.values()):
        return jsonify({
            "success": False, "error": "Probation evidence is incomplete",
            "requirements": requirements, "assessment": assessment,
        }), 409
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE wallet_discovery_cohorts SET cohort_status = 'VALIDATED',
                    eligible_signal_at = NOW(), updated_at = NOW() WHERE wallet = %s
            """, (wallet,))
        conn.commit()
    compute_and_persist_wallet_clusters()
    sync_result = synchronise_helius_webhook(dry_run=False, force=False) if HELIUS_AUTO_SYNC else None
    return jsonify({
        "success": True, "wallet": wallet, "cohort_status": "VALIDATED",
        "requirements": requirements, "helius_sync": sync_result,
        "source_tokens_permanently_excluded": True, "paper_only": True,
    })


@app.get("/dex-wallet-cohorts")
def dex_wallet_cohorts():
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.wallet, c.cohort_status, c.discovered_at, c.admitted_at,
                    c.eligible_signal_at, c.forward_trades, c.forward_tokens,
                    c.last_forward_activity_at, c.updated_at,
                    COUNT(s.token_address) AS source_tokens
                FROM wallet_discovery_cohorts c
                LEFT JOIN wallet_discovery_sources s ON s.wallet = c.wallet
                GROUP BY c.wallet, c.cohort_status, c.discovered_at, c.admitted_at,
                    c.eligible_signal_at, c.forward_trades, c.forward_tokens,
                    c.last_forward_activity_at, c.updated_at
                ORDER BY c.updated_at DESC
            """)
            rows = cur.fetchall()
    now = datetime.now(timezone.utc)
    items = []
    counts = {}
    for row in rows:
        status = row[1]
        counts[status] = counts.get(status, 0) + 1
        age = now - row[3] if row[3] else timedelta(0)
        items.append({
            "wallet": row[0], "cohort_status": status,
            "discovered_at": row[2], "admitted_at": row[3],
            "eligible_signal_at": row[4], "forward_trades": row[5],
            "forward_tokens": row[6], "last_forward_activity_at": row[7],
            "source_tokens_excluded": row[9], "updated_at": row[8],
            "promotion_ready": status == "PROBATION"
                and age >= timedelta(days=DEX_WALLET_PROBATION_MIN_DAYS)
                and row[5] >= DEX_WALLET_PROBATION_MIN_TRADES and row[6] >= 2,
        })
    return jsonify({
        "success": True, "version": VERSION, "counts": counts, "wallets": items,
        "policy": {
            "candidate_votes": 0, "probation_votes": 0,
            "minimum_probation_days": DEX_WALLET_PROBATION_MIN_DAYS,
            "minimum_forward_trades": DEX_WALLET_PROBATION_MIN_TRADES,
            "manual_promotion_required": True,
            "source_tokens_permanently_excluded": True,
        },
    })


@app.get("/dex-wallet-discovery-history")
def dex_wallet_discovery_history():
    initialise_database()
    try:
        limit = min(max(int(request.args.get("limit", 20)), 1), 100)
    except ValueError:
        limit = 20
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, started_at, completed_at, tokens_examined,
                    candidates_found, status, details
                FROM dex_wallet_discovery_runs ORDER BY id DESC LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    items = []
    for row in rows:
        try:
            details = json.loads(row[6] or "{}")
        except (TypeError, ValueError):
            details = {}
        items.append({
            "id": row[0], "started_at": row[1], "completed_at": row[2],
            "tokens_examined": row[3], "candidates_found": row[4],
            "status": row[5], "details": details,
        })
    return jsonify({"success": True, "version": VERSION, "runs": items})


@app.get("/probation-wallet-activity")
def probation_wallet_activity():
    initialise_database()
    try:
        limit = min(max(int(request.args.get("limit", 100)), 1), 500)
    except ValueError:
        limit = 100
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT signature, wallet, token_address, token_symbol, side,
                    token_amount, occurred_at, received_at
                FROM wallet_probation_activity
                ORDER BY occurred_at DESC LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    return jsonify({
        "success": True, "count": len(rows), "consensus_weight": 0,
        "events": [{
            "signature": row[0], "wallet": row[1], "token_address": row[2],
            "token_symbol": row[3], "side": row[4], "token_amount": row[5],
            "occurred_at": row[6], "received_at": row[7],
        } for row in rows],
    })

@app.get("/discover")
def discover():
    """
    V4.7 discovery inherited from the validated Premium funnel:
    - supports up to 50 trending tokens per run;
    - pages Birdeye trending results in bounded groups;
    - accepts ?offset=N so successive runs can rotate through the universe;
    - preserves cross-token observations in the existing evidence tables.
    """
    initialise_database()
    try:
        token_limit = int(request.args.get("tokens", 25))
    except ValueError:
        token_limit = 25
    token_limit = min(max(token_limit, 1), DISCOVERY_MAX_TOKENS)

    try:
        start_offset = int(request.args.get("offset", 0))
    except ValueError:
        start_offset = 0
    start_offset = max(start_offset, 0)

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO discovery_runs (status) VALUES ('running') RETURNING id")
            run_id = cur.fetchone()[0]
        conn.commit()

    # Premium access supports a broader run, while paging keeps responses bounded.
    tokens = []
    seen_token_addresses = set()
    trending_statuses = []
    api_429s = 0

    page_offset = start_offset
    while len(tokens) < token_limit:
        page_limit = min(DISCOVERY_PAGE_SIZE, token_limit - len(tokens))
        trending_response = birdeye_get("/defi/token_trending", {
            "sort_by": "rank",
            "sort_type": "asc",
            "offset": page_offset,
            "limit": page_limit,
            "interval": "24h",
        }, retry_429=False)

        trending_statuses.append({
            "offset": page_offset,
            "limit": page_limit,
            "status": trending_response.status_code,
        })

        if trending_response.status_code == 429:
            api_429s += 1
            break
        if trending_response.status_code != 200:
            break

        page_tokens = find_list(trending_response.json())
        added = 0
        for token in page_tokens:
            token_address = token.get("address") if isinstance(token, dict) else None
            if not is_valid_solana_address(token_address):
                continue
            if token_address in seen_token_addresses:
                continue
            seen_token_addresses.add(token_address)
            tokens.append(token)
            added += 1
            if len(tokens) >= token_limit:
                break

        # No more usable results on this page.
        if added == 0 or len(page_tokens) < page_limit:
            break

        page_offset += page_limit

    if not tokens:
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE discovery_runs SET completed_at = NOW(),
                        tokens_examined = 0, wallets_found = 0, api_429s = %s,
                        status = 'failed'
                    WHERE id = %s
                """, (api_429s, run_id))
            conn.commit()
        return jsonify({
            "success": False,
            "stage": "trending",
            "run_id": run_id,
            "api_429s": api_429s,
            "trending_pages": trending_statuses,
            "error": "No usable trending tokens returned.",
        }), 429 if api_429s else 502

    wallets_found = set()
    results = []
    tokens_examined = 0

    for token in tokens[:token_limit]:
        token_address = token.get("address")
        token_symbol = token.get("symbol")
        token_name = token.get("name")

        trader_response = birdeye_get("/defi/v2/tokens/top_traders", {
            "address": token_address,
            "time_frame": "30d",
            "sort_type": "desc",
            "sort_by": "realized_pnl",
            "offset": 0,
            "limit": DISCOVERY_WALLETS_PER_TOKEN,
        }, retry_429=False)
        tokens_examined += 1

        if trader_response.status_code == 429:
            api_429s += 1
            results.append({"token": token_symbol, "status": 429, "wallets": 0})
            break
        if trader_response.status_code != 200:
            results.append({
                "token": token_symbol,
                "status": trader_response.status_code,
                "wallets": 0,
            })
            continue

        traders = find_list(trader_response.json())
        valid_wallets = 0
        with db() as conn:
            with conn.cursor() as cur:
                for trader in traders:
                    wallet = wallet_address_from_trader(trader)
                    if not wallet:
                        continue

                    wallets_found.add(wallet)
                    valid_wallets += 1
                    token_realized = get_nested_number(trader, [
                        ("realized_pnl",), ("realizedPnl",),
                        ("realized_profit",), ("pnl", "realized_profit"),
                    ])

                    cur.execute("""
                        INSERT INTO wallet_token_hits (
                            wallet, token_address, token_symbol, token_name,
                            token_realized_pnl
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (wallet, token_address) DO UPDATE SET
                            token_symbol = EXCLUDED.token_symbol,
                            token_name = EXCLUDED.token_name,
                            token_realized_pnl = EXCLUDED.token_realized_pnl
                    """, (
                        wallet, token_address, token_symbol,
                        token_name, token_realized,
                    ))
                    cur.execute("""
                        INSERT INTO candidate_wallets (wallet, tokens_found)
                        VALUES (%s, 1)
                        ON CONFLICT (wallet) DO NOTHING
                    """, (wallet,))
                    cur.execute("""
                        UPDATE candidate_wallets SET
                            tokens_found = (
                                SELECT COUNT(*) FROM wallet_token_hits
                                WHERE wallet = %s
                            ),
                            updated_at = NOW()
                        WHERE wallet = %s
                    """, (wallet, wallet))
                    cur.execute("""
                        INSERT INTO discovery_observations (
                            run_id, wallet, token_address, token_symbol
                        ) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (run_id, wallet, token_address) DO NOTHING
                    """, (run_id, wallet, token_address, token_symbol))
            conn.commit()

        results.append({
            "token": token_symbol,
            "status": 200,
            "wallets": valid_wallets,
        })

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE discovery_runs SET completed_at = NOW(),
                    tokens_examined = %s, wallets_found = %s,
                    api_429s = %s, status = 'completed'
                WHERE id = %s
            """, (
                tokens_examined, len(wallets_found), api_429s, run_id,
            ))
        conn.commit()

    return jsonify({
        "success": True,
        "run_id": run_id,
        "requested_tokens": token_limit,
        "start_offset": start_offset,
        "tokens_examined": tokens_examined,
        "unique_wallets_found": len(wallets_found),
        "api_429s": api_429s,
        "trending_pages": trending_statuses,
        "results": results,
        "note": (
            "V4.6 Premium discovery. Rotate offset between runs to broaden token "
            "coverage. Candidates still require scoring, Helius screening and "
            "token-level validation."
        ),
    })


# =========================================================
# SCORE ONE WALLET / BATCH
# =========================================================

@app.get("/score-wallet/<wallet>")
def score_wallet(wallet):
    initialise_database()
    if not is_valid_solana_address(wallet):
        return jsonify({"success": False, "error": "Invalid Solana wallet address"}), 400

    response = birdeye_get("/wallet/v2/pnl/summary", {"wallet": wallet, "duration": "30d"})
    if response.status_code != 200:
        return jsonify({
            "success": False,
            "status_code": response.status_code,
            "birdeye_error": response.text[:1000],
        }), response.status_code

    try:
        parsed = parse_pnl_summary(response.json())
    except Exception:
        return jsonify({"success": False, "error": "Birdeye returned invalid JSON"}), 502

    metrics = parsed["metrics"]
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tokens_found FROM candidate_wallets WHERE wallet = %s", (wallet,))
            row = cur.fetchone()
            tokens_found = row[0] if row else 0

    metrics_available = any(value is not None for value in metrics.values())
    if metrics_available:
        score_result = calculate_score(
            metrics.get("realized_pnl"), metrics.get("total_pnl"),
            metrics.get("win_rate"), metrics.get("trades"),
            tokens_found, metrics.get("invested"),
        )
        score_status = "scored"
    else:
        score_result = {
            "score": 0,
            "flags": ["pnl_parse_incomplete"],
            "capital_efficiency": None,
        }
        score_status = "parse_incomplete"

    persist_score(wallet, metrics, score_result["score"], score_status)
    return jsonify({
        "success": True,
        "wallet": wallet,
        "tokens_found": tokens_found,
        "score": score_result["score"],
        "score_status": score_status,
        "risk_flags": score_result["flags"],
        "capital_efficiency": score_result["capital_efficiency"],
        "30d": {
            "realized_pnl_usd": metrics.get("realized_pnl"),
            "total_pnl_usd": metrics.get("total_pnl"),
            "win_rate": metrics.get("win_rate"),
            "trades": metrics.get("trades"),
            "total_invested_usd": metrics.get("invested"),
        },
        "parser_debug": parsed.get("debug", {}),
    })


@app.get("/score-batch")
def score_batch():
    """Score at most five candidates, throttling requests and stopping on 429."""
    initialise_database()
    try:
        limit = int(request.args.get("limit", 5))
    except ValueError:
        limit = 5
    limit = min(max(limit, 1), 5)

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wallet, tokens_found FROM candidate_wallets
                WHERE last_scored IS NULL OR score_status = 'parse_incomplete'
                ORDER BY EXISTS (
                    SELECT 1 FROM wallet_discovery_cohorts cohort
                    WHERE cohort.wallet = candidate_wallets.wallet
                        AND cohort.source = 'dexscreener'
                        AND cohort.cohort_status = 'CANDIDATE'
                ) DESC, tokens_found DESC, created_at ASC LIMIT %s
            """, (limit,))
            wallets = cur.fetchall()

    results = []
    stopped_on_429 = False
    for wallet, tokens_found in wallets:
        # Batch mode must stop immediately on a 429, so it deliberately disables retry.
        response = birdeye_get(
            "/wallet/v2/pnl/summary",
            {"wallet": wallet, "duration": "30d"},
            retry_429=False,
        )
        if response.status_code == 429:
            results.append({
                "wallet": wallet,
                "status": 429,
                "message": "Birdeye rate limit reached; batch stopped",
            })
            stopped_on_429 = True
            break
        if response.status_code != 200:
            results.append({"wallet": wallet, "status": response.status_code})
            continue

        try:
            parsed = parse_pnl_summary(response.json())
        except Exception:
            results.append({"wallet": wallet, "status": "parse_error"})
            continue

        metrics = parsed["metrics"]
        if any(value is not None for value in metrics.values()):
            scoring = calculate_score(
                metrics.get("realized_pnl"), metrics.get("total_pnl"),
                metrics.get("win_rate"), metrics.get("trades"),
                tokens_found, metrics.get("invested"),
            )
            score_status = "scored"
        else:
            scoring = {
                "score": 0,
                "flags": ["pnl_parse_incomplete"],
                "capital_efficiency": None,
            }
            score_status = "parse_incomplete"

        persist_score(wallet, metrics, scoring["score"], score_status)
        results.append({
            "wallet": wallet,
            "status": 200,
            "score": scoring["score"],
            "score_status": score_status,
            "tokens_found": tokens_found,
            "realized_pnl": metrics.get("realized_pnl"),
            "win_rate": metrics.get("win_rate"),
            "trades": metrics.get("trades"),
            "capital_efficiency": scoring["capital_efficiency"],
            "risk_flags": scoring["flags"],
        })

    return jsonify({
        "success": True,
        "requested": limit,
        "selected": len(wallets),
        "processed": len(results),
        "stopped_on_429": stopped_on_429,
        "results": results,
    })


# =========================================================
# V4.2 DEEP SCREENING
# =========================================================

def candidate_for_screening(wallet):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tokens_found, realized_pnl_30d, win_rate_30d, trades_30d,
                    score, score_status FROM candidate_wallets WHERE wallet = %s
            """, (wallet,))
            row = cur.fetchone()
    if not row:
        return None
    return {
        "tokens_found": row[0], "realized_pnl": row[1], "win_rate": row[2],
        "trades": row[3], "score": row[4], "score_status": row[5],
    }


@app.get("/screen-wallet/<wallet>")
def screen_wallet(wallet):
    initialise_database()
    if not is_valid_solana_address(wallet):
        return jsonify({"success": False, "error": "Invalid Solana wallet address"}), 400
    candidate = candidate_for_screening(wallet)
    if not candidate:
        return jsonify({"success": False, "error": "Wallet is not a candidate"}), 404
    if candidate["score_status"] != "scored":
        return jsonify({"success": False, "error": "Wallet must be scored first"}), 409

    response = helius_get_transactions(wallet)
    if response.status_code != 200:
        return jsonify({
            "success": False, "status_code": response.status_code,
            "helius_error": response.text[:1000],
        }), response.status_code
    try:
        transactions = response.json()
    except Exception:
        return jsonify({"success": False, "error": "Helius returned invalid JSON"}), 502
    if not isinstance(transactions, list):
        return jsonify({"success": False, "error": "Unexpected Helius response structure"}), 502

    screening = analyse_wallet_history(wallet, transactions, candidate)
    persist_screening(wallet, screening)
    return jsonify({
        "success": True, "wallet": wallet, "screening": compact_screening(screening),
        "disclaimer": "Heuristic risk signals only; they do not prove common ownership or insider activity.",
    })


@app.get("/screen-batch")
def screen_batch():
    """Deep-screen at most five scored candidates; stop immediately on Helius 429."""
    initialise_database()
    try:
        limit = int(request.args.get("limit", 5))
    except ValueError:
        limit = 5
    limit = min(max(limit, 1), 5)

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wallet, tokens_found, realized_pnl_30d, win_rate_30d,
                    trades_30d, score, score_status
                FROM candidate_wallets
                WHERE score_status = 'scored' AND score >= 30
                    AND (
                        last_screened IS NULL OR screening_status = 'error'
                        OR NOT EXISTS (
                            SELECT 1 FROM wallet_screenings ws
                            WHERE ws.wallet = candidate_wallets.wallet
                                AND ws.screening_version = %s
                        )
                    )
                ORDER BY EXISTS (
                    SELECT 1 FROM wallet_discovery_cohorts cohort
                    WHERE cohort.wallet = candidate_wallets.wallet
                        AND cohort.source = 'dexscreener'
                        AND cohort.cohort_status = 'CANDIDATE'
                ) DESC, score DESC, realized_pnl_30d DESC NULLS LAST
                LIMIT %s
            """, (SCREENING_VERSION, limit))
            rows = cur.fetchall()

    results = []
    stopped_on_429 = False
    for row in rows:
        wallet = row[0]
        candidate = {
            "tokens_found": row[1], "realized_pnl": row[2], "win_rate": row[3],
            "trades": row[4], "score": row[5], "score_status": row[6],
        }
        response = helius_get_transactions(wallet)
        if response.status_code == 429:
            results.append({"wallet": wallet, "status": 429, "message": "Helius rate limit reached; batch stopped"})
            stopped_on_429 = True
            break
        if response.status_code != 200:
            results.append({"wallet": wallet, "status": response.status_code})
            continue
        try:
            transactions = response.json()
        except Exception:
            results.append({"wallet": wallet, "status": "parse_error"})
            continue
        if not isinstance(transactions, list):
            results.append({"wallet": wallet, "status": "unexpected_response"})
            continue
        screening = analyse_wallet_history(wallet, transactions, candidate)
        persist_screening(wallet, screening)
        results.append({"wallet": wallet, "status": 200, **compact_screening(screening)})

    return jsonify({
        "success": True, "requested": limit, "selected": len(rows),
        "processed": len(results), "stopped_on_429": stopped_on_429,
        "results": results,
        "disclaimer": "Heuristic risk signals only; they do not prove common ownership or insider activity.",
    })


@app.get("/screenings")
def screenings():
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wallet, screening_status, risk_score, risk_flags,
                    transactions_sampled, unique_tokens_sampled, unique_counterparties,
                    largest_funder, largest_funder_share, failed_transaction_rate,
                    median_interval_seconds, screened_at, funding_transfer_count,
                    total_native_funding_lamports, transaction_types, screening_version
                FROM wallet_screenings ORDER BY risk_score ASC, screened_at DESC
            """)
            rows = cur.fetchall()
    return jsonify({"success": True, "count": len(rows), "screenings": [{
        "wallet": row[0], "screening_status": row[1], "risk_score": row[2],
        "risk_flags": json.loads(row[3]), "transactions_sampled": row[4],
        "unique_tokens_sampled": row[5], "unique_counterparties": row[6],
        "largest_funder": row[7], "largest_funder_share": row[8],
        "failed_transaction_rate": row[9], "median_interval_seconds": row[10],
        "screened_at": row[11], "funding_transfer_count": row[12],
        "total_native_funding_lamports": row[13],
        "transaction_types": json.loads(row[14]), "screening_version": row[15],
    } for row in rows]})


@app.get("/screening-relationships")
def screening_relationships():
    """Compare bounded samples after removing ubiquitous infrastructure addresses."""
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wallet, details FROM wallet_screenings
                WHERE screening_version = %s ORDER BY wallet
            """, (SCREENING_VERSION,))
            rows = cur.fetchall()

    samples = {}
    for wallet, details_text in rows:
        try:
            details = json.loads(details_text)
        except (TypeError, ValueError):
            details = {}
        samples[wallet] = {
            "counterparties": set(details.get("sampled_counterparties") or []),
            "funders": set(details.get("sampled_funders") or []),
        }

    relationships = []
    wallets = sorted(samples)
    candidate_wallets = set(wallets)
    address_frequency = {}
    for sample in samples.values():
        for address in sample["counterparties"]:
            address_frequency[address] = address_frequency.get(address, 0) + 1

    # In a five-wallet comparison, appearance in three or more samples is more
    # consistent with a router, protocol, exchange, or other shared infrastructure.
    infrastructure_addresses = {
        address for address, count in address_frequency.items()
        if count >= 3
    }
    for sample in samples.values():
        sample["counterparties"] -= infrastructure_addresses
        sample["counterparties"] -= candidate_wallets
        sample["funders"] -= infrastructure_addresses
        sample["funders"] -= candidate_wallets

    for left_index, left in enumerate(wallets):
        for right in wallets[left_index + 1:]:
            shared_funders = sorted(samples[left]["funders"] & samples[right]["funders"])
            left_counterparties = samples[left]["counterparties"]
            right_counterparties = samples[right]["counterparties"]
            shared_counterparties = sorted(left_counterparties & right_counterparties)
            union = left_counterparties | right_counterparties
            overlap_ratio = len(shared_counterparties) / len(union) if union else 0

            if shared_funders:
                strength = "high"
            elif len(shared_counterparties) >= 3 and overlap_ratio >= 0.25:
                strength = "high"
            elif len(shared_counterparties) >= 2 and overlap_ratio >= 0.10:
                strength = "moderate"
            else:
                strength = "low"

            if shared_funders or shared_counterparties:
                relationships.append({
                    "wallet_a": left, "wallet_b": right,
                    "relationship_strength": strength,
                    "counterparty_overlap_ratio": round(overlap_ratio, 4),
                    "shared_funders": shared_funders[:10],
                    "shared_funder_count": len(shared_funders),
                    "shared_counterparties": shared_counterparties[:10],
                    "shared_counterparty_count": len(shared_counterparties),
                    "addresses_truncated": (
                        len(shared_counterparties) > 10 or len(shared_funders) > 10
                    ),
                })

    relationships.sort(
        key=lambda item: (
            {"high": 3, "moderate": 2, "low": 1}[item["relationship_strength"]],
            item["shared_funder_count"], item["counterparty_overlap_ratio"],
        ),
        reverse=True,
    )
    return jsonify({
        "success": True, "screening_version": SCREENING_VERSION,
        "wallets_compared": len(wallets),
        "infrastructure_addresses_excluded": len(infrastructure_addresses),
        "relationships": relationships,
        "disclaimer": "Shared sampled addresses are leads only and do not prove common ownership.",
    })


# =========================================================
# V4.3 REPEAT EVIDENCE / TOKEN VALIDATION
# =========================================================

@app.get("/discovery-evidence")
def discovery_evidence():
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.wallet, c.tokens_found, o.run_id, o.token_address,
                    COALESCE(o.token_symbol, h.token_symbol), o.observed_at, c.score
                FROM candidate_wallets c
                LEFT JOIN discovery_observations o ON o.wallet = c.wallet
                LEFT JOIN wallet_token_hits h ON h.wallet = o.wallet
                    AND h.token_address = o.token_address
                ORDER BY c.score DESC, o.observed_at ASC
            """)
            rows = cur.fetchall()

    grouped = {}
    for wallet, tokens_found, run_id, token_address, token_symbol, observed_at, score in rows:
        record = grouped.setdefault(wallet, {
            "wallet": wallet, "tokens_found": tokens_found, "score": score,
            "observations": [],
        })
        if run_id is not None:
            record["observations"].append({
                "run_id": run_id, "token_address": token_address,
                "token_symbol": token_symbol, "observed_at": observed_at,
            })

    results = []
    for record in grouped.values():
        observations = record.pop("observations")
        runs = {item["run_id"] for item in observations}
        tokens = {item["token_address"] for item in observations}
        times = sorted(item["observed_at"] for item in observations if item["observed_at"])
        span_seconds = (times[-1] - times[0]).total_seconds() if len(times) >= 2 else 0
        token_labels = sorted({
            item["token_symbol"] or item["token_address"] for item in observations
        })
        record.update({
            "discovery_runs_observed": len(runs),
            "distinct_tokens_observed": len(tokens),
            "observed_tokens": token_labels,
            "first_observed": times[0] if times else None,
            "last_observed": times[-1] if times else None,
            "observation_span_seconds": span_seconds,
            "same_token_repeat": len(runs) >= 2 and len(tokens) == 1,
            "independent_repeat_evidence": (
                len(runs) >= 2 and span_seconds >= INDEPENDENT_REPEAT_SECONDS
            ),
            "cross_token_evidence": len(tokens) >= 2,
        })
        results.append(record)
    results.sort(key=lambda item: (
        item["cross_token_evidence"],
        item["distinct_tokens_observed"],
        item["independent_repeat_evidence"],
        min(item["discovery_runs_observed"], 3),
        item["score"],
    ), reverse=True)
    return jsonify({
        "success": True,
        "independent_repeat_minimum_hours": INDEPENDENT_REPEAT_SECONDS / 3600,
        "wallets": results,
    })


def validate_wallet_tokens(wallet, token_addresses=None):
    body = {
        "wallet": wallet, "duration": "30d", "position_scope": "duration_only",
        "sort_type": "desc", "sort_by": "last_trade", "limit": 20, "offset": 0,
    }
    if token_addresses:
        body["token_addresses"] = token_addresses[:20]
    return birdeye_post("/wallet/v2/pnl/details", body)


@app.get("/validate-wallet/<wallet>")
def validate_wallet(wallet):
    initialise_database()
    if not is_valid_solana_address(wallet):
        return jsonify({"success": False, "error": "Invalid Solana wallet address"}), 400
    candidate = candidate_for_screening(wallet)
    if not candidate:
        return jsonify({"success": False, "error": "Wallet is not a candidate"}), 404

    response = validate_wallet_tokens(wallet)
    if response.status_code != 200:
        return jsonify({
            "success": False, "status_code": response.status_code,
            "birdeye_error": response.text[:1000],
        }), response.status_code
    try:
        rows = parse_token_pnl_details(response.json())
    except Exception:
        return jsonify({"success": False, "error": "Birdeye returned invalid JSON"}), 502
    persist_token_validation(wallet, rows)
    return jsonify({
        "success": True, "wallet": wallet,
        "summary": summarize_token_validation(rows), "tokens": rows,
        "note": "Per-token results are a bounded 30-day sample and may omit unsupported protocol history.",
    })


@app.get("/validate-batch")
def validate_batch():
    """Validate at most two leading wallets per invocation."""
    initialise_database()
    try:
        limit = int(request.args.get("limit", 2))
    except ValueError:
        limit = 2
    limit = min(max(limit, 1), 2)

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.wallet FROM candidate_wallets c
                LEFT JOIN wallet_screenings ws ON ws.wallet = c.wallet
                    AND ws.screening_version = %s
                WHERE c.score_status = 'scored' AND c.score >= 30
                    AND c.validation_status IN ('unvalidated', 'parse_incomplete')
                    AND COALESCE(ws.risk_score, 100) <= 45
                    AND COALESCE(ws.risk_flags, '[]') NOT LIKE '%%service_like_activity%%'
                ORDER BY EXISTS (
                    SELECT 1 FROM wallet_discovery_cohorts cohort
                    WHERE cohort.wallet = c.wallet
                        AND cohort.source = 'dexscreener'
                        AND cohort.cohort_status = 'CANDIDATE'
                ) DESC, COALESCE(ws.risk_score, 100) ASC,
                    c.score DESC, c.realized_pnl_30d DESC NULLS LAST
                LIMIT %s
            """, (SCREENING_VERSION, limit))
            wallets = [row[0] for row in cur.fetchall()]

    results = []
    stopped_on_429 = False
    for wallet in wallets:
        response = validate_wallet_tokens(wallet)
        if response.status_code == 429:
            results.append({"wallet": wallet, "status": 429, "message": "Birdeye rate limit reached; batch stopped"})
            stopped_on_429 = True
            break
        if response.status_code != 200:
            results.append({"wallet": wallet, "status": response.status_code})
            continue
        try:
            rows = parse_token_pnl_details(response.json())
        except Exception:
            results.append({"wallet": wallet, "status": "parse_error"})
            continue
        persist_token_validation(wallet, rows)
        results.append({
            "wallet": wallet, "status": 200,
            "summary": summarize_token_validation(rows),
            "tokens_preview": rows[:5],
            "tokens_preview_truncated": len(rows) > 5,
        })

    return jsonify({
        "success": True, "requested": limit, "selected": len(wallets),
        "processed": len(results), "stopped_on_429": stopped_on_429,
        "results": results,
    })


@app.get("/validations")
def validations():
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wallet, token_address, token_symbol, realized_pnl,
                    total_pnl, invested, trades, win_rate, validated_at
                FROM wallet_token_validations
                ORDER BY wallet, realized_pnl DESC NULLS LAST
            """)
            rows = cur.fetchall()
    grouped = {}
    for row in rows:
        grouped.setdefault(row[0], []).append({
            "token_address": row[1], "token_symbol": row[2],
            "realized_pnl": row[3], "total_pnl": row[4], "invested": row[5],
            "trades": row[6], "win_rate": row[7], "validated_at": row[8],
        })
    return jsonify({"success": True, "wallets": [{
        "wallet": wallet, "summary": summarize_token_validation(token_rows),
        "tokens": token_rows,
    } for wallet, token_rows in grouped.items()]})


# =========================================================
# V4.4 AUTOMATIC CLASSIFICATION / SHORTLIST
# =========================================================

def load_validation_summaries():
    """Build current per-wallet validation summaries from persisted token rows."""
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wallet, token_address, token_symbol, realized_pnl,
                    total_pnl, invested, trades, win_rate, validated_at
                FROM wallet_token_validations
                ORDER BY wallet, realized_pnl DESC NULLS LAST
            """)
            rows = cur.fetchall()

    grouped = {}
    for row in rows:
        grouped.setdefault(row[0], []).append({
            "token_address": row[1],
            "token_symbol": row[2],
            "realized_pnl": row[3],
            "total_pnl": row[4],
            "invested": row[5],
            "trades": row[6],
            "win_rate": row[7],
            "validated_at": row[8],
        })
    return {
        wallet: summarize_token_validation(token_rows)
        for wallet, token_rows in grouped.items()
    }


def load_repeat_evidence():
    """Summarize independent and cross-token discovery evidence per wallet."""
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wallet, run_id, token_address, observed_at
                FROM discovery_observations
                ORDER BY wallet, observed_at ASC
            """)
            rows = cur.fetchall()

    grouped = {}
    for wallet, run_id, token_address, observed_at in rows:
        grouped.setdefault(wallet, []).append({
            "run_id": run_id,
            "token_address": token_address,
            "observed_at": observed_at,
        })

    evidence = {}
    for wallet, observations in grouped.items():
        runs = {item["run_id"] for item in observations}
        tokens = {item["token_address"] for item in observations}
        times = sorted(
            item["observed_at"] for item in observations if item["observed_at"]
        )
        span_seconds = (
            (times[-1] - times[0]).total_seconds() if len(times) >= 2 else 0
        )
        evidence[wallet] = {
            "discovery_runs_observed": len(runs),
            "distinct_tokens_observed": len(tokens),
            "observation_span_seconds": span_seconds,
            "independent_repeat_evidence": (
                len(runs) >= 2 and span_seconds >= INDEPENDENT_REPEAT_SECONDS
            ),
            "cross_token_evidence": len(tokens) >= 2,
        }
    return evidence


def classify_candidate(candidate, screening=None, validation=None, evidence=None):
    """
    Combine performance, Helius risk, token-level validation, and discovery
    evidence into a conservative research classification.

    WATCH means eligible to contribute to a future consensus signal.
    It is not a recommendation to buy or copy the wallet.
    """
    screening = screening or {}
    validation = validation or {}
    evidence = evidence or {}

    reasons = []
    blockers = []
    confidence_points = 0

    score = candidate.get("score") or 0
    realized = candidate.get("realized_pnl")
    total_pnl = candidate.get("total_pnl")
    trades = candidate.get("trades")
    invested = candidate.get("invested")
    score_status = candidate.get("score_status")
    validation_status = candidate.get("validation_status")
    risk_score = screening.get("risk_score")
    risk_flags = set(screening.get("risk_flags") or [])

    efficiency = None
    if realized is not None and invested is not None and invested > 0:
        efficiency = realized / invested

    # Hard rejection signals.
    if score_status != "scored":
        blockers.append("wallet_not_scored")
    if total_pnl is not None and total_pnl < 0:
        blockers.append("negative_total_pnl")
    if realized is not None and realized <= 0:
        blockers.append("non_positive_realized_pnl")
    if trades is not None and trades > 5000:
        blockers.append("extreme_hft_activity")
    if "service_like_activity" in risk_flags:
        blockers.append("service_like_activity")
    if "bursty_automated_activity" in risk_flags:
        blockers.append("bursty_automated_activity")
    if risk_score is not None and risk_score >= 60:
        blockers.append("high_screening_risk")

    if blockers:
        return {
            "classification": "REJECT",
            "confidence": "HIGH",
            "reasons": sorted(set(blockers)),
            "watch_eligible": False,
            "signal_eligible": False,
            "capital_efficiency": efficiency,
        }

    # Screening evidence.
    if risk_score is None:
        reasons.append("screening_required")
    elif risk_score <= 25:
        confidence_points += 2
        reasons.append("low_screening_risk")
    elif risk_score <= 45:
        confidence_points += 1
        reasons.append("moderate_screening_risk")
    else:
        reasons.append("elevated_screening_risk")

    # Token-level validation evidence.
    material_tokens = validation.get("material_tokens")
    material_rate = validation.get("material_token_profit_rate")
    profit_factor = validation.get("material_profit_factor")
    validation_strength = validation.get("validation_strength")

    if validation_status != "validated" or not validation:
        reasons.append("token_validation_required")
    else:
        if validation_strength == "strong":
            confidence_points += 2
            reasons.append("strong_token_validation")
        elif validation_strength == "moderate":
            confidence_points += 1
            reasons.append("moderate_token_validation")
        else:
            reasons.append("limited_token_validation")

        if material_tokens is not None and material_tokens >= 5:
            confidence_points += 1
            reasons.append("adequate_material_sample")
        else:
            reasons.append("small_material_sample")

        if material_rate is not None:
            if material_rate >= 0.60:
                confidence_points += 2
                reasons.append("high_material_token_hit_rate")
            elif material_rate >= 0.40:
                confidence_points += 1
                reasons.append("moderate_material_token_hit_rate")
            else:
                reasons.append("low_material_token_hit_rate")

        if profit_factor is not None:
            if profit_factor >= 2.0:
                confidence_points += 1
                reasons.append("healthy_material_profit_factor")
            elif profit_factor < 1.0:
                reasons.append("weak_material_profit_factor")
        elif validation.get("material_profit_factor_status") == "no_material_losses":
            # Positive, but do not over-reward a tiny sample with no observed losses.
            reasons.append("no_material_losses_observed")

    # Performance sanity checks.
    if score >= 50:
        confidence_points += 1
        reasons.append("strong_performance_score")
    elif score < 30:
        reasons.append("weak_performance_score")

    if efficiency is not None:
        if efficiency >= 0.05:
            confidence_points += 1
            reasons.append("positive_capital_efficiency")
        elif efficiency < 0.01:
            reasons.append("low_capital_efficiency")

    if trades is not None and 30 <= trades <= 1000:
        confidence_points += 1
        reasons.append("meaningful_trade_history")
    elif trades is not None and trades < 10:
        reasons.append("insufficient_trade_history")

    # Discovery evidence is a confidence enhancer, not a hard prerequisite yet.
    if evidence.get("cross_token_evidence"):
        confidence_points += 2
        reasons.append("cross_token_discovery_evidence")
    elif evidence.get("independent_repeat_evidence"):
        confidence_points += 1
        reasons.append("independent_repeat_discovery_evidence")
    else:
        reasons.append("single_discovery_evidence")

    # WATCH deliberately requires all core evidence, not just a high point total.
    watch_core = (
        risk_score is not None
        and risk_score <= 25
        and validation_status == "validated"
        and validation_strength == "strong"
        and material_tokens is not None
        and material_tokens >= 5
        and material_rate is not None
        and material_rate >= 0.60
        and (
            profit_factor is None
            or profit_factor >= 2.0
        )
        and trades is not None
        and trades >= 30
        and realized is not None
        and realized > 0
    )

    # ASYMMETRIC preserves low-risk wallets whose hit rate is below WATCH
    # standards but whose material winners substantially outweigh losers.
    asymmetric_core = (
        not watch_core
        and risk_score is not None
        and risk_score <= 25
        and validation_status == "validated"
        and validation_strength == "strong"
        and material_tokens is not None
        and material_tokens >= 5
        and material_rate is not None
        and 0.30 <= material_rate < 0.60
        and profit_factor is not None
        and profit_factor >= 5.0
        and trades is not None
        and trades >= 30
        and realized is not None
        and realized > 0
    )

    if watch_core:
        classification = "WATCH"
        confidence = (
            "HIGH"
            if evidence.get("cross_token_evidence")
            or evidence.get("independent_repeat_evidence")
            else "MEDIUM"
        )
    elif asymmetric_core:
        classification = "ASYMMETRIC"
        confidence = (
            "HIGH"
            if evidence.get("cross_token_evidence")
            or evidence.get("independent_repeat_evidence")
            else "MEDIUM"
        )
        reasons.append("asymmetric_payoff_profile")
    else:
        classification = "REVIEW"
        confidence = (
            "HIGH" if confidence_points >= 8
            else "MEDIUM" if confidence_points >= 5
            else "LOW"
        )

    return {
        "classification": classification,
        "confidence": confidence,
        "reasons": reasons,
        "watch_eligible": classification == "WATCH",
        "signal_eligible": classification in {"WATCH", "ASYMMETRIC"},
        "capital_efficiency": efficiency,
    }


def load_signal_eligible_wallets():
    """Return current WATCH/ASYMMETRIC wallets with conservative signal weights."""
    validation_summaries = load_validation_summaries()
    repeat_evidence = load_repeat_evidence()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.wallet, c.tokens_found, c.realized_pnl_30d,
                    c.total_pnl_30d, c.win_rate_30d, c.trades_30d,
                    c.total_invested_30d, c.score, c.score_status,
                    c.screening_status, c.screening_risk_score,
                    c.validation_status, COALESCE(ws.risk_flags, '[]')
                FROM candidate_wallets c
                LEFT JOIN wallet_screenings ws ON ws.wallet = c.wallet
                    AND ws.screening_version = %s
                WHERE c.score_status = 'scored'
                    AND NOT EXISTS (
                        SELECT 1 FROM wallet_discovery_cohorts dc
                        WHERE dc.wallet = c.wallet
                            AND dc.source = 'dexscreener'
                            AND dc.cohort_status <> 'VALIDATED'
                    )
            """, (SCREENING_VERSION,))
            rows = cur.fetchall()

    eligible = {}
    for row in rows:
        wallet = row[0]
        try:
            risk_flags = json.loads(row[12]) if row[12] else []
        except (TypeError, ValueError):
            risk_flags = []
        candidate = {
            "wallet": wallet, "tokens_found": row[1], "realized_pnl": row[2],
            "total_pnl": row[3], "win_rate": row[4], "trades": row[5],
            "invested": row[6], "score": row[7], "score_status": row[8],
            "screening_status": row[9], "validation_status": row[11],
        }
        classification = classify_candidate(
            candidate,
            {"risk_score": row[10], "risk_flags": risk_flags},
            validation_summaries.get(wallet), repeat_evidence.get(wallet, {}),
        )
        label = classification["classification"]
        if label not in {"WATCH", "ASYMMETRIC"}:
            continue
        base_weight = WATCH_WEIGHT if label == "WATCH" else ASYMMETRIC_WEIGHT
        confidence = classification["confidence"]
        eligible[wallet] = {
            "wallet": wallet, "classification": label, "confidence": confidence,
            "signal_weight": round(
                base_weight * CONFIDENCE_MULTIPLIERS.get(confidence, 0.5), 4
            ),
            "score": row[7], "screening_risk_score": row[10],
            "discovery_evidence": repeat_evidence.get(wallet, {}),
        }
    return eligible


def compute_and_persist_wallet_clusters():
    """Cluster eligible wallets using material shared-funder/counterparty evidence."""
    eligible = load_signal_eligible_wallets()
    wallets = sorted(eligible)
    parent = {wallet: wallet for wallet in wallets}

    def find(wallet):
        while parent[wallet] != wallet:
            parent[wallet] = parent[parent[wallet]]
            wallet = parent[wallet]
        return wallet

    def union(left, right):
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wallet, details FROM wallet_screenings
                WHERE screening_version = %s
            """, (SCREENING_VERSION,))
            rows = cur.fetchall()

    all_samples = {}
    for wallet, details_text in rows:
        try:
            details = json.loads(details_text or "{}")
        except (TypeError, ValueError):
            details = {}
        all_samples[wallet] = {
            "counterparties": set(details.get("sampled_counterparties") or []),
            "funders": set(details.get("sampled_funders") or []),
        }

    address_frequency = {}
    for sample in all_samples.values():
        for address in sample["counterparties"]:
            address_frequency[address] = address_frequency.get(address, 0) + 1
    infrastructure = {address for address, count in address_frequency.items() if count >= 3}
    samples = {
        wallet: all_samples.get(wallet, {"counterparties": set(), "funders": set()})
        for wallet in wallets
    }
    for sample in samples.values():
        sample["counterparties"] -= infrastructure
        sample["counterparties"] -= set(wallets)
        sample["funders"] -= infrastructure
        sample["funders"] -= set(wallets)

    material_edges = []
    for index, left in enumerate(wallets):
        left_sample = samples.get(left, {"counterparties": set(), "funders": set()})
        for right in wallets[index + 1:]:
            right_sample = samples.get(right, {"counterparties": set(), "funders": set()})
            shared_funders = left_sample["funders"] & right_sample["funders"]
            shared_counterparties = left_sample["counterparties"] & right_sample["counterparties"]
            union_addresses = left_sample["counterparties"] | right_sample["counterparties"]
            overlap = len(shared_counterparties) / len(union_addresses) if union_addresses else 0
            strength = (
                "high" if shared_funders
                else "high" if len(shared_counterparties) >= 3 and overlap >= 0.25
                else "moderate" if len(shared_counterparties) >= 2 and overlap >= 0.10
                else "low"
            )
            if strength in HARD_RELATIONSHIP_STRENGTHS:
                union(left, right)
                material_edges.append({
                    "wallet_a": left, "wallet_b": right, "strength": strength,
                    "shared_funder_count": len(shared_funders),
                    "shared_counterparty_count": len(shared_counterparties),
                    "overlap_ratio": round(overlap, 4),
                })

    clusters = {}
    for wallet in wallets:
        clusters.setdefault(find(wallet), []).append(wallet)
    edge_map = {wallet: [] for wallet in wallets}
    for edge in material_edges:
        edge_map[edge["wallet_a"]].append(edge)
        edge_map[edge["wallet_b"]].append(edge)

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM wallet_clusters")
            for wallet in wallets:
                record = eligible[wallet]
                cluster_id = find(wallet)
                cur.execute("""
                    INSERT INTO wallet_clusters (
                        wallet, cluster_id, classification, confidence,
                        signal_weight, relationship_basis, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """, (
                    wallet, cluster_id, record["classification"],
                    record["confidence"], record["signal_weight"],
                    json.dumps(edge_map[wallet] or [{"strength": "independent"}]),
                ))
        conn.commit()
    return eligible, clusters, material_edges, len(infrastructure)


def load_tracked_wallet_map():
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wallet, cluster_id, classification, confidence, signal_weight
                FROM wallet_clusters
            """)
            rows = cur.fetchall()
    tracked = {
        row[0]: {
            "cluster_id": row[1], "classification": row[2],
            "confidence": row[3], "signal_weight": row[4],
        }
        for row in rows
    }
    if not tracked:
        return tracked
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.wallet, c.eligible_signal_at, s.token_address
                FROM wallet_discovery_cohorts c
                LEFT JOIN wallet_discovery_sources s ON s.wallet = c.wallet
                WHERE c.cohort_status = 'VALIDATED'
            """)
            restrictions = cur.fetchall()
    for wallet, eligible_at, token_address in restrictions:
        if wallet not in tracked:
            continue
        tracked[wallet]["eligible_from"] = eligible_at
        tracked[wallet].setdefault("excluded_tokens", set())
        if token_address:
            tracked[wallet]["excluded_tokens"].add(token_address)
    return tracked


def load_probation_wallet_map():
    """Return monitored probation wallets without granting a consensus vote."""
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.wallet, c.eligible_signal_at, s.token_address
                FROM wallet_discovery_cohorts c
                LEFT JOIN wallet_discovery_sources s ON s.wallet = c.wallet
                WHERE c.cohort_status = 'PROBATION'
            """)
            rows = cur.fetchall()
    probation = {}
    for wallet, eligible_at, token_address in rows:
        record = probation.setdefault(wallet, {
            "cluster_id": f"probation:{wallet}",
            "classification": "PROBATION",
            "confidence": "UNPROVEN",
            "signal_weight": 0.0,
            "eligible_from": eligible_at,
            "excluded_tokens": set(),
        })
        if token_address:
            record["excluded_tokens"].add(token_address)
    return probation


def load_helius_monitored_wallets():
    return sorted(set(load_tracked_wallet_map()) | set(load_probation_wallet_map()))


def helius_webhook_headers():
    return {
        "Authorization": f"Bearer {os.getenv('HELIUS_API_KEY', '')}",
        "Content-Type": "application/json",
    }


def helius_webhook_id(webhook):
    return webhook.get("webhookID") or webhook.get("webhookId") or webhook.get("id")


def normalise_helius_webhook_list(payload):
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "webhooks", "result"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def record_helius_sync(result):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO helius_sync_history (
                    webhook_id, dry_run, sync_status, desired_wallet_count,
                    current_wallet_count, addresses_added, addresses_removed,
                    response_code, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                result.get("webhook_id"), bool(result.get("dry_run")),
                result.get("sync_status", "unknown"),
                result.get("desired_wallet_count", 0),
                result.get("current_wallet_count", 0),
                json.dumps(result.get("addresses_added") or []),
                json.dumps(result.get("addresses_removed") or []),
                result.get("response_code"), result.get("error_message"),
            ))
            sync_id = cur.fetchone()[0]
        conn.commit()
    result["sync_id"] = sync_id
    return result


def synchronise_helius_webhook(*, dry_run=True, force=False):
    """Sync validated plus probation wallets; only validated wallets can vote."""
    desired = load_helius_monitored_wallets()
    base_result = {
        "success": False, "dry_run": bool(dry_run), "webhook_id": None,
        "sync_status": "configuration_required",
        "desired_wallet_count": len(desired), "current_wallet_count": 0,
        "addresses_added": [], "addresses_removed": [],
        "response_code": None, "error_message": None,
    }
    if not os.getenv("HELIUS_API_KEY"):
        base_result["error_message"] = "HELIUS_API_KEY is not configured"
        return record_helius_sync(base_result)
    if not os.getenv("HELIUS_WEBHOOK_SECRET"):
        base_result["error_message"] = "HELIUS_WEBHOOK_SECRET is not configured"
        return record_helius_sync(base_result)
    if len(desired) < HELIUS_MIN_SYNC_WALLETS:
        base_result["sync_status"] = "blocked_minimum_wallet_guard"
        base_result["error_message"] = "Desired wallet count is below the configured safety minimum"
        return record_helius_sync(base_result)

    try:
        response = upstream_request(
            "GET", HELIUS_WEBHOOKS_URL, headers=helius_webhook_headers(),
            timeout=20, retries=1, provider="helius",
        )
    except (requests.Timeout, requests.ConnectionError) as exc:
        diagnostic_increment("helius_sync_failures")
        base_result["sync_status"] = "helius_unavailable"
        base_result["error_message"] = type(exc).__name__
        return record_helius_sync(base_result)

    base_result["response_code"] = response.status_code
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if response.status_code != 200:
        diagnostic_increment("helius_sync_failures")
        base_result["sync_status"] = "helius_get_failed"
        base_result["error_message"] = "Helius rejected the webhook-list request"
        return record_helius_sync(base_result)

    configured_id = os.getenv("HELIUS_WEBHOOK_ID", "").strip()
    webhooks = normalise_helius_webhook_list(payload)
    selected = None
    for webhook in webhooks:
        candidate_id = str(helius_webhook_id(webhook) or "")
        candidate_url = str(webhook.get("webhookURL") or "").rstrip("/")
        if configured_id and candidate_id == configured_id:
            selected = webhook
            break
        if not configured_id and candidate_url == HELIUS_TARGET_WEBHOOK_URL.rstrip("/"):
            selected = webhook
            break
    if not selected:
        base_result["sync_status"] = "webhook_not_found"
        base_result["error_message"] = "No Helius webhook matched the configured ID or target URL"
        return record_helius_sync(base_result)

    webhook_id = str(helius_webhook_id(selected) or "")
    current = sorted(set(selected.get("accountAddresses") or []))
    desired_set, current_set = set(desired), set(current)
    added = sorted(desired_set - current_set)
    removed = sorted(current_set - desired_set)
    base_result.update({
        "webhook_id": webhook_id,
        "current_wallet_count": len(current),
        "addresses_added": added,
        "addresses_removed": removed,
    })
    if not webhook_id:
        base_result["sync_status"] = "invalid_helius_response"
        base_result["error_message"] = "Matched webhook did not contain an identifier"
        return record_helius_sync(base_result)
    if not added and not removed:
        base_result.update({"success": True, "sync_status": "already_synchronised"})
        return record_helius_sync(base_result)

    removal_fraction = len(removed) / max(len(current), 1)
    base_result["removal_fraction"] = round(removal_fraction, 4)
    if removal_fraction > HELIUS_MAX_REMOVAL_FRACTION and not force:
        base_result["sync_status"] = "blocked_removal_guard"
        base_result["error_message"] = "Proposed removal exceeds the configured safety limit"
        return record_helius_sync(base_result)
    if dry_run:
        base_result.update({"success": True, "sync_status": "dry_run_changes_detected"})
        return record_helius_sync(base_result)

    update_body = {
        "webhookURL": HELIUS_TARGET_WEBHOOK_URL,
        "transactionTypes": selected.get("transactionTypes") or ["SWAP"],
        "accountAddresses": desired,
        "webhookType": selected.get("webhookType") or "enhanced",
        "authHeader": f"Bearer {os.getenv('HELIUS_WEBHOOK_SECRET', '')}",
    }
    for optional_key in ("encoding", "txnStatus"):
        if selected.get(optional_key):
            update_body[optional_key] = selected[optional_key]
    try:
        update_response = upstream_request(
            "PUT", f"{HELIUS_WEBHOOKS_URL}/{webhook_id}",
            headers=helius_webhook_headers(), json_body=update_body,
            timeout=25, retries=1, provider="helius",
        )
    except (requests.Timeout, requests.ConnectionError) as exc:
        diagnostic_increment("helius_sync_failures")
        base_result["sync_status"] = "helius_update_unavailable"
        base_result["error_message"] = type(exc).__name__
        return record_helius_sync(base_result)

    base_result["response_code"] = update_response.status_code
    if update_response.status_code not in {200, 201}:
        diagnostic_increment("helius_sync_failures")
        base_result["sync_status"] = "helius_update_failed"
        base_result["error_message"] = "Helius rejected the webhook update"
        return record_helius_sync(base_result)
    diagnostic_increment("helius_syncs")
    base_result.update({"success": True, "sync_status": "synchronised"})
    return record_helius_sync(base_result)


def webhook_authorized():
    configured = os.getenv("HELIUS_WEBHOOK_SECRET", "")
    supplied = (
        request.headers.get("Authorization", "")
        or request.headers.get("X-Webhook-Secret", "")
    )
    if not configured or not supplied:
        return False
    return (
        hmac.compare_digest(configured, supplied)
        or hmac.compare_digest(f"Bearer {configured}", supplied)
    )


def admin_authorized():
    configured = os.getenv("ADMIN_API_KEY", "")
    supplied = request.headers.get("X-Admin-Key", "")
    return bool(configured) and bool(supplied) and hmac.compare_digest(configured, supplied)


def telegram_configured():
    return bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID"))


def solana_status_label(status, buy_clusters=0, sell_clusters=0):
    """Human-readable labels; internal codes remain stable for compatibility."""
    status = str(status or "UNKNOWN").upper()
    if status == "EXPIRED":
        return f"NO ACTIVITY FOR {SIGNAL_WINDOW_MINUTES} MINUTES"
    buy_clusters = max(int(buy_clusters or 0), 0)
    sell_clusters = max(int(sell_clusters or 0), 0)
    if buy_clusters == 0 and sell_clusters > 0:
        return "INVALIDATED · SELL-ONLY" if status == "INVALIDATED" else "SELL-ONLY"
    if buy_clusters == 0:
        return "NO BUYERS"
    buyer_label = (
        "3+ BUYERS" if buy_clusters >= 3 else
        "2 BUYERS" if buy_clusters == 2 else
        "1 BUYER"
    )
    if sell_clusters > 0:
        seller_label = f"{sell_clusters} SELLER" + ("S" if sell_clusters != 1 else "")
        if status == "INVALIDATED":
            return f"INVALIDATED · MIXED: {buyer_label} / {seller_label}"
        return f"{buyer_label} · MIXED WITH {seller_label}"
    if status == "INVALIDATED":
        return "INVALIDATED"
    if status in {"OBSERVE", "BUILDING", "PAPER_CONFIRMED"}:
        return buyer_label
    return status.replace("_", " ")


def _birdeye_payload(response):
    if response.status_code != 200:
        return {}
    try:
        payload = response.json()
    except ValueError:
        return {}
    data = payload.get("data") if isinstance(payload, dict) else None
    return data if isinstance(data, dict) else {}


def _solana_safety_status(security):
    """Conservative provider screening; it never upgrades a token to 'safe'."""
    if not security:
        return "unverified", {"reason": "security_data_unavailable"}
    flags = []
    truthy_risk_fields = {
        "mintable": "mint_authority_active",
        "freezeable": "freeze_authority_active",
        "mutableMetadata": "mutable_metadata",
        "isToken2022": "token_2022_review_required",
    }
    for field, label in truthy_risk_fields.items():
        if security.get(field) is True:
            flags.append(label)
    top10 = safe_float(
        security.get("top10HolderPercent")
        or security.get("top10HolderPercentage")
    )
    owner = safe_float(security.get("ownerPercentage"))
    creator = safe_float(security.get("creatorPercentage"))
    for value, label, threshold in (
        (top10, "high_top10_concentration", 0.50),
        (owner, "high_owner_concentration", 0.10),
        (creator, "high_creator_concentration", 0.10),
    ):
        if value is not None and value >= threshold:
            flags.append(label)
    status = "review_required" if flags else "screened_unverified"
    return status, {
        "risk_flags": flags, "top10_holder_percent": top10,
        "owner_percentage": owner, "creator_percentage": creator,
        "provider_screened": True,
    }


def enrich_solana_token_metadata(token_addresses, limit=None):
    """Bounded Birdeye metadata/security enrichment with a persistent cache."""
    limit = SOLANA_METADATA_PER_REFRESH if limit is None else max(int(limit), 0)
    if not os.getenv("BIRDEYE_API_KEY") or limit == 0:
        return {}
    ordered = list(dict.fromkeys(address for address in token_addresses if address))
    if not ordered:
        return {}
    cutoff = datetime.now(timezone.utc) - timedelta(hours=SOLANA_METADATA_REFRESH_HOURS)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT token_address, token_symbol, token_name, safety_status,
                    safety_details, updated_at
                FROM solana_token_metadata WHERE token_address = ANY(%s)
            """, (ordered,))
            cached_rows = cur.fetchall()
    cached = {}
    for row in cached_rows:
        try:
            details = json.loads(row[4] or "{}")
        except (TypeError, ValueError):
            details = {}
        cached[row[0]] = {
            "token_symbol": row[1], "token_name": row[2],
            "safety_status": row[3], "safety_details": details,
            "updated_at": row[5],
        }
    due = [
        address for address in ordered
        if address not in cached or not cached[address].get("updated_at")
        or cached[address]["updated_at"] < cutoff
        or not cached[address].get("token_symbol")
    ][:limit]
    for address in due:
        symbol = name = None
        security_data = {}
        try:
            overview = _birdeye_payload(birdeye_get(
                "/defi/token_overview", {"address": address}, retry_429=False
            ))
            symbol = overview.get("symbol") or overview.get("tokenSymbol")
            name = overview.get("name") or overview.get("tokenName")
            security_data = _birdeye_payload(birdeye_get(
                "/defi/token_security", {"address": address}, retry_429=False
            ))
        except (requests.Timeout, requests.ConnectionError):
            pass
        safety_status, safety_details = _solana_safety_status(security_data)
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO solana_token_metadata (
                        token_address, token_symbol, token_name, safety_status,
                        safety_details, last_attempted_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (token_address) DO UPDATE SET
                        token_symbol = COALESCE(EXCLUDED.token_symbol, solana_token_metadata.token_symbol),
                        token_name = COALESCE(EXCLUDED.token_name, solana_token_metadata.token_name),
                        safety_status = EXCLUDED.safety_status,
                        safety_details = EXCLUDED.safety_details,
                        last_attempted_at = NOW(), updated_at = NOW()
                """, (address, symbol, name, safety_status, json.dumps(safety_details)))
            conn.commit()
        cached[address] = {
            "token_symbol": symbol or cached.get(address, {}).get("token_symbol"),
            "token_name": name or cached.get(address, {}).get("token_name"),
            "safety_status": safety_status, "safety_details": safety_details,
            "updated_at": datetime.now(timezone.utc),
        }
    return cached


def prioritise_solana_metadata(grouped):
    """Put active multi-buyer signals first, then the freshest observations."""
    return sorted(grouped, key=lambda token: (
        -len(grouped[token].get("BUY") or {}),
        -grouped[token]["last_activity_at"].timestamp(),
        token,
    ))


def format_signal_notification(signal, *, test=False):
    symbol = signal.get("token_symbol") or "Unknown token"
    status = signal.get("status") or "UNKNOWN"
    display_status = solana_status_label(
        status, signal.get("independent_buy_clusters", 0),
        signal.get("independent_sell_clusters", 0)
    )
    heading = "TEST — Wallet Monitor notification pipeline" if test else f"Paper signal: {display_status}"
    return "\n".join([
        heading,
        f"Token: {symbol}",
        f"Address: {signal.get('token_address') or 'test-only'}",
        f"Buy score: {signal.get('buy_score', 0)}",
        f"Sell score: {signal.get('sell_score', 0)}",
        f"Independent buy clusters: {signal.get('independent_buy_clusters', 0)}",
        f"Independent sell clusters: {signal.get('independent_sell_clusters', 0)}",
        "Safety: unverified",
        "PAPER RESEARCH ONLY — not a trade instruction.",
    ])


def telegram_send(message):
    """Deliver one Telegram message without exposing credentials in logs."""
    if not telegram_configured():
        return {
            "success": False, "status": "configuration_required",
            "response_code": None, "summary": {},
            "error": "Telegram environment variables are not configured",
        }

    diagnostic_increment("telegram_requests")
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
                "text": message,
                "disable_web_page_preview": True,
            },
            timeout=TELEGRAM_TIMEOUT_SECONDS,
        )
    except (requests.Timeout, requests.ConnectionError) as exc:
        diagnostic_increment("telegram_failures")
        return {
            "success": False, "status": "failed", "response_code": None,
            "summary": {}, "error": type(exc).__name__,
        }

    try:
        payload = response.json()
    except ValueError:
        payload = {}
    summary = {
        "ok": bool(payload.get("ok")),
        "error_code": payload.get("error_code"),
        "description": payload.get("description"),
        "message_id": (payload.get("result") or {}).get("message_id"),
    }
    success = response.status_code == 200 and summary["ok"]
    diagnostic_increment("telegram_deliveries" if success else "telegram_failures")
    return {
        "success": success, "status": "delivered" if success else "failed",
        "response_code": response.status_code, "summary": summary,
        "error": None if success else (summary.get("description") or "Telegram rejected delivery"),
    }


def queue_notification(event_key, notification_type, signal, message):
    """Create one idempotent delivery record and return its stable identifier."""
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO notification_deliveries (
                    event_key, notification_type, token_address, token_symbol,
                    signal_status, message
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_key) DO NOTHING
                RETURNING id
            """, (
                event_key, notification_type, signal.get("token_address"),
                signal.get("token_symbol"), signal.get("status"), message,
            ))
            row = cur.fetchone()
            if row:
                notification_id, created = row[0], True
            else:
                cur.execute(
                    "SELECT id FROM notification_deliveries WHERE event_key = %s",
                    (event_key,),
                )
                notification_id, created = cur.fetchone()[0], False
        conn.commit()
    return notification_id, created


def deliver_notification(notification_id):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT message, delivery_status, attempts
                FROM notification_deliveries WHERE id = %s
            """, (notification_id,))
            row = cur.fetchone()
    if not row:
        return {"success": False, "status": "not_found", "notification_id": notification_id}
    message, delivery_status, attempts = row
    if delivery_status == "delivered":
        return {"success": True, "status": "duplicate_suppressed",
                "notification_id": notification_id, "attempts": attempts}

    result = telegram_send(message)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE notification_deliveries
                SET delivery_status = %s, attempts = attempts + 1,
                    response_code = %s, response_summary = %s,
                    error_message = %s,
                    delivered_at = CASE WHEN %s = 'delivered' THEN NOW() ELSE delivered_at END,
                    updated_at = NOW()
                WHERE id = %s
            """, (
                result["status"], result.get("response_code"),
                json.dumps(result.get("summary") or {}), result.get("error"),
                result["status"], notification_id,
            ))
        conn.commit()
    return {**result, "notification_id": notification_id, "attempts": attempts + 1}


def queue_and_deliver_signal_notification(signal, event_key, *, test=False):
    message = format_signal_notification(signal, test=test)
    notification_id, created = queue_notification(
        event_key, "test" if test else "signal_transition", signal, message
    )
    result = deliver_notification(notification_id)
    result["created"] = created
    return result


# =========================================================
# V4.12 MULTI-CHAIN EVM EVIDENCE CALIBRATION
# =========================================================

def safe_float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def safe_int(value):
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def percentage_change(current, previous):
    if current is None or previous in {None, 0}:
        return None
    return ((current - previous) / abs(previous)) * 100.0


def _pair_activity(pair):
    transactions = pair.get("txns") or {}
    h1 = transactions.get("h1") or {}
    h24 = transactions.get("h24") or {}
    volume = pair.get("volume") or {}
    return {
        "transactions_h1": (safe_int(h1.get("buys")) or 0) + (safe_int(h1.get("sells")) or 0),
        "transactions_h24": (safe_int(h24.get("buys")) or 0) + (safe_int(h24.get("sells")) or 0),
        "volume_h1_usd": safe_float(volume.get("h1")) or 0.0,
        "volume_h24_usd": safe_float(volume.get("h24")) or 0.0,
    }


def select_evm_pair(chain_pairs, canonical_pair=None):
    """Use a locked pair, or choose an active initial pair before locking it."""
    if canonical_pair:
        for pair in chain_pairs:
            if str(pair.get("pairAddress") or "").lower() == canonical_pair.lower():
                return pair, "canonical_locked"
        return None, "canonical_unavailable"

    active_pairs = []
    for pair in chain_pairs:
        activity = _pair_activity(pair)
        if activity["transactions_h24"] > 0 or activity["volume_h24_usd"] > 0:
            active_pairs.append(pair)
    eligible = active_pairs or chain_pairs
    if not eligible:
        return None, "no_pair"
    return max(
        eligible,
        key=lambda item: (
            safe_float((item.get("liquidity") or {}).get("usd")) or 0,
            _pair_activity(item)["volume_h24_usd"],
        ),
    ), "initial_active_liquidity" if active_pairs else "initial_unverified_liquidity"


def fetch_evm_token_snapshot(token):
    """Fetch one bounded market/holder snapshot for a configured EVM chain."""
    address = token[1]
    chain = str(token[0]).lower()
    config = EVM_CHAIN_CONFIG.get(chain)
    if not config:
        raise ValueError("unsupported_evm_chain")
    snapshot = {
        "chain": chain, "token_address": address,
        "token_symbol": token[2], "provider_errors": [],
        "canonical_pair_address": token[4] if len(token) > 4 else None,
        "canonical_pair_dex_id": token[5] if len(token) > 5 else None,
        "source": token[6] if len(token) > 6 else None,
        "is_benchmark": bool(len(token) > 6 and token[6] == "benchmark"),
    }
    market_ok = False
    holders_ok = False

    try:
        response = upstream_request(
            "GET", DEXSCREENER_TOKEN_URL.format(address=address),
            timeout=EVM_PROVIDER_TIMEOUT_SECONDS, retries=0,
            provider="dexscreener",
        )
        if response.status_code == 200:
            payload = response.json()
            pairs = payload.get("pairs") if isinstance(payload, dict) else []
            pairs = pairs if isinstance(pairs, list) else []
            chain_pairs = [
                pair for pair in pairs
                if isinstance(pair, dict)
                and str(pair.get("chainId") or "").lower() in config["dex_chain_ids"]
                and str((pair.get("baseToken") or {}).get("address") or "").lower()
                == address.lower()
            ]
            if chain_pairs:
                pair, selection = select_evm_pair(
                    chain_pairs, snapshot.get("canonical_pair_address")
                )
                snapshot["pair_selection"] = selection
                if pair is None:
                    diagnostic_increment("evm_canonical_pair_misses")
                    snapshot["provider_errors"].append(
                        f"dexscreener_{selection}"
                    )
                else:
                    snapshot["pair_lock_candidate"] = not bool(
                        snapshot.get("canonical_pair_address")
                    )
                    transactions = pair.get("txns") or {}
                    snapshot.update({
                        "price_usd": safe_float(pair.get("priceUsd")),
                        "liquidity_usd": safe_float((pair.get("liquidity") or {}).get("usd")),
                        "market_cap_usd": safe_float(pair.get("marketCap")),
                        "fdv_usd": safe_float(pair.get("fdv")),
                        "volume_h1_usd": safe_float((pair.get("volume") or {}).get("h1")),
                        "volume_h24_usd": safe_float((pair.get("volume") or {}).get("h24")),
                        "buys_h1": safe_int((transactions.get("h1") or {}).get("buys")),
                        "sells_h1": safe_int((transactions.get("h1") or {}).get("sells")),
                        "buys_h24": safe_int((transactions.get("h24") or {}).get("buys")),
                        "sells_h24": safe_int((transactions.get("h24") or {}).get("sells")),
                        "price_change_h1_pct": safe_float((pair.get("priceChange") or {}).get("h1")),
                        "price_change_h24_pct": safe_float((pair.get("priceChange") or {}).get("h24")),
                        "pair_address": pair.get("pairAddress"),
                        "dex_id": pair.get("dexId"),
                    })
                    market_ok = True
            else:
                snapshot["provider_errors"].append(f"dexscreener_no_{chain}_base_pair")
        else:
            snapshot["provider_errors"].append(f"dexscreener_http_{response.status_code}")
    except (requests.RequestException, ValueError, TypeError) as exc:
        snapshot["provider_errors"].append(f"dexscreener_{type(exc).__name__}")

    try:
        response = upstream_request(
            "GET", f"{config['blockscout_url']}/tokens/{address}",
            timeout=EVM_PROVIDER_TIMEOUT_SECONDS, retries=0,
            provider="blockscout",
        )
        if response.status_code == 200:
            payload = response.json()
            holder_count = safe_int(payload.get("holders_count")) if isinstance(payload, dict) else None
            snapshot["holder_count"] = holder_count
            holders_ok = holder_count is not None
            if not holders_ok:
                snapshot["provider_errors"].append("blockscout_holder_count_missing")
        else:
            snapshot["provider_errors"].append(f"blockscout_http_{response.status_code}")
    except (requests.RequestException, ValueError, TypeError) as exc:
        snapshot["provider_errors"].append(f"blockscout_{type(exc).__name__}")

    snapshot["data_quality"] = (
        "complete" if market_ok and holders_ok
        else "market_only" if market_ok
        else "metadata_only" if holders_ok
        else "unavailable"
    )
    return snapshot


def evm_liquidity_tier(liquidity):
    if liquidity is None:
        return "UNKNOWN"
    if liquidity < 10000:
        return "CRITICAL"
    if liquidity < EVM_MIN_MOMENTUM_LIQUIDITY_USD:
        return "THIN"
    if liquidity < 100000:
        return "LIMITED"
    if liquidity < 1000000:
        return "HEALTHY"
    return "DEEP"


def _snapshot_baseline(history, target):
    candidates = [
        item for item in history
        if item.get("captured_at") and item["captured_at"] <= target
    ]
    return max(candidates, key=lambda item: item["captured_at"]) if candidates else None


def calculate_evm_horizons(snapshot, history):
    """Compare the current point with persisted 1h/6h/24h baselines."""
    captured_at = snapshot.get("captured_at") or datetime.now(timezone.utc)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    result = {}
    for hours in (1, 6, 24):
        baseline = _snapshot_baseline(history, captured_at - timedelta(hours=hours))
        result[f"{hours}h"] = {
            "available": baseline is not None,
            "price_change_pct": percentage_change(
                snapshot.get("price_usd"), baseline.get("price_usd") if baseline else None
            ),
            "liquidity_change_pct": percentage_change(
                snapshot.get("liquidity_usd"),
                baseline.get("liquidity_usd") if baseline else None,
            ),
            "holder_change": (
                snapshot["holder_count"] - baseline["holder_count"]
                if baseline and snapshot.get("holder_count") is not None
                and baseline.get("holder_count") is not None else None
            ),
            "baseline_at": baseline["captured_at"].isoformat() if baseline else None,
        }
    return result


def analyse_evm_structure(snapshot, history):
    """Snapshot-proxy structure detection; this is deliberately not OHLC analysis."""
    points = [
        item for item in reversed(history)
        if item.get("price_usd") is not None and item.get("price_usd") > 0
    ]
    current = {
        "price_usd": snapshot.get("price_usd"),
        "volume_h1_usd": snapshot.get("volume_h1_usd"),
        "captured_at": snapshot.get("captured_at") or datetime.now(timezone.utc),
    }
    if current["price_usd"] is not None and current["price_usd"] > 0:
        points.append(current)
    points = points[-32:]
    if len(points) < 12:
        return {
            "state": "COLLECTING", "confidence": 0, "developing": True,
            "sample_count": len(points), "method": "15m_snapshot_proxy",
            "volume_confirmed": False, "support": None, "resistance": None,
            "signals": ["minimum_12_samples_required"],
        }

    prices = [item["price_usd"] for item in points]
    prior_prices = prices[:-1]
    ordered = sorted(prior_prices)
    support = ordered[max(int(len(ordered) * 0.10) - 1, 0)]
    resistance = ordered[min(int(len(ordered) * 0.90), len(ordered) - 1)]
    midpoint = (support + resistance) / 2 if support and resistance else None
    range_pct = ((resistance - support) / midpoint * 100) if midpoint else None
    tolerance = 0.02
    support_tests = sum(abs(price - support) / support <= tolerance for price in prior_prices)
    resistance_tests = sum(
        abs(price - resistance) / resistance <= tolerance for price in prior_prices
    )

    thirds = [prices[index::3] for index in range(3)]
    # Contiguous thirds are more useful than alternating observations.
    size = max(len(prices) // 3, 1)
    thirds = [prices[:size], prices[size:2 * size], prices[2 * size:]]
    lows = [min(group) for group in thirds if group]
    highs = [max(group) for group in thirds if group]
    higher_lows = len(lows) == 3 and lows[1] > lows[0] * 1.003 and lows[2] > lows[1] * 1.003
    lower_highs = len(highs) == 3 and highs[1] < highs[0] * 0.997 and highs[2] < highs[1] * 0.997

    local_highs = []
    local_lows = []
    for index in range(1, len(prices) - 1):
        if prices[index] >= prices[index - 1] and prices[index] >= prices[index + 1]:
            local_highs.append((index, prices[index]))
        if prices[index] <= prices[index - 1] and prices[index] <= prices[index + 1]:
            local_lows.append((index, prices[index]))
    double_top = any(
        right[0] - left[0] >= 4 and abs(right[1] - left[1]) / left[1] <= 0.025
        for left, right in zip(local_highs, local_highs[1:])
    )
    double_bottom = any(
        right[0] - left[0] >= 4 and abs(right[1] - left[1]) / left[1] <= 0.025
        for left, right in zip(local_lows, local_lows[1:])
    )

    prior_volumes = [
        item.get("volume_h1_usd") for item in points[:-1]
        if item.get("volume_h1_usd") is not None
    ]
    median_volume = statistics.median(prior_volumes) if prior_volumes else None
    current_volume = current.get("volume_h1_usd")
    volume_confirmed = bool(
        median_volume and current_volume is not None and current_volume >= median_volume * 1.5
    )
    breakout = prices[-1] >= resistance * 1.02
    breakdown = prices[-1] <= support * 0.98

    signals = []
    state = "NO_CLEAR_STRUCTURE"
    confidence = 20
    developing = True
    if breakout and volume_confirmed:
        state, confidence, developing = "CONFIRMED_BREAKOUT", 85, False
        signals.append("price_above_resistance_with_volume")
    elif breakdown and volume_confirmed:
        state, confidence, developing = "CONFIRMED_BREAKDOWN", 85, False
        signals.append("price_below_support_with_volume")
    elif higher_lows and resistance_tests >= 2:
        state, confidence = "DEVELOPING_ASCENDING_TRIANGLE", 65
        signals.extend(["higher_lows", "repeated_resistance_tests"])
    elif lower_highs and support_tests >= 2:
        state, confidence = "DEVELOPING_DESCENDING_TRIANGLE", 65
        signals.extend(["lower_highs", "repeated_support_tests"])
    elif double_bottom:
        state, confidence = "DEVELOPING_DOUBLE_BOTTOM", 55
        signals.append("two_similar_local_lows")
    elif double_top:
        state, confidence = "DEVELOPING_DOUBLE_TOP", 55
        signals.append("two_similar_local_highs")
    elif range_pct is not None and range_pct <= 12 and support_tests >= 2 and resistance_tests >= 2:
        state, confidence = "DEVELOPING_RANGE", 55
        signals.extend(["repeated_support_tests", "repeated_resistance_tests"])
    elif higher_lows:
        state, confidence = "DEVELOPING_HIGHER_LOWS", 45
        signals.append("higher_lows")
    elif lower_highs:
        state, confidence = "DEVELOPING_LOWER_HIGHS", 45
        signals.append("lower_highs")

    return {
        "state": state, "confidence": confidence, "developing": developing,
        "sample_count": len(points), "method": "15m_snapshot_proxy",
        "volume_confirmed": volume_confirmed, "support": support,
        "resistance": resistance, "range_pct": range_pct,
        "support_tests": support_tests, "resistance_tests": resistance_tests,
        "signals": signals,
    }


def classify_evm_snapshot(snapshot, previous=None, history=None):
    """Conservative multi-horizon research state; never an execution recommendation."""
    history = history or ([previous] if previous else [])
    liquidity = snapshot.get("liquidity_usd")
    volume_h1 = snapshot.get("volume_h1_usd")
    buys = snapshot.get("buys_h1")
    sells = snapshot.get("sells_h1")
    price_change = snapshot.get("price_change_h1_pct")
    holder_change = percentage_change(
        snapshot.get("holder_count"), previous.get("holder_count") if previous else None
    )
    liquidity_change = percentage_change(
        liquidity, previous.get("liquidity_usd") if previous else None
    )
    volume_liquidity = (
        volume_h1 / liquidity if volume_h1 is not None and liquidity not in {None, 0} else None
    )
    buy_sell = (
        buys / sells if buys is not None and sells not in {None, 0}
        else 3.0 if buys and sells == 0 else None
    )
    transaction_count = (
        (buys or 0) + (sells or 0)
        if buys is not None or sells is not None else None
    )
    horizons = calculate_evm_horizons(snapshot, history)
    structure = analyse_evm_structure(snapshot, history)
    liquidity_tier = evm_liquidity_tier(liquidity)
    wallet_quality = {
        "coverage": "holder_count_only",
        "classification": "unresolved",
        "excluded_wallet_types": [],
        "note": "Address-level holder data is required for router, pool, bridge and deployer filtering.",
    }

    momentum_score = 0
    risk_score = 0
    reasons = []
    if liquidity is not None and liquidity < 10000:
        risk_score += 50
        reasons.append("very_low_liquidity")
    elif liquidity is not None and liquidity >= 25000:
        momentum_score += 10
        reasons.append("minimum_liquidity_present")
    if liquidity_change is not None and liquidity_change <= -20:
        risk_score += 35
        reasons.append("rapid_liquidity_decline")
    if holder_change is not None and holder_change <= -5:
        risk_score += 25
        reasons.append("holder_count_decline")
    elif holder_change is not None and holder_change >= 0.5:
        momentum_score += 20
        reasons.append("holder_growth")
    if price_change is not None and price_change >= 5:
        momentum_score += 20 if price_change < 20 else 15
        reasons.append("positive_hourly_momentum")
    if volume_liquidity is not None and volume_liquidity >= 0.25:
        momentum_score += 20
        reasons.append("material_volume_to_liquidity")
        if volume_liquidity >= 0.75:
            momentum_score += 10
    if buy_sell is not None and buy_sell >= 1.25:
        momentum_score += 20
        reasons.append("buy_transaction_imbalance")
        if buy_sell >= 2:
            momentum_score += 10
    if (
        price_change is not None and abs(price_change) >= 30
        and (volume_liquidity is None or volume_liquidity < 0.10)
    ):
        risk_score += 30
        reasons.append("large_move_without_market_depth")
    if snapshot.get("data_quality") == "unavailable":
        risk_score += 20
        reasons.append("providers_unavailable")
    if liquidity_tier == "THIN":
        risk_score += 30
        reasons.append("thin_liquidity")
    if structure["state"] == "CONFIRMED_BREAKOUT":
        momentum_score += 25
        reasons.append("snapshot_proxy_breakout_with_volume")
    elif structure["state"] == "CONFIRMED_BREAKDOWN":
        risk_score += 40
        reasons.append("snapshot_proxy_breakdown_with_volume")

    momentum_eligible = True
    if liquidity is None or liquidity < EVM_MIN_MOMENTUM_LIQUIDITY_USD:
        momentum_eligible = False
        reasons.append("momentum_gate_low_liquidity")
    if volume_h1 is None or volume_h1 < EVM_MIN_MOMENTUM_H1_VOLUME_USD:
        momentum_eligible = False
        reasons.append("momentum_gate_low_hourly_volume")
    if transaction_count is None or transaction_count < EVM_MIN_MOMENTUM_H1_TRANSACTIONS:
        momentum_eligible = False
        reasons.append("momentum_gate_small_transaction_sample")

    price_1h = horizons["1h"]["price_change_pct"]
    price_6h = horizons["6h"]["price_change_pct"]
    price_24h = horizons["24h"]["price_change_pct"]
    holders_24h = horizons["24h"]["holder_change"]
    liquidity_24h = horizons["24h"]["liquidity_change_pct"]
    rebound = bool(
        price_1h is not None and price_24h is not None
        and price_1h >= 5 and price_24h <= -10
    )
    distribution = bool(
        (price_24h is not None and price_24h <= -8 and holders_24h is not None and holders_24h < 0)
        or (liquidity_24h is not None and liquidity_24h <= -15)
    )
    holder_growth_floor = max(
        10, int((snapshot.get("holder_count") or 0) * 0.001)
    )
    accumulation_watch = bool(
        momentum_eligible and holders_24h is not None
        and holders_24h >= holder_growth_floor
        and price_24h is not None and -15 <= price_24h <= 8
    )

    if risk_score >= 50 and structure["state"] != "CONFIRMED_BREAKDOWN":
        status = "EVM_RISK"
    elif structure["state"] == "CONFIRMED_BREAKDOWN":
        status = "EVM_CONFIRMED_BREAKDOWN"
    elif liquidity_tier == "THIN":
        status = "EVM_THIN_LIQUIDITY"
    elif distribution:
        status = "EVM_DISTRIBUTION"
        reasons.append("multi_horizon_distribution_evidence")
    elif rebound:
        status = "EVM_REBOUND"
        reasons.append("short_rebound_inside_24h_decline")
    elif structure["state"] == "CONFIRMED_BREAKOUT" and momentum_eligible:
        status = "EVM_CONFIRMED_BREAKOUT"
    elif momentum_eligible and previous and momentum_score >= 70 and holder_change is not None:
        status = "EVM_HIGH_MOMENTUM"
    elif momentum_eligible and momentum_score >= 45:
        status = "EVM_MOMENTUM"
    elif accumulation_watch:
        status = "EVM_ACCUMULATION_WATCH"
        reasons.extend(["holder_growth_with_non_extended_price", "wallet_validation_required"])
    else:
        status = "EVM_OBSERVE"
    return {
        "status": status, "momentum_score": min(momentum_score, 100),
        "risk_score": min(risk_score, 100), "reasons": reasons,
        "holder_change_pct": holder_change,
        "liquidity_change_pct": liquidity_change,
        "volume_liquidity_ratio": volume_liquidity,
        "buy_sell_ratio": buy_sell,
        "transaction_count_h1": transaction_count,
        "momentum_eligible": momentum_eligible,
        "liquidity_tier": liquidity_tier,
        "structure_state": structure["state"],
        "structure_confidence": structure["confidence"],
        "structure_details": structure,
        "horizon_metrics": horizons,
        "wallet_quality": wallet_quality,
    }


def classify_evm_benchmark(snapshot, previous=None, history=None):
    """Retain market/structure evidence without treating ETH as a meme signal."""
    evidence = classify_evm_snapshot(snapshot, previous, history)
    return {
        **evidence,
        "status": "EVM_BENCHMARK",
        "momentum_score": 0,
        "risk_score": 0,
        "reasons": ["market_benchmark", "excluded_from_token_alert_scoring"],
        "momentum_eligible": False,
        "wallet_quality": {
            "coverage": "benchmark_only",
            "classification": "not_applicable",
            "note": "ETH provides market context and never generates token alerts.",
        },
    }


def format_evm_notification(signal, *, test=False):
    heading = "TEST — V4.12 EVM notification pipeline" if test else f"EVM state confirmed: {signal['status']}"
    chain = str(signal.get("chain") or "robinhood").lower()
    chain_label = EVM_CHAIN_CONFIG.get(chain, {}).get("label", chain.title())
    return "\n".join([
        heading,
        f"Token: {signal.get('token_symbol') or 'Unknown'}",
        f"Chain: {chain_label}",
        f"Address: {signal.get('token_address') or 'test-only'}",
        f"Momentum score: {signal.get('momentum_score', 0)}/100",
        f"Risk score: {signal.get('risk_score', 0)}/100",
        f"Liquidity tier: {signal.get('liquidity_tier', 'UNKNOWN')}",
        f"Structure: {signal.get('structure_state', 'COLLECTING')} ({signal.get('structure_confidence', 0)}%)",
        f"Data quality: {signal.get('data_quality', 'partial')}",
        f"Confirmations: {signal.get('alert_confirmation_count', EVM_ALERT_CONFIRMATIONS)}/{EVM_ALERT_CONFIRMATIONS}",
        f"Reasons: {', '.join(signal.get('reasons') or ['baseline observation'])}",
        "MULTI-CHAIN EVM — PAPER RESEARCH ONLY; not a trade instruction.",
    ])


def queue_and_deliver_evm_notification(signal, event_key, *, test=False):
    notification_id, created = queue_notification(
        event_key, "evm_test" if test else "evm_state_transition",
        signal, format_evm_notification(signal, test=test),
    )
    result = deliver_notification(notification_id)
    result["created"] = created
    return result


def _value_ratio(current, previous):
    if current is None or previous is None or current <= 0 or previous <= 0:
        return None
    return current / previous


def transient_market_provider_errors(provider_errors):
    transient_markers = (
        "Timeout", "ConnectionError", "ChunkedEncodingError",
        "http_429", "http_500", "http_502", "http_503", "http_504",
    )
    return [
        error for error in (provider_errors or [])
        if str(error).startswith("dexscreener_")
        and any(marker in str(error) for marker in transient_markers)
    ]


def assess_evm_snapshot_integrity(snapshot, previous=None):
    """Return quarantine reasons without confusing genuine traded moves with bad pools."""
    reasons = []
    canonical = str(snapshot.get("canonical_pair_address") or "").lower()
    selected = str(snapshot.get("pair_address") or "").lower()
    if snapshot.get("pair_selection") == "canonical_unavailable":
        reasons.append("canonical_pair_unavailable")
    if canonical and selected and canonical != selected:
        reasons.append("canonical_pair_mismatch")
    if snapshot.get("price_usd") is not None and snapshot["price_usd"] <= 0:
        reasons.append("invalid_nonpositive_price")
    if snapshot.get("liquidity_usd") is not None and snapshot["liquidity_usd"] < 0:
        reasons.append("invalid_negative_liquidity")

    transient_errors = transient_market_provider_errors(
        snapshot.get("provider_errors") or []
    )
    market_missing = (
        snapshot.get("price_usd") is None
        or snapshot.get("liquidity_usd") is None
    )
    provider_unavailable = bool(
        market_missing and transient_errors
        and snapshot.get("pair_selection") != "canonical_unavailable"
    )
    availability_reasons = (
        ["market_provider_temporarily_unavailable", *transient_errors]
        if provider_unavailable else []
    )

    metrics = {}
    if previous:
        price_ratio = _value_ratio(snapshot.get("price_usd"), previous.get("price_usd"))
        liquidity_ratio = _value_ratio(
            snapshot.get("liquidity_usd"), previous.get("liquidity_usd")
        )
        market_cap_ratio = _value_ratio(
            snapshot.get("market_cap_usd"), previous.get("market_cap_usd")
        )
        metrics = {
            "price_ratio_to_last_trusted": price_ratio,
            "liquidity_ratio_to_last_trusted": liquidity_ratio,
            "market_cap_ratio_to_last_trusted": market_cap_ratio,
        }
        transactions_h1 = (snapshot.get("buys_h1") or 0) + (snapshot.get("sells_h1") or 0)
        zero_activity = transactions_h1 == 0 and (snapshot.get("volume_h1_usd") or 0) == 0
        extreme_price = bool(
            price_ratio is not None and (
                price_ratio >= EVM_ANOMALY_PRICE_RATIO
                or price_ratio <= 1.0 / EVM_ANOMALY_PRICE_RATIO
            )
        )
        explosive_liquidity = bool(
            liquidity_ratio is not None
            and liquidity_ratio >= EVM_ANOMALY_LIQUIDITY_RATIO
        )
        if extreme_price and zero_activity:
            reasons.append("extreme_price_discontinuity_without_activity")
        if explosive_liquidity and zero_activity:
            reasons.append("liquidity_discontinuity_without_activity")
        if extreme_price and explosive_liquidity:
            reasons.append("correlated_price_liquidity_discontinuity")

    return {
        "is_anomaly": bool(reasons),
        "reasons": list(dict.fromkeys(reasons)),
        "provider_unavailable": provider_unavailable and not bool(reasons),
        "availability_reasons": list(dict.fromkeys(availability_reasons)),
        "metrics": metrics,
        "policy": {
            "price_ratio_threshold": EVM_ANOMALY_PRICE_RATIO,
            "liquidity_ratio_threshold": EVM_ANOMALY_LIQUIDITY_RATIO,
            "canonical_pair_locked": bool(canonical),
        },
    }


def classify_evm_anomaly(integrity):
    return {
        "status": "EVM_DATA_ANOMALY", "momentum_score": 0,
        "risk_score": 100, "reasons": integrity["reasons"],
        "holder_change_pct": None, "liquidity_change_pct": None,
        "volume_liquidity_ratio": None, "buy_sell_ratio": None,
        "transaction_count_h1": None, "momentum_eligible": False,
        "liquidity_tier": "QUARANTINED",
        "structure_state": "DATA_QUARANTINED",
        "structure_confidence": 0,
        "structure_details": {"state": "DATA_QUARANTINED", **integrity},
        "horizon_metrics": {},
        "wallet_quality": {
            "coverage": "quarantined",
            "classification": "not_evaluated",
            "note": "Wallet and market scoring skipped for an anomalous observation.",
        },
    }


def classify_evm_provider_unavailable(integrity):
    return {
        "status": "EVM_PROVIDER_UNAVAILABLE", "momentum_score": 0,
        "risk_score": 0, "reasons": integrity["availability_reasons"],
        "holder_change_pct": None, "liquidity_change_pct": None,
        "volume_liquidity_ratio": None, "buy_sell_ratio": None,
        "transaction_count_h1": None, "momentum_eligible": False,
        "liquidity_tier": "LAST_TRUSTED",
        "structure_state": "DATA_STALE", "structure_confidence": 0,
        "structure_details": {
            "state": "DATA_STALE", "provider_unavailable": True,
            "note": "Classification paused; the last trusted snapshot is retained.",
        },
        "horizon_metrics": {},
        "wallet_quality": {
            "coverage": "paused", "classification": "not_evaluated",
            "note": "Provider availability is not a token risk signal.",
        },
    }


def evm_snapshot_history(chain, address, hours=26):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT price_usd, liquidity_usd, volume_h1_usd, buys_h1,
                    sells_h1, holder_count, captured_at, pair_address,
                    market_cap_usd
                FROM evm_token_snapshots
                WHERE chain = %s AND token_address = %s
                    AND is_anomaly = FALSE
                    AND is_provider_unavailable = FALSE
                    AND captured_at >= NOW() - (%s * INTERVAL '1 hour')
                ORDER BY captured_at DESC LIMIT 192
            """, (chain, address, hours))
            rows = cur.fetchall()
    return [{
        "price_usd": row[0], "liquidity_usd": row[1],
        "volume_h1_usd": row[2], "buys_h1": row[3], "sells_h1": row[4],
        "holder_count": row[5], "captured_at": row[6],
        "pair_address": row[7], "market_cap_usd": row[8],
    } for row in rows]


def calibrate_evm_state(raw_classification, old_row, snapshot):
    """Require repeat evidence for market-state changes and alert delivery."""
    classification = dict(raw_classification)
    raw_status = classification["status"]
    infrastructure_state = bool(
        snapshot.get("is_anomaly") or snapshot.get("is_provider_unavailable")
        or snapshot.get("is_benchmark")
    )
    if not old_row or infrastructure_state:
        raw_count = 1
        classification.update({
            "raw_status": raw_status, "raw_status_count": raw_count,
            "pending_status": None, "pending_status_count": 0,
            "alert_confirmation_count": (
                raw_count if raw_status in EVM_ALERT_STATUSES else 0
            ),
        })
        return classification

    previous_status, previous_trusted, old_pending, old_pending_count, \
        old_raw, old_raw_count, old_alert_count = old_row
    effective_previous = (
        previous_trusted if previous_status in {
            "EVM_DATA_ANOMALY", "EVM_PROVIDER_UNAVAILABLE",
        } and previous_trusted else previous_status
    )
    raw_count = int(old_raw_count or 0) + 1 if old_raw == raw_status else 1
    pending_status, pending_count = None, 0
    effective_status = raw_status
    if effective_previous and raw_status != effective_previous:
        pending_status = raw_status
        pending_count = (
            int(old_pending_count or 0) + 1 if old_pending == raw_status else 1
        )
        if pending_count < EVM_VISIBLE_STATE_CONFIRMATIONS:
            effective_status = effective_previous
            classification["reasons"] = list(classification.get("reasons") or []) + [
                f"pending_state_confirmation:{raw_status}:{pending_count}/{EVM_VISIBLE_STATE_CONFIRMATIONS}"
            ]
        else:
            pending_status, pending_count = None, 0

    alert_count = (
        raw_count if effective_status == raw_status
        and raw_status in EVM_ALERT_STATUSES else 0
    )
    classification.update({
        "status": effective_status, "raw_status": raw_status,
        "raw_status_count": raw_count, "pending_status": pending_status,
        "pending_status_count": pending_count,
        "alert_confirmation_count": alert_count,
    })
    return classification


def persist_evm_snapshot(snapshot):
    snapshot["captured_at"] = snapshot.get("captured_at") or datetime.now(timezone.utc)
    history = evm_snapshot_history(snapshot["chain"], snapshot["token_address"])
    previous = history[0] if history else None
    integrity = assess_evm_snapshot_integrity(snapshot, previous)
    snapshot["is_anomaly"] = integrity["is_anomaly"]
    snapshot["anomaly_reasons"] = integrity["reasons"]
    snapshot["is_provider_unavailable"] = integrity["provider_unavailable"]
    snapshot["availability_reasons"] = integrity["availability_reasons"]
    if snapshot["is_anomaly"]:
        diagnostic_increment("evm_anomalies")
        classification = classify_evm_anomaly(integrity)
        snapshot["data_quality"] = "anomaly"
    elif snapshot["is_provider_unavailable"]:
        diagnostic_increment("evm_provider_unavailable")
        classification = classify_evm_provider_unavailable(integrity)
        snapshot["data_quality"] = "provider_unavailable"
    elif snapshot.get("is_benchmark"):
        classification = classify_evm_benchmark(snapshot, previous, history)
    else:
        classification = classify_evm_snapshot(snapshot, previous, history)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status, last_trusted_status, pending_status,
                    pending_status_count, raw_status, raw_status_count,
                    alert_confirmation_count
                FROM evm_token_signals
                WHERE chain = %s AND token_address = %s
            """, (snapshot["chain"], snapshot["token_address"]))
            old_row = cur.fetchone()
            previous_status = old_row[0] if old_row else None
            previous_trusted_status = old_row[1] if old_row else None
            classification = calibrate_evm_state(classification, old_row, snapshot)
            recovering_from_provider_outage = bool(
                previous_status == "EVM_PROVIDER_UNAVAILABLE"
                and not snapshot["is_provider_unavailable"]
            )
            if recovering_from_provider_outage:
                diagnostic_increment("evm_provider_recoveries")
            cur.execute("""
                INSERT INTO evm_token_snapshots (
                    chain, token_address, token_symbol, price_usd, liquidity_usd,
                    market_cap_usd, fdv_usd, volume_h1_usd, volume_h24_usd,
                    buys_h1, sells_h1, buys_h24, sells_h24,
                    price_change_h1_pct, price_change_h24_pct, holder_count,
                    pair_address, dex_id, data_quality, provider_errors,
                    is_anomaly, anomaly_reasons, pair_selection
                    , is_provider_unavailable, availability_reasons
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                ) RETURNING id
            """, (
                snapshot["chain"], snapshot["token_address"], snapshot["token_symbol"],
                snapshot.get("price_usd"), snapshot.get("liquidity_usd"),
                snapshot.get("market_cap_usd"), snapshot.get("fdv_usd"),
                snapshot.get("volume_h1_usd"), snapshot.get("volume_h24_usd"),
                snapshot.get("buys_h1"), snapshot.get("sells_h1"),
                snapshot.get("buys_h24"), snapshot.get("sells_h24"),
                snapshot.get("price_change_h1_pct"), snapshot.get("price_change_h24_pct"),
                snapshot.get("holder_count"), snapshot.get("pair_address"),
                snapshot.get("dex_id"), snapshot["data_quality"],
                json.dumps(snapshot.get("provider_errors") or []),
                snapshot["is_anomaly"],
                json.dumps(snapshot.get("anomaly_reasons") or []),
                snapshot.get("pair_selection"),
                snapshot["is_provider_unavailable"],
                json.dumps(snapshot.get("availability_reasons") or []),
            ))
            snapshot_id = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO evm_token_signals (
                    chain, token_address, token_symbol, status, momentum_score,
                    risk_score, reasons, holder_change_pct, liquidity_change_pct,
                    volume_liquidity_ratio, buy_sell_ratio, liquidity_tier,
                    structure_state, structure_confidence, structure_details,
                    horizon_metrics, wallet_quality, anomaly_details,
                    availability_details, last_trusted_status,
                    is_benchmark, latest_snapshot_id, data_quality, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (chain, token_address) DO UPDATE SET
                    token_symbol = EXCLUDED.token_symbol, status = EXCLUDED.status,
                    momentum_score = EXCLUDED.momentum_score,
                    risk_score = EXCLUDED.risk_score, reasons = EXCLUDED.reasons,
                    holder_change_pct = EXCLUDED.holder_change_pct,
                    liquidity_change_pct = EXCLUDED.liquidity_change_pct,
                    volume_liquidity_ratio = EXCLUDED.volume_liquidity_ratio,
                    buy_sell_ratio = EXCLUDED.buy_sell_ratio,
                    liquidity_tier = EXCLUDED.liquidity_tier,
                    structure_state = EXCLUDED.structure_state,
                    structure_confidence = EXCLUDED.structure_confidence,
                    structure_details = EXCLUDED.structure_details,
                    horizon_metrics = EXCLUDED.horizon_metrics,
                    wallet_quality = EXCLUDED.wallet_quality,
                    anomaly_details = EXCLUDED.anomaly_details,
                    availability_details = EXCLUDED.availability_details,
                    is_benchmark = EXCLUDED.is_benchmark,
                    last_trusted_status = CASE
                        WHEN EXCLUDED.status IN (
                            'EVM_DATA_ANOMALY', 'EVM_PROVIDER_UNAVAILABLE'
                        ) THEN COALESCE(
                            evm_token_signals.last_trusted_status,
                            CASE WHEN evm_token_signals.status NOT IN (
                                'EVM_DATA_ANOMALY', 'EVM_PROVIDER_UNAVAILABLE'
                            ) THEN evm_token_signals.status END
                        )
                        ELSE EXCLUDED.last_trusted_status
                    END,
                    latest_snapshot_id = CASE
                        WHEN EXCLUDED.status IN (
                            'EVM_DATA_ANOMALY', 'EVM_PROVIDER_UNAVAILABLE'
                        )
                        THEN evm_token_signals.latest_snapshot_id
                        ELSE EXCLUDED.latest_snapshot_id
                    END,
                    data_quality = EXCLUDED.data_quality, updated_at = NOW()
            """, (
                snapshot["chain"], snapshot["token_address"], snapshot["token_symbol"],
                classification["status"], classification["momentum_score"],
                classification["risk_score"], json.dumps(classification["reasons"]),
                classification["holder_change_pct"], classification["liquidity_change_pct"],
                classification["volume_liquidity_ratio"], classification["buy_sell_ratio"],
                classification["liquidity_tier"], classification["structure_state"],
                classification["structure_confidence"],
                json.dumps(classification["structure_details"]),
                json.dumps(classification["horizon_metrics"]),
                json.dumps(classification["wallet_quality"]),
                json.dumps(integrity if snapshot["is_anomaly"] else {}),
                json.dumps(integrity if snapshot["is_provider_unavailable"] else {}),
                None if (snapshot["is_anomaly"] or snapshot["is_provider_unavailable"])
                else classification["status"],
                bool(snapshot.get("is_benchmark")),
                None if (snapshot["is_anomaly"] or snapshot["is_provider_unavailable"])
                else snapshot_id,
                snapshot["data_quality"],
            ))
            cur.execute("""
                UPDATE evm_token_signals
                SET pending_status = %s, pending_status_count = %s,
                    raw_status = %s, raw_status_count = %s,
                    alert_confirmation_count = %s
                WHERE chain = %s AND token_address = %s
            """, (
                classification.get("pending_status"),
                classification.get("pending_status_count", 0),
                classification.get("raw_status", classification["status"]),
                classification.get("raw_status_count", 1),
                classification.get("alert_confirmation_count", 0),
                snapshot["chain"], snapshot["token_address"],
            ))
            history_id = None
            history_previous_status = (
                previous_trusted_status
                if previous_status == "EVM_PROVIDER_UNAVAILABLE"
                else previous_status
            )
            if (
                not snapshot["is_provider_unavailable"]
                and history_previous_status != classification["status"]
            ):
                cur.execute("""
                    INSERT INTO evm_signal_history (
                        chain, token_address, token_symbol, previous_status, status,
                        momentum_score, risk_score, details
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (
                    snapshot["chain"], snapshot["token_address"], snapshot["token_symbol"],
                    history_previous_status, classification["status"], classification["momentum_score"],
                    classification["risk_score"], json.dumps({
                        **classification, "snapshot_id": snapshot_id,
                        "data_quality": snapshot["data_quality"],
                        "provider_errors": snapshot.get("provider_errors") or [],
                        "recovered_from_provider_outage": recovering_from_provider_outage,
                    }),
                ))
                history_id = cur.fetchone()[0]
            if (
                not snapshot["is_anomaly"]
                and not snapshot["is_provider_unavailable"]
                and snapshot.get("pair_lock_candidate")
                and snapshot.get("pair_address")
            ):
                cur.execute("""
                    UPDATE token_watchlist
                    SET canonical_pair_address = %s,
                        canonical_pair_dex_id = %s,
                        pair_locked_at = COALESCE(pair_locked_at, NOW()),
                        updated_at = NOW()
                    WHERE chain = %s AND LOWER(token_address) = LOWER(%s)
                        AND canonical_pair_address IS NULL
                """, (
                    snapshot["pair_address"], snapshot.get("dex_id"),
                    snapshot["chain"], snapshot["token_address"],
                ))
            cur.execute("""
                UPDATE token_watchlist SET monitoring_status = %s, updated_at = NOW()
                WHERE chain = %s AND token_address = %s
            """, (
                "data_anomaly_quarantined" if snapshot["is_anomaly"]
                else "provider_temporarily_unavailable"
                if snapshot["is_provider_unavailable"]
                else "live_evm_monitoring" if snapshot["data_quality"] == "complete"
                else "partial_evm_monitoring",
                snapshot["chain"], snapshot["token_address"],
            ))
        conn.commit()

    result = {
        **classification, **snapshot, "snapshot_id": snapshot_id,
        "previous_status": previous_status, "history_id": history_id,
        "recovered_from_provider_outage": recovering_from_provider_outage,
    }
    notification_history_id = history_id
    if (
        not notification_history_id
        and classification["status"] in EVM_ALERT_STATUSES
        and classification.get("alert_confirmation_count", 0) >= EVM_ALERT_CONFIRMATIONS
    ):
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM evm_signal_history
                    WHERE chain = %s AND token_address = %s AND status = %s
                    ORDER BY id DESC LIMIT 1
                """, (
                    snapshot["chain"], snapshot["token_address"],
                    classification["status"],
                ))
                notification_row = cur.fetchone()
                notification_history_id = notification_row[0] if notification_row else None
    if (
        not snapshot["is_anomaly"] and not snapshot["is_provider_unavailable"]
        and not recovering_from_provider_outage and previous_status
        and notification_history_id
        and classification["status"] in EVM_ALERT_STATUSES
        and classification.get("alert_confirmation_count", 0) >= EVM_ALERT_CONFIRMATIONS
    ):
        try:
            notification = queue_and_deliver_evm_notification(
                result, f"evm-signal-confirmed:{notification_history_id}"
            )
            result["notification_status"] = notification.get("status")
        except Exception:
            diagnostic_increment("telegram_failures")
            result["notification_status"] = "failed_without_blocking_refresh"
    else:
        result["notification_status"] = "baseline_or_no_alert_transition"
    return result


def refresh_evm_watchlist(limit=10, offset=0):
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT w.chain, w.token_address, w.token_symbol, w.token_name,
                    w.canonical_pair_address, w.canonical_pair_dex_id, w.source
                FROM token_watchlist w
                WHERE w.active = TRUE AND w.chain IN ('robinhood', 'base')
                    AND w.token_address LIKE '0x%%'
                ORDER BY (
                    SELECT MAX(snapshot.captured_at)
                    FROM evm_token_snapshots snapshot
                    WHERE snapshot.chain = w.chain
                        AND LOWER(snapshot.token_address) = LOWER(w.token_address)
                        AND snapshot.is_anomaly = FALSE
                        AND snapshot.is_provider_unavailable = FALSE
                ) ASC NULLS FIRST, w.chain, w.token_symbol
                LIMIT %s OFFSET %s
            """, (limit, offset))
            tokens = cur.fetchall()
            cur.execute("""
                INSERT INTO evm_refresh_runs (tokens_selected)
                VALUES (%s) RETURNING id
            """, (len(tokens),))
            run_id = cur.fetchone()[0]
        conn.commit()

    deadline = time.monotonic() + EVM_REFRESH_MAX_SECONDS
    results = []
    transitions = 0
    stopped_reason = None
    executor = ThreadPoolExecutor(max_workers=min(EVM_FETCH_WORKERS, len(tokens) or 1))
    futures = {executor.submit(fetch_evm_token_snapshot, token): token for token in tokens}
    try:
        for future in as_completed(futures):
            token = futures[future]
            if time.monotonic() >= deadline:
                stopped_reason = "deadline_guard"
                break
            try:
                snapshot = future.result()
                if snapshot["data_quality"] == "unavailable":
                    diagnostic_increment("evm_refresh_failures")
                # Database writes remain single-threaded even though provider
                # reads are concurrent.
                result = persist_evm_snapshot(snapshot)
                transitions += int(result.get("history_id") is not None)
                results.append(result)
            except Exception as exc:
                diagnostic_increment("evm_refresh_failures")
                results.append({
                    "chain": token[0], "token_address": token[1],
                    "token_symbol": token[2], "success": False,
                    "error": type(exc).__name__,
                })
    finally:
        for future in futures:
            if not future.done():
                future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)

    deferred = max(len(tokens) - len(results), 0)
    failures = sum(item.get("success") is False for item in results)
    if deferred:
        status = "partial_deadline"
        stopped_reason = stopped_reason or "deadline_guard"
    elif failures:
        status = "partial_error"
    else:
        status = "complete"
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE evm_refresh_runs SET completed_at = NOW(),
                    snapshots_created = %s, transitions_created = %s,
                    status = %s, details = %s WHERE id = %s
            """, (
                sum(1 for item in results if item.get("snapshot_id")), transitions,
                status, json.dumps({
                    "stopped_reason": stopped_reason,
                    "processed": len(results), "deferred": deferred,
                    "failures": failures, "fetch_workers": EVM_FETCH_WORKERS,
                    "deferred_tokens_are_prioritised_next_run": True,
                }), run_id,
            ))
        conn.commit()
    diagnostic_increment("evm_refreshes")
    chain_counts = {}
    for item in results:
        chain = item.get("chain") or "unknown"
        chain_counts[chain] = chain_counts.get(chain, 0) + 1
    return {
        "success": status == "complete", "run_id": run_id,
        "selected": len(tokens), "processed": len(results),
        "deferred": deferred, "failures": failures, "status": status,
        "transitions": transitions, "stopped_reason": stopped_reason,
        "chain_counts": chain_counts,
        "results": results,
    }


def parse_helius_activity(payload, tracked_wallets):
    """Convert enhanced Helius SWAP transactions into bounded wallet-token deltas."""
    transactions = payload if isinstance(payload, list) else [payload]
    events = []
    for transaction in transactions:
        if not isinstance(transaction, dict):
            continue
        if str(transaction.get("type") or "").upper() != "SWAP":
            continue
        signature = transaction.get("signature")
        if not isinstance(signature, str) or not signature:
            continue
        timestamp = transaction.get("timestamp")
        if not isinstance(timestamp, (int, float)):
            continue
        occurred_at = datetime.fromtimestamp(timestamp, timezone.utc)
        transfers = transaction.get("tokenTransfers") or []
        for wallet, tracked in tracked_wallets.items():
            eligible_from = tracked.get("eligible_from")
            if eligible_from and occurred_at < eligible_from:
                continue
            deltas = {}
            symbols = {}
            for transfer in transfers:
                if not isinstance(transfer, dict):
                    continue
                mint = transfer.get("mint")
                if not is_valid_solana_address(mint) or mint in STABLE_MINTS or mint == SOL_MINT:
                    continue
                symbol = transfer.get("symbol") or transfer.get("tokenSymbol")
                if isinstance(symbol, str) and symbol.upper() in EXCLUDED_SIGNAL_SYMBOLS:
                    continue
                amount = transfer.get("tokenAmount")
                try:
                    amount = float(amount)
                except (TypeError, ValueError):
                    continue
                if transfer.get("toUserAccount") == wallet:
                    deltas[mint] = deltas.get(mint, 0.0) + amount
                if transfer.get("fromUserAccount") == wallet:
                    deltas[mint] = deltas.get(mint, 0.0) - amount
                if isinstance(symbol, str) and symbol:
                    symbols[mint] = symbol
            for mint, delta in deltas.items():
                if abs(delta) <= 0:
                    continue
                if mint in tracked.get("excluded_tokens", set()):
                    continue
                events.append({
                    "signature": signature, "wallet": wallet,
                    "cluster_id": tracked["cluster_id"],
                    "token_address": mint, "token_symbol": symbols.get(mint),
                    "side": "BUY" if delta > 0 else "SELL",
                    "token_amount": abs(delta), "occurred_at": occurred_at,
                    "raw_summary": {
                        "helius_type": transaction.get("type"),
                        "source": transaction.get("source"),
                        "fee_payer": transaction.get("feePayer"),
                    },
                })
    return events


def persist_wallet_activity(events):
    inserted = 0
    with db() as conn:
        with conn.cursor() as cur:
            for event in events:
                cur.execute("""
                    INSERT INTO wallet_activity (
                        signature, wallet, cluster_id, token_address, token_symbol,
                        side, token_amount, occurred_at, raw_summary
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (signature, wallet, token_address, side) DO NOTHING
                """, (
                    event["signature"], event["wallet"], event["cluster_id"],
                    event["token_address"], event.get("token_symbol"), event["side"],
                    event.get("token_amount"), event["occurred_at"],
                    json.dumps(event.get("raw_summary") or {}),
                ))
                inserted += cur.rowcount
        conn.commit()
    return inserted


def persist_probation_wallet_activity(events):
    """Store forward-only probation evidence outside the consensus ledger."""
    inserted = 0
    affected_wallets = set()
    with db() as conn:
        with conn.cursor() as cur:
            for event in events:
                cur.execute("""
                    INSERT INTO wallet_probation_activity (
                        signature, wallet, token_address, token_symbol, side,
                        token_amount, occurred_at, raw_summary
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (signature, wallet, token_address, side) DO NOTHING
                """, (
                    event["signature"], event["wallet"], event["token_address"],
                    event.get("token_symbol"), event["side"],
                    event.get("token_amount"), event["occurred_at"],
                    json.dumps(event.get("raw_summary") or {}),
                ))
                if cur.rowcount:
                    inserted += 1
                    affected_wallets.add(event["wallet"])
            for wallet in affected_wallets:
                cur.execute("""
                    UPDATE wallet_discovery_cohorts SET
                        forward_trades = (
                            SELECT COUNT(DISTINCT signature)
                            FROM wallet_probation_activity WHERE wallet = %s
                        ),
                        forward_tokens = (
                            SELECT COUNT(DISTINCT token_address)
                            FROM wallet_probation_activity WHERE wallet = %s
                        ),
                        last_forward_activity_at = (
                            SELECT MAX(occurred_at)
                            FROM wallet_probation_activity WHERE wallet = %s
                        ),
                        updated_at = NOW()
                    WHERE wallet = %s AND cohort_status = 'PROBATION'
                """, (wallet, wallet, wallet, wallet))
        conn.commit()
    if inserted:
        diagnostic_increment("probation_wallet_events")
    return inserted


def expire_stale_paper_signals():
    """Expire elapsed Solana signals without providers, alerts, or refresh work."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=SIGNAL_WINDOW_MINUTES)
    with db() as conn:
        with conn.cursor() as cur:
            # UPDATE ... RETURNING makes the sweep safe when dashboard and API
            # reads arrive together: only the request that changes a row records
            # its expiry transition.
            cur.execute("""
                UPDATE paper_signals SET status = 'EXPIRED', actionable = FALSE,
                    updated_at = NOW()
                WHERE last_activity_at < %s AND status <> 'EXPIRED'
                RETURNING token_address, token_symbol, buy_score, sell_score,
                    independent_buy_clusters, independent_sell_clusters
            """, (cutoff,))
            expired_rows = cur.fetchall()
            for expired in expired_rows:
                cur.execute("""
                    INSERT INTO paper_signal_history (
                        token_address, token_symbol, status, buy_score, sell_score,
                        independent_buy_clusters, independent_sell_clusters, details
                    ) VALUES (%s, %s, 'EXPIRED', %s, %s, %s, %s, %s)
                """, (
                    expired[0], expired[1], expired[2], expired[3],
                    expired[4], expired[5], json.dumps({
                        "reason": "signal_window_elapsed",
                        "trigger": "read_safe_expiry_sweep",
                    }),
                ))
        conn.commit()
    return len(expired_rows)


def refresh_paper_signals():
    """Aggregate recent events with one maximum-weight vote per wallet cluster."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=SIGNAL_WINDOW_MINUTES)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.token_address, a.token_symbol, a.wallet, c.cluster_id,
                    a.side, a.occurred_at, c.signal_weight
                FROM wallet_activity a
                JOIN wallet_clusters c ON c.wallet = a.wallet
                WHERE a.occurred_at >= %s
                ORDER BY a.token_address, a.occurred_at ASC
            """, (cutoff,))
            rows = cur.fetchall()

    grouped = {}
    for token, symbol, wallet, cluster_id, side, occurred_at, weight in rows:
        item = grouped.setdefault(token, {
            "token_address": token, "token_symbol": symbol,
            "first_activity_at": occurred_at, "last_activity_at": occurred_at,
            "BUY": {}, "SELL": {}, "wallets": set(),
        })
        if symbol and not item.get("token_symbol"):
            item["token_symbol"] = symbol
        item["first_activity_at"] = min(item["first_activity_at"], occurred_at)
        item["last_activity_at"] = max(item["last_activity_at"], occurred_at)
        item["wallets"].add(wallet)
        prior = item[side].get(cluster_id, 0)
        item[side][cluster_id] = max(prior, float(weight or 0))

    metadata_priority = prioritise_solana_metadata(grouped)
    metadata = enrich_solana_token_metadata(metadata_priority)
    for token, item in grouped.items():
        enriched = metadata.get(token) or {}
        if not item.get("token_symbol") and enriched.get("token_symbol"):
            item["token_symbol"] = enriched["token_symbol"]
        item["safety_status"] = enriched.get("safety_status") or "unverified"

    refreshed = []
    notification_transitions = []
    with db() as conn:
        with conn.cursor() as cur:
            for token, item in grouped.items():
                buy_score = round(sum(item["BUY"].values()), 4)
                sell_score = round(sum(item["SELL"].values()), 4)
                buy_clusters = len(item["BUY"])
                sell_clusters = len(item["SELL"])
                if sell_clusters >= 2 and sell_score >= max(1.5, buy_score * 0.75):
                    status = "INVALIDATED"
                elif buy_clusters >= 3 and buy_score >= 2.0:
                    status = "PAPER_CONFIRMED"
                elif buy_clusters >= 2:
                    status = "BUILDING"
                else:
                    status = "OBSERVE"
                wallets = sorted(item["wallets"])
                clusters = sorted(set(item["BUY"]) | set(item["SELL"]))
                cur.execute("SELECT status FROM paper_signals WHERE token_address = %s", (token,))
                prior = cur.fetchone()
                cur.execute("""
                    INSERT INTO paper_signals (
                        token_address, token_symbol, status, buy_score, sell_score,
                        independent_buy_clusters, independent_sell_clusters,
                        contributing_wallets, contributing_clusters,
                        first_activity_at, last_activity_at, safety_status,
                        actionable, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, FALSE, NOW())
                    ON CONFLICT (token_address) DO UPDATE SET
                        token_symbol = COALESCE(EXCLUDED.token_symbol, paper_signals.token_symbol),
                        status = EXCLUDED.status, buy_score = EXCLUDED.buy_score,
                        sell_score = EXCLUDED.sell_score,
                        independent_buy_clusters = EXCLUDED.independent_buy_clusters,
                        independent_sell_clusters = EXCLUDED.independent_sell_clusters,
                        contributing_wallets = EXCLUDED.contributing_wallets,
                        contributing_clusters = EXCLUDED.contributing_clusters,
                        first_activity_at = EXCLUDED.first_activity_at,
                        last_activity_at = EXCLUDED.last_activity_at,
                        safety_status = EXCLUDED.safety_status,
                        actionable = FALSE, updated_at = NOW()
                """, (
                    token, item.get("token_symbol"), status, buy_score, sell_score,
                    buy_clusters, sell_clusters, json.dumps(wallets),
                    json.dumps(clusters), item["first_activity_at"], item["last_activity_at"],
                    item.get("safety_status", "unverified"),
                ))
                if not prior or prior[0] != status:
                    cur.execute("""
                        INSERT INTO paper_signal_history (
                            token_address, token_symbol, status, buy_score, sell_score,
                            independent_buy_clusters, independent_sell_clusters, details
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        token, item.get("token_symbol"), status, buy_score, sell_score,
                        buy_clusters, sell_clusters,
                        json.dumps({"wallets": wallets, "clusters": clusters}),
                    ))
                    history_id = cur.fetchone()[0]
                    if status in TELEGRAM_ALERT_STATUSES:
                        notification_transitions.append({
                            "history_id": history_id,
                            "token_address": token,
                            "token_symbol": item.get("token_symbol"),
                            "status": status,
                            "buy_score": buy_score,
                            "sell_score": sell_score,
                            "independent_buy_clusters": buy_clusters,
                            "independent_sell_clusters": sell_clusters,
                        })
                refreshed.append({
                    "token_address": token, "token_symbol": item.get("token_symbol"),
                    "status": status, "buy_score": buy_score, "sell_score": sell_score,
                    "independent_buy_clusters": buy_clusters,
                    "independent_sell_clusters": sell_clusters,
                    "display_status": solana_status_label(
                        status, buy_clusters, sell_clusters
                    ),
                    "actionable": False,
                    "safety_status": item.get("safety_status", "unverified"),
                })

        conn.commit()

    expire_stale_paper_signals()

    for transition in notification_transitions:
        try:
            queue_and_deliver_signal_notification(
                transition,
                f"signal-history:{transition['history_id']}",
            )
        except Exception:
            # Signal persistence and Helius acknowledgement must not fail because
            # a downstream notification provider or audit write is unavailable.
            diagnostic_increment("telegram_failures")
    return refreshed


@app.get("/shortlist")
def shortlist():
    """
    Return classified candidates using persisted score, screening,
    validation, and discovery evidence.
    """
    initialise_database()
    try:
        limit = int(request.args.get("limit", 100))
    except ValueError:
        limit = 100
    limit = min(max(limit, 1), 500)

    validation_summaries = load_validation_summaries()
    repeat_evidence = load_repeat_evidence()

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.wallet, c.tokens_found, c.realized_pnl_30d,
                    c.total_pnl_30d, c.win_rate_30d, c.trades_30d,
                    c.total_invested_30d, c.score, c.score_status,
                    c.screening_status, c.screening_risk_score,
                    c.validation_status, c.last_scored, c.last_screened,
                    c.last_validated, ws.risk_flags
                FROM candidate_wallets c
                LEFT JOIN wallet_screenings ws ON ws.wallet = c.wallet
                    AND ws.screening_version = %s
                ORDER BY c.score DESC, c.realized_pnl_30d DESC NULLS LAST
                LIMIT %s
            """, (SCREENING_VERSION, limit))
            rows = cur.fetchall()

    items = []
    counts = {"WATCH": 0, "ASYMMETRIC": 0, "REVIEW": 0, "REJECT": 0}

    for row in rows:
        wallet = row[0]
        try:
            screening_flags = json.loads(row[15]) if row[15] else []
        except (TypeError, ValueError):
            screening_flags = []

        candidate = {
            "wallet": wallet,
            "tokens_found": row[1],
            "realized_pnl": row[2],
            "total_pnl": row[3],
            "win_rate": row[4],
            "trades": row[5],
            "invested": row[6],
            "score": row[7],
            "score_status": row[8],
            "screening_status": row[9],
            "validation_status": row[11],
        }
        screening = {
            "risk_score": row[10],
            "risk_flags": screening_flags,
        }
        validation = validation_summaries.get(wallet)
        evidence = repeat_evidence.get(wallet, {})
        classification = classify_candidate(
            candidate, screening, validation, evidence
        )
        counts[classification["classification"]] += 1

        items.append({
            "wallet": wallet,
            "classification": classification["classification"],
            "confidence": classification["confidence"],
            "watch_eligible": classification["watch_eligible"],
            "signal_eligible": classification["signal_eligible"],
            "reasons": classification["reasons"],
            "score": row[7],
            "realized_pnl_30d": row[2],
            "win_rate_30d": row[4],
            "trades_30d": row[5],
            "capital_efficiency": classification["capital_efficiency"],
            "screening_risk_score": row[10],
            "screening_risk_flags": screening_flags,
            "validation_status": row[11],
            "validation_summary": validation,
            "discovery_evidence": evidence,
            "last_scored": row[12],
            "last_screened": row[13],
            "last_validated": row[14],
        })

    class_order = {"WATCH": 0, "ASYMMETRIC": 1, "REVIEW": 2, "REJECT": 3}
    confidence_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    items.sort(key=lambda item: (
        class_order[item["classification"]],
        confidence_order[item["confidence"]],
        item["screening_risk_score"] if item["screening_risk_score"] is not None else 999,
        -(item["score"] or 0),
        -(item["realized_pnl_30d"] or 0),
    ))

    return jsonify({
        "success": True,
        "version": VERSION,
        "classification_policy": {
            "WATCH": (
                "Low Helius risk plus strong token validation, at least five "
                "material tokens, >=60% material-token profitability, healthy "
                "profit factor, meaningful trade history, and positive realized PnL."
            ),
            "ASYMMETRIC": (
                "Low Helius risk plus strong validation and >=5 material tokens; "
                "30-60% material-token profitability but material profit factor >=5. "
                "Kept separate from WATCH for future lower-weight signal use."
            ),
            "REVIEW": "No hard reject signal, but WATCH/ASYMMETRIC evidence is incomplete or mixed.",
            "REJECT": "Hard risk or performance exclusion triggered.",
            "note": (
                "WATCH is the primary future consensus cohort. ASYMMETRIC is a "
                "separate lower-consistency/high-payoff cohort and must not be treated "
                "as equivalent to WATCH. Neither is a recommendation to buy or copy a wallet."
            ),
        },
        "counts": counts,
        "candidates": items,
    })

# =========================================================
# CANDIDATES / DISCOVERY HISTORY
# =========================================================

@app.get("/candidates")
def candidates():
    initialise_database()
    try:
        limit = int(request.args.get("limit", 100))
    except ValueError:
        limit = 100
    limit = min(max(limit, 1), 500)

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wallet, tokens_found, realized_pnl_30d, total_pnl_30d,
                    win_rate_30d, trades_30d, total_invested_30d, score,
                    score_status, created_at, last_scored, screening_status,
                    screening_risk_score, last_screened, validation_status,
                    last_validated
                FROM candidate_wallets
                ORDER BY score DESC, tokens_found DESC, realized_pnl_30d DESC NULLS LAST
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()

    items = []
    for row in rows:
        scoring = calculate_score(row[2], row[3], row[4], row[5], row[1], row[6])
        items.append({
            "wallet": row[0], "tokens_found": row[1],
            "realized_pnl_30d": row[2], "total_pnl_30d": row[3],
            "win_rate_30d": row[4], "trades_30d": row[5],
            "total_invested_30d": row[6], "score": row[7],
            "score_status": row[8], "created_at": row[9], "last_scored": row[10],
            "screening_status": row[11], "screening_risk_score": row[12],
            "last_screened": row[13],
            "validation_status": row[14], "last_validated": row[15],
            "capital_efficiency": scoring["capital_efficiency"],
            "risk_flags": scoring["flags"] if row[8] == "scored" else [],
        })
    return jsonify({"success": True, "count": len(items), "candidates": items})


@app.get("/discovery-history")
def discovery_history():
    initialise_database()
    try:
        limit = int(request.args.get("limit", 20))
    except ValueError:
        limit = 20
    limit = min(max(limit, 1), 100)

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, started_at, completed_at, tokens_examined,
                    wallets_found, api_429s, status
                FROM discovery_runs ORDER BY id DESC LIMIT %s
            """, (limit,))
            rows = cur.fetchall()

    return jsonify({
        "success": True,
        "runs": [{
            "id": row[0], "started_at": row[1], "completed_at": row[2],
            "tokens_examined": row[3], "wallets_found": row[4],
            "api_429s": row[5], "status": row[6],
        } for row in rows],
    })


# =========================================================
# V4.7 RELATIONSHIP-AWARE PAPER SIGNALS
# =========================================================

@app.post("/refresh-wallet-clusters")
def refresh_wallet_clusters_endpoint():
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    initialise_database()
    eligible, clusters, edges, infrastructure_count = compute_and_persist_wallet_clusters()
    sync_result = None
    if HELIUS_AUTO_SYNC:
        sync_result = synchronise_helius_webhook(dry_run=False, force=False)
    return jsonify({
        "success": True, "version": VERSION, "eligible_wallets": len(eligible),
        "independent_clusters": len(clusters), "material_relationships": len(edges),
        "infrastructure_addresses_excluded": infrastructure_count,
        "helius_sync": sync_result,
        "paper_mode": True, "actionable": False,
    })


@app.post("/sync-helius-webhook")
def sync_helius_webhook_endpoint():
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    initialise_database()
    dry_run = request.args.get("dry_run", "true").lower() not in {
        "0", "false", "no", "off",
    }
    force = request.args.get("force", "false").lower() in {
        "1", "true", "yes", "on",
    }
    result = synchronise_helius_webhook(dry_run=dry_run, force=force)
    return jsonify({**result, "paper_mode": True, "actionable": False}), (
        200 if result.get("success") else 409
    )


@app.get("/helius-sync-status")
def helius_sync_status_endpoint():
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT dry_run, sync_status, desired_wallet_count,
                    current_wallet_count, response_code, error_message, created_at
                FROM helius_sync_history ORDER BY id DESC LIMIT 1
            """)
            row = cur.fetchone()
    last_sync = None if not row else {
        "dry_run": row[0], "sync_status": row[1],
        "desired_wallet_count": row[2], "current_wallet_count": row[3],
        "response_code": row[4], "error_message": row[5], "created_at": row[6],
    }
    return jsonify({
        "success": True, "auto_sync": HELIUS_AUTO_SYNC,
        "minimum_wallet_guard": HELIUS_MIN_SYNC_WALLETS,
        "maximum_removal_fraction": HELIUS_MAX_REMOVAL_FRACTION,
        "last_sync": last_sync, "paper_mode": True,
    })


@app.get("/tracked-wallets")
def tracked_wallets_endpoint():
    initialise_database()
    tracked = load_tracked_wallet_map()
    items = [{"wallet": wallet, **details} for wallet, details in sorted(tracked.items())]
    return jsonify({
        "success": True, "count": len(items), "wallets": items,
        "refresh_required": not bool(items), "paper_mode": True,
    })


@app.get("/watchlist")
def watchlist_endpoint():
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT chain, token_address, token_symbol, token_name, source,
                    monitoring_status, active, added_at, updated_at,
                    canonical_pair_address, canonical_pair_dex_id, pair_locked_at
                FROM token_watchlist
                WHERE active = TRUE
                ORDER BY chain, token_symbol
            """)
            rows = cur.fetchall()
    items = []
    for row in rows:
        explorer_url = None
        config = EVM_CHAIN_CONFIG.get(row[0])
        if config and row[1].startswith("0x"):
            explorer_url = config["explorer_url"].format(address=row[1])
        items.append({
            "chain": row[0], "chain_id": config["chain_id"] if config else None,
            "chain_label": config["label"] if config else row[0].title(),
            "token_address": row[1], "token_symbol": row[2],
            "token_name": row[3], "source": row[4],
            "monitoring_status": row[5], "active": row[6],
            "explorer_url": explorer_url, "added_at": row[7], "updated_at": row[8],
            "canonical_pair_address": row[9], "canonical_pair_dex_id": row[10],
            "pair_locked_at": row[11],
        })
    return jsonify({
        "success": True, "count": len(items), "tokens": items,
        "note": "V4.12 retains the expanded watchlist and adds evidence calibration, holder freshness and ETH-relative performance.",
        "paper_mode": True, "actionable": False,
    })


@app.post("/watchlist")
def add_watchlist_token_endpoint():
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    initialise_database()
    body = request.get_json(silent=True) or {}
    chain = str(body.get("chain") or "").strip().lower()
    address = str(body.get("token_address") or "").strip()
    symbol = str(body.get("token_symbol") or "").strip().upper()
    name = str(body.get("token_name") or symbol).strip()
    valid_evm_address = (
        len(address) == 42 and address.startswith("0x")
        and all(character in "0123456789abcdefABCDEF" for character in address[2:])
    )
    if chain not in EVM_CHAIN_CONFIG:
        return jsonify({
            "success": False, "error": "Unsupported EVM chain",
            "supported_chains": list(SUPPORTED_EVM_CHAINS),
        }), 400
    if not symbol or not (valid_evm_address or address.startswith("native:")):
        return jsonify({"success": False, "error": "Valid chain, token symbol and token address are required"}), 400
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO token_watchlist (
                    chain, token_address, token_symbol, token_name, source,
                    monitoring_status, active, updated_at
                ) VALUES (%s, %s, %s, %s, 'manual', 'evm_monitoring_ready', TRUE, NOW())
                ON CONFLICT (chain, token_address) DO UPDATE SET
                    token_symbol = EXCLUDED.token_symbol,
                    token_name = EXCLUDED.token_name,
                    source = 'manual', monitoring_status = 'evm_monitoring_ready',
                    active = TRUE, updated_at = NOW()
            """, (chain, address, symbol, name))
        conn.commit()
    return jsonify({
        "success": True, "chain": chain, "token_address": address,
        "token_symbol": symbol, "monitoring_status": "evm_monitoring_ready",
        "paper_mode": True, "actionable": False,
    })


@app.post("/evm-canonical-pair")
def set_evm_canonical_pair_endpoint():
    """Admin-only pair lock/reset for a verified pool migration."""
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    initialise_database()
    body = request.get_json(silent=True) or {}
    chain = str(body.get("chain") or "").strip().lower()
    token_address = str(body.get("token_address") or "").strip()
    pair_address = str(body.get("pair_address") or "").strip()
    dex_id = str(body.get("dex_id") or "").strip() or None
    reset = bool(body.get("reset"))
    valid_token = (
        len(token_address) == 42 and token_address.startswith("0x")
        and all(character in "0123456789abcdefABCDEF" for character in token_address[2:])
    )
    valid_pair = (
        pair_address.startswith("0x") and len(pair_address) in {42, 66}
        and all(character in "0123456789abcdefABCDEF" for character in pair_address[2:])
    )
    if chain not in EVM_CHAIN_CONFIG or not valid_token:
        return jsonify({"success": False, "error": "Valid chain and token_address are required"}), 400
    if not reset and not valid_pair:
        return jsonify({"success": False, "error": "A 20-byte or 32-byte pair_address is required"}), 400
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE token_watchlist
                SET canonical_pair_address = %s,
                    canonical_pair_dex_id = %s,
                    pair_locked_at = CASE WHEN %s IS NULL THEN NULL ELSE NOW() END,
                    monitoring_status = 'evm_monitoring_ready', updated_at = NOW()
                WHERE chain = %s AND LOWER(token_address) = LOWER(%s)
                RETURNING token_symbol
            """, (
                None if reset else pair_address, None if reset else dex_id,
                None if reset else pair_address, chain, token_address,
            ))
            row = cur.fetchone()
        conn.commit()
    if not row:
        return jsonify({"success": False, "error": "Token is not on the active watchlist"}), 404
    return jsonify({
        "success": True, "chain": chain, "token_address": token_address,
        "token_symbol": row[0],
        "canonical_pair_address": None if reset else pair_address,
        "canonical_pair_dex_id": None if reset else dex_id,
        "pair_lock_status": "reset" if reset else "locked",
        "paper_mode": True, "actionable": False,
    })


EVM_SIGNAL_SELECT = """
    SELECT s.chain, s.token_address, s.token_symbol, s.status,
        s.momentum_score, s.risk_score, s.reasons,
        s.holder_change_pct, s.liquidity_change_pct,
        s.volume_liquidity_ratio, s.buy_sell_ratio,
        s.data_quality, s.updated_at, p.price_usd, p.liquidity_usd,
        p.market_cap_usd, p.volume_h1_usd, p.price_change_h1_pct,
        p.holder_count, p.pair_address, p.captured_at,
        s.liquidity_tier, s.structure_state, s.structure_confidence,
        s.structure_details, s.horizon_metrics, s.wallet_quality,
        s.anomaly_details, s.availability_details, s.last_trusted_status,
        s.is_benchmark, s.pending_status, s.pending_status_count,
        s.raw_status, s.raw_status_count, s.alert_confirmation_count
    FROM evm_token_signals s
    LEFT JOIN evm_token_snapshots p ON p.id = s.latest_snapshot_id
"""


def serialize_evm_signal(row):
    try:
        reasons = json.loads(row[6] or "[]")
    except (TypeError, ValueError):
        reasons = []
    def parsed_json(index, fallback):
        try:
            return json.loads(row[index] or json.dumps(fallback))
        except (TypeError, ValueError):
            return fallback
    config = EVM_CHAIN_CONFIG.get(row[0], {})
    dexscreener_url = (
        f"https://dexscreener.com/{row[0]}/{row[19]}" if row[19] else None
    )
    trusted_snapshot_age_seconds = None
    if row[20]:
        captured_at = row[20]
        if captured_at.tzinfo is None:
            captured_at = captured_at.replace(tzinfo=timezone.utc)
        trusted_snapshot_age_seconds = max(
            int((datetime.now(timezone.utc) - captured_at).total_seconds()), 0
        )
    return {
        "chain": row[0], "chain_id": config.get("chain_id"),
        "chain_label": config.get("label", str(row[0]).title()),
        "token_address": row[1], "token_symbol": row[2], "status": row[3],
        "momentum_score": row[4], "risk_score": row[5], "reasons": reasons,
        "holder_change_pct": row[7], "liquidity_change_pct": row[8],
        "volume_liquidity_ratio": row[9], "buy_sell_ratio": row[10],
        "data_quality": row[11], "updated_at": row[12], "price_usd": row[13],
        "liquidity_usd": row[14], "market_cap_usd": row[15],
        "volume_h1_usd": row[16], "price_change_h1_pct": row[17],
        "holder_count": row[18], "pair_address": row[19],
        "dexscreener_url": dexscreener_url,
        "captured_at": row[20], "paper_mode": True, "actionable": False,
        "liquidity_tier": row[21] or "UNKNOWN",
        "structure_state": row[22] or "COLLECTING",
        "structure_confidence": row[23] or 0,
        "structure_details": parsed_json(24, {}),
        "horizon_metrics": parsed_json(25, {}),
        "wallet_quality": parsed_json(26, {}),
        "anomaly_details": parsed_json(27, {}),
        "availability_details": parsed_json(28, {}),
        "last_trusted_status": row[29],
        "is_benchmark": bool(row[30]),
        "pending_status": row[31], "pending_status_count": row[32] or 0,
        "raw_status": row[33], "raw_status_count": row[34] or 0,
        "alert_confirmation_count": row[35] or 0,
        "visible_state_confirmations_required": EVM_VISIBLE_STATE_CONFIRMATIONS,
        "alert_confirmations_required": EVM_ALERT_CONFIRMATIONS,
        "using_last_trusted_snapshot": row[3] in {
            "EVM_DATA_ANOMALY", "EVM_PROVIDER_UNAVAILABLE",
        },
        "trusted_snapshot_age_seconds": trusted_snapshot_age_seconds,
    }


@app.post("/refresh-evm-watchlist")
def refresh_evm_watchlist_endpoint():
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    try:
        limit = min(max(int(request.args.get("limit", 10)), 1), 10)
        offset = max(int(request.args.get("offset", 0)), 0)
    except ValueError:
        return jsonify({"success": False, "error": "limit and offset must be integers"}), 400
    result = refresh_evm_watchlist(limit=limit, offset=offset)
    return jsonify({
        **result, "version": VERSION, "deadline_seconds": EVM_REFRESH_MAX_SECONDS,
        "paper_mode": True, "actionable": False,
        "note": "First observations establish a baseline; alerts require a later state change.",
    }), 200 if result["success"] else 207


@app.get("/evm-status")
def evm_status_endpoint():
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status, COUNT(*) FROM evm_token_signals GROUP BY status
            """)
            counts = {row[0]: row[1] for row in cur.fetchall()}
            cur.execute("""
                SELECT chain, status, COUNT(*) FROM evm_token_signals
                GROUP BY chain, status ORDER BY chain, status
            """)
            chain_signal_counts = {}
            for chain, status, count in cur.fetchall():
                chain_signal_counts.setdefault(chain, {})[status] = count
            cur.execute("""
                SELECT id, started_at, completed_at, tokens_selected,
                    snapshots_created, transitions_created, status, details
                FROM evm_refresh_runs ORDER BY id DESC LIMIT 1
            """)
            row = cur.fetchone()
            cur.execute("""
                SELECT chain, COUNT(*) FROM token_watchlist
                WHERE active = TRUE AND chain IN ('robinhood', 'base')
                    AND token_address LIKE '0x%%'
                GROUP BY chain
            """)
            tracked_by_chain = {chain: count for chain, count in cur.fetchall()}
            tracked_contracts = sum(tracked_by_chain.values())
    latest_run = None if not row else {
        "id": row[0], "started_at": row[1], "completed_at": row[2],
        "tokens_selected": row[3], "snapshots_created": row[4],
        "transitions_created": row[5], "status": row[6],
        "details": json.loads(row[7] or "{}"),
    }
    return jsonify({
        "success": True, "version": VERSION, "chain": "multi", "chain_id": None,
        "chain_ids": {
            chain: config["chain_id"] for chain, config in EVM_CHAIN_CONFIG.items()
        },
        "chains": {
            chain: {
                "chain_id": config["chain_id"], "label": config["label"],
                "tracked_contracts": tracked_by_chain.get(chain, 0),
                "signal_counts": chain_signal_counts.get(chain, {}),
            }
            for chain, config in EVM_CHAIN_CONFIG.items()
        },
        "tracked_contracts": tracked_contracts,
        "signal_counts": counts, "latest_refresh": latest_run,
        "providers": {"market": "DexScreener", "holders": "Blockscout"},
        "paper_mode": True, "actionable": False,
    })


@app.get("/evm-signals")
def evm_signals_endpoint():
    initialise_database()
    status = str(request.args.get("status") or "").strip().upper()
    chain = str(request.args.get("chain") or "").strip().lower()
    if chain and chain not in EVM_CHAIN_CONFIG:
        return jsonify({
            "success": False, "error": "Unsupported EVM chain",
            "supported_chains": list(SUPPORTED_EVM_CHAINS),
        }), 400
    clauses = []
    params = []
    if status:
        clauses.append("s.status = %s")
        params.append(status)
    if chain:
        clauses.append("s.chain = %s")
        params.append(chain)
    query = EVM_SIGNAL_SELECT
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY s.updated_at DESC"
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
    return jsonify({
        "success": True, "count": len(rows),
        "signals": [serialize_evm_signal(row) for row in rows],
        "paper_mode": True, "actionable": False,
        "warning": "Contract analytics are observations, not buy or sell instructions.",
    })


@app.get("/evm-evidence")
def evm_evidence_endpoint():
    """Read-only V4.12 multi-chain evidence summary for calibration and review."""
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(EVM_SIGNAL_SELECT + " ORDER BY s.token_symbol")
            signals = [serialize_evm_signal(row) for row in cur.fetchall()]
    status_counts = {}
    tier_counts = {}
    structure_counts = {}
    chain_counts = {}
    for signal in signals:
        status_counts[signal["status"]] = status_counts.get(signal["status"], 0) + 1
        tier = signal["liquidity_tier"]
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
        structure = signal["structure_state"]
        structure_counts[structure] = structure_counts.get(structure, 0) + 1
        chain = signal["chain"]
        chain_counts[chain] = chain_counts.get(chain, 0) + 1
    return jsonify({
        "success": True, "version": VERSION, "generated_at": datetime.now(timezone.utc),
        "status_counts": status_counts, "liquidity_tier_counts": tier_counts,
        "structure_counts": structure_counts, "chain_counts": chain_counts,
        "signals": signals,
        "wallet_quality_coverage": "holder_count_only",
        "chart_method": "15-minute snapshot proxy; not OHLC candles",
        "paper_mode": True, "actionable": False,
    })


@app.get("/evm-transition-history")
def evm_transition_history_endpoint():
    """Read-only EVM classification history for churn and calibration review."""
    initialise_database()
    try:
        limit = min(max(int(request.args.get("limit", 200)), 1), 1000)
    except ValueError:
        limit = 200
    chain = str(request.args.get("chain") or "").strip().lower()
    clauses, params = [], []
    if chain:
        if chain not in EVM_CHAIN_CONFIG:
            return jsonify({"success": False, "error": "Unsupported EVM chain"}), 400
        clauses.append("chain = %s")
        params.append(chain)
    query = """
        SELECT id, chain, token_address, token_symbol, previous_status, status,
            momentum_score, risk_score, details, recorded_at
        FROM evm_signal_history
    """
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY id DESC LIMIT %s"
    params.append(limit)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
    return jsonify({
        "success": True, "version": VERSION, "count": len(rows),
        "transitions": [{
            "id": row[0], "chain": row[1], "token_address": row[2],
            "token_symbol": row[3], "previous_status": row[4],
            "status": row[5], "momentum_score": row[6],
            "risk_score": row[7], "details": json.loads(row[8] or "{}"),
            "recorded_at": row[9],
        } for row in rows],
        "paper_mode": True, "actionable": False,
    })


@app.get("/evm-token/<token_address>")
def evm_token_detail_endpoint(token_address):
    valid_address = (
        len(token_address) == 42 and token_address.startswith("0x")
        and all(character in "0123456789abcdefABCDEF" for character in token_address[2:])
    )
    if not valid_address:
        return jsonify({"success": False, "error": "Invalid EVM token address"}), 400
    chain = str(request.args.get("chain") or "").strip().lower()
    if chain and chain not in EVM_CHAIN_CONFIG:
        return jsonify({
            "success": False, "error": "Unsupported EVM chain",
            "supported_chains": list(SUPPORTED_EVM_CHAINS),
        }), 400
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            signal_query = EVM_SIGNAL_SELECT + " WHERE LOWER(s.token_address) = LOWER(%s)"
            signal_params = [token_address]
            if chain:
                signal_query += " AND s.chain = %s"
                signal_params.append(chain)
            signal_query += " ORDER BY s.updated_at DESC LIMIT 1"
            cur.execute(signal_query, tuple(signal_params))
            row = cur.fetchone()
            snapshot_query = """
                SELECT id, price_usd, liquidity_usd, market_cap_usd,
                    volume_h1_usd, price_change_h1_pct, holder_count,
                    data_quality, provider_errors, captured_at,
                    buys_h1, sells_h1, volume_h24_usd, price_change_h24_pct,
                    pair_address, is_anomaly, anomaly_reasons, pair_selection
                    , is_provider_unavailable, availability_reasons
                FROM evm_token_snapshots
                WHERE LOWER(token_address) = LOWER(%s)
            """
            snapshot_params = [token_address]
            if chain:
                snapshot_query += " AND chain = %s"
                snapshot_params.append(chain)
            elif row:
                snapshot_query += " AND chain = %s"
                snapshot_params.append(row[0])
            snapshot_query += " ORDER BY captured_at DESC LIMIT 192"
            cur.execute(snapshot_query, tuple(snapshot_params))
            snapshots = cur.fetchall()
    if not row:
        return jsonify({"success": False, "error": "EVM token has no snapshot yet"}), 404
    return jsonify({
        "success": True, "signal": serialize_evm_signal(row),
        "snapshots": [{
            "id": item[0], "price_usd": item[1], "liquidity_usd": item[2],
            "market_cap_usd": item[3], "volume_h1_usd": item[4],
            "price_change_h1_pct": item[5], "holder_count": item[6],
            "data_quality": item[7],
            "provider_errors": json.loads(item[8] or "[]"), "captured_at": item[9],
            "buys_h1": item[10], "sells_h1": item[11],
            "volume_h24_usd": item[12], "price_change_h24_pct": item[13],
            "pair_address": item[14], "is_anomaly": item[15],
            "anomaly_reasons": json.loads(item[16] or "[]"),
            "pair_selection": item[17],
            "is_provider_unavailable": item[18],
            "availability_reasons": json.loads(item[19] or "[]"),
        } for item in snapshots],
        "paper_mode": True, "actionable": False,
    })


@app.get("/evm-snapshots")
def evm_snapshots_endpoint():
    initialise_database()
    try:
        limit = min(max(int(request.args.get("limit", 100)), 1), 500)
    except ValueError:
        limit = 100
    symbol = str(request.args.get("symbol") or "").strip().upper()
    chain = str(request.args.get("chain") or "").strip().lower()
    if chain and chain not in EVM_CHAIN_CONFIG:
        return jsonify({
            "success": False, "error": "Unsupported EVM chain",
            "supported_chains": list(SUPPORTED_EVM_CHAINS),
        }), 400
    clauses = []
    params = []
    if symbol:
        clauses.append("token_symbol = %s")
        params.append(symbol)
    if chain:
        clauses.append("chain = %s")
        params.append(chain)
    snapshot_query = """
        SELECT id, chain, token_address, token_symbol, price_usd,
            liquidity_usd, market_cap_usd, volume_h1_usd,
            price_change_h1_pct, holder_count, data_quality,
            provider_errors, captured_at, pair_address, is_anomaly,
            anomaly_reasons, pair_selection, is_provider_unavailable,
            availability_reasons
        FROM evm_token_snapshots
    """
    if clauses:
        snapshot_query += " WHERE " + " AND ".join(clauses)
    snapshot_query += " ORDER BY captured_at DESC LIMIT %s"
    params.append(limit)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(snapshot_query, tuple(params))
            rows = cur.fetchall()
    return jsonify({"success": True, "count": len(rows), "snapshots": [{
        "id": row[0], "chain": row[1], "token_address": row[2],
        "chain_id": EVM_CHAIN_CONFIG.get(row[1], {}).get("chain_id"),
        "chain_label": EVM_CHAIN_CONFIG.get(row[1], {}).get("label", str(row[1]).title()),
        "token_symbol": row[3], "price_usd": row[4], "liquidity_usd": row[5],
        "market_cap_usd": row[6], "volume_h1_usd": row[7],
        "price_change_h1_pct": row[8], "holder_count": row[9],
        "data_quality": row[10], "provider_errors": json.loads(row[11] or "[]"),
        "captured_at": row[12], "pair_address": row[13],
        "is_anomaly": row[14], "anomaly_reasons": json.loads(row[15] or "[]"),
        "pair_selection": row[16], "actionable": False,
        "is_provider_unavailable": row[17],
        "availability_reasons": json.loads(row[18] or "[]"),
    } for row in rows], "paper_mode": True})


@app.get("/evm-anomalies")
def evm_anomalies_endpoint():
    initialise_database()
    try:
        limit = min(max(int(request.args.get("limit", 100)), 1), 500)
    except ValueError:
        limit = 100
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, chain, token_address, token_symbol, pair_address,
                    price_usd, liquidity_usd, market_cap_usd, volume_h1_usd,
                    data_quality, provider_errors, anomaly_reasons,
                    pair_selection, captured_at
                FROM evm_token_snapshots
                WHERE is_anomaly = TRUE
                ORDER BY captured_at DESC LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    return jsonify({
        "success": True, "count": len(rows),
        "anomalies": [{
            "id": row[0], "chain": row[1], "token_address": row[2],
            "token_symbol": row[3], "pair_address": row[4],
            "price_usd": row[5], "liquidity_usd": row[6],
            "market_cap_usd": row[7], "volume_h1_usd": row[8],
            "data_quality": row[9],
            "provider_errors": json.loads(row[10] or "[]"),
            "anomaly_reasons": json.loads(row[11] or "[]"),
            "pair_selection": row[12], "captured_at": row[13],
            "excluded_from_trends": True,
            "excluded_from_notifications": True,
        } for row in rows],
        "paper_mode": True, "actionable": False,
    })


@app.get("/evm-provider-events")
def evm_provider_events_endpoint():
    initialise_database()
    try:
        limit = min(max(int(request.args.get("limit", 100)), 1), 500)
    except ValueError:
        limit = 100
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, chain, token_address, token_symbol, data_quality,
                    provider_errors, availability_reasons, captured_at
                FROM evm_token_snapshots
                WHERE is_provider_unavailable = TRUE
                ORDER BY captured_at DESC LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    return jsonify({
        "success": True, "count": len(rows),
        "events": [{
            "id": row[0], "chain": row[1], "token_address": row[2],
            "token_symbol": row[3], "data_quality": row[4],
            "provider_errors": json.loads(row[5] or "[]"),
            "availability_reasons": json.loads(row[6] or "[]"),
            "captured_at": row[7], "token_risk_signal": False,
            "last_trusted_snapshot_preserved": True,
            "excluded_from_trends": True,
            "excluded_from_notifications": True,
        } for row in rows],
        "paper_mode": True, "actionable": False,
    })


@app.post("/test-evm-notification")
def test_evm_notification_endpoint():
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    initialise_database()
    signal = {
        "token_address": None, "token_symbol": "EVM-TEST",
        "status": "EVM_MOMENTUM", "momentum_score": 65, "risk_score": 10,
        "data_quality": "test", "reasons": ["notification_pipeline_test"],
    }
    result = queue_and_deliver_evm_notification(
        signal, f"evm-test:{time.time_ns()}:{random.randint(1000, 9999)}", test=True
    )
    return jsonify({
        "success": bool(result.get("success")), "delivery_status": result.get("status"),
        "notification_id": result.get("notification_id"),
        "telegram_configured": telegram_configured(), "paper_mode": True,
        "actionable": False,
    }), 200 if result.get("success") else 503


@app.get("/wallet-clusters")
def wallet_clusters_endpoint():
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cluster_id, wallet, classification, confidence,
                    signal_weight, relationship_basis, updated_at
                FROM wallet_clusters ORDER BY cluster_id, signal_weight DESC
            """)
            rows = cur.fetchall()
    grouped = {}
    for cluster_id, wallet, classification, confidence, weight, basis, updated_at in rows:
        cluster = grouped.setdefault(cluster_id, {
            "cluster_id": cluster_id, "wallets": [], "maximum_vote_weight": 0,
        })
        try:
            relationship_basis = json.loads(basis or "[]")
        except (TypeError, ValueError):
            relationship_basis = []
        cluster["wallets"].append({
            "wallet": wallet, "classification": classification,
            "confidence": confidence, "individual_weight": weight,
            "relationship_basis": relationship_basis, "updated_at": updated_at,
        })
        cluster["maximum_vote_weight"] = max(cluster["maximum_vote_weight"], weight)
    return jsonify({
        "success": True, "cluster_count": len(grouped),
        "clusters": list(grouped.values()),
        "policy": "Each cluster contributes only its highest eligible wallet weight.",
    })


@app.post("/helius-webhook")
def helius_webhook_endpoint():
    if not os.getenv("HELIUS_WEBHOOK_SECRET"):
        return jsonify({"success": False, "error": "HELIUS_WEBHOOK_SECRET is not configured"}), 503
    if not webhook_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    initialise_database()
    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify({"success": False, "error": "JSON body required"}), 400
    tracked = load_tracked_wallet_map()
    if not tracked:
        compute_and_persist_wallet_clusters()
        tracked = load_tracked_wallet_map()
    events = parse_helius_activity(payload, tracked)
    inserted = persist_wallet_activity(events)
    probation = load_probation_wallet_map()
    probation_events = parse_helius_activity(payload, probation)
    probation_inserted = persist_probation_wallet_activity(probation_events)
    signals = refresh_paper_signals() if inserted else []
    return jsonify({
        "success": True, "events_parsed": len(events), "events_inserted": inserted,
        "probation_events_parsed": len(probation_events),
        "probation_events_inserted": probation_inserted,
        "signals_refreshed": len(signals), "paper_mode": True,
        "actionable": False,
    })


@app.post("/refresh-paper-signals")
def refresh_paper_signals_endpoint():
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    initialise_database()
    refreshed = refresh_paper_signals()
    return jsonify({
        "success": True, "count": len(refreshed), "signals": refreshed,
        "paper_mode": True, "actionable": False,
    })


@app.get("/wallet-activity")
def wallet_activity_endpoint():
    initialise_database()
    try:
        limit = min(max(int(request.args.get("limit", 100)), 1), 500)
    except ValueError:
        limit = 100
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT signature, wallet, cluster_id, token_address, token_symbol,
                    side, token_amount, estimated_usd_value, occurred_at, received_at
                FROM wallet_activity ORDER BY occurred_at DESC LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    return jsonify({"success": True, "count": len(rows), "events": [{
        "signature": row[0], "wallet": row[1], "cluster_id": row[2],
        "token_address": row[3], "token_symbol": row[4], "side": row[5],
        "token_amount": row[6], "estimated_usd_value": row[7],
        "occurred_at": row[8], "received_at": row[9],
    } for row in rows]})


def build_solana_activity_diagnostics():
    """Measure where Solana evidence converges or disappears over 1h/6h/24h."""
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.wallet, a.cluster_id, a.token_address,
                    COALESCE(a.token_symbol, m.token_symbol) AS token_symbol,
                    a.side, a.occurred_at
                FROM wallet_activity a
                LEFT JOIN solana_token_metadata m
                    ON m.token_address = a.token_address
                WHERE a.occurred_at >= NOW() - INTERVAL '24 hours'
                ORDER BY a.occurred_at DESC
            """)
            rows = cur.fetchall()
            cur.execute("""
                SELECT c.wallet, c.cluster_id, MAX(a.occurred_at) AS last_activity_at
                FROM wallet_clusters c
                LEFT JOIN wallet_activity a ON a.wallet = c.wallet
                GROUP BY c.wallet, c.cluster_id
                ORDER BY last_activity_at DESC NULLS LAST
            """)
            wallet_rows = cur.fetchall()

    now = datetime.now(timezone.utc)
    windows = {}
    for hours in SOLANA_CONSENSUS_WINDOWS_HOURS:
        cutoff = now - timedelta(hours=hours)
        selected = [row for row in rows if row[5] >= cutoff]
        buyers, sellers = {}, {}
        symbols = {}
        for wallet, cluster, token, symbol, side, occurred_at in selected:
            symbols[token] = symbol or symbols.get(token)
            target = buyers if side == "BUY" else sellers
            target.setdefault(token, set()).add(cluster)
        tokens = set(buyers) | set(sellers)
        consensus = []
        for token in tokens:
            buy_count = len(buyers.get(token, set()))
            sell_count = len(sellers.get(token, set()))
            consensus.append({
                "token_address": token, "token_symbol": symbols.get(token),
                "independent_buy_clusters": buy_count,
                "independent_sell_clusters": sell_count,
                "display_status": solana_status_label(
                    "PAPER_CONFIRMED" if buy_count >= 3 else
                    "BUILDING" if buy_count == 2 else "OBSERVE",
                    buy_count, sell_count,
                ),
                "dexscreener_url": f"https://dexscreener.com/solana/{token}",
            })
        consensus.sort(key=lambda item: (
            item["independent_buy_clusters"],
            -item["independent_sell_clusters"],
        ), reverse=True)
        windows[f"{hours}h"] = {
            "events": len(selected),
            "active_wallets": len({row[0] for row in selected}),
            "independent_clusters": len({row[1] for row in selected}),
            "unique_tokens": len(tokens),
            "unique_tokens_bought": len(buyers),
            "tokens_with_2_buyers": sum(1 for item in consensus if item["independent_buy_clusters"] == 2),
            "tokens_with_3_plus_buyers": sum(1 for item in consensus if item["independent_buy_clusters"] >= 3),
            "missing_symbols": sum(1 for token in tokens if not symbols.get(token)),
            "top_consensus": consensus[:20],
        }
    return {
        "success": True, "version": VERSION, "generated_at": now,
        "tracked_wallets": len(wallet_rows),
        "tracked_clusters": len({row[1] for row in wallet_rows}),
        "windows": windows,
        "wallet_last_activity": [{
            "wallet": row[0], "cluster_id": row[1],
            "last_activity_at": row[2],
        } for row in wallet_rows],
        "paper_mode": True, "actionable": False,
        "note": "Buyer counts represent independent wallet clusters, not transaction count.",
    }


@app.get("/solana-activity")
def solana_activity_endpoint():
    return jsonify(build_solana_activity_diagnostics())


def serialize_signal_row(row):
    try:
        wallets = json.loads(row[8] or "[]")
        clusters = json.loads(row[9] or "[]")
    except (TypeError, ValueError):
        wallets, clusters = [], []
    last_activity_at = row[11]
    signal_age_seconds = None
    if last_activity_at:
        observed = last_activity_at
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        signal_age_seconds = max(
            int((datetime.now(timezone.utc) - observed).total_seconds()), 0
        )
    return {
        "token_address": row[0], "token_symbol": row[1], "status": row[2],
        "display_status": solana_status_label(row[2], row[5], row[6]),
        "buy_score": row[3], "sell_score": row[4],
        "independent_buy_clusters": row[5],
        "independent_sell_clusters": row[6],
        "contributing_wallets": wallets, "contributing_clusters": clusters,
        "first_activity_at": row[10], "last_activity_at": last_activity_at,
        "signal_age_seconds": signal_age_seconds,
        "dexscreener_url": (
            f"https://dexscreener.com/solana/{row[0]}" if row[0] else None
        ),
        "safety_status": row[12], "actionable": False, "updated_at": row[14],
    }


SIGNAL_SELECT = """
    SELECT token_address, token_symbol, status, buy_score, sell_score,
        independent_buy_clusters, independent_sell_clusters, actionable,
        contributing_wallets, contributing_clusters, first_activity_at,
        last_activity_at, safety_status, actionable, updated_at
    FROM paper_signals
"""


@app.get("/signals")
def signals_endpoint():
    initialise_database()
    expired_on_read = expire_stale_paper_signals()
    status = request.args.get("status")
    include_expired = str(request.args.get("include_expired", "true")).lower() in {
        "1", "true", "yes", "on",
    }
    with db() as conn:
        with conn.cursor() as cur:
            if status:
                cur.execute(SIGNAL_SELECT + " WHERE status = %s ORDER BY updated_at DESC", (status.upper(),))
            elif not include_expired:
                cur.execute(SIGNAL_SELECT + " WHERE status <> 'EXPIRED' ORDER BY updated_at DESC")
            else:
                cur.execute(SIGNAL_SELECT + " ORDER BY updated_at DESC")
            rows = cur.fetchall()
    return jsonify({
        "success": True, "count": len(rows),
        "signals": [serialize_signal_row(row) for row in rows],
        "expired_on_read": expired_on_read,
        "paper_mode": True, "actionable": False,
        "warning": "Paper research only; token safety is not yet verified.",
    })


@app.get("/signal/<token_address>")
def signal_detail_endpoint(token_address):
    initialise_database()
    if not is_valid_solana_address(token_address):
        return jsonify({"success": False, "error": "Invalid Solana token address"}), 400
    expire_stale_paper_signals()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(SIGNAL_SELECT + " WHERE token_address = %s", (token_address,))
            row = cur.fetchone()
            cur.execute("""
                SELECT signature, wallet, cluster_id, side, token_amount, occurred_at
                FROM wallet_activity WHERE token_address = %s
                ORDER BY occurred_at DESC LIMIT 100
            """, (token_address,))
            events = cur.fetchall()
    if not row:
        return jsonify({"success": False, "error": "Signal not found"}), 404
    return jsonify({
        "success": True, "signal": serialize_signal_row(row),
        "events": [{"signature": event[0], "wallet": event[1],
                    "cluster_id": event[2], "side": event[3],
                    "token_amount": event[4], "occurred_at": event[5]}
                   for event in events],
        "paper_mode": True, "actionable": False,
    })


@app.get("/signal-history")
def signal_history_endpoint():
    initialise_database()
    try:
        limit = min(max(int(request.args.get("limit", 100)), 1), 500)
    except ValueError:
        limit = 100
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, token_address, token_symbol, status, buy_score,
                    sell_score, independent_buy_clusters,
                    independent_sell_clusters, details, recorded_at
                FROM paper_signal_history ORDER BY id DESC LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    return jsonify({"success": True, "count": len(rows), "history": [{
        "id": row[0], "token_address": row[1], "token_symbol": row[2],
        "status": row[3], "buy_score": row[4], "sell_score": row[5],
        "independent_buy_clusters": row[6],
        "independent_sell_clusters": row[7],
        "details": json.loads(row[8] or "{}"), "recorded_at": row[9],
        "actionable": False,
    } for row in rows]})


@app.post("/test-notification")
def test_notification_endpoint():
    """Admin-only, non-trading end-to-end Telegram delivery test."""
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    initialise_database()
    test_signal = {
        "token_address": None,
        "token_symbol": "TEST",
        "status": "PAPER_CONFIRMED",
        "buy_score": 2.5,
        "sell_score": 0,
        "independent_buy_clusters": 3,
        "independent_sell_clusters": 0,
    }
    result = queue_and_deliver_signal_notification(
        test_signal,
        f"test:{time.time_ns()}:{random.randint(1000, 9999)}",
        test=True,
    )
    status_code = 200 if result.get("success") else 503
    return jsonify({
        "success": bool(result.get("success")),
        "delivery_status": result.get("status"),
        "notification_id": result.get("notification_id"),
        "attempts": result.get("attempts"),
        "telegram_configured": telegram_configured(),
        "paper_mode": True,
        "actionable": False,
    }), status_code


@app.post("/retry-notifications")
def retry_notifications_endpoint():
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    initialise_database()
    try:
        limit = min(max(int(request.args.get("limit", 20)), 1), 50)
    except ValueError:
        limit = 20
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM notification_deliveries
                WHERE delivery_status <> 'delivered' AND attempts < 5
                ORDER BY created_at ASC LIMIT %s
            """, (limit,))
            notification_ids = [row[0] for row in cur.fetchall()]
    results = [deliver_notification(notification_id) for notification_id in notification_ids]
    delivered = sum(1 for result in results if result.get("success"))
    return jsonify({
        "success": True, "selected": len(notification_ids),
        "delivered_or_already_delivered": delivered,
        "results": results, "paper_mode": True, "actionable": False,
    })


@app.get("/notification-deliveries")
def notification_deliveries_endpoint():
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    initialise_database()
    try:
        limit = min(max(int(request.args.get("limit", 50)), 1), 200)
    except ValueError:
        limit = 50
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, event_key, notification_type, channel, token_address,
                    token_symbol, signal_status, delivery_status, attempts,
                    response_code, error_message, created_at, delivered_at, updated_at
                FROM notification_deliveries ORDER BY id DESC LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    return jsonify({
        "success": True, "count": len(rows),
        "deliveries": [{
            "id": row[0], "event_key": row[1], "notification_type": row[2],
            "channel": row[3], "token_address": row[4], "token_symbol": row[5],
            "signal_status": row[6], "delivery_status": row[7],
            "attempts": row[8], "response_code": row[9],
            "error_message": row[10], "created_at": row[11],
            "delivered_at": row[12], "updated_at": row[13],
        } for row in rows],
        "paper_mode": True, "actionable": False,
    })


def dashboard_change(current, baseline):
    return percentage_change(current, baseline)


def build_dashboard_payload():
    """Assemble read-only EVM/Solana monitoring data and rolling comparisons."""
    initialise_database()
    expire_stale_paper_signals()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(EVM_SIGNAL_SELECT + " ORDER BY s.token_symbol")
            evm_rows = cur.fetchall()
            cur.execute("""
                SELECT chain, token_address, price_usd, liquidity_usd,
                    holder_count, captured_at
                FROM evm_token_snapshots
                WHERE captured_at >= NOW() - INTERVAL '26 hours'
                    AND is_anomaly = FALSE
                    AND is_provider_unavailable = FALSE
                ORDER BY chain, token_address, captured_at DESC
            """)
            snapshot_rows = cur.fetchall()
            cur.execute(SIGNAL_SELECT + " WHERE status <> 'EXPIRED' ORDER BY updated_at DESC LIMIT 50")
            solana_rows = cur.fetchall()
            cur.execute("""
                SELECT id, completed_at, snapshots_created, transitions_created,
                    status FROM evm_refresh_runs ORDER BY id DESC LIMIT 1
            """)
            refresh_row = cur.fetchone()
            cur.execute("""
                SELECT COUNT(*),
                    COUNT(*) FILTER (WHERE data_quality = 'complete'),
                    COUNT(*) FILTER (WHERE is_provider_unavailable = TRUE),
                    COUNT(*) FILTER (WHERE is_anomaly = TRUE),
                    COUNT(*) FILTER (WHERE holder_count IS NOT NULL)
                FROM evm_token_snapshots
                WHERE captured_at >= NOW() - INTERVAL '24 hours'
            """)
            quality_row = cur.fetchone()
            cur.execute("""
                SELECT cohort.wallet, cohort.admitted_at,
                    cohort.forward_trades, cohort.forward_tokens,
                    cohort.last_forward_activity_at, cohort.updated_at,
                    candidate.score, candidate.screening_risk_score,
                    candidate.repeat_early_entries, candidate.early_entry_score,
                    COUNT(DISTINCT source.token_address) AS source_tokens
                FROM wallet_discovery_cohorts cohort
                LEFT JOIN candidate_wallets candidate
                    ON candidate.wallet = cohort.wallet
                LEFT JOIN wallet_discovery_sources source
                    ON source.wallet = cohort.wallet
                WHERE cohort.cohort_status = 'PROBATION'
                GROUP BY cohort.wallet, cohort.admitted_at,
                    cohort.forward_trades, cohort.forward_tokens,
                    cohort.last_forward_activity_at, cohort.updated_at,
                    candidate.score, candidate.screening_risk_score,
                    candidate.repeat_early_entries, candidate.early_entry_score
                ORDER BY cohort.admitted_at DESC NULLS LAST, cohort.updated_at DESC
            """)
            probation_rows = cur.fetchall()
            cur.execute("""
                SELECT wallet, score, screening_risk_score,
                    realized_pnl_30d, trades_30d,
                    EXISTS (
                        SELECT 1 FROM wallet_discovery_cohorts cohort
                        WHERE cohort.wallet = candidate_wallets.wallet
                            AND cohort.source = 'dexscreener'
                    ) AS dex_discovered
                FROM candidate_wallets
                WHERE score_status = 'scored' AND score >= 30
                    AND screening_status = 'screened'
                    AND screening_risk_score IS NOT NULL
                    AND screening_risk_score <= 25
                ORDER BY score DESC, screening_risk_score ASC,
                    realized_pnl_30d DESC NULLS LAST
                LIMIT 100
            """)
            screened_wallet_rows = cur.fetchall()

    grouped = {}
    for row in snapshot_rows:
        grouped.setdefault((row[0], row[1]), []).append({
            "price_usd": row[2], "liquidity_usd": row[3],
            "holder_count": row[4], "captured_at": row[5],
        })

    now = datetime.now(timezone.utc)
    evm_signals = []
    for row in evm_rows:
        item = serialize_evm_signal(row)
        samples = grouped.get((row[0], row[1]), [])
        latest = samples[0] if samples else None
        latest_holder = next(
            (sample for sample in samples if sample.get("holder_count") is not None),
            None,
        )
        if latest_holder:
            item["holder_count"] = latest_holder["holder_count"]
            holder_at = latest_holder["captured_at"]
            if holder_at.tzinfo is None:
                holder_at = holder_at.replace(tzinfo=timezone.utc)
            item["holder_data_age_seconds"] = max(
                int((now - holder_at).total_seconds()), 0
            )
        else:
            item["holder_data_age_seconds"] = None
        trends = {}
        for hours in (1, 6, 24):
            target = now - timedelta(hours=hours)
            baseline = next(
                (sample for sample in samples if sample["captured_at"] <= target),
                None,
            )
            holder_baseline = next(
                (
                    sample for sample in samples
                    if sample["captured_at"] <= target
                    and sample.get("holder_count") is not None
                ),
                None,
            )
            trends[f"{hours}h"] = {
                "available": bool(latest and baseline),
                "price_change_pct": dashboard_change(
                    latest.get("price_usd") if latest else None,
                    baseline.get("price_usd") if baseline else None,
                ),
                "liquidity_change_pct": dashboard_change(
                    latest.get("liquidity_usd") if latest else None,
                    baseline.get("liquidity_usd") if baseline else None,
                ),
                "holder_change": (
                    latest_holder["holder_count"] - holder_baseline["holder_count"]
                    if latest_holder and holder_baseline else None
                ),
                "baseline_at": baseline.get("captured_at") if baseline else None,
            }
        item["trends"] = trends
        evm_signals.append(item)

    eth_benchmark = next(
        (item for item in evm_signals if item.get("is_benchmark")), None
    )
    for item in evm_signals:
        for window, metrics in item["trends"].items():
            eth_change = (
                eth_benchmark.get("trends", {}).get(window, {}).get("price_change_pct")
                if eth_benchmark else None
            )
            token_change = metrics.get("price_change_pct")
            metrics["eth_change_pct"] = eth_change
            metrics["relative_to_eth_pct"] = (
                round(token_change - eth_change, 4)
                if token_change is not None and eth_change is not None else None
            )

    solana_signals = [serialize_signal_row(row) for row in solana_rows]
    solana_activity = build_solana_activity_diagnostics()
    latest_refresh = None if not refresh_row else {
        "id": refresh_row[0], "completed_at": refresh_row[1],
        "snapshots_created": refresh_row[2],
        "transitions_created": refresh_row[3], "status": refresh_row[4],
    }
    total_snapshots = quality_row[0] or 0
    snapshot_quality = {
        "snapshots_24h": total_snapshots,
        "complete_pct": round((quality_row[1] or 0) * 100 / total_snapshots, 1)
        if total_snapshots else None,
        "provider_available_pct": round(
            (total_snapshots - (quality_row[2] or 0)) * 100 / total_snapshots, 1
        ) if total_snapshots else None,
        "anomaly_pct": round((quality_row[3] or 0) * 100 / total_snapshots, 1)
        if total_snapshots else None,
        "holder_coverage_pct": round((quality_row[4] or 0) * 100 / total_snapshots, 1)
        if total_snapshots else None,
    }
    probation_wallets = []
    for row in probation_rows:
        admitted_at = row[1]
        if admitted_at and admitted_at.tzinfo is None:
            admitted_at = admitted_at.replace(tzinfo=timezone.utc)
        probation_age_seconds = max(
            int((now - admitted_at).total_seconds()), 0
        ) if admitted_at else 0
        forward_trades = row[2] or 0
        forward_tokens = row[3] or 0
        probation_wallets.append({
            "wallet": row[0], "cohort_status": "PROBATION",
            "consensus_weight": 0, "admitted_at": admitted_at,
            "probation_age_seconds": probation_age_seconds,
            "forward_trades": forward_trades,
            "forward_tokens": forward_tokens,
            "last_forward_activity_at": row[4], "updated_at": row[5],
            "performance_score": row[6], "screening_risk_score": row[7],
            "repeat_early_entries": row[8],
            "average_early_entry_score": row[9],
            "source_tokens_excluded": row[10] or 0,
            "minimum_days_met": probation_age_seconds >= (
                DEX_WALLET_PROBATION_MIN_DAYS * 86400
            ),
            "minimum_forward_trades_met": (
                forward_trades >= DEX_WALLET_PROBATION_MIN_TRADES
            ),
            "multiple_forward_tokens_met": forward_tokens >= 2,
            "promotion_review_ready": (
                probation_age_seconds >= DEX_WALLET_PROBATION_MIN_DAYS * 86400
                and forward_trades >= DEX_WALLET_PROBATION_MIN_TRADES
                and forward_tokens >= 2
            ),
        })
    screened_wallets = [{
        "wallet": row[0], "performance_score": row[1],
        "screening_risk_score": row[2],
        "realized_pnl_30d": row[3], "trades_30d": row[4],
        "source": "DEX DISCOVERY" if row[5] else "EXISTING COHORT",
        "screening_result": "PROVISIONAL PASS",
        "consensus_weight": 0,
    } for row in screened_wallet_rows]
    candidate_pipeline = load_dex_wallet_pipeline_status()
    return {
        "success": True, "version": VERSION,
        "generated_at": now, "latest_refresh": latest_refresh,
        "evm_signals": evm_signals, "solana_signals": solana_signals,
        "solana_activity": solana_activity, "snapshot_quality": snapshot_quality,
        "screened_wallets": screened_wallets,
        "candidate_pipeline": candidate_pipeline,
        "initial_screening_policy": {
            "minimum_performance_score": 30,
            "maximum_risk_score": 25,
            "consensus_weight": 0,
            "probation_admission_is_separate": True,
        },
        "probation_wallets": probation_wallets,
        "probation_policy": {
            "consensus_weight": 0,
            "minimum_days": DEX_WALLET_PROBATION_MIN_DAYS,
            "minimum_forward_trades": DEX_WALLET_PROBATION_MIN_TRADES,
            "minimum_forward_tokens": 2,
            "manual_promotion_required": True,
            "source_tokens_permanently_excluded": True,
        },
        "summary": {
            "evm_tokens": len(evm_signals),
            "evm_chains": len({item["chain"] for item in evm_signals}),
            "evm_alert_states": sum(
                1 for item in evm_signals if item["status"] in EVM_ALERT_STATUSES
            ),
            "solana_signals": len(solana_signals),
            "solana_active": sum(
                1 for item in solana_signals if item["status"] != "EXPIRED"
            ),
            "probation_wallets": len(probation_wallets),
            "screened_wallets": len(screened_wallets),
            "dex_candidates": candidate_pipeline["counts"]["discovered"],
            "probation_ready": candidate_pipeline["counts"]["probation_ready"],
        },
        "paper_mode": True, "actionable": False,
    }


DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Wallet Monitor Dashboard</title>
  <style>
    :root{--bg:#07111f;--panel:#101d2e;--panel2:#13243a;--line:#24364d;--text:#e7eef8;--muted:#8fa3bb;--blue:#55a7ff;--green:#37d49b;--amber:#ffbd59;--red:#ff6577}
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(circle at 15% 0,#122c48 0,transparent 38%),var(--bg);color:var(--text);font:14px/1.5 Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif}
    .wrap{max-width:1460px;margin:auto;padding:28px}.top{display:flex;justify-content:space-between;gap:24px;align-items:flex-start;margin-bottom:22px}.eyebrow{color:var(--blue);font-weight:700;letter-spacing:.14em;text-transform:uppercase;font-size:11px}h1{font-size:30px;margin:5px 0 4px}.sub,.muted{color:var(--muted)}.live{display:flex;align-items:center;gap:8px;background:#102d2a;border:1px solid #1d5d4d;color:#77e4bd;padding:9px 13px;border-radius:999px}.dot{width:8px;height:8px;background:var(--green);border-radius:50%;box-shadow:0 0 12px var(--green)}
    .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px;margin-bottom:24px}.card,.section{background:linear-gradient(145deg,rgba(19,36,58,.96),rgba(12,25,42,.96));border:1px solid var(--line);border-radius:16px;box-shadow:0 18px 40px rgba(0,0,0,.16)}.card{padding:18px}.card b{display:block;font-size:25px;margin-top:4px}.label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.08em}
    .section{padding:18px;margin-bottom:20px}.section-head{display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:12px}.section h2{font-size:18px;margin:0}.links a{color:var(--blue);text-decoration:none;margin-left:14px}.table-wrap{overflow:auto}table{width:100%;border-collapse:collapse;min-width:1040px}th{text-align:left;color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.06em;padding:11px 10px;border-bottom:1px solid var(--line)}td{padding:13px 10px;border-bottom:1px solid rgba(36,54,77,.7);white-space:nowrap}tbody tr:hover{background:rgba(85,167,255,.04)}.token{font-weight:750}.address{font:11px ui-monospace,SFMono-Regular,Consolas;color:var(--muted)}.token-link{color:inherit;text-decoration:none;display:inline-block}.token-link:hover .token{color:var(--blue)}.external{color:var(--blue);font-size:11px;margin-left:4px}
    .badge{display:inline-block;padding:4px 8px;border-radius:999px;font-weight:700;font-size:11px}.observe{background:#19304b;color:#9ecbff}.benchmark{background:#252e58;color:#b7c7ff}.momentum,.accumulation{background:#123b31;color:#67e6b8}.high{background:#294320;color:#b4ef79}.risk,.distribution{background:#4a2028;color:#ff9aaa}.thin,.rebound,.provider{background:#4a381d;color:#ffd27d}.expired{background:#29313d;color:#aab5c3}.pos{color:var(--green)}.neg{color:var(--red)}.neutral{color:var(--muted)}.warning{margin-top:18px;padding:12px 15px;border:1px solid #54421f;background:#2a2415;color:#f2cf82;border-radius:12px}.empty{text-align:center;color:var(--muted);padding:24px}.error{border-color:#68303a;background:#321820;color:#ff9aaa}.footer{color:var(--muted);font-size:12px;text-align:center;padding:8px}
    @media(max-width:800px){.wrap{padding:18px}.top{display:block}.live{margin-top:14px;width:max-content}.cards{grid-template-columns:repeat(2,1fr)}h1{font-size:25px}.section-head{align-items:flex-start;flex-direction:column}.links a{margin:0 14px 0 0}}
  </style>
</head>
<body><main class="wrap">
  <header class="top"><div><div class="eyebrow">V4.14.3 · Candidate Pipeline</div><h1>Wallet Monitor Dashboard</h1><div class="sub">Wide DexScreener discovery + visible screening funnel + zero-weight probation + multi-chain evidence</div></div><div class="live"><span class="dot"></span><span id="refreshState">Loading live data…</span></div></header>
  <section class="cards">
    <div class="card"><span class="label">EVM tokens</span><b id="evmCount">—</b><span class="muted">Robinhood Chain + Base</span></div>
    <div class="card"><span class="label">EVM alert states</span><b id="evmAlerts">—</b><span class="muted">Configured evidence alerts</span></div>
    <div class="card"><span class="label">Solana signals</span><b id="solCount">—</b><span class="muted">Active paper states</span></div>
    <div class="card"><span class="label">Dex candidates</span><b id="dexCandidateCount">—</b><span class="muted">Wide discovery cohort</span></div>
    <div class="card"><span class="label">Passed screening</span><b id="screenedCount">—</b><span class="muted">Candidates · zero consensus weight</span></div>
    <div class="card"><span class="label">Probation ready</span><b id="probationReadyCount">—</b><span class="muted">Manual admission required</span></div>
    <div class="card"><span class="label">Probation wallets</span><b id="probationCount">—</b><span class="muted">Visible · zero consensus weight</span></div>
    <div class="card"><span class="label">Latest refresh</span><b id="runId">—</b><span class="muted" id="runTime">Waiting</span></div>
  </section>
  <section class="section"><div class="section-head"><div><h2>Multi-chain EVM watchlist</h2><div class="muted">Canonical-pair evidence; holder freshness and performance relative to ETH are explicit</div></div><div class="links"><a href="/evm-signals">JSON signals</a><a href="/evm-transition-history">Transitions</a><a href="/evm-snapshots?limit=100">Snapshots</a><a href="/evm-anomalies">Anomalies</a><a href="/evm-provider-events">Provider events</a></div></div><div class="table-wrap"><table><thead><tr><th>Token / chain</th><th>State</th><th>Structure</th><th>Price</th><th>Liquidity</th><th>Holders</th><th>1h price</th><th>6h price</th><th>24h price</th><th>24h vs ETH</th><th>24h holders</th><th>Volume 1h</th><th>Quality</th></tr></thead><tbody id="evmBody"><tr><td colspan="13" class="empty">Loading…</td></tr></tbody></table></div></section>
  <section class="section"><div class="section-head"><div><h2>Solana consensus windows</h2><div class="muted">Activity funnel across independent validated wallet clusters; probation activity cannot vote</div></div><div class="links"><a href="/solana-activity">Full diagnostics</a><a href="/dex-wallet-cohorts">Discovery cohorts</a><a href="/dex-wallet-discovery-history">Discovery runs</a><a href="/probation-wallet-activity">Probation activity</a></div></div><div class="table-wrap"><table><thead><tr><th>Window</th><th>Events</th><th>Active wallets</th><th>Clusters</th><th>Tokens bought</th><th>2 buyers</th><th>3+ buyers</th><th>Missing symbols</th></tr></thead><tbody id="solWindowBody"><tr><td colspan="8" class="empty">Loading…</td></tr></tbody></table></div></section>
  <section class="section"><div class="section-head"><div><h2>DexScreener candidate processing pipeline</h2><div class="muted">Newly discovered wallets are prioritised through bounded scoring, screening and validation batches</div></div><div class="links"><a href="/dex-wallet-pipeline-status">Pipeline JSON</a><a href="/dex-wallet-leaderboard">Candidate leaderboard</a><a href="/dex-wallet-cohorts">Discovery cohorts</a></div></div><div class="table-wrap"><table><thead><tr><th>Discovered</th><th>Awaiting score</th><th>Scored</th><th>Performance ≥30</th><th>Screened</th><th>Low risk</th><th>Validated</th><th>Probation ready</th><th>In probation</th><th>Consensus validated</th></tr></thead><tbody id="pipelineBody"><tr><td colspan="10" class="empty">Loading…</td></tr></tbody></table></div></section>
  <section class="section"><div class="section-head"><div><h2>Wallets passed initial performance and risk screening</h2><div class="muted">Performance ≥30 and risk ≤25; source identifies whether the wallet came from the new DexScreener cohort</div></div><div class="links"><a href="/dex-wallet-leaderboard">Candidate leaderboard</a><a href="/screenings">Screening evidence</a></div></div><div class="table-wrap"><table><thead><tr><th>Wallet</th><th>Source</th><th>Performance score</th><th>Risk score</th><th>30-day realised P&amp;L</th><th>Trades</th></tr></thead><tbody id="screenedWalletBody"><tr><td colspan="6" class="empty">Loading…</td></tr></tbody></table></div></section>
  <section class="section"><div class="section-head"><div><h2>DexScreener wallets ready for probation review</h2><div class="muted">All admission gates passed; entry remains manual and consensus weight remains zero during probation</div></div><div class="links"><a href="/dex-wallet-pipeline-status">Readiness details</a><a href="/dex-wallet-cohorts">All cohorts</a></div></div><div class="table-wrap"><table><thead><tr><th>Wallet</th><th>Performance</th><th>Risk</th><th>30-day realised P&amp;L</th><th>Trades</th><th>Validation</th><th>Repeat early entries</th><th>Early-entry score</th></tr></thead><tbody id="probationReadyBody"><tr><td colspan="8" class="empty">Loading…</td></tr></tbody></table></div></section>
  <section class="section"><div class="section-head"><div><h2>Solana probation wallets</h2><div class="muted">Forward-only evidence is visible here but contributes zero buyer votes until manual validation</div></div><div class="links"><a href="/dex-wallet-cohorts">All cohorts</a><a href="/probation-wallet-activity">Probation activity</a><a href="/dex-wallet-leaderboard">Candidate leaderboard</a></div></div><div class="table-wrap"><table><thead><tr><th>Wallet</th><th>Status</th><th>Admitted</th><th>Probation age</th><th>Performance</th><th>Risk</th><th>Early entries</th><th>Forward trades</th><th>Forward tokens</th><th>Last activity</th><th>Promotion review</th></tr></thead><tbody id="probationBody"><tr><td colspan="11" class="empty">Loading…</td></tr></tbody></table></div></section>
  <section class="section"><div class="section-head"><div><h2>Active Solana paper signals</h2><div class="muted">Buyer labels count independent wallet clusters—not transactions</div></div><div class="links"><a href="/signals?include_expired=false">Active JSON</a><a href="/signals?include_expired=true">History</a><a href="/wallet-activity?limit=100">Activity</a></div></div><div class="table-wrap"><table><thead><tr><th>Token</th><th>Result</th><th>Buy score</th><th>Sell score</th><th>Buy clusters</th><th>Sell clusters</th><th>Safety</th><th>Last activity</th></tr></thead><tbody id="solBody"><tr><td colspan="8" class="empty">Loading…</td></tr></tbody></table></div></section>
  <div class="warning">Paper research only. Signals are observations—not trade instructions—and token safety remains unverified.</div><div class="footer" id="footer">Auto-refreshes every 60 seconds.</div>
</main><script>
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const money=v=>v==null?'—':new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',notation:Math.abs(v)>=1e6?'compact':'standard',maximumFractionDigits:Math.abs(v)<1?8:2}).format(v);
const num=v=>v==null?'—':new Intl.NumberFormat('en-US',{maximumFractionDigits:2}).format(v);
const trend=v=>v==null?'<span class="neutral">collecting</span>':`<span class="${v>0?'pos':v<0?'neg':'neutral'}">${v>0?'+':''}${num(v)}%</span>`;
const when=v=>v?new Date(v).toLocaleString():'—';
const age=s=>s==null?'unknown age':s<60?`${s}s`:s<3600?`${Math.floor(s/60)}m`:s<86400?`${Math.floor(s/3600)}h ${Math.floor((s%3600)/60)}m`:`${Math.floor(s/86400)}d ${Math.floor((s%86400)/3600)}h`;
const badge=s=>{const c=s==='EVM_DATA_ANOMALY'||s==='EVM_RISK'||s==='EVM_CONFIRMED_BREAKDOWN'?'risk':s==='EVM_PROVIDER_UNAVAILABLE'?'provider':s==='EVM_BENCHMARK'?'benchmark':s==='EVM_DISTRIBUTION'?'distribution':s==='EVM_THIN_LIQUIDITY'?'thin':s==='EVM_REBOUND'?'rebound':s==='EVM_ACCUMULATION_WATCH'?'accumulation':s==='EVM_HIGH_MOMENTUM'||s==='EVM_CONFIRMED_BREAKOUT'?'high':s==='EVM_MOMENTUM'?'momentum':s==='EXPIRED'?'expired':'observe';return `<span class="badge ${c}">${esc(s)}</span>`};
async function load(){try{const r=await fetch('/dashboard-data',{cache:'no-store'});if(!r.ok)throw Error(`HTTP ${r.status}`);const d=await r.json();
document.getElementById('evmCount').textContent=d.summary.evm_tokens;document.getElementById('evmAlerts').textContent=d.summary.evm_alert_states;document.getElementById('solCount').textContent=d.summary.solana_active;document.getElementById('dexCandidateCount').textContent=d.summary.dex_candidates;document.getElementById('screenedCount').textContent=d.summary.screened_wallets;document.getElementById('probationReadyCount').textContent=d.summary.probation_ready;document.getElementById('probationCount').textContent=d.summary.probation_wallets;document.getElementById('runId').textContent=d.latest_refresh?`#${d.latest_refresh.id}`:'—';document.getElementById('runTime').textContent=d.latest_refresh?when(d.latest_refresh.completed_at):'No refresh yet';document.getElementById('refreshState').textContent='Live · updated '+new Date().toLocaleTimeString();
document.getElementById('evmBody').innerHTML=d.evm_signals.length?d.evm_signals.map(x=>`<tr><td>${x.dexscreener_url?`<a class="token-link" href="${esc(x.dexscreener_url)}" target="_blank" rel="noopener noreferrer" title="Open exact monitored pair on DexScreener"><div class="token">${esc(x.token_symbol)}<span class="external">↗</span></div><div class="address">${esc(x.chain_label)} · ${esc(x.token_address.slice(0,8))}…${esc(x.token_address.slice(-6))}</div></a>`:`<div class="token">${esc(x.token_symbol)}</div><div class="address">${esc(x.chain_label)} · ${esc(x.token_address.slice(0,8))}…${esc(x.token_address.slice(-6))}</div>`}</td><td>${badge(x.status)}</td><td><div class="token">${esc(x.structure_state)}</div><div class="address">${num(x.structure_confidence)}% · 15m proxy</div></td><td>${money(x.price_usd)}</td><td><div>${money(x.liquidity_usd)}</div><div class="address">${esc(x.liquidity_tier)}</div></td><td><div>${num(x.holder_count)}</div><div class="address">${age(x.holder_data_age_seconds)} old</div></td><td>${trend(x.trends['1h'].price_change_pct)}</td><td>${trend(x.trends['6h'].price_change_pct)}</td><td>${trend(x.trends['24h'].price_change_pct)}</td><td>${trend(x.trends['24h'].relative_to_eth_pct)}</td><td>${x.trends['24h'].holder_change==null?'<span class="neutral">collecting</span>':`<span class="${x.trends['24h'].holder_change>0?'pos':x.trends['24h'].holder_change<0?'neg':'neutral'}">${x.trends['24h'].holder_change>0?'+':''}${num(x.trends['24h'].holder_change)}</span>`}</td><td>${money(x.volume_h1_usd)}</td><td>${x.status==='EVM_PROVIDER_UNAVAILABLE'?`<div class="token">Provider unavailable</div><div class="address">trusted snapshot ${age(x.trusted_snapshot_age_seconds)} ago</div>`:x.status==='EVM_DATA_ANOMALY'?'<div class="token">Anomaly quarantined</div><div class="address">last trusted data retained</div>':x.status==='EVM_BENCHMARK'?'<div class="token">Market benchmark</div><div class="address">excluded from token alerts</div>':esc(x.data_quality)}</td></tr>`).join(''):'<tr><td colspan="13" class="empty">No EVM snapshots yet.</td></tr>';
const windows=d.solana_activity?.windows||{};document.getElementById('solWindowBody').innerHTML=['1h','6h','24h'].map(w=>{const x=windows[w]||{};return `<tr><td><div class="token">${w}</div></td><td>${num(x.events)}</td><td>${num(x.active_wallets)}</td><td>${num(x.independent_clusters)}</td><td>${num(x.unique_tokens_bought)}</td><td>${num(x.tokens_with_2_buyers)}</td><td>${num(x.tokens_with_3_plus_buyers)}</td><td>${num(x.missing_symbols)}</td></tr>`}).join('');
const pc=d.candidate_pipeline?.counts||{};document.getElementById('pipelineBody').innerHTML=`<tr><td>${num(pc.discovered)}</td><td>${num(pc.awaiting_score)}</td><td>${num(pc.scored)}</td><td>${num(pc.performance_pass)}</td><td>${num(pc.screened)}</td><td>${num(pc.low_risk)}</td><td>${num(pc.validated)}</td><td>${num(pc.probation_ready)}</td><td>${num(pc.probation)}</td><td>${num(pc.consensus_validated)}</td></tr>`;
const screened=d.screened_wallets||[];document.getElementById('screenedWalletBody').innerHTML=screened.length?screened.map(x=>`<tr><td><a class="token-link" href="https://solscan.io/account/${esc(x.wallet)}" target="_blank" rel="noopener noreferrer"><div class="token">${esc(x.wallet.slice(0,6))}…${esc(x.wallet.slice(-6))}<span class="external">↗</span></div><div class="address">provisional pass · zero consensus weight</div></a></td><td><span class="badge ${x.source==='DEX DISCOVERY'?'accumulation':'observe'}">${esc(x.source)}</span></td><td>${num(x.performance_score)}</td><td>${num(x.screening_risk_score)}</td><td>${money(x.realized_pnl_30d)}</td><td>${num(x.trades_30d)}</td></tr>`).join(''):'<tr><td colspan="6" class="empty">No wallets have passed both initial performance and risk screening yet.</td></tr>';
const ready=d.candidate_pipeline?.probation_ready_wallets||[];document.getElementById('probationReadyBody').innerHTML=ready.length?ready.map(x=>`<tr><td><a class="token-link" href="https://solscan.io/account/${esc(x.wallet)}" target="_blank" rel="noopener noreferrer"><div class="token">${esc(x.wallet.slice(0,6))}…${esc(x.wallet.slice(-6))}<span class="external">↗</span></div><div class="address">manual admission · zero consensus weight</div></a></td><td>${num(x.performance_score)}</td><td>${num(x.screening_risk_score)}</td><td>${money(x.realized_pnl_30d)}</td><td>${num(x.trades_30d)}</td><td>${esc(x.validation_status)}</td><td>${num(x.repeat_early_entries)}</td><td>${num(x.average_early_entry_score)}</td></tr>`).join(''):'<tr><td colspan="8" class="empty">No newly discovered wallets have passed every probation admission gate yet.</td></tr>';
const probation=d.probation_wallets||[];document.getElementById('probationBody').innerHTML=probation.length?probation.map(x=>`<tr><td><a class="token-link" href="https://solscan.io/account/${esc(x.wallet)}" target="_blank" rel="noopener noreferrer"><div class="token">${esc(x.wallet.slice(0,6))}…${esc(x.wallet.slice(-6))}<span class="external">↗</span></div><div class="address">zero consensus weight</div></a></td><td><span class="badge provider">PROBATION</span></td><td>${when(x.admitted_at)}</td><td>${age(x.probation_age_seconds)}</td><td>${num(x.performance_score)}</td><td>${x.screening_risk_score==null?'—':num(x.screening_risk_score)}</td><td><div>${num(x.repeat_early_entries)}</div><div class="address">avg ${num(x.average_early_entry_score)}</div></td><td><div>${num(x.forward_trades)}</div><div class="address">minimum ${num(d.probation_policy.minimum_forward_trades)}</div></td><td><div>${num(x.forward_tokens)}</div><div class="address">minimum ${num(d.probation_policy.minimum_forward_tokens)}</div></td><td>${when(x.last_forward_activity_at)}</td><td>${x.promotion_review_ready?'<span class="badge accumulation">READY FOR REVIEW</span>':'<span class="badge expired">COLLECTING</span>'}</td></tr>`).join(''):'<tr><td colspan="11" class="empty">No wallets are currently in probation. Candidates remain in the discovery funnel until all admission gates pass.</td></tr>';
document.getElementById('solBody').innerHTML=d.solana_signals.length?d.solana_signals.map(x=>`<tr><td>${x.dexscreener_url?`<a class="token-link" href="${esc(x.dexscreener_url)}" target="_blank" rel="noopener noreferrer"><div class="token">${esc(x.token_symbol||'Unknown')}<span class="external">↗</span></div><div class="address">${esc((x.token_address||'').slice(0,10))}…</div></a>`:`<div class="token">${esc(x.token_symbol||'Unknown')}</div>`}</td><td>${badge(x.display_status)}</td><td>${num(x.buy_score)}</td><td>${num(x.sell_score)}</td><td>${num(x.independent_buy_clusters)}</td><td>${num(x.independent_sell_clusters)}</td><td>${esc(x.safety_status)}</td><td><div>${when(x.last_activity_at)}</div><div class="address">${age(x.signal_age_seconds)} ago</div></td></tr>`).join(''):'<tr><td colspan="8" class="empty">No active Solana paper signals.</td></tr>';
document.getElementById('footer').textContent=`${esc(d.version)} · generated ${when(d.generated_at)} · auto-refreshes every 60 seconds.`;
}catch(e){document.getElementById('refreshState').textContent='Dashboard data unavailable';document.querySelector('.warning').classList.add('error');document.querySelector('.warning').textContent='Could not load dashboard data. The monitoring APIs continue running independently.';}}
load();setInterval(load,60000);
</script></body></html>"""


@app.get("/dashboard-data")
def dashboard_data_endpoint():
    return jsonify(build_dashboard_payload())


@app.get("/dashboard")
def dashboard_endpoint():
    return render_template_string(DASHBOARD_HTML)


@app.get("/premium-funnel")
def premium_funnel():
    """Bounded score -> prefilter -> Helius -> validation pipeline."""
    initialise_database()
    try:
        limit = min(max(int(request.args.get("limit", 5)), 1), 10)
    except ValueError:
        limit = 5
    deadline = time.monotonic() + PIPELINE_MAX_SECONDS
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.wallet FROM candidate_wallets c
                LEFT JOIN wallet_screenings ws ON ws.wallet = c.wallet
                    AND ws.screening_version = %s
                WHERE c.score_status <> 'parse_incomplete'
                    AND (
                        c.last_scored IS NULL
                        OR (
                            c.score_status = 'scored' AND c.score >= 30
                            AND (
                                ws.wallet IS NULL
                                OR (
                                    c.validation_status <> 'validated'
                                    AND COALESCE(ws.risk_score, 100) <= 45
                                    AND COALESCE(ws.risk_flags, '[]')
                                        NOT LIKE '%%service_like_activity%%'
                                    AND COALESCE(ws.risk_flags, '[]')
                                        NOT LIKE '%%bursty_automated_activity%%'
                                )
                            )
                        )
                    )
                ORDER BY CASE WHEN c.last_scored IS NULL THEN 0 ELSE 1 END,
                    c.tokens_found DESC, c.score DESC,
                    c.realized_pnl_30d DESC NULLS LAST, c.created_at ASC
                LIMIT %s
            """, (SCREENING_VERSION, limit))
            wallets = [row[0] for row in cur.fetchall()]

    results, stopped_reason = [], None
    for wallet in wallets:
        if time.monotonic() >= deadline:
            stopped_reason = "deadline_guard"
            break
        item = {"wallet": wallet, "stages": {}}
        candidate = candidate_for_screening(wallet)
        if not candidate or candidate["score_status"] != "scored":
            result = score_wallet(wallet)
            response, status = result if isinstance(result, tuple) else (result, result.status_code)
            item["stages"]["score"] = status
            if status != 200:
                results.append(item)
                if status == 429:
                    stopped_reason = "birdeye_429"
                    break
                continue
            candidate = candidate_for_screening(wallet)
        else:
            item["stages"]["score"] = "already_complete"

        # This protects Helius CUs without weakening WATCH or ASYMMETRIC rules.
        if not candidate or candidate["score"] < 30:
            item["stages"]["prefilter"] = "rejected_score_below_30"
            results.append(item)
            continue
        item["stages"]["prefilter"] = "passed"

        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM wallet_screenings WHERE wallet = %s AND screening_version = %s",
                            (wallet, SCREENING_VERSION))
                screened = cur.fetchone() is not None
        if not screened:
            result = screen_wallet(wallet)
            response, status = result if isinstance(result, tuple) else (result, result.status_code)
            item["stages"]["screen"] = status
            if status != 200:
                results.append(item)
                if status == 429:
                    stopped_reason = "helius_429"
                    break
                continue
        else:
            item["stages"]["screen"] = "already_complete"

        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.screening_risk_score, c.validation_status,
                        COALESCE(ws.risk_flags, '[]')
                    FROM candidate_wallets c
                    LEFT JOIN wallet_screenings ws ON ws.wallet = c.wallet
                        AND ws.screening_version = %s
                    WHERE c.wallet = %s
                """, (SCREENING_VERSION, wallet))
                risk_score, validation_status, risk_flags_text = cur.fetchone()
        try:
            risk_flags = set(json.loads(risk_flags_text or "[]"))
        except (TypeError, ValueError):
            risk_flags = set()
        hard_validation_blocks = {
            "service_like_activity", "bursty_automated_activity",
        }
        if risk_flags & hard_validation_blocks:
            item["stages"]["validation"] = "skipped_hard_risk_prefilter"
        elif risk_score is None or risk_score > 45:
            item["stages"]["validation"] = "skipped_risk_prefilter"
        elif validation_status != "validated" and time.monotonic() < deadline:
            result = validate_wallet(wallet)
            response, status = result if isinstance(result, tuple) else (result, result.status_code)
            item["stages"]["validation"] = status
            if status == 429:
                stopped_reason = "birdeye_429"
        else:
            item["stages"]["validation"] = "already_complete"
        results.append(item)
        if stopped_reason:
            break

    return jsonify({"success": True, "version": VERSION, "selected": len(wallets),
                    "processed": len(results), "stopped_reason": stopped_reason,
                    "deadline_seconds": PIPELINE_MAX_SECONDS, "results": results,
                    "note": "Repeat this idempotent endpoint to continue incomplete work."})


def run_evm_refresh_once():
    """Railway cron entrypoint with controlled partial-run semantics."""
    try:
        result = refresh_evm_watchlist(limit=10, offset=0)
    except Exception as exc:
        print(json.dumps({
            "success": False, "version": VERSION,
            "error": type(exc).__name__, "mode": "evm_cron_once",
        }))
        return 1
    controlled_partial = bool(
        result.get("stopped_reason") == "deadline_guard"
        and result.get("processed", 0) > 0
        and result.get("failures", 0) == 0
    )
    cron_success = bool(result["success"] or controlled_partial)
    print(json.dumps({
        "success": cron_success, "refresh_complete": result["success"],
        "version": VERSION,
        "run_id": result["run_id"], "selected": result["selected"],
        "processed": result["processed"], "transitions": result["transitions"],
        "deferred": result.get("deferred", 0),
        "failures": result.get("failures", 0), "status": result.get("status"),
        "chain_counts": result.get("chain_counts", {}),
        "stopped_reason": result["stopped_reason"], "mode": "evm_cron_once",
    }))
    return 0 if cron_success else 1


if __name__ == "__main__":
    if "--evm-refresh-once" in sys.argv:
        raise SystemExit(run_evm_refresh_once())
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
