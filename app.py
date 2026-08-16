import json
import hmac
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg
import requests
from flask import Flask, jsonify, request


app = Flask(__name__)

VERSION = "4.8.0-paper-evm"
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
ROBINHOOD_BLOCKSCOUT_URL = "https://robinhoodchain.blockscout.com/api/v2"
EVM_REFRESH_MAX_SECONDS = min(
    max(int(os.getenv("EVM_REFRESH_MAX_SECONDS", "55")), 15), 75
)
EVM_ALERT_STATUSES = {
    value.strip().upper()
    for value in os.getenv(
        "EVM_ALERT_STATUSES", "EVM_MOMENTUM,EVM_HIGH_MOMENTUM,EVM_RISK"
    ).split(",")
    if value.strip()
}
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
    ("robinhood", "native:ETH", "ETH", "Ether", "portfolio", "benchmark"),
    ("robinhood", "0x232CDFc415D10b673845D83Dc02ba2eaBe7e30d1", "IF", "What IF", "portfolio", "evm_monitoring_ready"),
    ("robinhood", "0xe934e36A439C94017B64a3FecE66AF12099aBF50", "STONKBROKER", "StonkBroker", "portfolio", "evm_monitoring_ready"),
    ("robinhood", "0x020bfC650A365f8BB26819deAAbF3E21291018b4", "CASHCAT", "Cash Cat", "portfolio", "evm_monitoring_ready"),
    ("robinhood", "0xfd181632e1F2335DaB74535E6dD29082d3191bb2", "RFLX", "RFLIX", "portfolio", "evm_monitoring_ready"),
    ("robinhood", "0xeC45C6C413b498Cf5aCF5a1a889F1a95cA9b6bB3", "PORTLY", "PORTLY", "existing_test_case", "evm_monitoring_ready"),
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
                "telegram_requests": 0, "telegram_deliveries": 0,
                "telegram_failures": 0, "retries": 0, "rate_limits": 0,
                "timeouts": 0, "upstream_errors": 0}


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


def helius_get_transactions(wallet):
    """Fetch a bounded, parsed history sample for heuristic screening."""
    return upstream_request(
        "GET", f"https://api-mainnet.helius-rpc.com/v0/addresses/{wallet}/transactions",
        params={
            "api-key": os.getenv("HELIUS_API_KEY", ""),
            "token-accounts": "balanceChanged",
            "sort-order": "desc",
            "limit": HELIUS_HISTORY_LIMIT,
        },
        timeout=25, retries=1, provider="helius",
    )


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
            cur.execute("ALTER TABLE discovery_observations ADD COLUMN IF NOT EXISTS token_symbol TEXT")
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
                                'live_evm_monitoring', 'partial_evm_monitoring'
                            ) THEN token_watchlist.monitoring_status
                            ELSE EXCLUDED.monitoring_status
                        END,
                        updated_at = NOW()
                """, (chain, address, symbol, name, source, monitoring_status))
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
        "service": "Solana Smart Wallet + Robinhood Chain Contract Monitor",
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
                    "helius_webhook_configured": bool(os.getenv("HELIUS_WEBHOOK_SECRET")),
                    "helius_webhook_sync_configured": bool(os.getenv("HELIUS_API_KEY")),
                    "helius_auto_sync": HELIUS_AUTO_SYNC,
                    "helius_target_webhook_url_configured": bool(HELIUS_TARGET_WEBHOOK_URL),
                    "admin_key_configured": bool(os.getenv("ADMIN_API_KEY")),
                    "telegram_configured": bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID")),
                    "telegram_alert_statuses": sorted(TELEGRAM_ALERT_STATUSES),
                    "evm_chain_id": ROBINHOOD_CHAIN_ID,
                    "evm_refresh_max_seconds": EVM_REFRESH_MAX_SECONDS,
                    "evm_alert_statuses": sorted(EVM_ALERT_STATUSES),
                    "counters": counters})


# =========================================================
# DISCOVERY
# =========================================================

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
                ORDER BY tokens_found DESC, created_at ASC LIMIT %s
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
                ORDER BY score DESC, realized_pnl_30d DESC NULLS LAST
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
                ORDER BY COALESCE(ws.risk_score, 100) ASC,
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
    return {
        row[0]: {
            "cluster_id": row[1], "classification": row[2],
            "confidence": row[3], "signal_weight": row[4],
        }
        for row in rows
    }


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
    """Compare eligible Solana wallets with Helius and update only on drift."""
    desired = sorted(load_tracked_wallet_map())
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


def format_signal_notification(signal, *, test=False):
    symbol = signal.get("token_symbol") or "Unknown token"
    status = signal.get("status") or "UNKNOWN"
    heading = "TEST — Wallet Monitor notification pipeline" if test else f"Paper signal: {status}"
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
# V4.8 ROBINHOOD CHAIN CONTRACT MONITORING
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


def fetch_robinhood_token_snapshot(token):
    """Fetch one bounded market/holder snapshot without wallet custody access."""
    address = token[1]
    snapshot = {
        "chain": token[0], "token_address": address,
        "token_symbol": token[2], "provider_errors": [],
    }
    market_ok = False
    holders_ok = False

    try:
        response = upstream_request(
            "GET", DEXSCREENER_TOKEN_URL.format(address=address),
            timeout=10, retries=1, provider="dexscreener",
        )
        if response.status_code == 200:
            payload = response.json()
            pairs = payload.get("pairs") if isinstance(payload, dict) else []
            pairs = pairs if isinstance(pairs, list) else []
            robinhood_pairs = [
                pair for pair in pairs if isinstance(pair, dict) and (
                    "robinhood" in str(pair.get("chainId") or "").lower()
                    or str(pair.get("chainId") or "") == str(ROBINHOOD_CHAIN_ID)
                )
            ]
            if robinhood_pairs:
                pair = max(
                    robinhood_pairs,
                    key=lambda item: safe_float((item.get("liquidity") or {}).get("usd")) or 0,
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
                snapshot["provider_errors"].append("dexscreener_no_robinhood_pair")
        else:
            snapshot["provider_errors"].append(f"dexscreener_http_{response.status_code}")
    except (requests.RequestException, ValueError, TypeError) as exc:
        snapshot["provider_errors"].append(f"dexscreener_{type(exc).__name__}")

    try:
        response = upstream_request(
            "GET", f"{ROBINHOOD_BLOCKSCOUT_URL}/tokens/{address}",
            timeout=10, retries=0, provider="blockscout",
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


def classify_evm_snapshot(snapshot, previous):
    """Conservative research state; never an execution recommendation."""
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

    if risk_score >= 50:
        status = "EVM_RISK"
    elif previous and momentum_score >= 70 and holder_change is not None:
        status = "EVM_HIGH_MOMENTUM"
    elif momentum_score >= 45:
        status = "EVM_MOMENTUM"
    else:
        status = "EVM_OBSERVE"
    return {
        "status": status, "momentum_score": min(momentum_score, 100),
        "risk_score": min(risk_score, 100), "reasons": reasons,
        "holder_change_pct": holder_change,
        "liquidity_change_pct": liquidity_change,
        "volume_liquidity_ratio": volume_liquidity,
        "buy_sell_ratio": buy_sell,
    }


def format_evm_notification(signal, *, test=False):
    heading = "TEST — V4.8 EVM notification pipeline" if test else f"EVM state change: {signal['status']}"
    return "\n".join([
        heading,
        f"Token: {signal.get('token_symbol') or 'Unknown'}",
        f"Address: {signal.get('token_address') or 'test-only'}",
        f"Momentum score: {signal.get('momentum_score', 0)}/100",
        f"Risk score: {signal.get('risk_score', 0)}/100",
        f"Data quality: {signal.get('data_quality', 'partial')}",
        f"Reasons: {', '.join(signal.get('reasons') or ['baseline observation'])}",
        "ROBINHOOD CHAIN — PAPER RESEARCH ONLY; not a trade instruction.",
    ])


def queue_and_deliver_evm_notification(signal, event_key, *, test=False):
    notification_id, created = queue_notification(
        event_key, "evm_test" if test else "evm_state_transition",
        signal, format_evm_notification(signal, test=test),
    )
    result = deliver_notification(notification_id)
    result["created"] = created
    return result


def previous_evm_snapshot(chain, address):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT liquidity_usd, holder_count FROM evm_token_snapshots
                WHERE chain = %s AND token_address = %s
                ORDER BY captured_at DESC LIMIT 1
            """, (chain, address))
            row = cur.fetchone()
    return None if not row else {"liquidity_usd": row[0], "holder_count": row[1]}


def persist_evm_snapshot(snapshot):
    previous = previous_evm_snapshot(snapshot["chain"], snapshot["token_address"])
    classification = classify_evm_snapshot(snapshot, previous)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status FROM evm_token_signals
                WHERE chain = %s AND token_address = %s
            """, (snapshot["chain"], snapshot["token_address"]))
            old_row = cur.fetchone()
            previous_status = old_row[0] if old_row else None
            cur.execute("""
                INSERT INTO evm_token_snapshots (
                    chain, token_address, token_symbol, price_usd, liquidity_usd,
                    market_cap_usd, fdv_usd, volume_h1_usd, volume_h24_usd,
                    buys_h1, sells_h1, buys_h24, sells_h24,
                    price_change_h1_pct, price_change_h24_pct, holder_count,
                    pair_address, dex_id, data_quality, provider_errors
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
            ))
            snapshot_id = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO evm_token_signals (
                    chain, token_address, token_symbol, status, momentum_score,
                    risk_score, reasons, holder_change_pct, liquidity_change_pct,
                    volume_liquidity_ratio, buy_sell_ratio, latest_snapshot_id,
                    data_quality, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (chain, token_address) DO UPDATE SET
                    token_symbol = EXCLUDED.token_symbol, status = EXCLUDED.status,
                    momentum_score = EXCLUDED.momentum_score,
                    risk_score = EXCLUDED.risk_score, reasons = EXCLUDED.reasons,
                    holder_change_pct = EXCLUDED.holder_change_pct,
                    liquidity_change_pct = EXCLUDED.liquidity_change_pct,
                    volume_liquidity_ratio = EXCLUDED.volume_liquidity_ratio,
                    buy_sell_ratio = EXCLUDED.buy_sell_ratio,
                    latest_snapshot_id = EXCLUDED.latest_snapshot_id,
                    data_quality = EXCLUDED.data_quality, updated_at = NOW()
            """, (
                snapshot["chain"], snapshot["token_address"], snapshot["token_symbol"],
                classification["status"], classification["momentum_score"],
                classification["risk_score"], json.dumps(classification["reasons"]),
                classification["holder_change_pct"], classification["liquidity_change_pct"],
                classification["volume_liquidity_ratio"], classification["buy_sell_ratio"],
                snapshot_id, snapshot["data_quality"],
            ))
            history_id = None
            if previous_status != classification["status"]:
                cur.execute("""
                    INSERT INTO evm_signal_history (
                        chain, token_address, token_symbol, previous_status, status,
                        momentum_score, risk_score, details
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (
                    snapshot["chain"], snapshot["token_address"], snapshot["token_symbol"],
                    previous_status, classification["status"], classification["momentum_score"],
                    classification["risk_score"], json.dumps({
                        **classification, "snapshot_id": snapshot_id,
                        "data_quality": snapshot["data_quality"],
                        "provider_errors": snapshot.get("provider_errors") or [],
                    }),
                ))
                history_id = cur.fetchone()[0]
            cur.execute("""
                UPDATE token_watchlist SET monitoring_status = %s, updated_at = NOW()
                WHERE chain = %s AND token_address = %s
            """, (
                "live_evm_monitoring" if snapshot["data_quality"] == "complete"
                else "partial_evm_monitoring",
                snapshot["chain"], snapshot["token_address"],
            ))
        conn.commit()

    result = {**classification, **snapshot, "snapshot_id": snapshot_id,
              "previous_status": previous_status, "history_id": history_id}
    if previous_status and history_id and classification["status"] in EVM_ALERT_STATUSES:
        try:
            notification = queue_and_deliver_evm_notification(
                result, f"evm-signal-history:{history_id}"
            )
            result["notification_status"] = notification.get("status")
        except Exception:
            diagnostic_increment("telegram_failures")
            result["notification_status"] = "failed_without_blocking_refresh"
    else:
        result["notification_status"] = "baseline_or_no_alert_transition"
    return result


def refresh_evm_watchlist(limit=6, offset=0):
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT chain, token_address, token_symbol, token_name
                FROM token_watchlist
                WHERE active = TRUE AND chain = 'robinhood'
                    AND token_address LIKE '0x%%'
                ORDER BY token_symbol LIMIT %s OFFSET %s
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
    for token in tokens:
        if time.monotonic() >= deadline:
            stopped_reason = "deadline_guard"
            break
        try:
            snapshot = fetch_robinhood_token_snapshot(token)
            if snapshot["data_quality"] == "unavailable":
                diagnostic_increment("evm_refresh_failures")
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
    status = "complete" if len(results) == len(tokens) else "partial"
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE evm_refresh_runs SET completed_at = NOW(),
                    snapshots_created = %s, transitions_created = %s,
                    status = %s, details = %s WHERE id = %s
            """, (
                sum(1 for item in results if item.get("snapshot_id")), transitions,
                status, json.dumps({"stopped_reason": stopped_reason}), run_id,
            ))
        conn.commit()
    diagnostic_increment("evm_refreshes")
    return {
        "success": status == "complete", "run_id": run_id,
        "selected": len(tokens), "processed": len(results),
        "transitions": transitions, "stopped_reason": stopped_reason,
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
                        'unverified', FALSE, NOW())
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
                        safety_status = 'unverified', actionable = FALSE, updated_at = NOW()
                """, (
                    token, item.get("token_symbol"), status, buy_score, sell_score,
                    buy_clusters, sell_clusters, json.dumps(wallets),
                    json.dumps(clusters), item["first_activity_at"], item["last_activity_at"],
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
                    "actionable": False, "safety_status": "unverified",
                })

            cur.execute("""
                SELECT token_address, token_symbol, buy_score, sell_score,
                    independent_buy_clusters, independent_sell_clusters
                FROM paper_signals
                WHERE last_activity_at < %s AND status <> 'EXPIRED'
            """, (cutoff,))
            expiring = cur.fetchall()
            for expired in expiring:
                cur.execute("""
                    INSERT INTO paper_signal_history (
                        token_address, token_symbol, status, buy_score, sell_score,
                        independent_buy_clusters, independent_sell_clusters, details
                    ) VALUES (%s, %s, 'EXPIRED', %s, %s, %s, %s, %s)
                """, (
                    expired[0], expired[1], expired[2], expired[3],
                    expired[4], expired[5], json.dumps({"reason": "signal_window_elapsed"}),
                ))
            cur.execute("""
                UPDATE paper_signals SET status = 'EXPIRED', actionable = FALSE,
                    updated_at = NOW()
                WHERE last_activity_at < %s AND status <> 'EXPIRED'
            """, (cutoff,))
        conn.commit()

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
                    monitoring_status, active, added_at, updated_at
                FROM token_watchlist
                WHERE active = TRUE
                ORDER BY chain, token_symbol
            """)
            rows = cur.fetchall()
    items = []
    for row in rows:
        explorer_url = None
        if row[0] == "robinhood" and row[1].startswith("0x"):
            explorer_url = f"https://robinhoodchain.blockscout.com/token/{row[1]}"
        items.append({
            "chain": row[0], "chain_id": 4663 if row[0] == "robinhood" else None,
            "token_address": row[1], "token_symbol": row[2],
            "token_name": row[3], "source": row[4],
            "monitoring_status": row[5], "active": row[6],
            "explorer_url": explorer_url, "added_at": row[7], "updated_at": row[8],
        })
    return jsonify({
        "success": True, "count": len(items), "tokens": items,
        "note": "V4.8 contract monitoring is live for Robinhood Chain ERC-20 assets; native ETH remains the benchmark.",
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
    if not chain or not symbol or not (valid_evm_address or address.startswith("native:")):
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


EVM_SIGNAL_SELECT = """
    SELECT s.chain, s.token_address, s.token_symbol, s.status,
        s.momentum_score, s.risk_score, s.reasons,
        s.holder_change_pct, s.liquidity_change_pct,
        s.volume_liquidity_ratio, s.buy_sell_ratio,
        s.data_quality, s.updated_at, p.price_usd, p.liquidity_usd,
        p.market_cap_usd, p.volume_h1_usd, p.price_change_h1_pct,
        p.holder_count, p.pair_address, p.captured_at
    FROM evm_token_signals s
    LEFT JOIN evm_token_snapshots p ON p.id = s.latest_snapshot_id
"""


def serialize_evm_signal(row):
    try:
        reasons = json.loads(row[6] or "[]")
    except (TypeError, ValueError):
        reasons = []
    return {
        "chain": row[0], "chain_id": ROBINHOOD_CHAIN_ID,
        "token_address": row[1], "token_symbol": row[2], "status": row[3],
        "momentum_score": row[4], "risk_score": row[5], "reasons": reasons,
        "holder_change_pct": row[7], "liquidity_change_pct": row[8],
        "volume_liquidity_ratio": row[9], "buy_sell_ratio": row[10],
        "data_quality": row[11], "updated_at": row[12], "price_usd": row[13],
        "liquidity_usd": row[14], "market_cap_usd": row[15],
        "volume_h1_usd": row[16], "price_change_h1_pct": row[17],
        "holder_count": row[18], "pair_address": row[19],
        "captured_at": row[20], "paper_mode": True, "actionable": False,
    }


@app.post("/refresh-evm-watchlist")
def refresh_evm_watchlist_endpoint():
    if not os.getenv("ADMIN_API_KEY"):
        return jsonify({"success": False, "error": "ADMIN_API_KEY is not configured"}), 503
    if not admin_authorized():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    try:
        limit = min(max(int(request.args.get("limit", 6)), 1), 10)
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
                SELECT id, started_at, completed_at, tokens_selected,
                    snapshots_created, transitions_created, status, details
                FROM evm_refresh_runs ORDER BY id DESC LIMIT 1
            """)
            row = cur.fetchone()
            cur.execute("""
                SELECT COUNT(*) FROM token_watchlist
                WHERE active = TRUE AND chain = 'robinhood'
                    AND token_address LIKE '0x%%'
            """)
            tracked_contracts = cur.fetchone()[0]
    latest_run = None if not row else {
        "id": row[0], "started_at": row[1], "completed_at": row[2],
        "tokens_selected": row[3], "snapshots_created": row[4],
        "transitions_created": row[5], "status": row[6],
        "details": json.loads(row[7] or "{}"),
    }
    return jsonify({
        "success": True, "version": VERSION, "chain": "robinhood",
        "chain_id": ROBINHOOD_CHAIN_ID, "tracked_contracts": tracked_contracts,
        "signal_counts": counts, "latest_refresh": latest_run,
        "providers": {"market": "DexScreener", "holders": "Blockscout"},
        "paper_mode": True, "actionable": False,
    })


@app.get("/evm-signals")
def evm_signals_endpoint():
    initialise_database()
    status = str(request.args.get("status") or "").strip().upper()
    with db() as conn:
        with conn.cursor() as cur:
            if status:
                cur.execute(EVM_SIGNAL_SELECT + " WHERE s.status = %s ORDER BY s.updated_at DESC", (status,))
            else:
                cur.execute(EVM_SIGNAL_SELECT + " ORDER BY s.updated_at DESC")
            rows = cur.fetchall()
    return jsonify({
        "success": True, "count": len(rows),
        "signals": [serialize_evm_signal(row) for row in rows],
        "paper_mode": True, "actionable": False,
        "warning": "Contract analytics are observations, not buy or sell instructions.",
    })


@app.get("/evm-token/<token_address>")
def evm_token_detail_endpoint(token_address):
    valid_address = (
        len(token_address) == 42 and token_address.startswith("0x")
        and all(character in "0123456789abcdefABCDEF" for character in token_address[2:])
    )
    if not valid_address:
        return jsonify({"success": False, "error": "Invalid EVM token address"}), 400
    initialise_database()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                EVM_SIGNAL_SELECT + " WHERE LOWER(s.token_address) = LOWER(%s)",
                (token_address,),
            )
            row = cur.fetchone()
            cur.execute("""
                SELECT id, price_usd, liquidity_usd, market_cap_usd,
                    volume_h1_usd, price_change_h1_pct, holder_count,
                    data_quality, provider_errors, captured_at
                FROM evm_token_snapshots
                WHERE LOWER(token_address) = LOWER(%s)
                ORDER BY captured_at DESC LIMIT 48
            """, (token_address,))
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
    with db() as conn:
        with conn.cursor() as cur:
            if symbol:
                cur.execute("""
                    SELECT id, chain, token_address, token_symbol, price_usd,
                        liquidity_usd, market_cap_usd, volume_h1_usd,
                        price_change_h1_pct, holder_count, data_quality,
                        provider_errors, captured_at
                    FROM evm_token_snapshots WHERE token_symbol = %s
                    ORDER BY captured_at DESC LIMIT %s
                """, (symbol, limit))
            else:
                cur.execute("""
                    SELECT id, chain, token_address, token_symbol, price_usd,
                        liquidity_usd, market_cap_usd, volume_h1_usd,
                        price_change_h1_pct, holder_count, data_quality,
                        provider_errors, captured_at
                    FROM evm_token_snapshots ORDER BY captured_at DESC LIMIT %s
                """, (limit,))
            rows = cur.fetchall()
    return jsonify({"success": True, "count": len(rows), "snapshots": [{
        "id": row[0], "chain": row[1], "token_address": row[2],
        "token_symbol": row[3], "price_usd": row[4], "liquidity_usd": row[5],
        "market_cap_usd": row[6], "volume_h1_usd": row[7],
        "price_change_h1_pct": row[8], "holder_count": row[9],
        "data_quality": row[10], "provider_errors": json.loads(row[11] or "[]"),
        "captured_at": row[12], "actionable": False,
    } for row in rows], "paper_mode": True})


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
    signals = refresh_paper_signals() if inserted else []
    return jsonify({
        "success": True, "events_parsed": len(events), "events_inserted": inserted,
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


def serialize_signal_row(row):
    try:
        wallets = json.loads(row[8] or "[]")
        clusters = json.loads(row[9] or "[]")
    except (TypeError, ValueError):
        wallets, clusters = [], []
    return {
        "token_address": row[0], "token_symbol": row[1], "status": row[2],
        "buy_score": row[3], "sell_score": row[4],
        "independent_buy_clusters": row[5],
        "independent_sell_clusters": row[6],
        "contributing_wallets": wallets, "contributing_clusters": clusters,
        "first_activity_at": row[10], "last_activity_at": row[11],
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
    status = request.args.get("status")
    with db() as conn:
        with conn.cursor() as cur:
            if status:
                cur.execute(SIGNAL_SELECT + " WHERE status = %s ORDER BY updated_at DESC", (status.upper(),))
            else:
                cur.execute(SIGNAL_SELECT + " ORDER BY updated_at DESC")
            rows = cur.fetchall()
    return jsonify({
        "success": True, "count": len(rows),
        "signals": [serialize_signal_row(row) for row in rows],
        "paper_mode": True, "actionable": False,
        "warning": "Paper research only; token safety is not yet verified.",
    })


@app.get("/signal/<token_address>")
def signal_detail_endpoint(token_address):
    initialise_database()
    if not is_valid_solana_address(token_address):
        return jsonify({"success": False, "error": "Invalid Solana token address"}), 400
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


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
