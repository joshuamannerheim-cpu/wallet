import json
import os
import random
import threading
import time
from datetime import datetime, timezone

import psycopg
import requests
from flask import Flask, jsonify, request


app = Flask(__name__)

VERSION = "4.6.0-premium"
SCREENING_VERSION = "4.2.2"
INDEPENDENT_REPEAT_SECONDS = 6 * 60 * 60
SOL_MINT = "So11111111111111111111111111111111111111112"
STABLE_SYMBOLS = {
    "USDC", "USDT", "USD1", "PYUSD", "USDS", "USDE", "DAI", "FDUSD",
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
BASE58_ALPHABET = (
    "123456789ABCDEFGHJKLMNPQRSTUVWXYZ"
    "abcdefghijkmnopqrstuvwxyz"
)

_rate_lock = threading.Lock()
_next_birdeye_request = 0.0
_diagnostic_lock = threading.Lock()
_diagnostics = {"birdeye_requests": 0, "helius_requests": 0, "retries": 0,
                "rate_limits": 0, "timeouts": 0, "upstream_errors": 0}


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
        "service": "Solana Smart Wallet Monitor",
        "status": "online",
        "version": VERSION,
        "mode": "candidate discovery and scoring",
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
                    "birdeye_rps": BIRDEYE_RPS,
                    "discovery_max_tokens": DISCOVERY_MAX_TOKENS,
                    "pipeline_max_seconds": PIPELINE_MAX_SECONDS,
                    "counters": counters})


# =========================================================
# DISCOVERY
# =========================================================

@app.get("/discover")
def discover():
    """
    V4.5 discovery:
    - supports up to 20 trending tokens per run;
    - pages Birdeye trending results in conservative groups of five;
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
                SELECT wallet FROM candidate_wallets
                WHERE score_status <> 'parse_incomplete'
                    AND (last_scored IS NULL OR last_screened IS NULL
                         OR validation_status <> 'validated')
                ORDER BY CASE WHEN last_scored IS NULL THEN 0 ELSE 1 END,
                    tokens_found DESC, score DESC,
                    realized_pnl_30d DESC NULLS LAST, created_at ASC
                LIMIT %s
            """, (limit,))
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
                cur.execute("SELECT screening_risk_score, validation_status FROM candidate_wallets WHERE wallet = %s",
                            (wallet,))
                risk_score, validation_status = cur.fetchone()
        if risk_score is None or risk_score > 45:
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
