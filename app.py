import json
import os
import time
from datetime import datetime, timezone

import psycopg
import requests
from flask import Flask, jsonify, request


app = Flask(__name__)

VERSION = "4.3.1"
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
BIRDEYE_THROTTLE_SECONDS = 2
HELIUS_THROTTLE_SECONDS = 2
HELIUS_HISTORY_LIMIT = 100
BASE58_ALPHABET = (
    "123456789ABCDEFGHJKLMNPQRSTUVWXYZ"
    "abcdefghijkmnopqrstuvwxyz"
)


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


def birdeye_get(path, params=None, retry_429=True):
    """Make a conservative Birdeye request with at most one 429 retry."""
    response = requests.get(
        f"{BIRDEYE_BASE}{path}",
        headers=birdeye_headers(),
        params=params,
        timeout=30,
    )

    if response.status_code == 429 and retry_429:
        time.sleep(5)
        response = requests.get(
            f"{BIRDEYE_BASE}{path}",
            headers=birdeye_headers(),
            params=params,
            timeout=30,
        )

    return response


def birdeye_post(path, body=None):
    """POST to Birdeye without automatic retry; callers stop on rate limits."""
    return requests.post(
        f"{BIRDEYE_BASE}{path}",
        headers={**birdeye_headers(), "content-type": "application/json"},
        json=body or {},
        timeout=45,
    )


def helius_get_transactions(wallet):
    """Fetch a bounded, parsed history sample for heuristic screening."""
    return requests.get(
        f"https://api-mainnet.helius-rpc.com/v0/addresses/{wallet}/transactions",
        params={
            "api-key": os.getenv("HELIUS_API_KEY", ""),
            "token-accounts": "balanceChanged",
            "sort-order": "desc",
            "limit": HELIUS_HISTORY_LIMIT,
        },
        timeout=45,
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
        "checks": checks,
    }), 200 if healthy else 503


# =========================================================
# DISCOVERY
# =========================================================

@app.get("/discover")
def discover():
    initialise_database()
    try:
        token_limit = int(request.args.get("tokens", 3))
    except ValueError:
        token_limit = 3
    token_limit = min(max(token_limit, 1), 5)

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO discovery_runs (status) VALUES ('running') RETURNING id")
            run_id = cur.fetchone()[0]
        conn.commit()

    trending_response = birdeye_get("/defi/token_trending", {
        "sort_by": "rank", "sort_type": "asc", "offset": 0,
        "limit": token_limit, "interval": "24h",
    })
    if trending_response.status_code != 200:
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE discovery_runs SET completed_at = NOW(), status = 'failed'
                    WHERE id = %s
                """, (run_id,))
            conn.commit()
        return jsonify({
            "success": False,
            "stage": "trending",
            "status_code": trending_response.status_code,
            "error": trending_response.text[:500],
        }), trending_response.status_code

    tokens = find_list(trending_response.json())
    wallets_found = set()
    results = []
    api_429s = 0
    tokens_examined = 0

    for token in tokens[:token_limit]:
        token_address = token.get("address")
        if not is_valid_solana_address(token_address):
            continue

        token_symbol = token.get("symbol")
        token_name = token.get("name")
        time.sleep(BIRDEYE_THROTTLE_SECONDS)
        trader_response = birdeye_get("/defi/v2/tokens/top_traders", {
            "address": token_address,
            "time_frame": "30d",
            "sort_type": "desc",
            "sort_by": "realized_pnl",
            "offset": 0,
            "limit": 5,
        })
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
                            wallet, token_address, token_symbol, token_name, token_realized_pnl
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (wallet, token_address) DO UPDATE SET
                            token_symbol = EXCLUDED.token_symbol,
                            token_name = EXCLUDED.token_name,
                            token_realized_pnl = EXCLUDED.token_realized_pnl
                    """, (wallet, token_address, token_symbol, token_name, token_realized))
                    cur.execute("""
                        INSERT INTO candidate_wallets (wallet, tokens_found)
                        VALUES (%s, 1) ON CONFLICT (wallet) DO NOTHING
                    """, (wallet,))
                    cur.execute("""
                        UPDATE candidate_wallets SET
                            tokens_found = (SELECT COUNT(*) FROM wallet_token_hits WHERE wallet = %s),
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
        results.append({"token": token_symbol, "status": 200, "wallets": valid_wallets})

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE discovery_runs SET completed_at = NOW(), tokens_examined = %s,
                    wallets_found = %s, api_429s = %s, status = 'completed'
                WHERE id = %s
            """, (tokens_examined, len(wallets_found), api_429s, run_id))
        conn.commit()

    return jsonify({
        "success": True,
        "run_id": run_id,
        "tokens_examined": tokens_examined,
        "unique_wallets_found": len(wallets_found),
        "api_429s": api_429s,
        "results": results,
        "note": "Discovery only. Candidates still require wallet-level performance and risk screening.",
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
        time.sleep(BIRDEYE_THROTTLE_SECONDS)
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
        time.sleep(HELIUS_THROTTLE_SECONDS)
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
        item["cross_token_evidence"], item["independent_repeat_evidence"],
        item["distinct_tokens_observed"], item["discovery_runs_observed"], item["score"],
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
        time.sleep(BIRDEYE_THROTTLE_SECONDS)
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


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
