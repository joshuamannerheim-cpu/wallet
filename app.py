import os
import time
from datetime import datetime, timezone

import psycopg
import requests
from flask import Flask, jsonify, request


app = Flask(__name__)

VERSION = "4.1"
BIRDEYE_BASE = "https://public-api.birdeye.so"
BIRDEYE_THROTTLE_SECONDS = 2
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

            # These are idempotent and preserve databases created by earlier V4 builds.
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS total_pnl_30d DOUBLE PRECISION")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS total_invested_30d DOUBLE PRECISION")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS score_status TEXT NOT NULL DEFAULT 'unscored'")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
            cur.execute("ALTER TABLE candidate_wallets ADD COLUMN IF NOT EXISTS last_scored TIMESTAMPTZ")
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
                    score_status, created_at, last_scored
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
