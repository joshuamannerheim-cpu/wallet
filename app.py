import os
import time
from datetime import datetime, timezone

import psycopg
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

BIRDEYE_BASE = "https://public-api.birdeye.so"


# ---------------------------------------------------------
# BASIC HELPERS
# ---------------------------------------------------------

def birdeye_headers():
    return {
        "accept": "application/json",
        "X-API-KEY": os.getenv("BIRDEYE_API_KEY", ""),
        "x-chain": "solana"
    }


def birdeye_get(path, params=None, retries=1):
    """
    Conservative Birdeye request helper.
    If Birdeye returns 429, wait and retry only once.
    """

    for attempt in range(retries + 1):
        response = requests.get(
            f"{BIRDEYE_BASE}{path}",
            params=params or {},
            headers=birdeye_headers(),
            timeout=30
        )

        if response.status_code != 429:
            return response

        if attempt < retries:
            time.sleep(5)

    return response


def db():
    return psycopg.connect(os.environ["DATABASE_URL"])


# ---------------------------------------------------------
# DATABASE
# ---------------------------------------------------------

def initialise_database():
    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                CREATE TABLE IF NOT EXISTS candidate_wallets (
                    wallet TEXT PRIMARY KEY,
                    tokens_found INTEGER DEFAULT 0,
                    realized_pnl_30d DOUBLE PRECISION,
                    total_pnl_30d DOUBLE PRECISION,
                    win_rate_30d DOUBLE PRECISION,
                    trades_30d INTEGER,
                    total_invested_30d DOUBLE PRECISION,
                    score DOUBLE PRECISION DEFAULT 0,
                    status TEXT DEFAULT 'candidate',
                    last_scored TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallet_token_hits (
                    wallet TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    token_symbol TEXT,
                    token_name TEXT,
                    token_realized_pnl DOUBLE PRECISION,
                    discovered_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (wallet, token_address)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS discovery_runs (
                    id SERIAL PRIMARY KEY,
                    started_at TIMESTAMPTZ DEFAULT NOW(),
                    completed_at TIMESTAMPTZ,
                    tokens_examined INTEGER DEFAULT 0,
                    wallets_found INTEGER DEFAULT 0,
                    api_429s INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'running'
                )
            """)

            conn.commit()


# ---------------------------------------------------------
# JSON PARSING HELPERS
# ---------------------------------------------------------

def find_list(obj):
    """
    Birdeye response structures can change/wrap lists.
    Search recursively for the first useful list of dicts.
    """

    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            return obj

    if isinstance(obj, dict):
        preferred = [
            "items",
            "tokens",
            "traders",
            "data"
        ]

        for key in preferred:
            if key in obj:
                result = find_list(obj[key])
                if result:
                    return result

        for value in obj.values():
            result = find_list(value)
            if result:
                return result

    return []


def is_valid_solana_address(value):
    """
    Basic Solana address validation.
    Solana addresses are base58 encoded and normally
    decode to exactly 32 bytes.
    """
    if not isinstance(value, str):
        return False

    if not (32 <= len(value) <= 44):
        return False

    # Base58 alphabet excludes 0, O, I and l
    alphabet = set(
        "123456789ABCDEFGHJKLMNPQRSTUVWXYZ"
        "abcdefghijkmnopqrstuvwxyz"
    )

    if not all(char in alphabet for char in value):
        return False

    try:
        number = 0

        for char in value:
            number *= 58
            number += (
                "123456789ABCDEFGHJKLMNPQRSTUVWXYZ"
                "abcdefghijkmnopqrstuvwxyz"
            ).index(char)

        decoded = number.to_bytes(
            (number.bit_length() + 7) // 8,
            byteorder="big"
        )

        # Account for leading base58 zeroes ("1")
        leading_zeros = len(value) - len(value.lstrip("1"))
        decoded = (b"\x00" * leading_zeros) + decoded

        return len(decoded) == 32

    except Exception:
        return False


def wallet_address_from_trader(item):
    """
    Extract only a plausible Solana wallet/account address
    from a Birdeye trader record.
    """

    preferred_keys = [
        "owner",
        "wallet",
        "wallet_address",
        "walletAddress"
    ]

    for key in preferred_keys:
        value = item.get(key)

        if is_valid_solana_address(value):
            return value

    return None
    for key in [
        "owner",
        "wallet",
        "walletAddress",
        "address"
    ]:
        value = item.get(key)

        if isinstance(value, str) and len(value) >= 30:
            return value

    return None


def number(item, *keys):
    for key in keys:
        value = item.get(key)

        if isinstance(value, (int, float)):
            return float(value)

    return None


# ---------------------------------------------------------
# WALLET SCORING
# ---------------------------------------------------------

def calculate_score(
    realized_pnl,
    total_pnl,
    win_rate,
    trades,
    tokens_found
):
    """
    Initial 0-100 research score.

    This is deliberately conservative and will be refined
    after we inspect real wallet data.
    """

    score = 0

    # Repeat appearances across different successful tokens
    score += min(tokens_found * 8, 24)

    # Win rate
    if win_rate is not None:
        normalized_win_rate = (
            win_rate * 100 if win_rate <= 1 else win_rate
        )

        if normalized_win_rate >= 70:
            score += 25
        elif normalized_win_rate >= 60:
            score += 20
        elif normalized_win_rate >= 55:
            score += 15
        elif normalized_win_rate >= 50:
            score += 8

    # Realized profit
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

    # Meaningful trading history
    if trades is not None:
        if trades >= 100:
            score += 16
        elif trades >= 50:
            score += 12
        elif trades >= 30:
            score += 8
        elif trades >= 10:
            score += 3

    # Penalise negative total P&L
    if total_pnl is not None and total_pnl < 0:
        score -= 20

    return max(0, min(round(score, 1), 100))


# ---------------------------------------------------------
# BASIC ROUTES
# ---------------------------------------------------------

@app.get("/")
def home():
    return jsonify({
        "service": "Solana Smart Wallet Monitor",
        "status": "online",
        "version": "3.0",
        "mode": "candidate discovery"
    })


@app.get("/health")
def health():
    checks = {
        "database": False,
        "helius_key": bool(os.getenv("HELIUS_API_KEY")),
        "birdeye_key": bool(os.getenv("BIRDEYE_API_KEY"))
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
        "checks": checks
    }), 200 if healthy else 503


# ---------------------------------------------------------
# DISCOVERY
# ---------------------------------------------------------

@app.get("/discover")
def discover():
    """
    Small controlled discovery run.

    Default:
      3 trending tokens
      5 top traders per token

    Maximum browser-triggered run:
      5 tokens

    We deliberately start small.
    """

    initialise_database()

    token_limit = min(
        max(int(request.args.get("tokens", 3)), 1),
        5
    )

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO discovery_runs (status)
                VALUES ('running')
                RETURNING id
            """)

            run_id = cur.fetchone()[0]
            conn.commit()

    trending_response = birdeye_get(
        "/defi/token_trending",
        {
            "sort_by": "rank",
            "sort_type": "asc",
            "offset": 0,
            "limit": token_limit,
            "interval": "24h"
        }
    )

    if trending_response.status_code != 200:
        return jsonify({
            "success": False,
            "stage": "trending",
            "status_code": trending_response.status_code
        }), trending_response.status_code

    trending_payload = trending_response.json()
    tokens = find_list(trending_payload)

    results = []
    wallets_found = set()
    api_429s = 0
    tokens_examined = 0

    for token in tokens[:token_limit]:

        token_address = token.get("address")

        if not token_address:
            continue

        token_symbol = token.get("symbol")
        token_name = token.get("name")

        # One request at a time.
        time.sleep(2)

        traders_response = birdeye_get(
            "/defi/v2/tokens/top_traders",
            {
                "address": token_address,
                "time_frame": "30d",
                "sort_type": "desc",
                "sort_by": "realized_pnl",
                "offset": 0,
                "limit": 5
            },
            retries=1
        )

        tokens_examined += 1

        if traders_response.status_code == 429:
            api_429s += 1

            results.append({
                "token": token_symbol,
                "status": 429,
                "message": "Birdeye rate limited this request"
            })

            # Do not continue hammering the endpoint.
            break

        if traders_response.status_code != 200:
            results.append({
                "token": token_symbol,
                "status": traders_response.status_code
            })
            continue

        traders = find_list(traders_response.json())
        token_wallet_count = 0

        with db() as conn:
            with conn.cursor() as cur:

                for trader in traders:

                    wallet = wallet_address_from_trader(trader)

                    if not wallet:
                        continue

                    wallets_found.add(wallet)
                    token_wallet_count += 1

                    realized = number(
                        trader,
                        "realizedPnl",
                        "realized_pnl"
                    )

                    cur.execute("""
                        INSERT INTO wallet_token_hits (
                            wallet,
                            token_address,
                            token_symbol,
                            token_name,
                            token_realized_pnl
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (wallet, token_address)
                        DO UPDATE SET
                            token_symbol = EXCLUDED.token_symbol,
                            token_name = EXCLUDED.token_name,
                            token_realized_pnl =
                                EXCLUDED.token_realized_pnl
                    """, (
                        wallet,
                        token_address,
                        token_symbol,
                        token_name,
                        realized
                    ))

                    cur.execute("""
                        INSERT INTO candidate_wallets (
                            wallet,
                            tokens_found
                        )
                        VALUES (%s, 1)
                        ON CONFLICT (wallet)
                        DO UPDATE SET
                            tokens_found = (
                                SELECT COUNT(*)
                                FROM wallet_token_hits
                                WHERE wallet = %s
                            )
                    """, (
                        wallet,
                        wallet
                    ))

                conn.commit()

        results.append({
            "token": token_symbol,
            "status": 200,
            "wallets": token_wallet_count
        })

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                UPDATE discovery_runs
                SET
                    completed_at = NOW(),
                    tokens_examined = %s,
                    wallets_found = %s,
                    api_429s = %s,
                    status = 'completed'
                WHERE id = %s
            """, (
                tokens_examined,
                len(wallets_found),
                api_429s,
                run_id
            ))

            conn.commit()

    return jsonify({
        "success": True,
        "run_id": run_id,
        "tokens_examined": tokens_examined,
        "unique_wallets_found": len(wallets_found),
        "api_429s": api_429s,
        "results": results,
        "note": (
            "Discovery only. Wallets have not yet passed "
            "performance or risk screening."
        )
    })


# ---------------------------------------------------------
# SCORE ONE WALLET
# ---------------------------------------------------------

# ---------------------------------------------------------
# SCORE ONE WALLET
# ---------------------------------------------------------

@app.get("/score-wallet/<wallet>")
def score_wallet(wallet):
    initialise_database()

    params = {
        "wallet": wallet,
        "duration": "30d"
    }

    response = birdeye_get(
        "/wallet/v2/pnl/summary",
        params,
        retries=1
    )

    # Return Birdeye's actual error so we can diagnose
    # invalid parameters, rate limits or plan restrictions.
    if response.status_code != 200:
        return jsonify({
            "success": False,
            "status_code": response.status_code,
            "birdeye_error": response.text[:1000],
            "request_parameters": params
        }), response.status_code

    try:
        payload = response.json()
    except Exception:
        return jsonify({
            "success": False,
            "stage": "parse_response",
            "message": "Birdeye returned a non-JSON response."
        }), 502

    data = payload.get("data", payload)

if not isinstance(data, dict):
    return jsonify({
        "success": False,
        "stage": "parse_data",
        "message": "Unexpected Birdeye response structure.",
        "response_preview": str(data)[:1000]
    }), 502

# Birdeye P&L Summary may wrap the actual metrics
# inside data["summary"].
summary = data.get("summary")

if isinstance(summary, dict):
    data = summary
        return jsonify({
            "success": False,
            "stage": "parse_data",
            "message": "Unexpected Birdeye response structure.",
            "response_preview": str(data)[:1000]
        }), 502

    counts = data.get("counts") or {}
    pnl = data.get("pnl") or {}
    cashflow = data.get("cashflow_usd") or {}

    win_rate = counts.get("win_rate")
    trades = counts.get("total_trade")

    realized = pnl.get("realized_profit_usd")
    total_pnl = pnl.get("total_usd")

    invested = cashflow.get("total_invested")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tokens_found
                FROM candidate_wallets
                WHERE wallet = %s
            """, (wallet,))

            row = cur.fetchone()
            tokens_found = row[0] if row else 0

            score = calculate_score(
                realized,
                total_pnl,
                win_rate,
                trades,
                tokens_found
            )

            cur.execute("""
                INSERT INTO candidate_wallets (
                    wallet,
                    tokens_found,
                    realized_pnl_30d,
                    total_pnl_30d,
                    win_rate_30d,
                    trades_30d,
                    total_invested_30d,
                    score,
                    last_scored
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (wallet)
                DO UPDATE SET
                    realized_pnl_30d = EXCLUDED.realized_pnl_30d,
                    total_pnl_30d = EXCLUDED.total_pnl_30d,
                    win_rate_30d = EXCLUDED.win_rate_30d,
                    trades_30d = EXCLUDED.trades_30d,
                    total_invested_30d = EXCLUDED.total_invested_30d,
                    score = EXCLUDED.score,
                    last_scored = NOW()
            """, (
                wallet,
                tokens_found,
                realized,
                total_pnl,
                win_rate,
                trades,
                invested,
                score
            ))

            conn.commit()

    return jsonify({
        "success": True,
        "wallet": wallet,
        "score": score,
        "tokens_found": tokens_found,
        "30d": {
            "realized_pnl_usd": realized,
            "total_pnl_usd": total_pnl,
            "win_rate": win_rate,
            "trades": trades,
            "total_invested_usd": invested
        },
        "raw_data_keys": list(data.keys())
    })

# ---------------------------------------------------------
# CANDIDATE LIST
# ---------------------------------------------------------

@app.get("/candidates")
def candidates():

    initialise_database()

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    wallet,
                    tokens_found,
                    realized_pnl_30d,
                    total_pnl_30d,
                    win_rate_30d,
                    trades_30d,
                    score,
                    status,
                    last_scored
                FROM candidate_wallets
                ORDER BY
                    score DESC,
                    tokens_found DESC,
                    realized_pnl_30d DESC NULLS LAST
                LIMIT 100
            """)

            rows = cur.fetchall()

    result = []

    for row in rows:
        result.append({
            "wallet": row[0],
            "tokens_found": row[1],
            "realized_pnl_30d": row[2],
            "total_pnl_30d": row[3],
            "win_rate_30d": row[4],
            "trades_30d": row[5],
            "score": row[6],
            "status": row[7],
            "last_scored": (
                row[8].isoformat()
                if row[8]
                else None
            )
        })

    return jsonify({
        "count": len(result),
        "candidates": result
    })


@app.get("/discovery-runs")
def discovery_runs():

    initialise_database()

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    id,
                    started_at,
                    completed_at,
                    tokens_examined,
                    wallets_found,
                    api_429s,
                    status
                FROM discovery_runs
                ORDER BY id DESC
                LIMIT 20
            """)

            rows = cur.fetchall()

    return jsonify({
        "runs": [
            {
                "id": r[0],
                "started": r[1].isoformat(),
                "completed": (
                    r[2].isoformat()
                    if r[2]
                    else None
                ),
                "tokens_examined": r[3],
                "wallets_found": r[4],
                "api_429s": r[5],
                "status": r[6]
            }
            for r in rows
        ]
    })

@app.get("/reset-candidates")
def reset_candidates():
    initialise_database()

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM wallet_token_hits")
            cur.execute("DELETE FROM candidate_wallets")
            conn.commit()

    return jsonify({
        "success": True,
        "message": "Candidate wallet data cleared."
    })
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(
        host="0.0.0.0",
        port=port
    )
