import os
import time
from datetime import datetime, timezone

import psycopg
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

VERSION = "4.0"
BIRDEYE_BASE = "https://public-api.birdeye.so"
BASE58_ALPHABET = (
    "123456789ABCDEFGHJKLMNPQRSTUVWXYZ"
    "abcdefghijkmnopqrstuvwxyz"
)


# =========================================================
# CONFIGURATION
# =========================================================

def birdeye_headers():
    return {
        "accept": "application/json",
        "X-API-KEY": os.getenv("BIRDEYE_API_KEY", ""),
        "x-chain": "solana"
    }


def db():
    return psycopg.connect(os.environ["DATABASE_URL"])


def birdeye_get(path, params=None, retry_429=True):
    """
    Conservative Birdeye request helper.

    One retry only after a 429.
    We deliberately avoid aggressive API polling.
    """
    response = requests.get(
        f"{BIRDEYE_BASE}{path}",
        params=params or {},
        headers=birdeye_headers(),
        timeout=30
    )

    if response.status_code == 429 and retry_429:
        time.sleep(5)

        response = requests.get(
            f"{BIRDEYE_BASE}{path}",
            params=params or {},
            headers=birdeye_headers(),
            timeout=30
        )

    return response


# =========================================================
# DATABASE
# =========================================================

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
                    score_status TEXT DEFAULT 'unscored',

                    status TEXT DEFAULT 'candidate',

                    last_scored TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            # Add newer columns if table existed in V3.
            cur.execute("""
                ALTER TABLE candidate_wallets
                ADD COLUMN IF NOT EXISTS score_status TEXT
                DEFAULT 'unscored'
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


# =========================================================
# SOLANA ADDRESS VALIDATION
# =========================================================

def is_valid_solana_address(value):
    if not isinstance(value, str):
        return False

    if not 32 <= len(value) <= 44:
        return False

    if not all(char in BASE58_ALPHABET for char in value):
        return False

    try:
        number = 0

        for char in value:
            number *= 58
            number += BASE58_ALPHABET.index(char)

        if number == 0:
            decoded = b""
        else:
            decoded = number.to_bytes(
                (number.bit_length() + 7) // 8,
                byteorder="big"
            )

        leading_zeros = (
            len(value) -
            len(value.lstrip("1"))
        )

        decoded = (
            b"\x00" * leading_zeros
        ) + decoded

        return len(decoded) == 32

    except Exception:
        return False


def wallet_address_from_trader(item):
    """
    Extract a wallet only from wallet-specific fields.

    Generic 'address' is deliberately excluded because
    token responses frequently use it for token addresses.
    """
    possible_keys = [
        "owner",
        "wallet",
        "wallet_address",
        "walletAddress"
    ]

    for key in possible_keys:
        value = item.get(key)

        if is_valid_solana_address(value):
            return value

    return None


# =========================================================
# RESPONSE PARSING
# =========================================================

def find_list(obj):
    """
    Recursively locate a useful list inside a Birdeye
    response without assuming one fixed wrapper structure.
    """

    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            return obj

    if isinstance(obj, dict):

        preferred_keys = [
            "items",
            "tokens",
            "traders",
            "list"
        ]

        for key in preferred_keys:
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
    """
    Search several possible JSON paths for a number.

    Birdeye response schemas can vary slightly between
    endpoint versions/packages.
    """

    for path in candidate_paths:

        current = obj

        try:
            for key in path:
                current = current[key]

            if isinstance(current, (int, float)):
                return float(current)

        except (KeyError, TypeError):
            continue

    return None


def parse_pnl_summary(payload):
    """
    Parse Birdeye P&L Summary defensively.

    Returns both parsed metrics and useful debug information.
    """

    root = payload

    if isinstance(payload, dict):
        root = payload.get("data", payload)

    if not isinstance(root, dict):
        return {
            "metrics": {},
            "debug": {
                "error": "data_not_dictionary"
            }
        }

    # Birdeye currently may wrap metrics in "summary".
    summary = root.get("summary", root)

    if not isinstance(summary, dict):
        return {
            "metrics": {},
            "debug": {
                "error": "summary_not_dictionary",
                "root_keys": list(root.keys())
            }
        }

    realized = get_nested_number(
        summary,
        [
            ("pnl", "realized_profit_usd"),
            ("pnl", "realized_profit"),
            ("realized_profit_usd",),
            ("realized_pnl",),
            ("realizedPnl",)
        ]
    )

    total_pnl = get_nested_number(
        summary,
        [
            ("pnl", "total_usd"),
            ("pnl", "total_pnl_usd"),
            ("total_pnl_usd",),
            ("total_pnl",),
            ("totalPnl",)
        ]
    )

    win_rate = get_nested_number(
        summary,
        [
            ("counts", "win_rate"),
            ("win_rate",),
            ("winRate",)
        ]
    )

    trades = get_nested_number(
        summary,
        [
            ("counts", "total_trade"),
            ("counts", "total_trades"),
            ("total_trade",),
            ("total_trades",),
            ("trade_count",)
        ]
    )

    invested = get_nested_number(
        summary,
        [
            ("cashflow_usd", "total_invested"),
            ("cashflow", "total_invested"),
            ("total_invested",),
            ("total_invested_usd",)
        ]
    )

    if trades is not None:
        trades = int(trades)

    return {
        "metrics": {
            "realized_pnl": realized,
            "total_pnl": total_pnl,
            "win_rate": win_rate,
            "trades": trades,
            "invested": invested
        },
        "debug": {
            "root_keys": list(root.keys()),
            "summary_keys": list(summary.keys())
        }
    }


# =========================================================
# SCORING
# =========================================================

def calculate_score(
    realized_pnl,
    total_pnl,
    win_rate,
    trades,
    tokens_found
):
    """
    Initial research score from 0-100.

    This is NOT a buy recommendation.

    Cross-token consistency is deliberately rewarded.
    """

    score = 0

    # Cross-token consistency: max 24
    score += min(tokens_found * 8, 24)

    # Win rate: max 25
    if win_rate is not None:

        wr = (
            win_rate * 100
            if win_rate <= 1
            else win_rate
        )

        if wr >= 70:
            score += 25

        elif wr >= 60:
            score += 20

        elif wr >= 55:
            score += 15

        elif wr >= 50:
            score += 8

    # Realized P&L: max 25
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
            score -= 10

    # Trade history: max 16
    if trades is not None:

        if trades >= 100:
            score += 16

        elif trades >= 50:
            score += 12

        elif trades >= 30:
            score += 8

        elif trades >= 10:
            score += 3

    # Negative overall P&L penalty
    if total_pnl is not None and total_pnl < 0:
        score -= 20

    return max(
        0,
        min(round(score, 1), 100)
    )


# =========================================================
# HOME / HEALTH
# =========================================================

@app.get("/")
def home():
    return jsonify({
        "service": "Solana Smart Wallet Monitor",
        "status": "online",
        "version": VERSION,
        "mode": "candidate discovery and scoring"
    })


@app.get("/health")
def health():

    checks = {
        "database": False,
        "helius_key": bool(
            os.getenv("HELIUS_API_KEY")
        ),
        "birdeye_key": bool(
            os.getenv("BIRDEYE_API_KEY")
        )
    }

    try:
        initialise_database()

        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")

                checks["database"] = (
                    cur.fetchone()[0] == 1
                )

    except Exception:
        pass

    healthy = all(checks.values())

    return jsonify({
        "status": (
            "healthy"
            if healthy
            else "configuration_required"
        ),
        "checks": checks
    }), 200 if healthy else 503


# =========================================================
# DISCOVERY
# =========================================================

@app.get("/discover")
def discover():

    initialise_database()

    try:
        token_limit = int(
            request.args.get("tokens", 3)
        )

    except ValueError:
        token_limit = 3

    # Deliberately conservative.
    token_limit = min(
        max(token_limit, 1),
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

        with db() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    UPDATE discovery_runs
                    SET
                        completed_at = NOW(),
                        status = 'failed'
                    WHERE id = %s
                """, (run_id,))

                conn.commit()

        return jsonify({
            "success": False,
            "stage": "trending",
            "status_code":
                trending_response.status_code,
            "error":
                trending_response.text[:500]
        }), trending_response.status_code

    tokens = find_list(
        trending_response.json()
    )

    wallets_found = set()
    results = []

    api_429s = 0
    tokens_examined = 0

    for token in tokens[:token_limit]:

        token_address = token.get("address")

        if not is_valid_solana_address(
            token_address
        ):
            continue

        token_symbol = token.get("symbol")
        token_name = token.get("name")

        # Keep discovery gentle.
        time.sleep(2)

        trader_response = birdeye_get(
            "/defi/v2/tokens/top_traders",
            {
                "address": token_address,
                "time_frame": "30d",
                "sort_type": "desc",
                "sort_by": "realized_pnl",
                "offset": 0,
                "limit": 5
            }
        )

        tokens_examined += 1

        if trader_response.status_code == 429:

            api_429s += 1

            results.append({
                "token": token_symbol,
                "status": 429,
                "wallets": 0
            })

            # Stop the run after rate limiting.
            break

        if trader_response.status_code != 200:

            results.append({
                "token": token_symbol,
                "status":
                    trader_response.status_code,
                "wallets": 0
            })

            continue

        traders = find_list(
            trader_response.json()
        )

        valid_wallets = 0

        with db() as conn:
            with conn.cursor() as cur:

                for trader in traders:

                    wallet = (
                        wallet_address_from_trader(
                            trader
                        )
                    )

                    if not wallet:
                        continue

                    wallets_found.add(wallet)
                    valid_wallets += 1

                    token_realized = (
                        get_nested_number(
                            trader,
                            [
                                ("realized_pnl",),
                                ("realizedPnl",),
                                ("realized_profit",),
                                (
                                    "pnl",
                                    "realized_profit"
                                )
                            ]
                        )
                    )

                    cur.execute("""
                        INSERT INTO wallet_token_hits (
                            wallet,
                            token_address,
                            token_symbol,
                            token_name,
                            token_realized_pnl
                        )
                        VALUES (%s,%s,%s,%s,%s)

                        ON CONFLICT (
                            wallet,
                            token_address
                        )
                        DO UPDATE SET
                            token_symbol =
                                EXCLUDED.token_symbol,
                            token_name =
                                EXCLUDED.token_name,
                            token_realized_pnl =
                                EXCLUDED.token_realized_pnl
                    """, (
                        wallet,
                        token_address,
                        token_symbol,
                        token_name,
                        token_realized
                    ))

                    cur.execute("""
                        INSERT INTO candidate_wallets (
                            wallet,
                            tokens_found
                        )
                        VALUES (%s, 1)

                        ON CONFLICT (wallet)
                        DO NOTHING
                    """, (wallet,))

                    cur.execute("""
                        UPDATE candidate_wallets
                        SET tokens_found = (
                            SELECT COUNT(*)
                            FROM wallet_token_hits
                            WHERE wallet = %s
                        )
                        WHERE wallet = %s
                    """, (
                        wallet,
                        wallet
                    ))

                conn.commit()

        results.append({
            "token": token_symbol,
            "status": 200,
            "wallets": valid_wallets
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
        "tokens_examined":
            tokens_examined,
        "unique_wallets_found":
            len(wallets_found),
        "api_429s": api_429s,
        "results": results,
        "note":
            "Discovery only. Candidates still require "
            "wallet-level performance and risk screening."
    })


# =========================================================
# SCORE ONE WALLET
# =========================================================

@app.get("/score-wallet/<wallet>")
def score_wallet(wallet):

    initialise_database()

    if not is_valid_solana_address(wallet):

        return jsonify({
            "success": False,
            "error": "Invalid Solana wallet address"
        }), 400

    params = {
        "wallet": wallet,
        "duration": "30d"
    }

    response = birdeye_get(
        "/wallet/v2/pnl/summary",
        params
    )

    if response.status_code != 200:

        return jsonify({
            "success": False,
            "status_code": response.status_code,
            "birdeye_error":
                response.text[:1000]
        }), response.status_code

    try:
        payload = response.json()

    except Exception:

        return jsonify({
            "success": False,
            "error":
                "Birdeye returned invalid JSON"
        }), 502

    parsed = parse_pnl_summary(payload)

    metrics = parsed["metrics"]

    realized = metrics.get(
        "realized_pnl"
    )

    total_pnl = metrics.get(
        "total_pnl"
    )

    win_rate = metrics.get(
        "win_rate"
    )

    trades = metrics.get(
        "trades"
    )

    invested = metrics.get(
        "invested"
    )

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT tokens_found
                FROM candidate_wallets
                WHERE wallet = %s
            """, (wallet,))

            row = cur.fetchone()

            tokens_found = (
                row[0]
                if row
                else 0
            )

            metrics_available = any([
                realized is not None,
                total_pnl is not None,
                win_rate is not None,
                trades is not None,
                invested is not None
            ])

            if metrics_available:

                score = calculate_score(
                    realized,
                    total_pnl,
                    win_rate,
                    trades,
                    tokens_found
                )

                score_status = "scored"

            else:

                score = 0
                score_status = "parse_incomplete"

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
                    score_status,
                    last_scored
                )
                VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()
                )

                ON CONFLICT (wallet)
                DO UPDATE SET
                    realized_pnl_30d =
                        EXCLUDED.realized_pnl_30d,
                    total_pnl_30d =
                        EXCLUDED.total_pnl_30d,
                    win_rate_30d =
                        EXCLUDED.win_rate_30d,
                    trades_30d =
                        EXCLUDED.trades_30d,
                    total_invested_30d =
                        EXCLUDED.total_invested_30d,
                    score =
                        EXCLUDED.score,
                    score_status =
                        EXCLUDED.score_status,
                    last_scored = NOW()
            """, (
                wallet,
                tokens_found,
                realized,
                total_pnl,
                win_rate,
                trades,
                invested,
                score,
                score_status
            ))

            conn.commit()

    return jsonify({
        "success": True,
        "wallet": wallet,
        "score": score,
        "score_status": score_status,
        "tokens_found": tokens_found,

        "30d": {
            "realized_pnl_usd":
                realized,
            "total_pnl_usd":
                total_pnl,
            "win_rate":
                win_rate,
            "trades":
                trades,
            "total_invested_usd":
                invested
        },

        "parser_debug":
            parsed["debug"]
    })


# =========================================================
# SCORE UNSCORED CANDIDATES
# =========================================================

@app.get("/score-next")
def score_next():
    """
    Return the next unscored wallet.

    Deliberately does NOT recursively call the scoring
    endpoint or perform a large batch yet.
    """

    initialise_database()

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT wallet
                FROM candidate_wallets
                WHERE
                    last_scored IS NULL
                    OR score_status =
                        'parse_incomplete'
                ORDER BY
                    tokens_found DESC,
                    created_at ASC
                LIMIT 1
            """)

            row = cur.fetchone()

    if not row:

        return jsonify({
            "success": True,
            "message":
                "No unscored candidates remaining."
        })

    wallet = row[0]

    return jsonify({
        "success": True,
        "next_wallet": wallet,
        "score_url":
            f"/score-wallet/{wallet}"
    })


# =========================================================
# CANDIDATE LIST
# =========================================================

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
                    total_invested_30d,
                    score,
                    score_status,
                    status,
                    last_scored

                FROM candidate_wallets

                ORDER BY
                    score DESC,
                    tokens_found DESC,
                    realized_pnl_30d
                        DESC NULLS LAST

                LIMIT 100
            """)

            rows = cur.fetchall()

    result = []

    for row in rows:

        result.append({
            "wallet": row[0],
            "tokens_found": row[1],

            "realized_pnl_30d":
                row[2],

            "total_pnl_30d":
                row[3],

            "win_rate_30d":
                row[4],

            "trades_30d":
                row[5],

            "total_invested_30d":
                row[6],

            "score":
                row[7],

            "score_status":
                row[8],

            "status":
                row[9],

            "last_scored":
                (
                    row[10].isoformat()
                    if row[10]
                    else None
                )
        })

    return jsonify({
        "count": len(result),
        "candidates": result
    })


# =========================================================
# DISCOVERY HISTORY
# =========================================================

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
                "id": row[0],

                "started":
                    row[1].isoformat(),

                "completed":
                    (
                        row[2].isoformat()
                        if row[2]
                        else None
                    ),

                "tokens_examined":
                    row[3],

                "wallets_found":
                    row[4],

                "api_429s":
                    row[5],

                "status":
                    row[6]
            }

            for row in rows
        ]
    })


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    port = int(
        os.getenv("PORT", 8080)
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
