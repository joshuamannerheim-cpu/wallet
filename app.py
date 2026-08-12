import os
from flask import Flask, jsonify, request
import psycopg
import requests

app = Flask(__name__)

BIRDEYE_BASE = "https://public-api.birdeye.so"


def birdeye_headers():
    return {
        "accept": "application/json",
        "X-API-KEY": os.getenv("BIRDEYE_API_KEY", ""),
        "x-chain": "solana"
    }


def birdeye_get(path, params=None):
    return requests.get(
        f"{BIRDEYE_BASE}{path}",
        params=params or {},
        headers=birdeye_headers(),
        timeout=20
    )


@app.get("/")
def home():
    return jsonify({
        "service": "Solana Smart Wallet Monitor",
        "status": "online",
        "version": "2.0",
        "purpose": "wallet discovery and monitoring"
    })


@app.get("/health")
def health():
    checks = {
        "database": False,
        "helius_key": bool(os.getenv("HELIUS_API_KEY")),
        "birdeye_key": bool(os.getenv("BIRDEYE_API_KEY"))
    }

    try:
        with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
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


@app.get("/test-birdeye")
def test_birdeye():
    wallet = "eJpBLoF3bgXpzjxqJRAvMchjEo4EqdAmiQh3ASmEtZT"

    response = birdeye_get(
        "/wallet/v2/pnl/summary",
        {
            "wallet": wallet,
            "duration": "30d"
        }
    )

    return jsonify({
        "connected": response.status_code == 200,
        "birdeye_http_status": response.status_code
    })


@app.get("/trending")
def trending():
    """
    Fetch a small list of currently trending Solana tokens.
    Keep the default small to conserve Birdeye compute units.
    """

    limit = min(int(request.args.get("limit", 10)), 20)

    response = birdeye_get(
        "/defi/token_trending",
        {
            "sort_by": "rank",
            "sort_type": "asc",
            "offset": 0,
            "limit": limit,
            "interval": "24h"
        }
    )

    if response.status_code != 200:
        return jsonify({
            "success": False,
            "status_code": response.status_code,
            "error": response.text[:500]
        }), response.status_code

    payload = response.json()

    return jsonify({
        "success": True,
        "source": "Birdeye",
        "chain": "solana",
        "requested_limit": limit,
        "data": payload.get("data", payload)
    })


@app.get("/test-discovery-access")
def test_discovery_access():
    """
    Tests which Birdeye discovery endpoints are available
    without performing a large wallet scan.
    """

    results = {}

    # Trending token access
    trending_response = birdeye_get(
        "/defi/token_trending",
        {
            "sort_by": "rank",
            "sort_type": "asc",
            "offset": 0,
            "limit": 3,
            "interval": "24h"
        }
    )

    results["trending_tokens"] = {
        "status": trending_response.status_code,
        "available": trending_response.status_code == 200
    }

    # New listings access
    new_response = birdeye_get(
        "/defi/v2/tokens/new_listing",
        {
            "limit": 3,
            "meme_platform_enabled": "true"
        }
    )

    results["new_listings"] = {
        "status": new_response.status_code,
        "available": new_response.status_code == 200
    }

    return jsonify({
        "chain": "solana",
        "birdeye_plan": "Standard",
        "tests": results
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
