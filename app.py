import os
from flask import Flask, jsonify
import psycopg
import requests

app = Flask(__name__)


@app.get("/")
def home():
    return jsonify({
        "service": "Solana Smart Wallet Monitor",
        "status": "online",
        "version": "1.0"
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
    key = os.getenv("BIRDEYE_API_KEY")

    if not key:
        return jsonify({"error": "BIRDEYE_API_KEY missing"}), 500

    headers = {
        "X-API-KEY": key,
        "x-chain": "solana"
    }

    # Test using a public Solana wallet address.
    wallet = "eJpBLoF3bgXpzjxqJRAvMchjEo4EqdAmiQh3ASmEtZT"

    try:
        response = requests.get(
            "https://public-api.birdeye.so/wallet/v2/pnl/summary",
            params={
                "wallet": wallet,
                "duration": "30d"
            },
            headers=headers,
            timeout=15
        )

        return jsonify({
            "birdeye_http_status": response.status_code,
            "connected": response.status_code == 200
        })

    except Exception as e:
        return jsonify({
            "connected": False,
            "error": str(e)
        }), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
