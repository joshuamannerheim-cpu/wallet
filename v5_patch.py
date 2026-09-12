"""V5 portfolio enrichment bridge.

Keeps held EVM tokens in the normal snapshot pipeline and adds a lightweight
Solana portfolio review layer so wallet-only Solana rows do not remain
permanently DATA PENDING.
"""

import threading
import requests
import app as wallet_app

_ORIGINAL_REFRESH_EVM_WATCHLIST = wallet_app.refresh_evm_watchlist
_ORIGINAL_BUILD_DASHBOARD_PAYLOAD = wallet_app.build_dashboard_payload


def sync_portfolio_watchlist():
    wallet_app.initialise_database()
    supported = tuple(wallet_app.SUPPORTED_EVM_CHAINS)
    with wallet_app.db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT chain, token_address, token_symbol, token_name
                FROM evm_wallet_holdings
                WHERE wallet = %s AND is_held = TRUE
                  AND chain = ANY(%s) AND token_address LIKE '0x%%'
            """, (wallet_app.EVM_PORTFOLIO_WALLET, list(supported)))
            holdings = cur.fetchall()
            for chain, address, symbol, name in holdings:
                cur.execute("""
                    INSERT INTO token_watchlist (
                        chain, token_address, token_symbol, token_name,
                        source, monitoring_status, active, updated_at
                    ) VALUES (%s,%s,%s,%s,'portfolio_auto',
                              'portfolio_enrichment_pending',TRUE,NOW())
                    ON CONFLICT (chain, token_address) DO UPDATE SET
                        token_symbol = CASE WHEN EXCLUDED.token_symbol <> 'UNKNOWN'
                            THEN EXCLUDED.token_symbol ELSE token_watchlist.token_symbol END,
                        token_name = COALESCE(EXCLUDED.token_name, token_watchlist.token_name),
                        active = TRUE,
                        monitoring_status = CASE
                            WHEN token_watchlist.monitoring_status='portfolio_not_held'
                            THEN 'portfolio_enrichment_pending'
                            ELSE token_watchlist.monitoring_status END,
                        updated_at=NOW()
                """, (chain,address,symbol or 'UNKNOWN',name or symbol or 'Unknown'))
            cur.execute("""
                UPDATE token_watchlist w SET active=FALSE,
                    monitoring_status='portfolio_not_held',updated_at=NOW()
                WHERE w.source='portfolio_auto' AND NOT EXISTS (
                    SELECT 1 FROM evm_wallet_holdings h
                    WHERE h.wallet=%s AND h.chain=w.chain
                      AND LOWER(h.token_address)=LOWER(w.token_address)
                      AND h.is_held=TRUE)
            """, (wallet_app.EVM_PORTFOLIO_WALLET,))
        conn.commit()
    return len(holdings)


def refresh_evm_watchlist(limit=10, offset=0):
    held_count = sync_portfolio_watchlist()
    return _ORIGINAL_REFRESH_EVM_WATCHLIST(
        limit=max(int(limit or 0), held_count + 1, 20), offset=0
    )


def _solana_market_snapshot(address):
    if not address or address == 'native':
        return None
    try:
        response = requests.get(
            wallet_app.DEXSCREENER_TOKEN_URL.format(address=address), timeout=7
        )
        response.raise_for_status()
        pairs = response.json().get('pairs') or []
        pairs = [p for p in pairs if str(p.get('chainId','')).lower() == 'solana']
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: float((p.get('liquidity') or {}).get('usd') or 0))
        return pair
    except Exception:
        return None


def _solana_review(pair):
    if not pair:
        return 'DATA PENDING', 'HELD'
    change = pair.get('priceChange') or {}
    h1 = wallet_app.safe_float(change.get('h1'))
    h6 = wallet_app.safe_float(change.get('h6'))
    h24 = wallet_app.safe_float(change.get('h24'))
    liquidity = wallet_app.safe_float((pair.get('liquidity') or {}).get('usd'))
    volume = wallet_app.safe_float((pair.get('volume') or {}).get('h24'))
    tx = (pair.get('txns') or {}).get('h24') or {}
    buys, sells = int(tx.get('buys') or 0), int(tx.get('sells') or 0)
    vals = [v for v in (h1,h6,h24) if v is not None]
    if liquidity is not None and liquidity < 10000:
        return 'RISK', 'THIN LIQUIDITY'
    if vals and sum(v > 0 for v in vals) >= 2 and (buys >= sells or (volume or 0) > 5000):
        return 'STRENGTHENING', 'SOL_MOMENTUM'
    if vals and sum(v < 0 for v in vals) >= 2 and sells > buys:
        return 'WEAKENING', 'SOL_WEAKENING'
    return 'STABLE', 'SOL_OBSERVE'


def build_dashboard_payload():
    payload = _ORIGINAL_BUILD_DASHBOARD_PAYLOAD()
    for item in payload.get('evm_signals', []):
        if item.get('chain') != 'solana':
            continue
        symbol = str(item.get('token_symbol') or '').upper()
        address = item.get('token_address')
        if symbol == 'SOL' or address == 'native':
            item['status'] = 'BENCHMARK'
            item['structure_state'] = 'CORE ASSET'
            item['data_quality'] = 'benchmark'
            item['is_benchmark'] = True
            continue
        pair = _solana_market_snapshot(address)
        review, structure = _solana_review(pair)
        item['status'] = structure
        item['structure_state'] = review
        if pair:
            item['price_usd'] = wallet_app.safe_float(pair.get('priceUsd'))
            item['liquidity_usd'] = wallet_app.safe_float((pair.get('liquidity') or {}).get('usd'))
            item['volume_h1_usd'] = wallet_app.safe_float((pair.get('volume') or {}).get('h1'))
            item['data_quality'] = 'solana market snapshot'
            item['dexscreener_url'] = pair.get('url') or item.get('dexscreener_url')
            change = pair.get('priceChange') or {}
            for window, key in (('1h','h1'),('6h','h6'),('24h','h24')):
                item['trends'][window]['price_change_pct'] = wallet_app.safe_float(change.get(key))
                item['trends'][window]['available'] = change.get(key) is not None
        else:
            item['data_quality'] = 'solana market data unavailable'
    return payload


wallet_app.refresh_evm_watchlist = refresh_evm_watchlist
wallet_app.build_dashboard_payload = build_dashboard_payload
wallet_app.VERSION = '5.0.4-solana-portfolio-review'


def _initial_portfolio_refresh():
    try:
        refresh_evm_watchlist(limit=20, offset=0)
    except Exception:
        pass

threading.Thread(target=_initial_portfolio_refresh, daemon=True).start()
app = wallet_app.app
