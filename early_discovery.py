"""V5.1 early-discovery and signal-performance layer.

This module is deliberately isolated from the legacy dashboard.  It provides a
small persistent contract for recording a token at the moment a proven wallet
finds it, scoring the evidence available at that moment, and measuring forward
returns without rewriting history.
"""
from datetime import datetime, timezone

HORIZONS_HOURS = (1, 6, 24, 168)


def clamp(value, low=0.0, high=100.0):
    return max(low, min(high, float(value or 0)))


def early_score(wallet_quality=0, independent_wallets=0, holder_acceleration=0,
                volume_acceleration=0, liquidity_quality=0,
                distribution_quality=0, contract_quality=0):
    """Return an intentionally transparent 0-100 discovery score.

    Confluence saturates at three independent wallets; a single excellent
    wallet can therefore create a candidate but cannot receive full confluence
    credit.  Acceleration combines holder and volume evidence.
    """
    wallet = clamp(wallet_quality)
    confluence = clamp((max(int(independent_wallets or 0), 0) / 3.0) * 100)
    acceleration = (clamp(holder_acceleration) + clamp(volume_acceleration)) / 2
    return round(
        0.30 * wallet
        + 0.20 * confluence
        + 0.20 * acceleration
        + 0.15 * clamp(liquidity_quality)
        + 0.10 * clamp(distribution_quality)
        + 0.05 * clamp(contract_quality),
        1,
    )


def initialise(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS early_discoveries (
                id BIGSERIAL PRIMARY KEY,
                chain TEXT NOT NULL,
                token_address TEXT NOT NULL,
                token_symbol TEXT,
                discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                pair_created_at TIMESTAMPTZ,
                token_age_minutes DOUBLE PRECISION,
                discovery_price_usd DOUBLE PRECISION,
                discovery_market_cap_usd DOUBLE PRECISION,
                discovery_liquidity_usd DOUBLE PRECISION,
                discovery_volume_h1_usd DOUBLE PRECISION,
                triggering_wallets JSONB NOT NULL DEFAULT '[]'::jsonb,
                independent_wallets INTEGER NOT NULL DEFAULT 1,
                wallet_quality DOUBLE PRECISION,
                holder_acceleration DOUBLE PRECISION,
                volume_acceleration DOUBLE PRECISION,
                liquidity_quality DOUBLE PRECISION,
                distribution_quality DOUBLE PRECISION,
                contract_quality DOUBLE PRECISION,
                early_score DOUBLE PRECISION,
                reason TEXT,
                source TEXT NOT NULL DEFAULT 'wallet_activity',
                peak_price_usd DOUBLE PRECISION,
                peak_return_pct DOUBLE PRECISION,
                last_measured_at TIMESTAMPTZ,
                UNIQUE(chain, token_address)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS early_discovery_measurements (
                discovery_id BIGINT NOT NULL REFERENCES early_discoveries(id) ON DELETE CASCADE,
                horizon_hours INTEGER NOT NULL,
                measured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                price_usd DOUBLE PRECISION,
                market_cap_usd DOUBLE PRECISION,
                liquidity_usd DOUBLE PRECISION,
                return_pct DOUBLE PRECISION,
                PRIMARY KEY(discovery_id, horizon_hours)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS early_discoveries_score_idx ON early_discoveries(early_score DESC, discovered_at DESC)")
    conn.commit()


def record_candidate(conn, *, chain, token_address, token_symbol=None,
                     pair_created_at=None, price_usd=None, market_cap_usd=None,
                     liquidity_usd=None, volume_h1_usd=None,
                     triggering_wallets=None, independent_wallets=1,
                     wallet_quality=0, holder_acceleration=0,
                     volume_acceleration=0, liquidity_quality=0,
                     distribution_quality=0, contract_quality=0,
                     reason=None, source='wallet_activity'):
    now = datetime.now(timezone.utc)
    age_minutes = None
    if pair_created_at:
        age_minutes = max(0, (now - pair_created_at).total_seconds() / 60.0)
    score = early_score(wallet_quality, independent_wallets,
                        holder_acceleration, volume_acceleration,
                        liquidity_quality, distribution_quality,
                        contract_quality)
    wallets = triggering_wallets or []
    import json
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO early_discoveries (
                chain,token_address,token_symbol,discovered_at,pair_created_at,
                token_age_minutes,discovery_price_usd,discovery_market_cap_usd,
                discovery_liquidity_usd,discovery_volume_h1_usd,
                triggering_wallets,independent_wallets,wallet_quality,
                holder_acceleration,volume_acceleration,liquidity_quality,
                distribution_quality,contract_quality,early_score,reason,source)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (chain,token_address) DO UPDATE SET
                token_symbol=COALESCE(EXCLUDED.token_symbol,early_discoveries.token_symbol),
                triggering_wallets=EXCLUDED.triggering_wallets,
                independent_wallets=GREATEST(early_discoveries.independent_wallets,EXCLUDED.independent_wallets),
                wallet_quality=GREATEST(COALESCE(early_discoveries.wallet_quality,0),COALESCE(EXCLUDED.wallet_quality,0)),
                early_score=GREATEST(COALESCE(early_discoveries.early_score,0),COALESCE(EXCLUDED.early_score,0)),
                reason=COALESCE(EXCLUDED.reason,early_discoveries.reason)
            RETURNING id, early_score
        """, (chain,token_address.lower() if token_address.startswith('0x') else token_address,
              token_symbol,now,pair_created_at,age_minutes,price_usd,market_cap_usd,
              liquidity_usd,volume_h1_usd,json.dumps(wallets),independent_wallets,
              wallet_quality,holder_acceleration,volume_acceleration,liquidity_quality,
              distribution_quality,contract_quality,score,reason,source))
        row = cur.fetchone()
    conn.commit()
    return {'id': row[0], 'early_score': float(row[1] or score)}


def record_measurement(conn, discovery_id, horizon_hours, price_usd,
                       market_cap_usd=None, liquidity_usd=None):
    if horizon_hours not in HORIZONS_HOURS:
        raise ValueError('unsupported measurement horizon')
    with conn.cursor() as cur:
        cur.execute("SELECT discovery_price_usd,peak_price_usd FROM early_discoveries WHERE id=%s", (discovery_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError('unknown discovery')
        entry, old_peak = row
        ret = ((float(price_usd) / float(entry)) - 1) * 100 if entry and price_usd else None
        peak = max([v for v in (old_peak, price_usd) if v is not None], default=None)
        peak_ret = ((float(peak) / float(entry)) - 1) * 100 if entry and peak else None
        cur.execute("""
            INSERT INTO early_discovery_measurements
                (discovery_id,horizon_hours,price_usd,market_cap_usd,liquidity_usd,return_pct)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (discovery_id,horizon_hours) DO UPDATE SET
                measured_at=NOW(),price_usd=EXCLUDED.price_usd,
                market_cap_usd=EXCLUDED.market_cap_usd,
                liquidity_usd=EXCLUDED.liquidity_usd,return_pct=EXCLUDED.return_pct
        """, (discovery_id,horizon_hours,price_usd,market_cap_usd,liquidity_usd,ret))
        cur.execute("UPDATE early_discoveries SET peak_price_usd=%s,peak_return_pct=%s,last_measured_at=NOW() WHERE id=%s",
                    (peak,peak_ret,discovery_id))
    conn.commit()
    return ret


def recent_discoveries(conn, limit=30):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id,chain,token_address,token_symbol,discovered_at,
                   token_age_minutes,early_score,independent_wallets,
                   discovery_market_cap_usd,discovery_liquidity_usd,
                   peak_return_pct,reason
            FROM early_discoveries
            ORDER BY discovered_at DESC LIMIT %s
        """, (min(max(int(limit),1),100),))
        columns = [d.name for d in cur.description]
        return [dict(zip(columns,row)) for row in cur.fetchall()]
