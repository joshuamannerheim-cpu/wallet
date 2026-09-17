"""Bridge existing V5 wallet intelligence into the V5.1 discovery ledger.

Runs after the legacy payload is built, so V5.1 can learn from current pipelines
without destabilising them.  The legacy dashboard remains available while the
new decision surface is validated.
"""
from datetime import datetime, timezone

import early_discovery


def _dt(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        for candidate in (value, value.replace('Z', '+00:00')):
            try:
                parsed = datetime.fromisoformat(candidate)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                pass
    return None


def _quality_from_history(successful, history, best_rank=None):
    successful = max(int(successful or 0), 0)
    history = max(int(history or 0), successful, 1)
    hit_rate = min(successful / history, 1.0)
    rank_bonus = 0
    try:
        rank = float(best_rank)
        rank_bonus = 20 if rank <= 10 else 10 if rank <= 25 else 0
    except (TypeError, ValueError):
        pass
    return min(100.0, 25 + hit_rate * 55 + rank_bonus)


def _market_quality(liquidity, volume_h1):
    liquidity = float(liquidity or 0)
    volume = float(volume_h1 or 0)
    liq = 100 if liquidity >= 100000 else 80 if liquidity >= 50000 else 60 if liquidity >= 25000 else 35 if liquidity >= 10000 else 10
    vol = 100 if volume >= 50000 else 80 if volume >= 10000 else 60 if volume >= 5000 else 35 if volume >= 1000 else 10
    return liq, vol


def ingest_payload(wallet_app, payload):
    """Record new candidates from current EVM/Solana wallet pipelines."""
    try:
        with wallet_app.db() as conn:
            early_discovery.initialise(conn)
    except Exception as exc:
        return {'recorded': 0, 'errors': [f'initialise: {exc}']}

    candidates = []
    outbound = (payload.get('evm_outbound_discoveries') or {}).get('discoveries') or []
    for row in outbound:
        candidates.append(('robinhood', row, 'early-buyer discovery'))
    paper = payload.get('evm_paper_signals') or {}
    for row in (paper.get('signals') or []) + (paper.get('candidates') or []):
        candidates.append(('robinhood', row, 'paper wallet signal'))
    for row in payload.get('solana_signals') or []:
        candidates.append(('solana', row, 'live wallet consensus'))

    recorded, errors = 0, []
    for chain, row, source in candidates:
        address = row.get('token_address')
        if not address:
            continue
        buyers = int(row.get('qualified_buyers') or row.get('independent_buy_clusters') or row.get('buy_clusters') or 1)
        successful = int(row.get('source_successful_history_tokens') or row.get('successful_history_tokens') or 0)
        history = int(row.get('source_history_tokens') or row.get('history_tokens') or max(successful, 1))
        best_rank = row.get('best_source_entry_rank') or row.get('best_entry_rank')
        wallet_quality = _quality_from_history(successful, history, best_rank)
        liquidity = row.get('liquidity_usd')
        volume = row.get('volume_h1_usd')
        liquidity_quality, volume_quality = _market_quality(liquidity, volume)
        sellers = int(row.get('sell_clusters') or row.get('independent_sell_clusters') or 0)
        distribution_quality = max(0, min(100, 70 + buyers * 8 - sellers * 20))
        reason = f'{source}: {buyers} independent buyer' + ('s' if buyers != 1 else '')
        if successful:
            reason += f'; {successful} successful historical token' + ('s' if successful != 1 else '')
        try:
            with wallet_app.db() as conn:
                early_discovery.initialise(conn)
                early_discovery.record_candidate(
                    conn, chain=chain, token_address=address,
                    token_symbol=row.get('token_symbol'),
                    pair_created_at=_dt(row.get('pair_created_at')),
                    price_usd=row.get('price_usd'),
                    market_cap_usd=row.get('market_cap_usd') or row.get('fdv_usd'),
                    liquidity_usd=liquidity, volume_h1_usd=volume,
                    triggering_wallets=row.get('wallets') or row.get('buyer_wallets') or [],
                    independent_wallets=max(buyers, 1),
                    wallet_quality=wallet_quality,
                    holder_acceleration=50,  # neutral until holder velocity is available at discovery
                    volume_acceleration=volume_quality,
                    liquidity_quality=liquidity_quality,
                    distribution_quality=distribution_quality,
                    contract_quality=50,  # neutral, never treated as verified safety
                    reason=reason, source=source.replace(' ', '_'),
                )
            recorded += 1
        except Exception as exc:
            errors.append(f'{chain}:{address}: {exc}')
    return {'recorded': recorded, 'errors': errors[:5]}


def dashboard_rows(wallet_app, limit=30):
    try:
        with wallet_app.db() as conn:
            early_discovery.initialise(conn)
            rows = early_discovery.recent_discoveries(conn, limit=limit)
        for row in rows:
            address = row.get('token_address')
            chain = row.get('chain')
            if chain == 'solana':
                row['gmgn_url'] = f'https://gmgn.ai/sol/token/{address}'
            elif chain == 'base':
                row['gmgn_url'] = f'https://gmgn.ai/base/token/{address}'
            elif chain == 'bsc':
                row['gmgn_url'] = f'https://gmgn.ai/bsc/token/{address}'
            else:
                row['gmgn_url'] = None
        return rows
    except Exception:
        return []
