"""V5 portfolio bridge plus V5.2 calibrated wallet-first discovery."""
import html
import threading
import requests
import app as wallet_app
import v51_bridge
import early_discovery

_ORIGINAL_REFRESH_EVM_WATCHLIST=wallet_app.refresh_evm_watchlist
_ORIGINAL_BUILD_DASHBOARD_PAYLOAD=wallet_app.build_dashboard_payload


def sync_portfolio_watchlist():
    wallet_app.initialise_database(); supported=tuple(wallet_app.SUPPORTED_EVM_CHAINS)
    with wallet_app.db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chain,token_address,token_symbol,token_name FROM evm_wallet_holdings WHERE wallet=%s AND is_held=TRUE AND chain=ANY(%s) AND token_address LIKE '0x%%'",(wallet_app.EVM_PORTFOLIO_WALLET,list(supported)))
            holdings=cur.fetchall()
            for chain,address,symbol,name in holdings:
                cur.execute("""INSERT INTO token_watchlist(chain,token_address,token_symbol,token_name,source,monitoring_status,active,updated_at)
                VALUES(%s,%s,%s,%s,'portfolio_auto','portfolio_enrichment_pending',TRUE,NOW()) ON CONFLICT(chain,token_address) DO UPDATE SET
                token_symbol=CASE WHEN EXCLUDED.token_symbol<>'UNKNOWN' THEN EXCLUDED.token_symbol ELSE token_watchlist.token_symbol END,
                token_name=COALESCE(EXCLUDED.token_name,token_watchlist.token_name),active=TRUE,
                monitoring_status=CASE WHEN token_watchlist.monitoring_status='portfolio_not_held' THEN 'portfolio_enrichment_pending' ELSE token_watchlist.monitoring_status END,updated_at=NOW()""",(chain,address,symbol or 'UNKNOWN',name or symbol or 'Unknown'))
            cur.execute("""UPDATE token_watchlist w SET active=FALSE,monitoring_status='portfolio_not_held',updated_at=NOW() WHERE w.source='portfolio_auto' AND NOT EXISTS
            (SELECT 1 FROM evm_wallet_holdings h WHERE h.wallet=%s AND h.chain=w.chain AND LOWER(h.token_address)=LOWER(w.token_address) AND h.is_held=TRUE)""",(wallet_app.EVM_PORTFOLIO_WALLET,))
        conn.commit()
    return len(holdings)


def refresh_evm_watchlist(limit=10,offset=0):
    held_count=sync_portfolio_watchlist()
    return _ORIGINAL_REFRESH_EVM_WATCHLIST(limit=max(int(limit or 0),held_count+1,20),offset=0)


def _solana_market_snapshot(address):
    if not address or address=='native': return None
    try:
        response=requests.get(wallet_app.DEXSCREENER_TOKEN_URL.format(address=address),timeout=7); response.raise_for_status()
        pairs=[p for p in (response.json().get('pairs') or []) if str(p.get('chainId','')).lower()=='solana']
        return max(pairs,key=lambda p:float((p.get('liquidity') or {}).get('usd') or 0)) if pairs else None
    except Exception: return None


def _solana_review(pair):
    if not pair:return 'DATA PENDING','HELD'
    change=pair.get('priceChange') or {}; h1=wallet_app.safe_float(change.get('h1'));h6=wallet_app.safe_float(change.get('h6'));h24=wallet_app.safe_float(change.get('h24'))
    liquidity=wallet_app.safe_float((pair.get('liquidity') or {}).get('usd'));volume=wallet_app.safe_float((pair.get('volume') or {}).get('h24'));tx=(pair.get('txns') or {}).get('h24') or {};buys,sells=int(tx.get('buys') or 0),int(tx.get('sells') or 0);vals=[v for v in(h1,h6,h24) if v is not None]
    if liquidity is not None and liquidity<10000:return 'RISK','THIN LIQUIDITY'
    if vals and sum(v>0 for v in vals)>=2 and (buys>=sells or (volume or 0)>5000):return 'STRENGTHENING','SOL_MOMENTUM'
    if vals and sum(v<0 for v in vals)>=2 and sells>buys:return 'WEAKENING','SOL_WEAKENING'
    return 'STABLE','SOL_OBSERVE'


def build_dashboard_payload():
    payload=_ORIGINAL_BUILD_DASHBOARD_PAYLOAD()
    for item in payload.get('evm_signals',[]):
        if item.get('chain')!='solana':continue
        symbol=str(item.get('token_symbol') or '').upper();address=item.get('token_address')
        if symbol=='SOL' or address=='native':item['status']='BENCHMARK';item['structure_state']='CORE ASSET';item['data_quality']='benchmark';item['is_benchmark']=True;continue
        pair=_solana_market_snapshot(address);review,structure=_solana_review(pair);item['status']=structure;item['structure_state']=review
        if pair:
            item['price_usd']=wallet_app.safe_float(pair.get('priceUsd'));item['liquidity_usd']=wallet_app.safe_float((pair.get('liquidity') or {}).get('usd'));item['volume_h1_usd']=wallet_app.safe_float((pair.get('volume') or {}).get('h1'));item['data_quality']='solana market snapshot';item['dexscreener_url']=pair.get('url') or item.get('dexscreener_url');change=pair.get('priceChange') or {}
            for window,key in(('1h','h1'),('6h','h6'),('24h','h24')):item['trends'][window]['price_change_pct']=wallet_app.safe_float(change.get(key));item['trends'][window]['available']=change.get(key) is not None
        else:item['data_quality']='solana market data unavailable'
    payload['v51_ingest']=v51_bridge.ingest_payload(wallet_app,payload);payload['early_discoveries']=v51_bridge.dashboard_rows(wallet_app,limit=30)
    return payload

wallet_app.refresh_evm_watchlist=refresh_evm_watchlist;wallet_app.build_dashboard_payload=build_dashboard_payload;wallet_app.VERSION='5.2.0-calibration'

@wallet_app.app.get('/api/v51/discoveries')
def discoveries_api():
    rows=v51_bridge.dashboard_rows(wallet_app,limit=50);return wallet_app.jsonify({'version':wallet_app.VERSION,'count':len(rows),'discoveries':rows})

@wallet_app.app.get('/api/v52/calibration')
def calibration_api():
    with wallet_app.db() as conn:
        early_discovery.initialise(conn);summary=early_discovery.calibration_summary(conn);wallets=early_discovery.wallet_performance(conn,30)
    return wallet_app.jsonify({'version':wallet_app.VERSION,'calibration':summary,'measured_wallets':wallets})


def _money(v):
    if v is None:return '—'
    v=float(v)
    return f'${v/1_000_000:.2f}m' if v>=1_000_000 else f'${v/1_000:.1f}k' if v>=1_000 else f'${v:,.0f}'
def _age(v):
    if v is None:return '—'
    v=float(v);return f'{v:.0f}m' if v<60 else f'{v/60:.1f}h' if v<1440 else f'{v/1440:.1f}d'
def _ret(v):return '—' if v is None else f'{float(v):+.1f}%'

@wallet_app.app.get('/v51')
def v51_dashboard():
    payload=build_dashboard_payload();rows=payload.get('early_discoveries') or [];rows=sorted(rows,key=lambda r:float(r.get('early_score') or 0),reverse=True);body=[]
    for r in rows[:30]:
        score=float(r.get('early_score') or 0);n=int(r.get('independent_wallets') or 0);symbol=html.escape(str(r.get('token_symbol') or 'Unknown'));chain=html.escape(str(r.get('chain') or ''));address=html.escape(str(r.get('token_address') or ''));reason=html.escape(str(r.get('reason') or 'wallet activity'));link=r.get('gmgn_url');token=f'<a href="{html.escape(link)}" target="_blank">{symbol}</a>' if link else symbol;conf=early_discovery.confidence_band(n);cls='confirmed' if n>=3 else 'double' if n==2 else 'single'
        body.append(f'''<tr><td><strong>{token}</strong><small>{chain}<br>{address[:8]}…{address[-6:]}</small></td><td><span class="score">{score:.0f}</span></td><td><span class="conf {cls}">{conf}</span></td><td>{_age(r.get('token_age_minutes'))}</td><td>{_money(r.get('discovery_market_cap_usd'))}</td><td>{_ret(r.get('return_1h'))}</td><td>{_ret(r.get('return_6h'))}</td><td>{_ret(r.get('return_24h'))}</td><td>{_ret(r.get('return_7d'))}</td><td>{_ret(r.get('peak_return_pct'))}</td><td class="why">{reason}</td></tr>''')
    empty='<tr><td colspan="11" class="empty">No discoveries yet.</td></tr>'
    return f'''<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>V5.2 Early Discovery</title><style>body{{font-family:system-ui;background:#0b0d10;color:#e8edf2;margin:0;padding:22px}}.wrap{{max-width:1500px;margin:auto}}p{{color:#98a2ad}}.card{{background:#12161b;border:1px solid #252b33;border-radius:14px;padding:18px;overflow:auto}}table{{width:100%;border-collapse:collapse;min-width:1250px}}th,td{{text-align:left;padding:11px;border-bottom:1px solid #252b33}}th{{color:#98a2ad;font-size:11px;text-transform:uppercase}}a{{color:#70b7ff;text-decoration:none}}small{{display:block;color:#727d89}}.score{{font-weight:800;font-size:20px}}.conf{{font-size:11px;font-weight:800;padding:5px 7px;border-radius:7px;white-space:nowrap}}.single{{background:#403716;color:#f1d56b}}.double{{background:#17364b;color:#8ed2ff}}.confirmed{{background:#153b2a;color:#72e2a6}}.why{{max-width:300px;color:#b9c1ca}}.empty{{text-align:center;padding:30px}}</style></head><body><div class="wrap"><h1>🔥 New Discoveries</h1><p>V5.2 separates early one-wallet observations from independent confirmation and measures what happened after discovery.</p><div class="card"><table><thead><tr><th>Token</th><th>Score</th><th>Evidence</th><th>Age found</th><th>Entry MC</th><th>+1h</th><th>+6h</th><th>+24h</th><th>+7d</th><th>Peak</th><th>Why</th></tr></thead><tbody>{''.join(body) if body else empty}</tbody></table></div><p>V5.2 · One-wallet scores are capped at 69 and two-wallet scores at 84 until measured outcomes justify recalibration.</p></div></body></html>'''

def _initial_portfolio_refresh():
    try:refresh_evm_watchlist(limit=20,offset=0)
    except Exception:pass
threading.Thread(target=_initial_portfolio_refresh,daemon=True).start();app=wallet_app.app
