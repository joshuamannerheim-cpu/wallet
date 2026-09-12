"""V5 portfolio enrichment bridge.

Wallet discovery and EVM market review are separate pipelines in app.py. This
bridge keeps current EVM holdings enrolled and makes the refresh budget large
enough to actually process the held portfolio rather than leaving most rows as
wallet-balance-only / DATA PENDING.
"""

import threading
import app as wallet_app


_ORIGINAL_REFRESH_EVM_WATCHLIST = wallet_app.refresh_evm_watchlist


def sync_portfolio_watchlist():
    """Mirror current held EVM tokens into the active monitoring watchlist."""
    wallet_app.initialise_database()
    supported = tuple(wallet_app.SUPPORTED_EVM_CHAINS)
    with wallet_app.db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT chain, token_address, token_symbol, token_name
                FROM evm_wallet_holdings
                WHERE wallet = %s
                  AND is_held = TRUE
                  AND chain = ANY(%s)
                  AND token_address LIKE '0x%%'
                """,
                (wallet_app.EVM_PORTFOLIO_WALLET, list(supported)),
            )
            holdings = cur.fetchall()

            for chain, address, symbol, name in holdings:
                cur.execute(
                    """
                    INSERT INTO token_watchlist (
                        chain, token_address, token_symbol, token_name,
                        source, monitoring_status, active, updated_at
                    ) VALUES (%s, %s, %s, %s, 'portfolio_auto',
                              'portfolio_enrichment_pending', TRUE, NOW())
                    ON CONFLICT (chain, token_address) DO UPDATE SET
                        token_symbol = CASE
                            WHEN EXCLUDED.token_symbol <> 'UNKNOWN'
                            THEN EXCLUDED.token_symbol
                            ELSE token_watchlist.token_symbol
                        END,
                        token_name = COALESCE(EXCLUDED.token_name, token_watchlist.token_name),
                        active = TRUE,
                        monitoring_status = CASE
                            WHEN token_watchlist.monitoring_status = 'portfolio_not_held'
                            THEN 'portfolio_enrichment_pending'
                            ELSE token_watchlist.monitoring_status
                        END,
                        updated_at = NOW()
                    """,
                    (chain, address, symbol or "UNKNOWN", name or symbol or "Unknown"),
                )

            cur.execute(
                """
                UPDATE token_watchlist w
                SET active = FALSE,
                    monitoring_status = 'portfolio_not_held',
                    updated_at = NOW()
                WHERE w.source = 'portfolio_auto'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM evm_wallet_holdings h
                      WHERE h.wallet = %s
                        AND h.chain = w.chain
                        AND LOWER(h.token_address) = LOWER(w.token_address)
                        AND h.is_held = TRUE
                  )
                """,
                (wallet_app.EVM_PORTFOLIO_WALLET,),
            )
        conn.commit()
    return len(holdings)


def refresh_evm_watchlist(limit=10, offset=0):
    """Enroll holdings and guarantee the refresh can cover the held portfolio."""
    held_count = sync_portfolio_watchlist()
    # The old cron commonly requests only 10 tokens. Once portfolio discovery
    # grew beyond that, many holdings never reached snapshot enrichment.
    effective_limit = max(int(limit or 0), held_count + 1, 20)
    return _ORIGINAL_REFRESH_EVM_WATCHLIST(limit=effective_limit, offset=0)


wallet_app.refresh_evm_watchlist = refresh_evm_watchlist
wallet_app.VERSION = "5.0.3-portfolio-refresh-budget"


def _initial_portfolio_refresh():
    """Seed snapshots after deploy without delaying gunicorn startup."""
    try:
        refresh_evm_watchlist(limit=20, offset=0)
    except Exception:
        # Cron retries this path; web availability should not depend on a market
        # data provider being healthy during process startup.
        pass


threading.Thread(target=_initial_portfolio_refresh, daemon=True).start()

app = wallet_app.app
