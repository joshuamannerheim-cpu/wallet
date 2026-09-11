"""V5 portfolio enrichment bridge.

Ensures wallet-discovered EVM holdings are enrolled in the normal EVM market
snapshot/signal pipeline before each refresh. This prevents portfolio holdings
from remaining indefinitely as wallet-balance-only / DATA PENDING rows.
"""

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
                        updated_at = NOW()
                    """,
                    (chain, address, symbol or "UNKNOWN", name or symbol or "Unknown"),
                )

            # Stop spending provider calls on portfolio-auto tokens that are no
            # longer held. Manually maintained/research watchlist rows are left alone.
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
    """Enroll portfolio holdings first, then run the existing trusted refresh."""
    sync_portfolio_watchlist()
    return _ORIGINAL_REFRESH_EVM_WATCHLIST(limit=limit, offset=offset)


# Patch the module global used by Flask endpoints and cron routes.
wallet_app.refresh_evm_watchlist = refresh_evm_watchlist
wallet_app.VERSION = "5.0.2-portfolio-enrichment"

# Enroll existing holdings immediately when the web process starts. The normal
# refresh cron then prioritises tokens with no trusted snapshot (NULLS FIRST).
try:
    sync_portfolio_watchlist()
except Exception:
    # Startup must remain available even if the database/provider is transiently
    # unavailable; the next refresh retries the sync.
    pass

app = wallet_app.app
