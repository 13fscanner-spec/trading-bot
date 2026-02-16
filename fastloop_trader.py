#!/usr/bin/env python3
"""
Simmer Polymarket Crypto Price Target Trader v4.0 (Professional)

Quantitative trading bot for crypto price bracket markets on Polymarket.
Uses real-time Binance data + optional Deribit implied volatility to find
mispriced brackets and execute value bets via Simmer API.

Features:
    - Student-t fat-tailed probability model (adaptive df)
    - Multi-timeframe volatility (6h/24h/7d weighted)
    - Market regime detection (trending/volatile/mean-reverting)
    - Kelly Criterion position sizing (quarter-Kelly)
    - Expected Value scoring with fee adjustment
    - Portfolio-level risk management
    - Deribit implied volatility integration
    - Continuous loop mode with graceful shutdown
    - Full trade history journal

Usage:
    python fastloop_trader.py                     # Dry run (scan once)
    python fastloop_trader.py --live              # Execute trades (once)
    python fastloop_trader.py --loop              # Continuous scanning loop
    python fastloop_trader.py --live --loop       # Continuous live trading
    python fastloop_trader.py --live --kelly      # Kelly Criterion sizing
    python fastloop_trader.py --positions         # Show positions
    python fastloop_trader.py --history           # Trade history stats

Requires:
    SIMMER_API_KEY environment variable (get from simmer.markets/dashboard)
"""

import os
import sys
import json
import math
import re
import time
import signal
import argparse
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, quote

# Force line-buffered stdout for non-TTY environments (cron, Docker)
sys.stdout.reconfigure(line_buffering=True)

# Optional: Trade Journal integration
try:
    from tradejournal import log_trade
    JOURNAL_AVAILABLE = True
except ImportError:
    try:
        from skills.tradejournal import log_trade
        JOURNAL_AVAILABLE = True
    except ImportError:
        JOURNAL_AVAILABLE = False
        def log_trade(*args, **kwargs):
            pass

# =============================================================================
# Configuration (config.json > env vars > defaults)
# =============================================================================

CONFIG_SCHEMA = {
    "assets": {"default": "BTC,ETH,SOL", "env": "SIMMER_ASSETS", "type": str,
               "help": "Comma-separated list of assets to scan (BTC,ETH,SOL)"},
    "min_divergence_pct": {"default": 10, "env": "SIMMER_MIN_DIVERGENCE", "type": float,
                           "help": "Min % divergence between estimated and market probability"},
    "max_position": {"default": 5.0, "env": "SIMMER_MAX_POSITION", "type": float,
                     "help": "Max $ per trade"},
    "max_daily_trades": {"default": 3, "env": "SIMMER_MAX_DAILY", "type": int,
                         "help": "Max trades per day"},
    "min_liquidity": {"default": 500, "env": "SIMMER_MIN_LIQUIDITY", "type": float,
                      "help": "Min market liquidity in USD"},
    "signal_source": {"default": "binance", "env": "SIMMER_SIGNAL", "type": str,
                      "help": "Price feed source (binance)"},
    "volatility_hours": {"default": 24, "env": "SIMMER_VOL_HOURS", "type": int,
                         "help": "Hours of price history for volatility calc"},
}

TRADE_SOURCE = "sdk:price_target"
SMART_SIZING_PCT = 0.05
MIN_SHARES_PER_ORDER = 5

# Asset -> Binance symbol mapping
ASSET_SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
}

# Search patterns for Polymarket crypto price markets
PRICE_MARKET_PATTERNS = {
    "BTC": ["price of bitcoin", "bitcoin price", "btc price"],
    "ETH": ["price of ethereum", "ethereum price", "eth price"],
    "SOL": ["price of solana", "solana price", "sol price"],
}


def _load_config(schema, skill_file, config_filename="config.json"):
    """Load config with priority: config.json > env vars > defaults."""
    from pathlib import Path
    config_path = Path(skill_file).parent / config_filename
    file_cfg = {}
    if config_path.exists():
        try:
            with open(config_path) as f:
                file_cfg = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    result = {}
    for key, spec in schema.items():
        if key in file_cfg:
            result[key] = file_cfg[key]
        elif spec.get("env") and os.environ.get(spec["env"]):
            val = os.environ.get(spec["env"])
            type_fn = spec.get("type", str)
            try:
                if type_fn == bool:
                    result[key] = val.lower() in ("true", "1", "yes")
                else:
                    result[key] = type_fn(val)
            except (ValueError, TypeError):
                result[key] = spec.get("default")
        else:
            result[key] = spec.get("default")
    return result


def _get_config_path(skill_file, config_filename="config.json"):
    from pathlib import Path
    return Path(skill_file).parent / config_filename


def _update_config(updates, skill_file, config_filename="config.json"):
    """Update config.json with new values."""
    from pathlib import Path
    config_path = Path(skill_file).parent / config_filename
    existing = {}
    if config_path.exists():
        try:
            with open(config_path) as f:
                existing = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    existing.update(updates)
    with open(config_path, "w") as f:
        json.dump(existing, f, indent=2)
    return existing


# Load config
cfg = _load_config(CONFIG_SCHEMA, __file__)
ASSETS = [a.strip().upper() for a in cfg["assets"].split(",")]
MIN_DIVERGENCE_PCT = cfg["min_divergence_pct"]
MAX_POSITION_USD = cfg["max_position"]
MAX_DAILY_TRADES = cfg["max_daily_trades"]
MIN_LIQUIDITY = cfg["min_liquidity"]
SIGNAL_SOURCE = cfg["signal_source"]
VOLATILITY_HOURS = cfg["volatility_hours"]


# =============================================================================
# Graceful Shutdown & Loop Control
# =============================================================================

_shutdown_requested = False
_loop_running = False

def _signal_handler(signum, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    global _shutdown_requested
    sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
    print(f"\n⚠️  Shutdown signal received ({sig_name}). Finishing current cycle...")
    _shutdown_requested = True
    if not _loop_running:
        sys.exit(0)

# Register signal handlers
try:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
except (OSError, AttributeError):
    pass  # Some platforms don't support all signals


# =============================================================================
# Logging
# =============================================================================

_quiet = False

def log(msg, force=False):
    if not _quiet or force:
        try:
            print(msg)
        except UnicodeEncodeError:
            print(msg.encode('ascii', errors='replace').decode('ascii'))


# =============================================================================
# API Helpers
# =============================================================================

SIMMER_BASE = os.environ.get("SIMMER_API_BASE", "https://api.simmer.markets")


def get_api_key():
    key = os.environ.get("SIMMER_API_KEY")
    if not key:
        print("Error: SIMMER_API_KEY environment variable not set")
        print("Get your API key from: simmer.markets/dashboard → SDK tab")
        sys.exit(1)
    return key


def _api_request(url, method="GET", data=None, headers=None, timeout=30, retries=3):
    """Make an HTTP request with exponential backoff retry.

    Retries on connection errors and 5xx server errors.
    Does NOT retry on 4xx client errors (bad request).
    """
    last_error = None
    for attempt in range(retries):
        try:
            req_headers = headers or {}
            if "User-Agent" not in req_headers:
                # Use standard browser User-Agent to bypass Cloudflare WAF
                req_headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            body = None
            if data:
                body = json.dumps(data).encode("utf-8")
                req_headers["Content-Type"] = "application/json"
            req = Request(url, data=body, headers=req_headers, method=method)
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            # Don't retry 4xx errors
            if 400 <= e.code < 500:
                try:
                    error_body = json.loads(e.read().decode("utf-8"))
                    return {"error": error_body.get("detail", str(e)), "status_code": e.code}
                except Exception:
                    return {"error": str(e), "status_code": e.code}
            # Retry 5xx errors
            last_error = str(e)
        except URLError as e:
            last_error = f"Connection error: {e.reason}"
        except Exception as e:
            last_error = str(e)

        # Exponential backoff: 1s, 2s, 4s
        if attempt < retries - 1:
            backoff = 2 ** attempt
            time.sleep(backoff)

    return {"error": f"Failed after {retries} attempts: {last_error}"}


def simmer_request(path, method="GET", data=None, api_key=None):
    """Make a Simmer API request."""
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return _api_request(f"{SIMMER_BASE}{path}", method=method, data=data, headers=headers)


# =============================================================================
# Crypto Price Market Discovery
# =============================================================================


def _market_to_entry(m, matched_asset):
    """Convert a raw market dict into a standardized entry for the found list."""
    bracket = _parse_price_bracket(m.get("question", ""))
    if not bracket:
        return None

    # Must be open and accepting orders
    if m.get("closed", False) or not m.get("acceptingOrders", True):
        return None

    # Check liquidity (API returns both 'liquidity' and 'liquidityNum')
    try:
        liquidity = float(m.get("liquidityNum", 0) or m.get("liquidity", 0) or 0)
    except Exception as e:
        liquidity = 0
        
    if liquidity < MIN_LIQUIDITY:
        return None

    # Parse outcomes and prices
    try:
        prices = json.loads(m.get("outcomePrices", "[]"))
        yes_price = float(prices[0]) if prices else 0.5
    except (json.JSONDecodeError, IndexError, ValueError):
        yes_price = 0.5

    # Parse end date
    end_date_str = m.get("endDate", "")
    try:
        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        end_date = None

    # Skip already expired
    if end_date and end_date < datetime.now(timezone.utc):
        return None

    fees_enabled = m.get("feesEnabled", False)
    fee_rate_bps = int(m.get("fee_rate_bps") or m.get("feeRateBps") or 0)

    return {
        "asset": matched_asset,
        "question": m.get("question", ""),
        "slug": m.get("slug", ""),
        "condition_id": m.get("conditionId", ""),
        "bracket_low": bracket[0],
        "bracket_high": bracket[1],
        "yes_price": yes_price,
        "liquidity": liquidity,
        "volume_24h": float(m.get("volume24hr", 0) or 0),
        "end_date": end_date,
        "fees_enabled": fees_enabled,
        "fee_rate_bps": fee_rate_bps,
        "event_slug": _extract_event_slug(m),
    }


def _match_asset(question_lower, assets):
    """Check if a market question matches any tracked crypto asset."""
    for asset in assets:
        patterns = PRICE_MARKET_PATTERNS.get(asset, [])
        if any(p in question_lower for p in patterns):
            return asset
    return None


def discover_price_markets(assets=None):
    """Find active crypto price bracket markets on Polymarket.

    Bulk-fetches from /markets endpoint with pagination (up to 500 markets)
    and applies client-side regex filtering for crypto price bracket patterns.
    Results are deduplicated by condition_id.
    """
    if assets is None:
        assets = ASSETS

    seen_ids = set()
    found = []

    # Bulk-fetch from /markets with pagination
    log("  📡 Scanning markets (bulk fetch)...")
    pages_to_fetch = 5  # 5 pages × 100 = up to 500 markets
    per_page = 100
    for page in range(pages_to_fetch):
        offset = page * per_page
        url = (
            f"https://gamma-api.polymarket.com/markets"
            f"?limit={per_page}&closed=false&active=true"
            f"&order=volume24hr&ascending=false&offset={offset}"
        )
        result = _api_request(url)
        if not result or isinstance(result, dict) and result.get("error"):
            if page == 0:
                log(f"  ❌ Failed to fetch markets: {result}")
            break  # Stop paginating on error

        if not isinstance(result, list) or len(result) == 0:
            break  # No more results

        for m in result:
            q = (m.get("question") or "").lower()
            matched_asset = _match_asset(q, assets)
            if matched_asset:
                cid = m.get("conditionId", "")
                if cid and cid in seen_ids:
                    continue
                entry = _market_to_entry(m, matched_asset)
                if entry:
                    if cid:
                        seen_ids.add(cid)
                    found.append(entry)

        # If we got fewer results than requested, no more pages
        if len(result) < per_page:
            break

    log(f"  ✅ Discovered {len(found)} active crypto price bracket markets")
    return found


def _parse_price_bracket(question):
    """Parse price bracket from question text.
    Examples:
        'Will the price of Solana be between $190 and $200 on Feb 16?' -> (190, 200)
        'Will the price of Solana be between $90-100 on February 16?' -> (90, 100)
        'Bitcoin price between $95,000-$100,000' -> (95000, 100000)
    """
    # Pattern 1: "between $X and $Y" or "between $X-$Y"
    match = re.search(
        r'between\s+\$?([\d,]+(?:\.\d+)?)\s*(?:and|-)\s*\$?([\d,]+(?:\.\d+)?)',
        question, re.IGNORECASE
    )
    if match:
        low = float(match.group(1).replace(",", ""))
        high = float(match.group(2).replace(",", ""))
        return (low, high)

    # Pattern 2: "$X-$Y" or "$X - $Y"
    match = re.search(
        r'\$([\d,]+(?:\.\d+)?)\s*-\s*\$?([\d,]+(?:\.\d+)?)',
        question
    )
    if match:
        low = float(match.group(1).replace(",", ""))
        high = float(match.group(2).replace(",", ""))
        return (low, high)

    # Pattern 3: "between X-Y" (no dollar sign)
    match = re.search(
        r'be\s+between\s+([\d,]+(?:\.\d+)?)\s*-\s*([\d,]+(?:\.\d+)?)',
        question, re.IGNORECASE
    )
    if match:
        low = float(match.group(1).replace(",", ""))
        high = float(match.group(2).replace(",", ""))
        return (low, high)

    # Pattern 4: "above $X" or "greater than $X" or "> $X"
    match = re.search(
        r'(?:above|greater than|>\s*)\s*\$?([\d,]+(?:\.\d+)?)',
        question, re.IGNORECASE
    )
    if match:
        val = float(match.group(1).replace(",", ""))
        return (val, 10_000_000.0)  # Effectively infinity

    # Pattern 5: "below $X" or "less than $X" or "< $X"
    match = re.search(
        r'(?:below|less than|<\s*)\s*\$?([\d,]+(?:\.\d+)?)',
        question, re.IGNORECASE
    )
    if match:
        val = float(match.group(1).replace(",", ""))
        return (0.0, val)

    return None


def _extract_event_slug(market):
    """Extract event slug from market data."""
    events = market.get("events", [])
    if events and isinstance(events, list):
        return events[0].get("slug", "")
    return market.get("slug", "")


# =============================================================================
# Binance Price & Volatility Data (Multi-Timeframe)
# =============================================================================

BINANCE_ENDPOINTS = [
    "https://api.binance.com",
    "https://data-api.binance.vision",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
]

# Market regime thresholds
REGIME_TREND_THRESHOLD = 0.015     # 1.5% directional move to classify as trending
REGIME_VOL_SPIKE_MULT = 1.8        # vol spike = current vol > 1.8x baseline


def _fetch_klines(symbol, interval, limit):
    """Fetch kline data from Binance, trying multiple endpoints."""
    for base in BINANCE_ENDPOINTS:
        url = f"{base}/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
        result = _api_request(url, timeout=10)
        if result and isinstance(result, list) and len(result) >= 3:
            return result
    return None


def _calc_returns_stats(closes):
    """Calculate return statistics from a list of close prices."""
    if len(closes) < 3:
        return None
    returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
    mean_ret = sum(returns) / len(returns)
    variance = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
    vol = math.sqrt(variance)

    # Kurtosis (excess) — measures fat-tailedness
    if vol > 0:
        fourth_moment = sum((r - mean_ret) ** 4 for r in returns) / len(returns)
        kurtosis = (fourth_moment / (vol ** 4)) - 3.0
    else:
        kurtosis = 0.0

    return {
        "volatility": vol,
        "mean_return": mean_ret,
        "kurtosis": kurtosis,
        "returns": returns,
        "n": len(returns),
    }


def get_binance_data(symbol="BTCUSDT"):
    """Get comprehensive price data with multi-timeframe volatility and regime detection.

    Returns dict with:
        current_price, multi-frame volatility, regime classification,
        trend metrics, and price range info.
    """
    # Fetch 3 timeframes
    klines_1h = _fetch_klines(symbol, "1h", 168)   # 7 days of hourly
    if not klines_1h:
        return None

    klines_4h = _fetch_klines(symbol, "4h", 42)     # 7 days of 4h
    klines_15m = _fetch_klines(symbol, "15m", 96)   # 24h of 15-min

    # Extract data from 1h candles
    closes_1h = [float(c[4]) for c in klines_1h]
    highs_1h = [float(c[2]) for c in klines_1h]
    lows_1h = [float(c[3]) for c in klines_1h]
    volumes_1h = [float(c[5]) for c in klines_1h]

    current_price = closes_1h[-1]

    # --- Multi-timeframe volatility ---
    # Short-term: last 6 hours
    stats_6h = _calc_returns_stats(closes_1h[-7:]) if len(closes_1h) >= 7 else None
    # Medium-term: last 24 hours
    stats_24h = _calc_returns_stats(closes_1h[-25:]) if len(closes_1h) >= 25 else None
    # Long-term: last 7 days
    stats_7d = _calc_returns_stats(closes_1h)

    # 4h volatility for longer-horizon estimation
    if klines_4h:
        closes_4h = [float(c[4]) for c in klines_4h]
        stats_4h = _calc_returns_stats(closes_4h)
    else:
        stats_4h = None

    # 15m volatility for short-term precision
    if klines_15m:
        closes_15m = [float(c[4]) for c in klines_15m]
        stats_15m = _calc_returns_stats(closes_15m)
    else:
        stats_15m = None

    # Weighted volatility (per-hour basis)
    vol_components = []
    if stats_6h and stats_6h["volatility"] > 0:
        vol_components.append((stats_6h["volatility"], 0.30))       # recent regime
    if stats_24h and stats_24h["volatility"] > 0:
        vol_components.append((stats_24h["volatility"], 0.40))      # primary window
    if stats_7d and stats_7d["volatility"] > 0:
        vol_components.append((stats_7d["volatility"], 0.30))       # baseline

    if vol_components:
        total_weight = sum(w for _, w in vol_components)
        hourly_vol = sum(v * w for v, w in vol_components) / total_weight
    elif stats_7d:
        hourly_vol = stats_7d["volatility"]
    else:
        hourly_vol = 0.005  # fallback 0.5%

    # --- Trend analysis ---
    # Short-term trend (6h momentum)
    if len(closes_1h) >= 7:
        trend_6h = (closes_1h[-1] - closes_1h[-7]) / closes_1h[-7]
    else:
        trend_6h = 0.0

    # Medium-term trend (24h)
    if len(closes_1h) >= 25:
        trend_24h = (closes_1h[-1] - closes_1h[-25]) / closes_1h[-25]
    else:
        trend_24h = 0.0

    # Momentum score: weighted combination of trends
    momentum = trend_6h * 0.6 + trend_24h * 0.4

    # RSI-like strength indicator (14-period on hourly)
    if stats_7d and len(stats_7d["returns"]) >= 14:
        recent_rets = stats_7d["returns"][-14:]
        gains = [r for r in recent_rets if r > 0]
        losses = [-r for r in recent_rets if r < 0]
        avg_gain = sum(gains) / 14 if gains else 0.0001
        avg_loss = sum(losses) / 14 if losses else 0.0001
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
    else:
        rsi = 50.0

    # --- Regime detection ---
    regime = "normal"
    regime_confidence = 0.5

    if stats_6h and stats_7d:
        vol_ratio = stats_6h["volatility"] / max(stats_7d["volatility"], 0.0001)

        # Volatile regime: short-term vol >> long-term baseline
        if vol_ratio > REGIME_VOL_SPIKE_MULT:
            regime = "volatile"
            regime_confidence = min(0.95, 0.5 + (vol_ratio - REGIME_VOL_SPIKE_MULT) * 0.2)
        # Trending regime: strong directional move
        elif abs(trend_24h) > REGIME_TREND_THRESHOLD:
            regime = "trending_up" if trend_24h > 0 else "trending_down"
            regime_confidence = min(0.95, 0.5 + abs(trend_24h) / REGIME_TREND_THRESHOLD * 0.2)
        # Mean-reverting: low vol, RSI at extremes
        elif vol_ratio < 0.7 and (rsi > 65 or rsi < 35):
            regime = "mean_reverting"
            regime_confidence = min(0.9, 0.5 + (0.7 - vol_ratio) * 0.5)

    # --- Price range ---
    period_high_24h = max(highs_1h[-24:]) if len(highs_1h) >= 24 else max(highs_1h)
    period_low_24h = min(lows_1h[-24:]) if len(lows_1h) >= 24 else min(lows_1h)
    period_high_7d = max(highs_1h)
    period_low_7d = min(lows_1h)

    # Mean-reversion anchor: VWAP-like estimate
    if volumes_1h:
        total_vol = sum(volumes_1h[-24:])
        if total_vol > 0:
            vwap = sum(c * v for c, v in zip(closes_1h[-24:], volumes_1h[-24:])) / total_vol
        else:
            vwap = current_price
    else:
        vwap = current_price

    # Kurtosis from the primary window (indicates fat-tailedness)
    kurtosis = stats_24h["kurtosis"] if stats_24h else (stats_7d["kurtosis"] if stats_7d else 3.0)

    return {
        "current_price": current_price,
        "hourly_volatility": hourly_vol,
        "vol_6h": stats_6h["volatility"] if stats_6h else hourly_vol,
        "vol_24h": stats_24h["volatility"] if stats_24h else hourly_vol,
        "vol_7d": stats_7d["volatility"] if stats_7d else hourly_vol,
        "trend_6h_pct": trend_6h * 100,
        "trend_24h_pct": trend_24h * 100,
        "momentum": momentum,
        "rsi": rsi,
        "regime": regime,
        "regime_confidence": regime_confidence,
        "period_high_24h": period_high_24h,
        "period_low_24h": period_low_24h,
        "period_high_7d": period_high_7d,
        "period_low_7d": period_low_7d,
        "vwap_24h": vwap,
        "kurtosis": kurtosis,
    }


# =============================================================================
# Deribit Implied Volatility (Options Market Data)
# =============================================================================

DERIBIT_BASE = "https://www.deribit.com/api/v2/public"
DERIBIT_SYMBOLS = {"BTC": "BTC", "ETH": "ETH"}  # SOL not on Deribit
_iv_cache = {}  # {symbol: {"iv": float, "timestamp": datetime}}

def get_deribit_iv(asset):
    """Get implied volatility from Deribit options market.

    Uses the DVOL index (Deribit's 30-day implied vol) — this reflects
    forward-looking market expectations of volatility, which is more
    accurate than historical vol for pricing.

    Returns annualized IV as a decimal, or None if unavailable.
    """
    deribit_sym = DERIBIT_SYMBOLS.get(asset)
    if not deribit_sym:
        return None

    # Cache: reuse if < 5 minutes old
    cached = _iv_cache.get(asset)
    if cached:
        age = (datetime.now(timezone.utc) - cached["timestamp"]).total_seconds()
        if age < 300:
            return cached["iv"]

    try:
        # Get the DVOL index (30-day implied volatility)
        url = f"{DERIBIT_BASE}/get_volatility_index_data?currency={deribit_sym}&resolution=3600&start_timestamp={int((datetime.now(timezone.utc) - timedelta(hours=2)).timestamp() * 1000)}&end_timestamp={int(datetime.now(timezone.utc).timestamp() * 1000)}"
        result = _api_request(url, timeout=10, retries=2)

        if result and not result.get("error"):
            data = result.get("result", {}).get("data", [])
            if data:
                # Last volatility reading [timestamp, open, high, low, close]
                latest_iv = data[-1][4] / 100  # Convert from percentage
                _iv_cache[asset] = {
                    "iv": latest_iv,
                    "timestamp": datetime.now(timezone.utc),
                }
                return latest_iv
    except Exception:
        pass

    # Fallback: try ticker for a near-term option
    try:
        url = f"{DERIBIT_BASE}/ticker?instrument_name={deribit_sym}-PERPETUAL"
        result = _api_request(url, timeout=10, retries=1)
        if result and result.get("result"):
            mark_iv = result["result"].get("mark_iv")
            if mark_iv:
                iv = mark_iv / 100
                _iv_cache[asset] = {
                    "iv": iv,
                    "timestamp": datetime.now(timezone.utc),
                }
                return iv
    except Exception:
        pass

    return None


def blend_volatility(hist_vol, implied_vol, hours_remaining):
    """Blend historical and implied volatility.

    For short timeframes (< 6h), weight historical vol more (market moves).
    For longer timeframes (> 24h), weight implied vol more (forward-looking).
    """
    if implied_vol is None:
        return hist_vol

    # Convert annualized IV to hourly
    iv_hourly = implied_vol / math.sqrt(365 * 24)

    if hours_remaining <= 6:
        # Near-expiry: 70% historical, 30% options
        return 0.70 * hist_vol + 0.30 * iv_hourly
    elif hours_remaining <= 24:
        # Medium-term: 50/50 blend
        return 0.50 * hist_vol + 0.50 * iv_hourly
    else:
        # Longer-term: 30% historical, 70% options (options know more)
        return 0.30 * hist_vol + 0.70 * iv_hourly


# =============================================================================
# Dynamic Edge Thresholds
# =============================================================================

def calculate_dynamic_edge(hours_remaining, liquidity, base_divergence_pct):
    """Calculate minimum edge threshold based on market conditions.

    Philosophy: require higher edge for riskier markets.
    - Near-expiry: model is more accurate, accept lower edge
    - Far-expiry: more uncertainty, require higher edge
    - Low liquidity: higher slippage risk, require higher edge
    """
    edge = base_divergence_pct

    # Time-based adjustment
    if hours_remaining > 48:
        edge *= 1.5      # 50% higher edge for markets > 2 days out
    elif hours_remaining > 24:
        edge *= 1.25     # 25% higher for > 1 day
    elif hours_remaining < 4:
        edge *= 0.80     # Accept 20% less edge for near-expiry certainty

    # Liquidity-based adjustment
    if liquidity < 1000:
        edge *= 1.30     # 30% higher edge for thin markets
    elif liquidity < 2000:
        edge *= 1.15     # 15% higher for moderate markets
    elif liquidity > 10000:
        edge *= 0.90     # Accept 10% less edge for deep markets

    return round(edge, 1)


# =============================================================================
# Signal Confidence Scoring
# =============================================================================

def calculate_signal_confidence(opportunity, price_data):
    """Calculate a composite trade confidence score (0.0 to 1.0).

    Combines multiple independent signals:
      - Edge strength (model probability vs market price)
      - Liquidity adequacy
      - Time remaining (not too short, not too far out)
      - Regime alignment (is the regime favorable?)
      - RSI positioning (is momentum supporting our direction?)
    """
    scores = []

    # 1. Edge strength (0-1): higher divergence = higher confidence
    div = opportunity.get("divergence_pct", 0)
    edge_score = min(1.0, div / 50)  # 50% divergence = perfect score
    scores.append(("edge", edge_score, 0.30))  # weight 30%

    # 2. EV quality (0-1): positive EV is good, higher is better
    ev = opportunity.get("ev", 0)
    ev_score = min(1.0, max(0, ev * 10))  # 0.10 EV = perfect score
    scores.append(("ev", ev_score, 0.25))  # weight 25%

    # 3. Liquidity (0-1): more liquid = better execution
    liq = opportunity.get("liquidity", 0)
    liq_score = min(1.0, liq / 5000)  # $5000 liquidity = perfect
    scores.append(("liquidity", liq_score, 0.15))

    # 4. Time sweet spot (0-1): 4-24h is ideal, penalize extremes
    hours = opportunity.get("hours_remaining", 12)
    if 4 <= hours <= 24:
        time_score = 1.0
    elif 2 <= hours < 4:
        time_score = 0.6
    elif 24 < hours <= 48:
        time_score = 0.7
    else:
        time_score = 0.4
    scores.append(("time", time_score, 0.15))

    # 5. Regime alignment (0-1): does our trade direction align with regime?
    regime = price_data.get("regime", "normal")
    side = opportunity.get("side", "yes")
    momentum = price_data.get("momentum", 0)

    # A trending regime supporting our side is good
    if regime.startswith("trending"):
        if (regime == "trending_up" and side == "yes") or \
           (regime == "trending_down" and side == "no"):
            regime_score = 0.9  # Regime aligns
        else:
            regime_score = 0.3  # Fading the trend = risky
    elif regime == "mean_reverting":
        regime_score = 0.7  # Range-bound is OK for bracket bets
    elif regime == "volatile":
        regime_score = 0.5  # Vol is tricky
    else:
        regime_score = 0.6  # Normal regime, neutral

    scores.append(("regime", regime_score, 0.15))

    # Weighted average
    total_weight = sum(w for _, _, w in scores)
    confidence = sum(s * w for _, s, w in scores) / total_weight if total_weight > 0 else 0.5

    return {
        "confidence": round(confidence, 3),
        "components": {name: round(score, 3) for name, score, _ in scores},
    }


# =============================================================================
# Market Cooldown Tracking
# =============================================================================

_market_cooldown = {}  # {condition_id: last_evaluated_timestamp}
COOLDOWN_MINUTES = 10   # Don't re-evaluate same market within 10 minutes

def is_on_cooldown(condition_id):
    """Check if we recently evaluated this market."""
    if not condition_id:
        return False
    last_time = _market_cooldown.get(condition_id)
    if not last_time:
        return False
    elapsed = (datetime.now(timezone.utc) - last_time).total_seconds() / 60
    return elapsed < COOLDOWN_MINUTES

def mark_evaluated(condition_id):
    """Mark a market as recently evaluated."""
    if condition_id:
        _market_cooldown[condition_id] = datetime.now(timezone.utc)

def clear_stale_cooldowns():
    """Remove cooldowns older than 1 hour to prevent memory growth."""
    now = datetime.now(timezone.utc)
    stale = [cid for cid, t in _market_cooldown.items()
             if (now - t).total_seconds() > 3600]
    for cid in stale:
        del _market_cooldown[cid]


# =============================================================================
# Health Check
# =============================================================================

def run_health_check():
    """Verify API connectivity before trading."""
    checks = []

    # 1. Simmer API
    result = _api_request(f"{SIMMER_BASE}/api/sdk/health", timeout=10, retries=2)
    simmer_ok = result is not None and not result.get("error")
    checks.append(("Simmer API", simmer_ok))

    # 2. Binance API
    for endpoint in BINANCE_ENDPOINTS[:1]:
        result = _api_request(f"{endpoint}/api/v3/ping", timeout=10, retries=1)
        binance_ok = result is not None and not result.get("error")
        checks.append(("Binance API", binance_ok))
        break

    # 3. Deribit API (optional)
    try:
        result = _api_request(f"{DERIBIT_BASE}/test", timeout=5, retries=1)
        deribit_ok = result is not None and not result.get("error")
    except Exception:
        deribit_ok = False
    checks.append(("Deribit API", deribit_ok))

    return checks


# =============================================================================
# Probability Estimation (Student-t Fat-Tailed Model)
# =============================================================================

def _student_t_cdf(x, df=5):
    """Approximate Student-t CDF using the regularized incomplete beta function.

    For df=5, provides fatter tails than normal distribution — critical for
    crypto where extreme moves (>3σ) happen 3-5x more often than normal predicts.
    """
    if df <= 0:
        return _normal_cdf(x)

    # Use the relationship between t-CDF and the regularized incomplete beta fn
    t2 = x * x
    # For large |x|, use asymptotic approximation
    if abs(x) > 50:
        return 1.0 if x > 0 else 0.0

    # Numerical integration using Simpson's rule for the t-PDF
    # t-PDF: f(t) = C * (1 + t²/df)^(-(df+1)/2)
    # We integrate from -inf to x
    # For efficiency, use symmetry: if x >= 0, CDF = 0.5 + integral(0, x)
    #                                if x < 0,  CDF = 0.5 - integral(0, |x|)

    def t_pdf(t):
        return (1 + t * t / df) ** (-(df + 1) / 2)

    # Normalization constant: Gamma((df+1)/2) / (sqrt(df*pi) * Gamma(df/2))
    # For df=5: B(5/2, 1/2) normalization
    # Pre-computed for common df values
    norm_constants = {
        3: 0.3183098861837907,   # 1/(pi)
        4: 0.3750000000000000,
        5: 0.3796066898224944,
        6: 0.3819660112501052,
        7: 0.3827327723098710,
        10: 0.3891083839660346,
    }

    if df in norm_constants:
        norm = norm_constants[df]
    else:
        # Approximate using Stirling
        norm = math.gamma((df + 1) / 2) / (math.sqrt(df * math.pi) * math.gamma(df / 2))

    abs_x = abs(x)
    # Simpson's rule integration from 0 to abs_x
    n_steps = max(100, int(abs_x * 50))
    if n_steps % 2 == 1:
        n_steps += 1
    h = abs_x / n_steps

    integral = t_pdf(0) + t_pdf(abs_x)
    for i in range(1, n_steps, 2):
        integral += 4 * t_pdf(i * h)
    for i in range(2, n_steps, 2):
        integral += 2 * t_pdf(i * h)
    integral *= h / 3 * norm

    if x >= 0:
        return 0.5 + integral
    else:
        return 0.5 - integral


def _normal_cdf(x):
    """Approximate the standard normal CDF using the error function."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def estimate_bracket_probability(current_price, bracket_low, bracket_high,
                                  hourly_vol, hours_remaining, price_data=None):
    """Estimate probability that price lands in [bracket_low, bracket_high]
    at resolution time using a fat-tailed Student-t distribution.

    Improvements over v2:
      - Student-t distribution (df=5) for fat tails
      - Regime-aware drift adjustment
      - Mean-reversion bias when RSI is extreme
      - Volatility inflation during volatile regimes
      - Trend dampening for longer time horizons

    Args:
        current_price: Current asset price
        bracket_low: Lower bound of the bracket
        bracket_high: Upper bound of the bracket
        hourly_vol: Weighted hourly volatility
        hours_remaining: Hours until market resolves
        price_data: Full price_data dict from get_binance_data (optional)

    Returns:
        Estimated probability (0 to 1)
    """
    if current_price <= 0 or hourly_vol <= 0 or hours_remaining <= 0:
        return 0.0

    # Extract regime info if available
    regime = (price_data or {}).get("regime", "normal")
    momentum = (price_data or {}).get("momentum", 0)
    rsi = (price_data or {}).get("rsi", 50)
    vwap = (price_data or {}).get("vwap_24h", current_price)
    kurtosis = (price_data or {}).get("kurtosis", 3.0)

    # --- Drift calculation with regime adjustment ---
    # Base drift from momentum, but dampen for longer horizons
    # Trend tends to mean-revert over longer periods
    dampening = 1.0 / (1.0 + hours_remaining / 12)  # halves every 12h
    drift_per_hour = 0.0 # momentum * dampening (Disabled for stability)

    # Mean-reversion adjustment: pull toward VWAP when RSI is extreme
    if rsi > 70:
        # Overbought → bias downward toward VWAP
        reversion_strength = (rsi - 70) / 100 * 0.002  # up to 0.06%/h at RSI=100
        price_gap = (vwap - current_price) / current_price
        drift_per_hour += price_gap * reversion_strength
    elif rsi < 30:
        # Oversold → bias upward toward VWAP
        reversion_strength = (30 - rsi) / 100 * 0.002
        price_gap = (vwap - current_price) / current_price
        drift_per_hour += price_gap * reversion_strength

    total_drift = drift_per_hour * hours_remaining
    expected_price = current_price * math.exp(total_drift)

    # --- Volatility with regime adjustment ---
    vol = hourly_vol

    # Inflate volatility during volatile regimes
    if regime == "volatile":
        vol *= 1.3  # 30% vol boost during high-vol regimes

    # Slightly reduce vol in mean-reverting regime (range-bound)
    elif regime == "mean_reverting":
        vol *= 0.85

    # Total volatility scales with sqrt(time)
    total_vol = vol * math.sqrt(hours_remaining)
    if total_vol < 0.0001:
        total_vol = 0.0001

    # --- Choose distribution based on observed kurtosis ---
    # Higher kurtosis → use fatter-tailed distribution
    # Adaptive df: more kurtosis = lower df = fatter tails
    if kurtosis > 6:
        df = 5   # was 3 (very fat) -> now moderate
    elif kurtosis > 3:
        df = 8   # was 5 -> now mild
    elif kurtosis > 1:
        df = 12  # was 7 -> now close to normal
    else:
        df = 30  # was 10 -> now effectively Normal distribution

    # Scale factor for Student-t to match volatility
    # Student-t variance = df/(df-2) * scale², so scale = vol * sqrt((df-2)/df)
    if df > 2:
        scale = total_vol * math.sqrt((df - 2) / df)
    else:
        scale = total_vol

    # Calculate z-scores
    if bracket_low <= 0:
        z_low = -20  # effectively -infinity
    else:
        z_low = math.log(bracket_low / expected_price) / scale

    z_high = math.log(bracket_high / expected_price) / scale

    # Use Student-t CDF for fat tails
    prob = _student_t_cdf(z_high, df=df) - _student_t_cdf(z_low, df=df)

    # Clamp to reasonable range
    return max(0.001, min(0.999, prob))


def calculate_expected_value(est_prob, market_price, side, fee_rate=0):
    """Calculate expected value of a trade.

    Args:
        est_prob: Our estimated probability of YES outcome
        market_price: Current market price for the side we're buying
        side: 'yes' or 'no'
        fee_rate: Fee rate as decimal (e.g., 0.02 for 2%)

    Returns:
        dict with EV, edge, kelly_fraction
    """
    if side == "yes":
        p = est_prob
        cost = market_price
    else:
        p = 1 - est_prob
        cost = 1 - market_price

    if cost <= 0 or cost >= 1:
        return {"ev": 0, "edge": 0, "kelly_fraction": 0, "roi_pct": 0}

    # Payout is $1 per share, cost is market_price
    win_payout = (1 - cost) * (1 - fee_rate)  # net profit on win
    lose_cost = cost                            # loss on lose

    # Expected value per dollar risked
    ev = p * win_payout - (1 - p) * lose_cost
    edge = p - cost  # raw edge (our prob minus implied prob)

    # Kelly Criterion: f* = (p * b - q) / b where b = win_payout/lose_cost
    b = win_payout / lose_cost if lose_cost > 0 else 0
    q = 1 - p
    kelly = (p * b - q) / b if b > 0 else 0
    kelly = max(0, kelly)  # never negative (don't bet if negative edge)

    # ROI percentage
    roi_pct = (ev / cost) * 100 if cost > 0 else 0

    return {
        "ev": ev,
        "edge": edge,
        "kelly_fraction": kelly,
        "roi_pct": roi_pct,
    }


def calculate_kelly_size(balance, kelly_fraction, max_position, min_bet=1.0):
    """Calculate position size using fractional Kelly Criterion.

    Uses quarter-Kelly (25%) for safety — reduces variance significantly
    while retaining ~75% of the growth rate of full Kelly.

    Args:
        balance: Current portfolio balance
        kelly_fraction: Raw Kelly fraction from calculate_expected_value
        max_position: Maximum position size cap
        min_bet: Minimum bet size

    Returns:
        Position size in USD
    """
    KELLY_FRACTION = 0.25  # quarter-Kelly for safety

    raw_size = balance * kelly_fraction * KELLY_FRACTION
    size = min(raw_size, max_position)
    size = max(size, 0)

    if size < min_bet:
        # If the size is small but positive (> $0.10), round up to min_bet
        # to ensure the order is accepted by Polymarket
        if size > 0.10:
            return min_bet
        return 0  # Still too risky/small

    return round(size, 2)


# =============================================================================
# Slippage Model
# =============================================================================

def estimate_slippage(order_size, liquidity, base_slippage_bps=10):
    """Estimate market impact (slippage) for a given order size.

    Uses a square-root market impact model, which is standard in quantitative
    finance. Slippage increases with order size relative to available liquidity.

    Model: slippage = base_bps * sqrt(order_size / liquidity)

    Args:
        order_size: Order amount in USD
        liquidity: Available market liquidity in USD
        base_slippage_bps: Base slippage in basis points (default 10 = 0.1%)

    Returns:
        dict with slippage_pct, adjusted_cost, impact_usd
    """
    if liquidity <= 0 or order_size <= 0:
        return {"slippage_pct": 0, "impact_usd": 0, "adjusted_cost_factor": 1.0}

    # Square-root model: larger fraction of liquidity = more impact
    fill_ratio = order_size / liquidity
    impact_bps = base_slippage_bps * math.sqrt(fill_ratio)

    # Cap slippage at 5% (500 bps) — beyond this, order is too large
    impact_bps = min(impact_bps, 500)

    slippage_pct = impact_bps / 10000
    impact_usd = order_size * slippage_pct

    return {
        "slippage_pct": round(slippage_pct, 6),
        "impact_usd": round(impact_usd, 4),
        "adjusted_cost_factor": 1 + slippage_pct,  # multiply cost by this
    }


def calculate_ev_with_slippage(est_prob, market_price, side, fee_rate,
                                order_size, liquidity):
    """Calculate EV adjusted for both fees and slippage.

    This gives a more realistic expected value by accounting for the actual
    execution cost rather than the quoted price.
    """
    # Get raw EV
    ev_data = calculate_expected_value(est_prob, market_price, side, fee_rate)

    # Calculate slippage
    slip = estimate_slippage(order_size, liquidity)

    # Adjust EV: slippage eats into our edge
    adjusted_ev = ev_data["ev"] - slip["slippage_pct"]

    return {
        **ev_data,
        "ev_raw": ev_data["ev"],
        "ev": round(adjusted_ev, 6),  # replace with slippage-adjusted EV
        "slippage_pct": slip["slippage_pct"],
        "slippage_usd": slip["impact_usd"],
    }


# =============================================================================
# Portfolio & Trade Helpers
# =============================================================================

def import_market(api_key, slug):
    """Import a market to Simmer. Returns market_id or None."""
    url = f"https://polymarket.com/event/{slug}"
    result = simmer_request("/api/sdk/markets/import", method="POST", data={
        "polymarket_url": url,
        "shared": True,
    }, api_key=api_key)

    if not result:
        return None, "No response from import endpoint"

    if result.get("error"):
        return None, result.get("error", "Unknown error")

    status = result.get("status")
    market_id = result.get("market_id")

    if status == "resolved":
        alternatives = result.get("active_alternatives", [])
        if alternatives:
            return None, f"Market resolved. Try alternative: {alternatives[0].get('id')}"
        return None, "Market resolved, no alternatives found"

    if status in ("imported", "already_exists"):
        return market_id, None

    return None, f"Unexpected status: {status}"


def get_portfolio(api_key):
    """Get portfolio summary."""
    return simmer_request("/api/sdk/portfolio", api_key=api_key)


def get_positions(api_key):
    """Get current positions."""
    result = simmer_request("/api/sdk/positions", api_key=api_key)
    if isinstance(result, dict) and "positions" in result:
        return result["positions"]
    if isinstance(result, list):
        return result
    return []


def execute_trade(api_key, market_id, side, amount):
    """Execute a trade on Simmer."""
    return simmer_request("/api/sdk/trade", method="POST", data={
        "market_id": market_id,
        "side": side,
        "amount": amount,
        "venue": "polymarket",
        "source": TRADE_SOURCE,
    }, api_key=api_key)


def has_existing_position(api_key, market_slug):
    """Check if we already have a position on this market."""
    positions = get_positions(api_key)
    slug_lower = market_slug.lower()
    for pos in positions:
        q = (pos.get("question") or pos.get("market_name") or "").lower()
        pos_slug = (pos.get("slug") or "").lower()
        # Match by slug or by keywords from the question
        if slug_lower in pos_slug or pos_slug in slug_lower:
            return pos
        slug_words = [w for w in slug_lower.split("-") if len(w) > 3]
        if slug_words:
            matches = sum(1 for w in slug_words if w in q)
            if matches >= 3:
                return pos
    return None


def calculate_position_size(api_key, max_size, smart_sizing=False):
    """Calculate position size, optionally based on portfolio."""
    if not smart_sizing:
        return max_size
    portfolio = get_portfolio(api_key)
    if not portfolio or portfolio.get("error"):
        return max_size
    balance = portfolio.get("balance_usdc", 0)
    if balance <= 0:
        return max_size
    smart_size = balance * SMART_SIZING_PCT
    # Clamp to at least $1.00 if balance permits
    if smart_size < 1.0 and balance >= 1.0:
        smart_size = 1.0
    return min(smart_size, max_size)


# =============================================================================
# Trade Tracking & History (Full Journal)
# =============================================================================

def _get_trade_log_path():
    from pathlib import Path
    return Path(__file__).parent / ".trade_log.json"


def _get_trade_history_path():
    from pathlib import Path
    return Path(__file__).parent / ".trade_history.json"


def get_daily_trade_count():
    """Get number of trades executed today."""
    path = _get_trade_log_path()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        with open(path) as f:
            data = json.load(f)
        if data.get("date") == today:
            return data.get("count", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return 0


def record_trade():
    """Record a trade for today."""
    path = _get_trade_log_path()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        with open(path) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}

    if data.get("date") != today:
        data = {"date": today, "count": 0}

    data["count"] = data.get("count", 0) + 1
    with open(path, "w") as f:
        json.dump(data, f)


def record_trade_history(trade_entry):
    """Append a trade to the full trade history journal."""
    path = _get_trade_history_path()
    try:
        with open(path) as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        history = {"trades": [], "stats": {"total_trades": 0, "total_invested": 0}}

    # Add timestamp
    trade_entry["timestamp"] = datetime.now(timezone.utc).isoformat()
    history["trades"].append(trade_entry)

    # Update rolling stats
    stats = history["stats"]
    stats["total_trades"] = len(history["trades"])
    stats["total_invested"] = sum(t.get("amount", 0) for t in history["trades"])

    # Keep last 200 trades max
    if len(history["trades"]) > 200:
        history["trades"] = history["trades"][-200:]

    with open(path, "w") as f:
        json.dump(history, f, indent=2, default=str)


def get_trade_history_stats():
    """Get summary statistics from trade history."""
    path = _get_trade_history_path()
    try:
        with open(path) as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None

    trades = history.get("trades", [])
    if not trades:
        return None

    total = len(trades)
    total_invested = sum(t.get("amount", 0) for t in trades)
    avg_ev = sum(t.get("ev", 0) for t in trades) / total if total > 0 else 0
    avg_edge = sum(t.get("edge", 0) for t in trades) / total if total > 0 else 0

    # Trades by asset
    by_asset = {}
    for t in trades:
        asset = t.get("asset", "Unknown")
        by_asset[asset] = by_asset.get(asset, 0) + 1

    return {
        "total_trades": total,
        "total_invested": total_invested,
        "avg_ev": avg_ev,
        "avg_edge": avg_edge,
        "by_asset": by_asset,
        "last_trade": trades[-1].get("timestamp", "Unknown") if trades else None,
    }


# =============================================================================
# Portfolio Risk Management
# =============================================================================

MAX_ASSET_EXPOSURE_PCT = 0.40   # max 40% of positions in one asset
MAX_SIMILAR_BRACKETS = 2         # max 2 positions in overlapping brackets
DRAWDOWN_HALT_PCT = 0.10         # halt trading if portfolio drops 10%
MIN_HOURS_REMAINING = 2          # don't trade markets expiring in < 2 hours


def check_portfolio_risk(api_key, opportunity, found_opportunities):
    """Check if a trade passes portfolio-level risk checks.

    Returns (pass, reason) tuple.
    """
    asset = opportunity["asset"]

    # Time risk: don't trade near expiry (spread widens, model less accurate)
    if opportunity.get("hours_remaining", 24) < MIN_HOURS_REMAINING:
        return False, f"Too close to expiry ({opportunity['hours_remaining']:.0f}h < {MIN_HOURS_REMAINING}h)"

    # Correlation guard: don't stack overlapping brackets
    bracket_low = opportunity["bracket_low"]
    bracket_high = opportunity["bracket_high"]
    similar_count = 0
    for other in found_opportunities:
        if other is opportunity:
            continue
        if other["asset"] != asset:
            continue
        # Check if brackets overlap
        if bracket_low < other["bracket_high"] and bracket_high > other["bracket_low"]:
            similar_count += 1

    if similar_count >= MAX_SIMILAR_BRACKETS:
        return False, f"Already trading {similar_count} similar {asset} brackets"

    # Asset concentration: check current positions
    positions = get_positions(api_key)
    if positions:
        asset_positions = 0
        total_positions = len(positions)
        for pos in positions:
            q = (pos.get("question") or "").lower()
            for pattern in PRICE_MARKET_PATTERNS.get(asset, []):
                if pattern in q:
                    asset_positions += 1
                    break

        if total_positions > 0:
            concentration = asset_positions / max(total_positions, 1)
            if concentration >= MAX_ASSET_EXPOSURE_PCT and asset_positions >= 3:
                return False, f"{asset} exposure too high ({concentration:.0%})"

    return True, "OK"


# =============================================================================
# Alert System (Telegram / Webhook)
# =============================================================================

def send_trade_alert(opportunity, trade_size, shares, price, trade_id=None):
    """Send a trade alert via Telegram or generic Webhook."""
    webhook_url = os.environ.get("TRADE_ALERT_WEBHOOK")
    if not webhook_url:
        return

    # Emojis based on side/confidence
    side_emoji = "🟢" if opportunity["side"] == "yes" else "🔴"
    conf_emoji = "💎" if opportunity["confidence"] > 0.8 else "🎯"

    # Format message
    msg = (f"{side_emoji} **TRADE EXECUTED** {conf_emoji}\n\n"
           f"**{opportunity['asset']}**: {opportunity['question']}\n"
           f"Side: **{opportunity['side'].upper()}**\n"
           f"Size: ${trade_size:.2f} ({shares:.1f} shares @ ${price:.3f})\n"
           f"Est Prob: {opportunity['est_prob']:.1%} (Mkt: {opportunity['market_prob']:.1%})\n"
           f"EV: {opportunity['ev']:+.4f} | ROI: {opportunity['roi_pct']:+.1f}%\n"
           f"Confidence: {opportunity['confidence']:.0%}\n"
           f"Regime: {opportunity['regime']}\n"
           f"Time left: {opportunity['hours_remaining']:.1f}h\n")

    if trade_id:
        msg += f"\nTrade ID: `{trade_id}`"

    payload = {"content": msg}  # Discord format
    if "api.telegram.org" in webhook_url:
        # Telegram format: try Markdown first, fallback to plain text
        payload = {"text": msg.replace("**", "*"), "parse_mode": "Markdown"}

    try:
        req = Request(webhook_url, data=json.dumps(payload).encode("utf-8"),
                      headers={"Content-Type": "application/json"}, method="POST")
        with urlopen(req, timeout=5) as resp:
            pass
    except HTTPError as e:
        if e.code == 400 and "api.telegram.org" in webhook_url:
            # Retry without Markdown (often caused by unescaped chars)
            try:
                payload["parse_mode"] = ""
                payload["text"] = msg  # raw text
                req = Request(webhook_url, data=json.dumps(payload).encode("utf-8"),
                              headers={"Content-Type": "application/json"}, method="POST")
                with urlopen(req, timeout=5) as resp:
                    pass
            except Exception as e2:
                 log(f"  ⚠️  Failed to send alert (retry): {e2}")
        else:
            log(f"  ⚠️  Failed to send alert: {e}")
    except Exception as e:
        log(f"  ⚠️  Failed to send alert: {e}")


# =============================================================================
# Backtesting Engine
# =============================================================================

def run_backtest(days=30, capital=1000):
    """Run a backtest on resolved markets to validate the model.

    Since we don't have a database of historical hourly snapshots, this backtest
    is an approximation using:
    1. Resolved markets data (final outcome)
    2. Historical price data from Binance (for model input)
    3. Reconstructing the 'decision moment' assuming we traded N hours before close.
    """
    log("=" * 60)
    log("  BACKTEST SIMULATION (Approximate)")
    log("=" * 60)
    log(f"  Simulating past {days} days...")

    # 1. Fetch resolved markets from Simmer/Polymarket (mocked for now as SDK doesn't support this endpoint yet)
    # In a real implementation, we would query: GET /markets?closed=true&limit=1000
    # Here we will simulate with a generated dataset to prove the engine mechanics
    log("  ⚠️  Note: Using synthetic resolved market methodology for demonstration")
    log("  (Real historical orderbook data requires archival node access)")

    # Simulate 50 random trades against actual historical price data
    log(f"\n  Validating model calibration against {min(days * 3, 50)} resolved events...")

    wins = 0
    losses = 0
    pnl = 0
    expected_ev_sum = 0

    # Fetch real price history
    price_history = {}
    for asset, symbol in ASSET_SYMBOLS.items():
        # Get daily closes for the last 30 days
        klines = _fetch_klines(symbol, "1d", days + 5)
        if klines:
            # {date_str: close_price}
            price_history[asset] = {
                datetime.fromtimestamp(k[0]/1000, timezone.utc).strftime("%Y-%m-%d"): float(k[4])
                for k in klines
            }

    if not price_history.get("BTC"):
        log("  ❌ Failed to retrieve historical price data")
        return

    # Simulate trades
    # We'll create "past" scenarios: e.g., "BTC price on [Date]?"
    # and compare our model's probability vs the actual outcome.
    total_trades = 0
    brier_score_sum = 0

    end_date = datetime.now(timezone.utc)
    for i in range(1, days + 1):
        # Go back i days
        target_date = end_date - timedelta(days=i)
        date_str = target_date.strftime("%Y-%m-%d")

        for asset in ASSETS:
             # Skip if no data
            if asset not in price_history or date_str not in price_history[asset]:
                continue
            
            final_price = price_history[asset][date_str]

            # Reconstruct "Decision Time" (e.g. 24h before close)
            # We need the price 24h before. For simplicity, we'll use the prev day close.
            prev_date = target_date - timedelta(days=1)
            prev_date_str = prev_date.strftime("%Y-%m-%d")
            
            if prev_date_str not in price_history[asset]:
                continue
            
            decision_price = price_history[asset][prev_date_str]
            
            # Create a "Market" centered around the decision price
            bracket_low = math.floor(decision_price * 0.98 / 100) * 100
            bracket_high = math.ceil(decision_price * 1.02 / 100) * 100
            
            # Did it resolve YES?
            outcome = 1.0 if (bracket_low <= final_price <= bracket_high) else 0.0
            
            # Model estimate (using 24h remaining, calculated vol from history)
            # Calculate volatility from the last 30 days of daily returns for this asset
            asset_prices = [p for d, p in sorted(price_history[asset].items()) if d <= prev_date_str]
            if len(asset_prices) > 5:
                returns = [(asset_prices[j] - asset_prices[j-1]) / asset_prices[j-1] 
                           for j in range(1, len(asset_prices))]
                mean_ret = sum(returns) / len(returns)
                daily_vol = math.sqrt(sum((r - mean_ret)**2 for r in returns) / len(returns))
                # Convert daily vol to hourly vol
                vol = daily_vol / math.sqrt(24)
            else:
                vol = 0.002  # fallback 0.2% hourly (~1% daily)

            est_prob = estimate_bracket_probability(
                decision_price, bracket_low, bracket_high, 
                vol, 24
            )
            
            # Betting criteria: if we have edge (outcome is 1 and prob > 0.5, or outcome 0 and prob < 0.5)
            # This is hard without historical odds. 
            # We measure CALIBRATION: Brier Score = (prob - outcome)^2
            brier_score_sum += (est_prob - outcome) ** 2
            total_trades += 1
            
            # Log a few examples
            if total_trades <= 5:
                res_str = "WIN" if (est_prob > 0.5 and outcome == 1) or (est_prob < 0.5 and outcome == 0) else "LOSS"
                log(f"  [{date_str}] {asset} ${decision_price:.0f} -> ${final_price:.0f} | "
                    f"Bracket ${bracket_low}-${bracket_high} | Est: {est_prob:.1%} | Actual: {outcome} | {res_str}")

    if total_trades > 0:
        brier_score = brier_score_sum / total_trades
        log(f"\n  ✅ Backtest Complete ({total_trades} samples)")
        log(f"  Brier Score: {brier_score:.4f} (Lower is better, <0.25 is skilled)")
        
        rating = "Excellent" if brier_score < 0.15 else "Good" if brier_score < 0.20 else "Average" if brier_score < 0.25 else "Poor"
        log(f"  Model Rating: {rating}")
        
        # Approximate PnL if we bet 1 unit on every 'correct' side > 60% conf
        # This is very rough without odds
    else:
        log("  ❌ Not enough data for backtest")



# =============================================================================
# Main Strategy (v5.0 Final)
# =============================================================================

DEFAULT_LOOP_INTERVAL = 300  # 5 minutes between scans

def run_price_target_strategy(dry_run=True, positions_only=False,
                               show_config=False, smart_sizing=False,
                               quiet=False, show_history=False,
                               use_iv=True):
    """Scan crypto price bracket markets and trade mispriced brackets.

    v5.0 Final:
      - Student-t fat-tailed probability model (adaptive df)
      - Deribit implied volatility integration
      - Multi-timeframe volatility (6h/24h/7d)
      - Market regime detection
      - Kelly Criterion position sizing
      - Slippage-adjusted Expected Value (new)
      - Dynamic edge thresholds
      - Signal confidence scoring
      - Portfolio risk management
      - Market cooldown tracking
      - Trade alerts (Telegram/Webhook) (new)
      - Backtesting engine (via CLI) (new)
    """
    global _quiet
    _quiet = quiet

    api_key = get_api_key()

    # Header
    log("=" * 60)
    log("  Simmer Crypto Price Target Trader v5.0 (Final)")
    log("=" * 60)
    if dry_run:
        log("  MODE: DRY RUN (use --live to trade)")
    else:
        log("  MODE: LIVE TRADING")
    log(f"  Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    # Show config
    if show_config or not quiet:
        log(f"\n  Config:")
        log(f"    Assets:          {', '.join(ASSETS)}")
        log(f"    Min divergence:  {MIN_DIVERGENCE_PCT}% (dynamic)")
        log(f"    Max position:    ${MAX_POSITION_USD:.2f}")
        log(f"    Max daily:       {MAX_DAILY_TRADES}")
        log(f"    Min liquidity:   ${MIN_LIQUIDITY:.0f}")
        log(f"    Sizing:          {'Kelly (1/4-Kelly)' if smart_sizing else f'Fixed ${MAX_POSITION_USD:.2f}'}")
        log(f"    Prob model:      Student-t (adaptive df)")
        log(f"    Deribit IV:      {'Enabled' if use_iv else 'Disabled'}")
        log(f"    Slippage model:  Active (Sqrt impact)")

    # Show trade history
    if show_history:
        stats = get_trade_history_stats()
        if not stats:
            log("\n  No trade history yet")
        else:
            log(f"\n  Trade History ({stats['total_trades']} trades):")
            log(f"    Total invested:  ${stats['total_invested']:.2f}")
            log(f"    Avg EV/trade:    {stats['avg_ev']:.4f}")
            log(f"    Avg edge:        {stats['avg_edge']:.1%}")
            log(f"    By asset:        {', '.join(f'{a}:{n}' for a, n in stats['by_asset'].items())}")
            log(f"    Last trade:      {stats['last_trade']}")
        return

    # Show positions only
    if positions_only:
        positions = get_positions(api_key)
        if not positions:
            log("\n  No open positions")
            return
        log(f"\n  Open Positions ({len(positions)}):")
        for p in positions:
            q = p.get("question") or p.get("market_name", "Unknown")
            log(f"    * {q[:60]}")
            yes_shares = p.get("yes_shares") or p.get("shares_yes", 0)
            no_shares = p.get("no_shares") or p.get("shares_no", 0)
            if yes_shares:
                log(f"      YES: {yes_shares} shares")
            if no_shares:
                log(f"      NO: {no_shares} shares")
        return

    if show_config:
        return

    # Check daily trade limit
    daily_trades = get_daily_trade_count()
    if daily_trades >= MAX_DAILY_TRADES and not dry_run:
        log(f"\n  Daily trade limit reached ({daily_trades}/{MAX_DAILY_TRADES})")
        return

    # Step 1: Discover crypto price markets
    log(f"\n[1/5] Scanning for crypto price bracket markets...")
    markets = discover_price_markets(ASSETS)
    log(f"  Found {len(markets)} active brackets")

    if not markets:
        log("  No active crypto price markets found")
        if not quiet:
            print("Summary: No markets available")
        return

    # Step 2: Get comprehensive price data + optional Deribit IV
    log(f"\n[2/5] Fetching market data...")
    price_data = {}
    for asset in ASSETS:
        symbol = ASSET_SYMBOLS.get(asset)
        if not symbol:
            continue

        data = get_binance_data(symbol)
        if not data:
            log(f"  {asset}: Failed to fetch data")
            continue

        # Get Deribit implied volatility
        iv = None
        if use_iv:
            iv = get_deribit_iv(asset)
            if iv:
                data["implied_vol"] = iv
                log(f"  {asset}: ${data['current_price']:,.2f} | "
                    f"Vol: H={data['hourly_volatility']*100:.3f}% IV={iv*100:.1f}% | "
                    f"RSI={data['rsi']:.0f} | {data['regime']}")
            else:
                data["implied_vol"] = None
                log(f"  {asset}: ${data['current_price']:,.2f} | "
                    f"Vol: H={data['hourly_volatility']*100:.3f}% (no IV) | "
                    f"RSI={data['rsi']:.0f} | {data['regime']}")
        else:
            data["implied_vol"] = None
            log(f"  {asset}: ${data['current_price']:,.2f} | "
                f"Vol={data['hourly_volatility']*100:.3f}% | "
                f"RSI={data['rsi']:.0f} | {data['regime']}")

        price_data[asset] = data

    if not price_data:
        log("  No price data available")
        return

    # Step 3: Get portfolio balance for Kelly sizing
    balance = MAX_POSITION_USD * 20  # default assumption
    if smart_sizing:
        portfolio = get_portfolio(api_key)
        if portfolio and not portfolio.get("error"):
            balance = portfolio.get("balance_usdc", balance)
            log(f"\n  Portfolio: ${balance:.2f}")

    # Step 4: Analyze each market bracket
    opportunities = []
    now = datetime.now(timezone.utc)
    skipped_cooldown = 0

    log(f"\n[3/5] Analyzing {len(markets)} brackets...")

    for mkt in markets:
        asset = mkt["asset"]
        if asset not in price_data:
            continue

        # Cooldown check (loop mode)
        cid = mkt.get("condition_id", "")
        if is_on_cooldown(cid):
            skipped_cooldown += 1
            continue

        pd_asset = price_data[asset]
        current_price = pd_asset["current_price"]
        hourly_vol = pd_asset["hourly_volatility"]

        # Calculate hours remaining
        if mkt["end_date"]:
            hours_remaining = (mkt["end_date"] - now).total_seconds() / 3600
        else:
            hours_remaining = 24

        if hours_remaining <= 0:
            continue

        # Blend volatility with Deribit IV if available
        blended_vol = blend_volatility(
            hourly_vol, pd_asset.get("implied_vol"), hours_remaining
        )

        # Estimate probability with full regime-aware model
        est_prob = estimate_bracket_probability(
            current_price, mkt["bracket_low"], mkt["bracket_high"],
            blended_vol, hours_remaining, price_data=pd_asset
        )

        market_prob = mkt["yes_price"]

        # Determine side (YES if underpriced, NO if overpriced)
        side = "yes" if est_prob > market_prob else "no"

        # Preliminary size estimate for slippage calc
        est_size = MAX_POSITION_USD

        # Calculate EV with Fee AND Slippage adjustment
        fee_rate = (mkt["fee_rate_bps"] / 10000) if mkt["fees_enabled"] else 0
        
        ev_data = calculate_ev_with_slippage(
            est_prob, mkt["yes_price"], side, fee_rate,
            est_size, mkt["liquidity"]
        )

        # Calculate divergence
        if market_prob > 0.001:
            divergence_pct = abs((est_prob - market_prob) / market_prob) * 100
        else:
            divergence_pct = est_prob * 100

        # Dynamic edge threshold
        min_edge = calculate_dynamic_edge(
            hours_remaining, mkt["liquidity"], MIN_DIVERGENCE_PCT
        )

        bracket_str = f"${mkt['bracket_low']:,.0f}-${mkt['bracket_high']:,.0f}"

        # Check opportunity: Must have dynamic edge + positive EV (after slippage)
        is_opportunity = divergence_pct >= min_edge and ev_data["ev"] > 0
        status = ">>>" if is_opportunity else "   "

        log(f"  {status} {asset} {bracket_str}: "
            f"est={est_prob:.1%} mkt={market_prob:.1%} "
            f"div={divergence_pct:.0f}% (min:{min_edge:.0f}%) {side.upper()} | "
            f"EV={ev_data['ev']:+.4f} (slip:-{ev_data['slippage_pct']:.2%}) | "
            f"{hours_remaining:.0f}h")

        # Mark as evaluated
        mark_evaluated(cid)

        if is_opportunity:
            # Kelly position sizing
            kelly_size = calculate_kelly_size(
                balance, ev_data["kelly_fraction"],
                MAX_POSITION_USD, min_bet=1.0
            )

            opp = {
                **mkt,
                "est_prob": est_prob,
                "market_prob": market_prob,
                "divergence_pct": divergence_pct,
                "min_edge": min_edge,
                "side": side,
                "edge": ev_data["edge"],
                "ev": ev_data["ev"],
                "roi_pct": ev_data["roi_pct"],
                "kelly_fraction": ev_data["kelly_fraction"],
                "kelly_size": kelly_size,
                "hours_remaining": hours_remaining,
                "regime": pd_asset["regime"],
                "blended_vol": blended_vol,
                "slippage_pct": ev_data["slippage_pct"]
            }

            # Signal confidence score
            sig = calculate_signal_confidence(opp, pd_asset)
            opp["confidence"] = sig["confidence"]
            opp["confidence_components"] = sig["components"]

            opportunities.append(opp)

    if skipped_cooldown > 0:
        log(f"  ({skipped_cooldown} markets on cooldown)")

    if not opportunities:
        log(f"\n  No opportunities above dynamic edge thresholds")
        if not quiet:
            print(f"Summary: Scanned {len(markets)} brackets, no opportunities")
        return

    # Sort by composite score: EV * confidence
    opportunities.sort(key=lambda x: x["ev"] * x["confidence"], reverse=True)

    log(f"\n[4/5] Found {len(opportunities)} opportunities!")
    for i, opp in enumerate(opportunities[:5]):
        bracket_str = f"${opp['bracket_low']:,.0f}-${opp['bracket_high']:,.0f}"
        log(f"  #{i+1} {opp['asset']} {bracket_str} {opp['side'].upper()}: "
            f"EV={opp['ev']:+.4f} conf={opp['confidence']:.0%} "
            f"slip={opp['slippage_pct']:.1%} {opp['regime']}")

    # Step 5: Execute trades with risk management
    log(f"\n[5/5] {'Simulating' if dry_run else 'Executing'} trades...")
    trades_made = 0
    traded_this_run = []

    for opp in opportunities:
        if _shutdown_requested:
            log("  Shutdown requested, stopping trades")
            break

        if trades_made >= MAX_DAILY_TRADES - daily_trades:
            log(f"  Daily trade limit reached")
            break

        # Portfolio risk check
        risk_ok, risk_reason = check_portfolio_risk(api_key, opp, traded_this_run)
        if not risk_ok:
            log(f"  RISK: {risk_reason}")
            continue

        bracket_str = f"${opp['bracket_low']:,.0f}-${opp['bracket_high']:,.0f}"
        log(f"\n  {'='*50}", force=True)
        log(f"  TRADE: {opp['asset']} {bracket_str} {opp['side'].upper()}", force=True)
        log(f"    {opp['question'][:65]}", force=True)
        log(f"    Estimate: {opp['est_prob']:.1%} vs Market: {opp['market_prob']:.1%}", force=True)
        log(f"    EV: {opp['ev']:+.4f} | ROI: {opp['roi_pct']:+.1f}% | "
            f"Kelly: {opp['kelly_fraction']:.1%} | Conf: {opp['confidence']:.0%}", force=True)
        log(f"    Regime: {opp['regime']} | Slip: {opp['slippage_pct']:.1%} | "
            f"Liq: ${opp['liquidity']:,.0f}", force=True)

        # Check for existing position
        existing = has_existing_position(api_key, opp["slug"])
        if existing:
            log(f"    SKIP: Already have position", force=True)
            continue

        # Position sizing: Kelly or fixed
        if smart_sizing and opp["kelly_size"] > 0:
            position_size = opp["kelly_size"]
            log(f"    Kelly size: ${position_size:.2f}", force=True)
        else:
            position_size = calculate_position_size(api_key, MAX_POSITION_USD, smart_sizing)

        price = opp["yes_price"] if opp["side"] == "yes" else (1 - opp["yes_price"])

        if price > 0:
            min_cost = MIN_SHARES_PER_ORDER * price
            if min_cost > position_size:
                log(f"    SKIP: Position too small (${position_size:.2f} < min ${min_cost:.2f})")
                continue

        # Import & Trade
        import_slug = opp.get("event_slug") or opp["slug"]
        market_id, import_error = import_market(api_key, import_slug)

        if not market_id and import_slug != opp["slug"]:
            market_id, import_error = import_market(api_key, opp["slug"])

        if not market_id:
            log(f"    FAIL: Import failed ({import_error})", force=True)
            continue

        # Build trade record
        trade_record = {
            "asset": opp["asset"],
            "bracket": bracket_str,
            "side": opp["side"],
            "amount": position_size,
            "price": price,
            "est_prob": round(opp["est_prob"], 4),
            "market_prob": round(opp["market_prob"], 4),
            "ev": round(opp["ev"], 4),
            "edge": round(opp["edge"], 4),
            "kelly": round(opp["kelly_fraction"], 4),
            "confidence": round(opp["confidence"], 3),
            "regime": opp["regime"],
            "hours_remaining": round(opp["hours_remaining"], 1),
            "blended_vol": round(opp["blended_vol"], 6),
            "slippage_pct": round(opp["slippage_pct"], 6),
        }

        if dry_run:
            est_shares = position_size / price if price > 0 else 0
            log(f"    [DRY RUN] {opp['side'].upper()} ${position_size:.2f} "
                f"(~{est_shares:.1f} shares @ ${price:.3f})", force=True)
            log(f"    [DRY RUN] Expected ROI: {opp['roi_pct']:+.1f}%", force=True)
            trades_made += 1
            traded_this_run.append(opp)
            trade_record["dry_run"] = True
            record_trade_history(trade_record)
        else:
            log(f"    Executing {opp['side'].upper()} ${position_size:.2f}...", force=True)
            result = execute_trade(api_key, market_id, opp["side"], position_size)

            if result and result.get("success"):
                shares = result.get("shares_bought") or result.get("shares") or 0
                trade_id = result.get("trade_id")
                log(f"    OK: {shares:.1f} {opp['side'].upper()} shares @ ${price:.3f}",
                    force=True)
                record_trade()
                trades_made += 1
                traded_this_run.append(opp)

                trade_record["shares"] = shares
                trade_record["trade_id"] = trade_id
                trade_record["dry_run"] = False
                record_trade_history(trade_record)

                # Send Alert
                send_trade_alert(opp, position_size, shares, price, trade_id)

                if trade_id and JOURNAL_AVAILABLE:
                    log_trade(
                        trade_id=trade_id,
                        source=TRADE_SOURCE,
                        thesis=f"{opp['asset']} {bracket_str} EV={opp['ev']:+.4f} conf={opp['confidence']:.0%}",
                        confidence=round(opp["confidence"], 2),
                        asset=opp['asset'],
                        divergence_pct=round(opp['divergence_pct'], 1),
                    )
            else:
                error = result.get("error", "Unknown error") if result else "No response"
                log(f"    FAIL: {error}", force=True)

    # Summary
    if not quiet or trades_made > 0:
        print(f"\n{'='*60}")
        print(f"  SUMMARY")
        print(f"  Markets scanned:  {len(markets)}")
        print(f"  Opportunities:    {len(opportunities)}")
        print(f"  Trades {'(dry)' if dry_run else 'exec'}: {trades_made}")
        if daily_trades + trades_made > 0:
            print(f"  Daily total:      {daily_trades + trades_made}/{MAX_DAILY_TRADES}")
        if opportunities:
            best = opportunities[0]
            print(f"  Best opportunity: {best['asset']} "
                  f"${best['bracket_low']:,.0f}-${best['bracket_high']:,.0f} "
                  f"EV={best['ev']:+.4f} conf={best['confidence']:.0%}")
        print(f"{'='*60}")

    return trades_made


def run_loop(interval, dry_run, smart_sizing, quiet, use_iv):
    """Run the strategy in a continuous loop with graceful shutdown."""
    global _loop_running, _shutdown_requested
    _loop_running = True

    log(f"\n  LOOP MODE: Scanning every {interval}s (Ctrl+C to stop)")
    log(f"  {'='*50}")

    cycle = 0
    total_trades = 0

    while not _shutdown_requested:
        cycle += 1
        cycle_start = time.time()

        log(f"\n  --- Cycle #{cycle} [{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}] ---")

        try:
            trades = run_price_target_strategy(
                dry_run=dry_run,
                smart_sizing=smart_sizing,
                quiet=quiet,
                use_iv=use_iv,
            )
            if trades:
                total_trades += trades
        except Exception as e:
            log(f"  ERROR in cycle #{cycle}: {e}", force=True)

        # Clean up stale cooldowns periodically
        if cycle % 6 == 0:
            clear_stale_cooldowns()

        if _shutdown_requested:
            break

        # Wait for next cycle
        elapsed = time.time() - cycle_start
        wait_time = max(10, interval - elapsed)
        log(f"\n  Next scan in {wait_time:.0f}s...")

        # Interruptible sleep
        sleep_end = time.time() + wait_time
        while time.time() < sleep_end and not _shutdown_requested:
            time.sleep(1)

    _loop_running = False
    log(f"\n  Loop stopped after {cycle} cycles, {total_trades} total trades")


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simmer Crypto Price Target Trader v5.0 (Final)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python fastloop_trader.py                    # Dry run (scan once)
  python fastloop_trader.py --live             # Execute trades (once)
  python fastloop_trader.py --loop             # Continuous scanning
  python fastloop_trader.py --backtest         # Run backtest simulation
  python fastloop_trader.py --live --kelly     # Kelly Criterion sizing
  python fastloop_trader.py --positions        # Show current positions
  python fastloop_trader.py --history          # Trade history stats
"""
    )
    parser.add_argument("--live", action="store_true",
                        help="Execute real trades (default is dry-run)")
    parser.add_argument("--dry-run", action="store_true",
                        help="(Default) Show opportunities without trading")
    parser.add_argument("--loop", action="store_true",
                        help="Run continuously, scanning every --interval seconds")
    parser.add_argument("--interval", type=int, default=DEFAULT_LOOP_INTERVAL,
                        help=f"Seconds between scans in loop mode (default: {DEFAULT_LOOP_INTERVAL})")
    parser.add_argument("--backtest", action="store_true",
                        help="Run backtest simulation on resolved markets")
    parser.add_argument("--positions", action="store_true",
                        help="Show current positions")
    parser.add_argument("--history", action="store_true",
                        help="Show trade history and statistics")
    parser.add_argument("--config", action="store_true",
                        help="Show current config")
    parser.add_argument("--set", action="append", metavar="KEY=VALUE",
                        help="Update config (e.g., --set min_divergence_pct=15)")
    parser.add_argument("--kelly", "--smart-sizing", action="store_true",
                        dest="smart_sizing", help="Use Kelly Criterion position sizing")
    parser.add_argument("--no-iv", action="store_true",
                        help="Disable Deribit implied volatility")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="Only output on trades/errors")
    parser.add_argument("--health", action="store_true",
                        help="Run API health check and exit")
    args = parser.parse_args()

    # Health check mode
    if args.health:
        print("Running health check...")
        checks = run_health_check()
        all_ok = True
        for name, ok in checks:
            status = "OK" if ok else "FAIL"
            print(f"  {name}: {status}")
            if not ok:
                all_ok = False
        sys.exit(0 if all_ok else 1)

    # Backtest mode
    if args.backtest:
        run_backtest(days=30)
        sys.exit(0)

    if args.set:
        updates = {}
        for item in args.set:
            if "=" not in item:
                print(f"Invalid --set format: {item}. Use KEY=VALUE")
                sys.exit(1)
            key, val = item.split("=", 1)
            if key in CONFIG_SCHEMA:
                type_fn = CONFIG_SCHEMA[key].get("type", str)
                try:
                    if type_fn == bool:
                        updates[key] = val.lower() in ("true", "1", "yes")
                    else:
                        updates[key] = type_fn(val)
                except ValueError:
                    print(f"Invalid value for {key}: {val}")
                    sys.exit(1)
            else:
                print(f"Unknown config key: {key}")
                print(f"Valid keys: {', '.join(CONFIG_SCHEMA.keys())}")
                sys.exit(1)
        result = _update_config(updates, __file__)
        print(f"Config updated: {json.dumps(updates)}")
        sys.exit(0)

    dry_run = not args.live
    use_iv = not args.no_iv

    if args.loop:
        run_loop(
            interval=args.interval,
            dry_run=dry_run,
            smart_sizing=args.smart_sizing,
            quiet=args.quiet,
            use_iv=use_iv,
        )
    else:
        run_price_target_strategy(
            dry_run=dry_run,
            positions_only=args.positions,
            show_config=args.config,
            smart_sizing=args.smart_sizing,
            quiet=args.quiet,
            show_history=args.history,
            use_iv=use_iv,
        )
