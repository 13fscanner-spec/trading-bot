import os
from fastloop_trader import send_trade_alert

# Set the webhook URL provided by the user
webhook = os.environ.get("TRADE_ALERT_WEBHOOK")
if not webhook:
    print("❌ Error: TRADE_ALERT_WEBHOOK environment variable not set.")
    print("Usage: $env:TRADE_ALERT_WEBHOOK='your_url'; python test_alert.py")
    exit(1)

print("Sending test alert to Telegram...")

# Mock opportunity
opp = {
    "asset": "BTC",
    "question": "TEST ALERT: Will BTC be > $100k?",
    "side": "yes",
    "est_prob": 0.85,
    "market_prob": 0.60,
    "ev": 0.25,
    "roi_pct": 41.6,
    "confidence": 0.95,
    "regime": "trending_up",
    "hours_remaining": 12.5,
    "slug": "test-slug"
}

try:
    send_trade_alert(opp, trade_size=10.0, shares=15.5, price=0.60, trade_id="TEST-1234")
    print("\n✅ Alert sent! Check your Telegram.")
except Exception as e:
    print(f"\n❌ Failed: {e}")
