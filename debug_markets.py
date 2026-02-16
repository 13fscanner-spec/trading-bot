import os
import json
import urllib.request
import urllib.error

# Use the key from environment or a placeholder
API_KEY = os.environ.get("SIMMER_API_KEY", "")

def debug_markets():
    print(f"DEBUG: Using API Key: {'***' + API_KEY[-4:] if API_KEY else 'NONE'}")
    
    url = "https://clob.polymarket.com/markets" # Trying direct Polymarket CLOB if Simmer fails, or Simmer?
    # The bot uses Simmer. let's check Simmer.
    
    # Scan 500 markets from Gamma
    print(f"DEBUG: Scanning 500 markets for crypto keywords...")
    found_any = False
    
    for offset in range(0, 500, 100):
        gamma_url = f"https://gamma-api.polymarket.com/markets?limit=100&active=true&closed=false&order=volume24hr&ascending=false&offset={offset}"
        req = urllib.request.Request(gamma_url)
        req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode())
                if isinstance(data, list):
                    for m in data:
                        q = m.get("question", "").lower()
                        if "bitcoin" in q or "ethereum" in q or "solana" in q or "btc" in q or "eth" in q or "sol" in q:
                            print(f"MATCH: {m.get('question')}")
                            print(f"  End: {m.get('endDate')} | Liq: {m.get('liquidity')} | Vol: {m.get('volume24hr')}")
                            print(f"  Active: {m.get('active')} | Closed: {m.get('closed')}")
                            found_any = True
        except Exception as e:
            print(f"Error fetching offset {offset}: {e}")
            
    if not found_any:
        print("DEBUG: No crypto markets found in top 500 by volume.")


if __name__ == "__main__":
    debug_markets()
