import sys
import time
import json
from urllib.request import urlopen

def get_chat_id(token):
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    print(f"Checking for messages on bot: {token[:10]}...")
    
    try:
        with urlopen(url) as response:
            data = json.loads(response.read().decode())
            
        if not data.get("ok"):
            print("Error: Invalid token or API error.")
            return

        results = data.get("result", [])
        if not results:
            print("\n❌ No messages found!")
            print("👉 Please send a message (e.g., 'hello') to your bot on Telegram and run this script again.")
            return

        chat = results[-1]["message"]["chat"]
        chat_id = chat["id"]
        username = chat.get("username", "Unknown")
        
        print(f"\n✅ Success! Found chat with @{username}")
        print(f"Your Chat ID is: {chat_id}")
        print("\nYour Webhook URL is:")
        print(f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python get_telegram_id.py <YOUR_BOT_TOKEN>")
        sys.exit(1)
    
    get_chat_id(sys.argv[1])
