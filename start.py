import subprocess
import sys
import time
import os

def start_services():
    print("🚀 Starting Library System Services...")
    
    # Start Flask Backend (brain.py)
    print("🧠 Starting Backend (brain.py)...")
    backend_process = subprocess.Popen([sys.executable, "brain.py"])
    
    # Wait a moment for the backend to initialize
    time.sleep(3)
    
    # Start Telegram Bot (telegram_bot.py)
    print("🤖 Starting Telegram Bot (telegram_bot.py)...")
    bot_process = subprocess.Popen([sys.executable, "telegram_bot.py"])
    
    try:
        # Keep the script running while processes are alive
        while True:
            if backend_process.poll() is not None:
                print("❌ Backend process stopped unexpectedly.")
                break
            if bot_process.poll() is not None:
                print("❌ Bot process stopped unexpectedly.")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping services...")
        backend_process.terminate()
        bot_process.terminate()
        print("✅ Services stopped.")

if __name__ == "__main__":
    start_services()
