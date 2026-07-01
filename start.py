import threading
import sys
import time
import os

def start_bot():
    """Start Telegram bot in a background thread."""
    time.sleep(3)  # Wait for Flask backend to initialize
    print("🤖 Starting Telegram Bot...")
    from telegram_bot import init_bot, Update
    app = init_bot()
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)

def start_services():
    print("🚀 Starting Library System Services...")

    # Start Telegram Bot in background thread
    bot_thread = threading.Thread(target=start_bot, daemon=True)
    bot_thread.start()

    # Start Flask Backend in main thread (binds to PORT)
    print("🧠 Starting Backend (brain.py)...")
    from brain import app, init_db, DB_PATH, IMAGES_DIR
    import logging

    logger = logging.getLogger(__name__)

    init_db()

    if not os.path.exists(IMAGES_DIR):
        os.makedirs(IMAGES_DIR)
        logger.info(f"Created images directory: {IMAGES_DIR}")

    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Python Brain Backend starting on port {port}...")
    app.run(host='0.0.0.0', port=port, debug=False)

if __name__ == "__main__":
    start_services()
