import threading
import sys
import time
import os

def start_flask():
    """Start Flask backend in a background thread."""
    from brain import app, init_db, IMAGES_DIR, restore_db_from_github
    import logging

    logger = logging.getLogger(__name__)

    # Restore DB from GitHub Releases if missing (Railway ephemeral storage)
    restore_db_from_github()

    init_db()

    if not os.path.exists(IMAGES_DIR):
        os.makedirs(IMAGES_DIR)
        logger.info(f"Created images directory: {IMAGES_DIR}")

    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Python Brain Backend starting on port {port}...")
    app.run(host='0.0.0.0', port=port, debug=False)

def start_services():
    print("🚀 Starting Library System Services...")

    # Start Flask Backend in background thread
    flask_thread = threading.Thread(target=start_flask, daemon=True)
    flask_thread.start()

    # Wait for Flask to initialize
    time.sleep(3)

    # Start Telegram Bot in main thread (required for signal handlers)
    print("🤖 Starting Telegram Bot...")
    from telegram_bot import init_bot, Update
    app = init_bot()
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    start_services()
