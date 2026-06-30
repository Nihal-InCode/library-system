"""
Shared Telegram utilities for the Islamic Library system.
Centralizes bot token management and message sending functions.
"""

import os
import requests
from typing import Optional, List, Union
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()


def get_bot_token() -> str:
    """Get bot token from environment variable."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable is not set")
    return token


def get_admin_chat_id() -> int:
    """Get admin chat ID from environment variable."""
    chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "0")
    return int(chat_id)


def send_message(
    chat_id: Union[int, str],
    text: str,
    parse_mode: str = "HTML",
    token: Optional[str] = None,
) -> bool:
    """Send a text message via Telegram Bot API."""
    if token is None:
        token = get_bot_token()
    if not chat_id:
        print("Warning: Telegram chat_id not configured")
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
        response = requests.post(url, json=payload, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"Telegram send error: {e}")
        return False


def send_photo(
    chat_id: Union[int, str],
    photo_path: str,
    caption: Optional[str] = None,
    parse_mode: str = "HTML",
    token: Optional[str] = None,
) -> bool:
    """Send a photo with optional caption via Telegram Bot API."""
    if token is None:
        token = get_bot_token()
    if not chat_id:
        print("Warning: Telegram chat_id not configured")
        return False
    if not os.path.exists(photo_path):
        print(f"Warning: Photo not found at {photo_path}")
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        with open(photo_path, "rb") as f:
            files = {"photo": f}
            data = {"chat_id": chat_id, "parse_mode": parse_mode}
            if caption:
                data["caption"] = caption
            response = requests.post(url, data=data, files=files, timeout=30)
        return response.status_code == 200
    except Exception as e:
        print(f"Telegram send photo error: {e}")
        return False


def send_message_to_multiple(
    chat_ids: List[Union[int, str]],
    text: str,
    parse_mode: str = "HTML",
    token: Optional[str] = None,
) -> dict:
    """Send a message to multiple chat IDs. Returns dict with success count and errors."""
    if token is None:
        token = get_bot_token()
    success_count = 0
    errors = []
    for chat_id in chat_ids:
        if not chat_id:
            continue
        if send_message(chat_id, text, parse_mode, token):
            success_count += 1
        else:
            errors.append(f"Chat {chat_id}: Failed to send")
    return {"success": success_count, "errors": errors}


def send_photo_to_multiple(
    chat_ids: List[Union[int, str]],
    photo_path: str,
    caption: Optional[str] = None,
    parse_mode: str = "HTML",
    token: Optional[str] = None,
) -> dict:
    """Send a photo to multiple chat IDs. Returns dict with success count and errors."""
    if token is None:
        token = get_bot_token()
    success_count = 0
    errors = []
    for chat_id in chat_ids:
        if not chat_id:
            continue
        if send_photo(chat_id, photo_path, caption, parse_mode, token):
            success_count += 1
        else:
            errors.append(f"Chat {chat_id}: Failed to send photo")
    return {"success": success_count, "errors": errors}