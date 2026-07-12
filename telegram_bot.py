import os
import asyncio
import json
import logging
import threading
from datetime import datetime
from typing import Set, Dict, Any
import httpx
import base64
import sqlite3
import shutil

# For python-telegram-bot v20+
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    CallbackQueryHandler,
)
from telegram.constants import ChatAction

# Import shared Telegram utilities
from telegram_utils import get_bot_token, get_admin_chat_id

# DB backup/restore (from brain module)
from brain import backup_db_to_github

# --- CONFIGURATION ---
TOKEN = get_bot_token()
ADMIN_CHAT_ID = get_admin_chat_id()
PORT = int(os.environ.get("PORT", "8080"))
API_BASE = os.environ.get("API_BASE", f"http://127.0.0.1:{PORT}")
DB_PATH = "islamic_library.db"
APPROVED_USERS_FILE = "approved_users.txt"

# --- PERSISTED APPROVALS (file-based, survives DB replacements) ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO = "Nihal-InCode/library-system"
GITHUB_BRANCH = "main"

def _load_approved_users() -> Set[int]:
    """Load approved user IDs from file."""
    users = set()
    try:
        if os.path.exists(APPROVED_USERS_FILE):
            with open(APPROVED_USERS_FILE, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and line.isdigit():
                        users.add(int(line))
    except Exception:
        pass
    return users

def _push_approved_users_to_github():
    """Push approved_users.txt to GitHub so it survives Railway redeploys."""
    if not GITHUB_TOKEN:
        logger.warning("GITHUB_TOKEN not set — approved users won't persist across deploys")
        return
    try:
        import requests as req

        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        api_base = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{APPROVED_USERS_FILE}"

        # Get current file SHA (needed for updates)
        sha = None
        try:
            r = req.get(api_base, headers=headers, timeout=10)
            if r.status_code == 200:
                sha = r.json().get("sha")
        except Exception:
            pass

        # Read file content
        with open(APPROVED_USERS_FILE, "r") as f:
            content = f.read()
        import base64
        encoded = base64.b64encode(content.encode()).decode()

        # Create or update file
        payload = {
            "message": "chore: update approved users",
            "content": encoded,
            "branch": GITHUB_BRANCH
        }
        if sha:
            payload["sha"] = sha

        r = req.put(api_base, json=payload, headers=headers, timeout=15)
        if r.status_code in (200, 201):
            logger.info("Approved users pushed to GitHub successfully")
        else:
            logger.error(f"GitHub push failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        logger.error(f"Failed to push approved users to GitHub: {e}")

def _save_approved_users():
    """Save approved user IDs to file AND push to GitHub."""
    try:
        with open(APPROVED_USERS_FILE, "w") as f:
            for uid in APPROVED_USERS:
                f.write(f"{uid}\n")
    except Exception as e:
        logger.error(f"Failed to save approved users: {e}")
        return

    # Push to GitHub synchronously so it completes before deploy kills the process
    _push_approved_users_to_github()

APPROVED_USERS: Set[int] = {ADMIN_CHAT_ID}  # Admin is always approved
APPROVED_USERS.update(_load_approved_users())  # Load persisted approvals

# --- LOGGING ---
class InMemoryLogHandler(logging.Handler):
    def __init__(self, capacity=200):
        super().__init__()
        self.capacity = capacity
        self.logs = []

    def emit(self, record):
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name
        }
        self.logs.append(log_entry)
        if len(self.logs) > self.capacity:
            self.logs.pop(0)

    def get_logs(self, level=None):
        if level:
            return [l for l in self.logs if l["level"] == level]
        return self.logs

in_memory_logs = InMemoryLogHandler()
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(), in_memory_logs]
)
logger = logging.getLogger(__name__)

# --- MANUAL STATE MANAGEMENT ---
USER_STATES = {}  # {user_id: state_name}
SEARCH_CONTEXT = {}  # {user_id: {"term": str, "page": int}}
USER_PAGINATION_CONTEXT = {}  # {user_id: {"page": int}}
ANALYTICS_CONTEXT = {}  # {user_id: {"type": str, "page": int}}
# --- CLEANUP CONFIGURATION ---
QUICK_DELETE_SECONDS = 5
LONG_DELETE_SECONDS = 300
VANISH_ANIMATION_SECONDS = 2
MENU_TAP_DELETE_DELAY = QUICK_DELETE_SECONDS
PROMPT_TTL_SECONDS = 45
INPUT_DELETE_DELAY = QUICK_DELETE_SECONDS
RESULT_TTL_SECONDS = LONG_DELETE_SECONDS

CLEANUP_CONTEXT = {}  # {user_id: {"menu_tap_id": int, "prompt_id": int, "last_result_id": int}}
DELETION_TASKS = {}  # {(chat_id, message_id): asyncio.Task}
LAST_BOT_MESSAGES = {}  # {user_id: message_id} (Tracks last menu/prompt for replacement)
COUNTDOWN_TASKS = {}  # {(chat_id, message_id): asyncio.Task}
ORIGINAL_TEXTS = {}  # {(chat_id, message_id): str}
PAGE_SIZE = 5
ANALYTICS_PAGE_SIZE = 10

# State constants
CHOOSING = "CHOOSING"
SEARCHING_BOOK = "SEARCHING_BOOK"
CHECKING_STATUS = "CHECKING_STATUS"
STUDENT_DETAILS = "STUDENT_DETAILS"
ISSUE_HISTORY = "ISSUE_HISTORY"
ADMIN_DASHBOARD = "ADMIN_DASHBOARD"
ADMIN_RESET_USER = "ADMIN_RESET_USER"
ADMIN_USER_HISTORY = "ADMIN_USER_HISTORY"
BROWSING_PRESENTATIONS = "BROWSING_PRESENTATIONS"

# --- STATE HELPERS ---
def get_user_state(user_id: int) -> str:
    """Get current state for user, default to CHOOSING."""
    return USER_STATES.get(user_id, CHOOSING)

def set_user_state(user_id: int, state: str):
    """Set state for user."""
    USER_STATES[user_id] = state

def clear_user_state(user_id: int):
    """Clear state for user."""
    USER_STATES.pop(user_id, None)

def ensure_user_context(user_id: int):
    """Ensures the user has a valid entry in CLEANUP_CONTEXT with all keys."""
    if user_id not in CLEANUP_CONTEXT:
        CLEANUP_CONTEXT[user_id] = {}
    
    # Default structure
    defaults = {
        "menu_tap_id": None,
        "prompt_id": None,
        "last_result_id": None,
        "tasks": {}
    }
    
    for key, value in defaults.items():
        if key not in CLEANUP_CONTEXT[user_id]:
            CLEANUP_CONTEXT[user_id][key] = value

def store_menu_tap(user_id: int, message_id: int):
    """Stores the message ID of a menu button tap."""
    ensure_user_context(user_id)
    CLEANUP_CONTEXT[user_id]["menu_tap_id"] = message_id

def store_prompt(user_id: int, message_id: int):
    """Stores the message ID of a bot prompt."""
    ensure_user_context(user_id)
    CLEANUP_CONTEXT[user_id]["prompt_id"] = message_id

async def run_clean_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Schedules deletion for stored menu tap, prompt, and current user input messages."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if user_id in CLEANUP_CONTEXT:
        data = CLEANUP_CONTEXT[user_id]
        if data.get("menu_tap_id"):
            await schedule_delete(context, chat_id, data["menu_tap_id"], QUICK_DELETE_SECONDS, show_vanish=True)
            data["menu_tap_id"] = None
        if data.get("prompt_id"):
            await schedule_delete(context, chat_id, data["prompt_id"], QUICK_DELETE_SECONDS, show_vanish=True)
            data["prompt_id"] = None
            
    # Also delete the current user input message after a short delay
    if update.message:
        # User messages cannot be edited, so show_vanish=False
        await schedule_delete(context, chat_id, update.message.message_id, QUICK_DELETE_SECONDS, show_vanish=False)

# --- MESSAGE CLEANUP HELPERS ---

# --- MESSAGE CLEANUP HELPERS ---

async def _perform_scheduled_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int, show_vanish: bool):
    """Internal coroutine to handle delay, animation, and deletion."""
    try:
        # 0. Cancel any running countdown for this message
        old_countdown = COUNTDOWN_TASKS.pop((chat_id, message_id), None)
        if old_countdown:
            old_countdown.cancel()
        ORIGINAL_TEXTS.pop((chat_id, message_id), None)

        # 1. Wait for the visible period (minus animation time)
        wait_time = max(0, delay - VANISH_ANIMATION_SECONDS)
        await asyncio.sleep(wait_time)
        
        # 2. Show vanish animation if enabled
        if show_vanish:
            frames = ["🫧 Vanishing in 2…", "✨ Vanishing in 1…", "💨 …"]
            frame_delay = VANISH_ANIMATION_SECONDS / len(frames)
            for frame in frames:
                try:
                    await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=frame)
                except Exception:
                    break
                await asyncio.sleep(frame_delay)
        
        # 3. Final deletion
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except asyncio.CancelledError:
        pass
    except Exception:
        pass
    finally:
        if DELETION_TASKS.get((chat_id, message_id)) == asyncio.current_task():
            DELETION_TASKS.pop((chat_id, message_id), None)

async def schedule_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int = QUICK_DELETE_SECONDS, show_vanish: bool = True):
    """Unified scheduler for message deletion. Cancels existing tasks for the same message."""
    # Cancel any existing auto-delete task for this message
    old_task = DELETION_TASKS.pop((chat_id, message_id), None)
    if old_task:
        old_task.cancel()
    
    # Cancel any running countdown for this message
    old_countdown = COUNTDOWN_TASKS.pop((chat_id, message_id), None)
    if old_countdown:
        old_countdown.cancel()
    ORIGINAL_TEXTS.pop((chat_id, message_id), None)
        
    task = asyncio.create_task(_perform_scheduled_delete(context, chat_id, message_id, delay, show_vanish))
    DELETION_TASKS[(chat_id, message_id)] = task
    return task

async def safe_delete_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int = QUICK_DELETE_SECONDS):
    """Safely schedules a message for deletion after a quick delay (default 5s)."""
    return await schedule_delete(context, chat_id, message_id, delay=delay, show_vanish=True)

async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int = LONG_DELETE_SECONDS, show_vanish: bool = True):
    """Schedules a message for deletion after a long delay (default 300s)."""
    return await schedule_delete(context, chat_id, message_id, delay, show_vanish)

def _build_countdown_footer(seconds_left: int) -> str:
    """Build a clean countdown timer footer."""
    if seconds_left >= 60:
        m, s = divmod(seconds_left, 60)
        timer = f"{m}m {s:02d}s" if s else f"{m}m"
    else:
        timer = f"{seconds_left}s"
    return f"\n\n⏱ _Auto-delete in {timer}_"

async def _live_countdown(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, total_delay: int):
    """Periodically edits a message to show a live countdown timer."""
    try:
        await asyncio.sleep(10)  # Initial wait before first update

        elapsed = 10
        while elapsed < total_delay - VANISH_ANIMATION_SECONDS:
            remaining = total_delay - elapsed
            key = (chat_id, message_id)
            original = ORIGINAL_TEXTS.get(key, "")
            footer = _build_countdown_footer(remaining)
            full_text = f"{original}{footer}"

            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=message_id,
                    text=full_text, parse_mode='Markdown'
                )
            except Exception:
                pass  # Message may have been deleted or edited already

            # Update frequency: every 30s if >60s left, every 5s if <=60s
            if remaining > 60:
                await asyncio.sleep(30)
                elapsed += 30
            else:
                await asyncio.sleep(5)
                elapsed += 5
    except asyncio.CancelledError:
        pass
    except Exception:
        pass
    finally:
        COUNTDOWN_TASKS.pop((chat_id, message_id), None)

async def send_and_track_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str = None, photo: bytes = None, reply_markup=None, parse_mode='Markdown', auto_delete: bool = True, delay: int = LONG_DELETE_SECONDS, is_result: bool = False, cleanup_previous: bool = True):
    """Sends a message, tracks it, and schedules auto-deletion with live countdown."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    # 1. Immediate cleanup of previous bot message (only if it's NOT a result we want to keep)
    if cleanup_previous and user_id in LAST_BOT_MESSAGES:
        await safe_delete_message(context, chat_id, LAST_BOT_MESSAGES[user_id])
        LAST_BOT_MESSAGES.pop(user_id)

    # 2. Send new message (no static watermark — live countdown handles it)
    sent_msg = None
    
    try:
        if photo:
            sent_msg = await context.bot.send_photo(
                chat_id=chat_id,
                photo=photo,
                caption=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )
        elif text:
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )
    except Exception as e:
        logger.error(f"Error sending tracked message: {e}")
        return None

    if sent_msg:
        # 3. Track this message
        if is_result:
            ensure_user_context(user_id)
            
            last_id = CLEANUP_CONTEXT[user_id].get("last_result_id")
            if last_id:
                await safe_delete_message(context, chat_id, last_id)
            
            CLEANUP_CONTEXT[user_id]["last_result_id"] = sent_msg.message_id
        else:
            LAST_BOT_MESSAGES[user_id] = sent_msg.message_id
        
        # 4. Schedule auto-deletion + live countdown
        if auto_delete and text:
            ORIGINAL_TEXTS[(chat_id, sent_msg.message_id)] = text
            await schedule_delete(context, chat_id, sent_msg.message_id, delay, show_vanish=True)
            
            # Start live countdown only for longer delays (skip for quick deletes)
            if delay >= 30:
                countdown = asyncio.create_task(
                    _live_countdown(context, chat_id, sent_msg.message_id, delay)
                )
                COUNTDOWN_TASKS[(chat_id, sent_msg.message_id)] = countdown
            
    return sent_msg

# --- SECURITY HANDLER ---
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_CHAT_ID

def is_authorized(user_id: int) -> bool:
    if is_admin(user_id):
        return True
    if user_id in APPROVED_USERS:
        return True
    # Reload from file (in case of restart)
    APPROVED_USERS.update(_load_approved_users())
    return user_id in APPROVED_USERS

async def log_user_action(user, action, details=""):
    """Logs a user action to the backend audit trail."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(f"{API_BASE}/log_user_action", json={
                "user_id": user.id,
                "name": user.full_name,
                "username": user.username,
                "action": action,
                "details": details
            })
    except Exception as e:
        logger.error(f"Failed to log user action: {e}")

async def log_admin_action(admin_id, action, target_user_id=None, details=""):
    """Logs an admin action to the backend audit trail."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(f"{API_BASE}/log_admin_action", json={
                "admin_id": admin_id,
                "action": action,
                "target_user_id": target_user_id,
                "details": details
            })
    except Exception as e:
        logger.error(f"Failed to log admin action: {e}")

async def upsert_bot_user(user):
    """Update user's last active status and info in the DB."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(f"{API_BASE}/upsert_user", json={
                "chat_id": user.id,
                "name": user.full_name,
                "username": user.username
            })
    except Exception as e:
        logger.error(f"Failed to upsert bot user {user.id}: {e}")

async def request_admin_approval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends approval request to admin."""
    user = update.effective_user
    user_id = user.id
    
    # Check if already approved
    if is_authorized(user_id):
        await send_and_track_message(update, context, text="✅ *Access Granted*\n\nYou already have full access to all features.")
        set_user_state(user_id, CHOOSING)
        return
    
    # Notify User
    await send_and_track_message(update, context, text=(
        "🔐 *Access Request*\n\n"
        "Your request is being sent to the administrator.\n"
        "You will receive a notification once it has been reviewed."
    ))
    
    # Build admin notification message
    first_name = user.first_name or "Unknown"
    last_name = user.last_name or ""
    full_name = f"{first_name} {last_name}".strip()
    username = f"@{user.username}" if user.username else "Not set"

    # Escape Markdown special characters in user data
    for ch in ('_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!'):
        full_name = full_name.replace(ch, f'\\{ch}')
        username = username.replace(ch, f'\\{ch}')

    msg = (
        f"🔐 *New Access Request*\n\n"
        f"👤 *Name:* {full_name}\n"
        f"🔖 *Username:* {username}\n"
        f"🆔 *User ID:* `{user_id}`\n\n"
        f"Would you like to approve this user?"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Accept", callback_data=f"approve_{user_id}"),
            InlineKeyboardButton("❌ Decline", callback_data=f"decline_{user_id}")
        ]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    
    try:
        # Try to send profile photo if available
        photo_file_id = None
        try:
            photos = await user.get_profile_photos(limit=1)
            if photos.total_count > 0:
                photo_file_id = photos.photos[0][0].file_id
        except Exception:
            pass  # Photo fetch failed, continue without photo

        if photo_file_id:
            await context.bot.send_photo(
                chat_id=ADMIN_CHAT_ID,
                photo=photo_file_id,
                caption=msg,
                parse_mode='Markdown',
                reply_markup=markup
            )
        else:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=msg,
                parse_mode='Markdown',
                reply_markup=markup
            )
    except Exception as e:
        logger.error(f"Failed to send admin request for user {user_id}: {e}")
        await send_and_track_message(update, context, text="⚠️ *Request Failed*\n\nUnable to send your request. Please try again later.")
        set_user_state(user_id, CHOOSING)
        return

    await send_and_track_message(update, context, text=(
        "✅ *Request Submitted*\n\n"
        "Waiting for admin approval......."
    ))
    set_user_state(user_id, CHOOSING)

# --- BOT HANDLERS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for the bot."""
    user = update.effective_user
    logger.info(f"Start command received from user {user.id} ({user.full_name})")
    
    try:
        await run_clean_chat(update, context)
        set_user_state(user.id, CHOOSING)
        await show_main_menu(update, context)
        logger.info(f"Main menu sent to user {user.id}")
    except Exception as e:
        logger.error(f"Error in start handler: {e}", exc_info=True)
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nAn unexpected error occurred. Please try again.")
        clear_user_state(user.id)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancels and ends the conversation."""
    user_id = update.effective_user.id
    
    # Clear per-user state
    await run_clean_chat(update, context)
    clear_user_state(user_id)
    SEARCH_CONTEXT.pop(user_id, None)
    USER_PAGINATION_CONTEXT.pop(user_id, None)
    ANALYTICS_CONTEXT.pop(user_id, None)
    
    await send_and_track_message(update, context, text="❌ *Action Cancelled*\n\nReturning to main menu.", reply_markup=ReplyKeyboardRemove())
    
    # Return to main menu
    await show_main_menu(update, context)
    set_user_state(user_id, CHOOSING)
    
    # Return ConversationHandler.END for compatibility
    from telegram.ext import ConversationHandler
    return ConversationHandler.END

async def handle_exit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Exits the conversation."""
    user_id = update.effective_user.id
    await send_and_track_message(update, context, text="👋 *Session Closed*\n\nThank you for using the Library Bot.\nUse /start to begin a new session.", reply_markup=ReplyKeyboardRemove())
    clear_user_state(user_id)
    SEARCH_CONTEXT.pop(user_id, None)
    USER_PAGINATION_CONTEXT.pop(user_id, None)
    ANALYTICS_CONTEXT.pop(user_id, None)

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display the main menu buttons."""
    user_id = update.effective_user.id
    
    # Show different menus based on authorization
    if is_authorized(user_id):
        # Full menu for authorized users
        keyboard = [
            ["🔍 Find a Book", "📖 Check Status"],
            ["👤 Student Profile", "🕘 Reading History"],
            ["📊 Library Stats", "📊 Advanced Analytics"],
            ["📎 Presentations", "👥 Bot Users"],
            ["👑 Admin Dashboard"],
            ["❌ Exit"]
        ]
        msg = "📚 *MCL BOT*\n\n_Please select an option from the menu below._"
    else:
        # Limited menu for public users (not approved yet)
        keyboard = [
            ["🔍 Find a Book", "📖 Check Status"],
            ["📊 Library Stats"],
            ["🔐 Request Access"],
            ["❌ Exit"]
        ]
        msg = (
            "📚 *MCL BOT*\n\n"
            "You can search for books and view library statistics.\n\n"
            "🔒 *Student profiles and reading history require authorization.*\n"
            "Use the *Request Access* button to apply."
        )
    
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)
    
    await send_and_track_message(update, context, text=msg, reply_markup=reply_markup)

async def handle_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Routes the user choice from the main menu."""
    await run_clean_chat(update, context)
    text = update.message.text
    user_id = update.effective_user.id

    # PUBLIC FEATURES (no authorization required)
    if text == "🔍 Find a Book":
        msg = await send_and_track_message(update, context, text="🔍 *Search Books*\n\nPlease enter the *Book Code* or *Title* to search:", reply_markup=ReplyKeyboardRemove(), delay=PROMPT_TTL_SECONDS)
        store_prompt(user_id, msg.message_id if msg else None)
        set_user_state(user_id, SEARCHING_BOOK)
        
    elif text == "📖 Check Status":
        msg = await send_and_track_message(update, context, text="📖 *Check Book Status*\n\nPlease enter the *Book Code* to check its availability:", reply_markup=ReplyKeyboardRemove(), delay=PROMPT_TTL_SECONDS)
        store_prompt(user_id, msg.message_id if msg else None)
        set_user_state(user_id, CHECKING_STATUS)
        
    elif text == "📊 Library Stats":
        await handle_book_count(update, context)
        set_user_state(user_id, CHOOSING)
    
    elif text == "📎 Presentations":
        if not is_authorized(user_id):
            await send_and_track_message(update, context, text="Access Required. Presentations require authorization.")
            set_user_state(user_id, CHOOSING)
            return
        await show_presentations_list(update, context)
    
    elif text == "🔐 Request Access":
        if is_authorized(user_id):
            await send_and_track_message(update, context, text="✅ *Access Granted*\n\nYou already have full access to all features.")
            set_user_state(user_id, CHOOSING)
        else:
            await request_admin_approval(update, context)
    
    elif text == "📊 Advanced Analytics":
        if not is_authorized(user_id):
            await send_and_track_message(update, context, text="🔒 *Access Restricted*\n\nThis section requires authorization.\nPlease use the *Request Access* button to apply.")
            set_user_state(user_id, CHOOSING)
            return
        
        keyboard = [
            [InlineKeyboardButton("🔥 Most Issued Books", callback_data="ana_most")],
            [InlineKeyboardButton("🧑‍🎓 Top Readers", callback_data="ana_readers")],
            [InlineKeyboardButton("⏰ Overdue List", callback_data="ana_overdue")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="nav_menu")]
        ]
        await send_and_track_message(update, context, text="📊 *Advanced Analytics*\nSelect a report to view:", reply_markup=InlineKeyboardMarkup(keyboard), is_result=True)
        set_user_state(user_id, CHOOSING)

    elif text == "👥 Bot Users":
        if not is_admin(user_id):
            await send_and_track_message(update, context, text="🔒 *Access Restricted*\n\nThis section is for administrators only.")
            set_user_state(user_id, CHOOSING)
            return
        
        USER_PAGINATION_CONTEXT[user_id] = {"page": 1}
        await send_bot_users_page(update, context, user_id, 1)

    elif text == "👑 Admin Dashboard":
        if not is_admin(user_id):
            await send_and_track_message(update, context, text="🔒 *Access Restricted*\n\nThis section is for administrators only.")
            set_user_state(user_id, CHOOSING)
            return
        await show_admin_dashboard(update, context)
    
    elif text == "❌ Exit":
        await handle_exit(update, context)
    
    # RESTRICTED FEATURES (authorization required)
    elif text == "👤 Student Profile":
        if not is_authorized(user_id):
            await send_and_track_message(update, context, text=(
                "🔒 *Access Required*\n\n"
                "This feature requires administrator approval.\n\n"
                "Please use the *Request Access* button to apply."
            ))
            set_user_state(user_id, CHOOSING)
            return
        msg = await send_and_track_message(update, context, text="👤 *Student Profile*\n\nPlease enter the *Student ID* or *Name*:", reply_markup=ReplyKeyboardRemove(), delay=PROMPT_TTL_SECONDS)
        store_prompt(user_id, msg.message_id if msg else None)
        set_user_state(user_id, STUDENT_DETAILS)
        
    elif text == "🕘 Reading History":
        if not is_authorized(user_id):
            await send_and_track_message(update, context, text=(
                "🔒 *Access Required*\n\n"
                "This feature requires administrator approval.\n\n"
                "Please use the *Request Access* button to apply."
            ))
            set_user_state(user_id, CHOOSING)
            return
        msg = await send_and_track_message(update, context, text="🕘 *Reading History*\n\nPlease enter the *Book Code* to view its transaction history:", reply_markup=ReplyKeyboardRemove(), delay=PROMPT_TTL_SECONDS)
        store_prompt(user_id, msg.message_id if msg else None)
        set_user_state(user_id, ISSUE_HISTORY)
    
async def show_admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the Admin Dashboard."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return

    keyboard = [
        [InlineKeyboardButton("📊 System Health", callback_data="admin_health"),
         InlineKeyboardButton("📜 Logs", callback_data="admin_logs")],
        [InlineKeyboardButton("👥 Bot Users", callback_data="page_user_list"),
         InlineKeyboardButton("🛡 Audit Trail", callback_data="admin_audit")],
        [InlineKeyboardButton("🔄 Reset User", callback_data="admin_reset")],
        [InlineKeyboardButton("🗄 Database", callback_data="admin_db")],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="nav_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    msg = (
        "⚙️ *Admin Panel*\n"
        "━━━━━━━━━━━━━━━\n"
        "Manage your library bot from here."
    )
    await send_and_track_message(update, context, text=msg, reply_markup=reply_markup)
    set_user_state(user_id, ADMIN_DASHBOARD)

async def show_admin_db_tools(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the Database Management menu."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return

    keyboard = [
        [InlineKeyboardButton("⬇️ Download Database", callback_data="admin_export")],
        [InlineKeyboardButton("📤 Upload Database", callback_data="admin_import_info")],
        [InlineKeyboardButton("☁️ Backup to Cloud", callback_data="admin_backup")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_dash")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    msg = (
        "🗄 *Database*\n"
        "━━━━━━━━━━━━━━━\n"
        "⬇️ *Download* — Export current database\n"
        "📤 *Upload* — Send a `.db` file to auto-import\n"
        "☁️ *Backup* — Save to cloud (survives restarts)"
    )
    
    if update.callback_query:
        await update.callback_query.edit_message_text(msg, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await send_and_track_message(update, context, text=msg, reply_markup=reply_markup)
    
    set_user_state(user_id, ADMIN_DASHBOARD)

async def handle_search_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processes search book request and initializes pagination."""
    user_id = update.effective_user.id
    term = update.message.text.strip()
    
    # Initialize search context
    SEARCH_CONTEXT[user_id] = {"term": term, "page": 1}
    await log_user_action(update.effective_user, "Search Book", f"Term: {term}")
    
    await update.effective_chat.send_action(ChatAction.TYPING)
    await send_search_page(update, context, user_id, 1)

async def send_search_page(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, page: int):
    """Fetches and displays a specific page of search results."""
    context_data = SEARCH_CONTEXT.get(user_id)
    if not context_data:
        return

    term = context_data["term"]
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/search_book", json={
                "term": term,
                "page": page,
                "page_size": PAGE_SIZE
            })
            data = response.json()
        
        if data["status"] != "ok":
            msg = f"⚠️ *Error*\n\n{data.get('message', 'An unknown error occurred.')}"
            if update.callback_query:
                await update.callback_query.edit_message_text(msg)
            else:
                await send_and_track_message(update, context, text=msg)
            return
        
        books = data["data"]["books"]
        total_pages = data["data"]["total_pages"]
        total_count = data["data"]["total_count"]
        
        if not books:
            msg = "📭 *No Results*\n\nNo books found matching your search term.\nPlease try a different keyword or code."
            if update.callback_query:
                await update.callback_query.edit_message_text(msg)
            else:
                await send_and_track_message(update, context, text=msg)
            return

        # Format results into a single professional message
        message_text = f"🔍 *Search Results*\n\n"
        message_text += f"📗 *Query:* `{term}`\n"
        message_text += f"📊 *Found:* {total_count} result(s) — Page {page}/{total_pages}\n\n"
        
        for book in books:
            status_icon = "✅" if book["available"] > 0 else "❌"
            status_text = "Available" if book['available'] > 0 else "Issued"
            message_text += (
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"📘 *{book['title']}*\n"
                f"🔖 Code: `{book['id']}`\n"
                f"📊 Status: {status_icon} *{status_text}*\n"
            )

        # Build pagination keyboard
        keyboard = []
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("⏮ Prev", callback_data=f"page_prev"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("⏭ Next", callback_data=f"page_next"))
        
        if nav_row:
            keyboard.append(nav_row)
            
        keyboard.append([InlineKeyboardButton("🔙 Main Menu", callback_data="nav_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)

        if update.callback_query:
            await update.callback_query.edit_message_text(
                message_text, 
                reply_markup=reply_markup, 
                parse_mode='Markdown'
            )
        else:
            await send_and_track_message(update, context, text=message_text, reply_markup=reply_markup, is_result=True)
            
    except Exception as e:
        logger.error(f"Error in send_search_page: {e}")
        error_msg = "⚠️ *Error*\n\nFailed to fetch search results. Please try again."
        if update.callback_query:
            await update.callback_query.edit_message_text(error_msg)
        else:
            await send_and_track_message(update, context, text=error_msg, is_result=True)
    
    set_user_state(user_id, CHOOSING)

async def handle_book_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processes book status request."""
    user_id = update.effective_user.id
    book_id = update.message.text.strip().upper()
    await log_user_action(update.effective_user, "Check Status", f"Book: {book_id}")
    
    await update.effective_chat.send_action(ChatAction.TYPING)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/book_status", json={"book_id": book_id})
            data = response.json()
        
        if data["status"] != "ok":
            await send_and_track_message(update, context, text="📭 *Not Found*\n\nNo book found with that code.\nPlease verify the code and try again.")
            return
        
        book = data["data"]
        state_text = "✅ Available" if book["available"] > 0 else "❌ Issued"
        
        msg = (
            f"📖 *Book Status*\n\n"
            f"🔖 *Code:* `{book['id']}`\n"
            f"📘 *Title:* {book['title']}\n"
            f"📊 *Status:* {state_text}\n"
        )
        
        if book["available"] == 0 and "issued_to" in book:
            issued = book["issued_to"]
            msg += (
                f"\n👤 *Issued To:* {issued['name']} ({issued['batch'] or 'N/A'})\n"
                f"📅 *Issue Date:* {issued['issue_date']}\n"
                f"📅 *Due Date:* {issued['due_date']}\n\n"
                f"_Please ensure timely return._"
            )
        
        # Inline Navigation
        keyboard = [
            [InlineKeyboardButton("📖 Check Another", callback_data="nav_status"),
             InlineKeyboardButton("🔙 Main Menu", callback_data="nav_menu")]
        ]
        await send_and_track_message(update, context, text=msg, reply_markup=InlineKeyboardMarkup(keyboard), is_result=True)
    except Exception as e:
        logger.error(f"Error fetching book status: {e}")
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to fetch book status. Please try again.", is_result=True)
    set_user_state(user_id, CHOOSING)

async def handle_student_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processes student details request."""
    user_id = update.effective_user.id
    student_id = update.message.text.strip()
    await log_user_action(update.effective_user, "View Student Profile", f"Student: {student_id}")
    
    await update.effective_chat.send_action(ChatAction.TYPING)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/student_details", json={"student_id": student_id})
            data = response.json()
        
        if data["status"] != "ok":
            await send_and_track_message(update, context, text="📭 *Not Found*\n\nNo student found with that ID or name.\nPlease verify and try again.")
            await show_main_menu(update, context)
            set_user_state(user_id, CHOOSING)
            return
        
        student = data["data"]
        
        # 1. Photo Handling
        photo_bytes = None
        if student["has_photo"] and student["photo"]:
            try:
                photo_bytes = base64.b64decode(student["photo"])
            except Exception as e:
                logger.error(f"Error decoding student photo: {e}")

        # 2. Construct Message
        issued_books = student["issued"]
        returned_books = student["returned"]
        
        msg = ""
        if not issued_books and not returned_books:
            msg = (
                f"👤 *Student Profile*\n\n"
                f"📛 *Name:* {student['name']}\n"
                f"🏫 *Batch:* {student['batch']}\n\n"
                f"No records found for this student."
            )
        else:
            msg = (
                f"👤 *Student Profile*\n\n"
                f"📛 *Name:* {student['name']}\n"
                f"🏫 *Batch:* {student['batch']}\n\n"
            )
            
            msg += "📚 *Currently Issued:*\n"
            if issued_books:
                for book in issued_books:
                    msg += f"• `{book['id']}` — {book['title']} (Issued: {book['issue_date']})\n"
            else:
                msg += "• None\n"
                
            msg += "\n📜 *Returned Books:*\n"
            if returned_books:
                for book in returned_books:
                    msg += f"• `{book['id']}` — {book['title']} ({book['issue_date']} → {book['return_date']})\n"
            else:
                msg += "• None\n"
        
        # Inline Navigation - Smart Actions
        keyboard = [
            [InlineKeyboardButton("🕘 View History", callback_data="nav_history"),
             InlineKeyboardButton("📚 Search Another", callback_data="nav_student")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="nav_menu")]
        ]
        
        await send_and_track_message(update, context, text=msg, photo=photo_bytes, reply_markup=InlineKeyboardMarkup(keyboard), is_result=True)
    except Exception as e:
        logger.error(f"Error fetching student details: {e}")
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to fetch student details. Please try again.")
    set_user_state(user_id, CHOOSING)

async def handle_issue_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processes issue history for a book."""
    user_id = update.effective_user.id
    book_id = update.message.text.strip().upper()
    await log_user_action(update.effective_user, "View Issue History", f"Book: {book_id}")
    
    await update.effective_chat.send_action(ChatAction.TYPING)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/issue_history", json={"book_id": book_id})
            data = response.json()
        
        if data["status"] != "ok":
            await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to fetch transaction history.")
            return
        
        history = data["data"]["history"]
        
        if not history:
            await send_and_track_message(update, context, text="📭 *No History*\n\nNo transaction history found for this book code.")
        else:
            msg = (
                f"🕘 *Transaction History*\n\n"
                f"📖 *Book:* `{book_id}`\n"
                f"📋 *Recent Transactions:* {len(history)}\n\n"
            )
            for trans in history:
                ret_text = trans["return_date"] if trans["return_date"] else "_Pending_"
                msg += (
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"👤 *{trans['name']}*\n"
                    f"📅 Issued: {trans['issue_date']}\n"
                    f"📅 Returned: {ret_text}\n"
                )
            
            keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="nav_menu")]]
            await send_and_track_message(update, context, text=msg, reply_markup=InlineKeyboardMarkup(keyboard), is_result=True)
    except Exception as e:
        logger.error(f"Error fetching issue history: {e}")
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to fetch transaction history. Please try again.")
    set_user_state(user_id, CHOOSING)

async def handle_book_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays aggregate book counts."""
    await log_user_action(update.effective_user, "View Library Stats")
    await update.effective_chat.send_action(ChatAction.TYPING)
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{API_BASE}/library_stats")
            data = response.json()
        
        if data["status"] != "ok":
            await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to fetch library statistics.")
            return
        
        stats = data["data"]
        msg = (
            f"📊 *Library Statistics*\n\n"
            f"📚 *Total Books:* {stats['total_books']}\n"
            f"✅ *Available Copies:* {stats['available_copies']}\n"
            f"📖 *Currently Issued:* {stats['issued_books']}\n\n"
            f"_Last updated: {stats['timestamp']}_"
        )
        await send_and_track_message(update, context, text=msg, is_result=True)
    except Exception as e:
        logger.error(f"Error fetching library stats: {e}")
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to fetch library statistics. Please try again.")
        
    # If called from menu (update.message present), show menu again
    if update.message:
        await show_main_menu(update, context)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles all inline button clicks."""
    query = update.callback_query
    
    data = query.data
    user_id = update.effective_user.id
    
    # 1. Admin Actions
    if data.startswith("approve_") or data.startswith("decline_"):
        if not is_admin(user_id):
            await query.answer("❌ Unauthorized action", show_alert=True)
            return
            
        target_id = int(data.split("_")[1])
        
        if data.startswith("approve_"):
            # Prevent double approval
            if target_id in APPROVED_USERS:
                await query.answer("✅ User already approved", show_alert=True)
                return
            
            APPROVED_USERS.add(target_id)
            _save_approved_users()
            logger.info(f"Admin approved user {target_id}")
            await log_admin_action(user_id, "Approve User", target_id)
            
            # Update Admin Message
            try:
                if query.message.caption:
                    await query.edit_message_caption(
                        caption=f"{query.message.caption}\n\n✅ *APPROVED by Admin*",
                        parse_mode='Markdown'
                    )
                else:
                    await query.edit_message_text(
                        text=f"{query.message.text}\n\n✅ *APPROVED by Admin*",
                        parse_mode='Markdown'
                    )
            except Exception as e:
                logger.error(f"Failed to update admin message: {e}")
            
            # Notify User
            try:
                await context.bot.send_message(
                    chat_id=target_id,
                    text=(
                        "✅ *Access Approved*\n\n"
                        "Your access request has been approved by the administrator.\n\n"
                        "You now have access to:\n"
                        "• 👤 Student Profile\n"
                        "• 🕘 Reading History\n\n"
                        "Use /start to view the updated menu."
                    ),
                    parse_mode='Markdown'
                )
                logger.info(f"Notified user {target_id} of approval")
            except Exception as e:
                logger.error(f"Failed to notify user {target_id}: {e}")
            
            await query.answer("✅ User approved", show_alert=False)
                
        elif data.startswith("decline_"):
            logger.info(f"Admin declined user {target_id}")
            await log_admin_action(user_id, "Decline User", target_id)
            
            # Remove from approved users
            APPROVED_USERS.discard(target_id)
            _save_approved_users()
            
            # Update Admin Message
            try:
                if query.message.caption:
                    await query.edit_message_caption(
                        caption=f"{query.message.caption}\n\n❌ *DECLINED by Admin*",
                        parse_mode='Markdown'
                    )
                else:
                    await query.edit_message_text(
                        text=f"{query.message.text}\n\n❌ *DECLINED by Admin*",
                        parse_mode='Markdown'
                    )
            except Exception as e:
                logger.error(f"Failed to update admin message: {e}")
            
            # Notify User
            try:
                await context.bot.send_message(
                    chat_id=target_id,
                    text=(
                        "❌ *Access Denied*\n\n"
                        "Your access request has been declined.\n"
                        "Please contact the library administrator for more information."
                    ),
                    parse_mode='Markdown'
                )
                logger.info(f"Notified user {target_id} of decline")
            except Exception as e:
                logger.error(f"Failed to notify user {target_id}: {e}")
            
            await query.answer("❌ User declined", show_alert=False)
        
        return
    
    # 5. Admin Dashboard Actions
    if data.startswith("admin_"):
        if not is_admin(user_id):
            await query.answer("🔒 Admin only", show_alert=True)
            return

        if data == "admin_health":
            await show_admin_health(update, context)
        elif data == "admin_logs":
            await show_admin_logs(update, context, "ERROR", 1)
        elif data == "admin_user_history":
            await send_and_track_message(update, context, text="👤 *User History*\n\nPlease enter the *User ID* to view their activity:", reply_markup=ReplyKeyboardRemove())
            set_user_state(user_id, ADMIN_USER_HISTORY)
        elif data == "admin_audit":
            await show_admin_audit(update, context, "all", 1)
        elif data == "admin_reset":
            await send_and_track_message(update, context, text="🔄 *Reset User Session*\n\nPlease enter the *User ID* to reset:", reply_markup=ReplyKeyboardRemove())
            set_user_state(user_id, ADMIN_RESET_USER)
        elif data == "admin_dash":
            await show_admin_dashboard(update, context)
        elif data == "admin_db":
            await show_admin_db_tools(update, context)
        elif data == "admin_export":
            await handle_db_export(update, context)
        elif data == "admin_import_info":
            await query.answer("💡 Just send a .db file in chat — it auto-imports!", show_alert=True)
        elif data == "admin_backup":
            await handle_db_backup(update, context)
        elif data == "page_user_list":
            USER_PAGINATION_CONTEXT[user_id] = {"page": 1}
            await send_bot_users_page(update, context, user_id, 1)
        return

    # 6. Admin Log Navigation
    if data.startswith("log_"):
        # log_{level}_{page}
        parts = data.split("_")
        level = parts[1]
        page = int(parts[2])
        await show_admin_logs(update, context, level, page)
        return

    # 7. Admin Audit Navigation
    if data.startswith("audit_"):
        # audit_{filter}_{page}
        parts = data.split("_")
        filter_type = parts[1]
        page = int(parts[2])
        await show_admin_audit(update, context, filter_type, page)
        return

    # 8. User History Navigation
    if data.startswith("uhist_"):
        # uhist_{target_id}_{page}
        parts = data.split("_")
        target_id = int(parts[1])
        page = int(parts[2])
        await show_user_history(update, context, target_id, page)
        return

    # 3. Analytics Actions
    if data.startswith("ana_"):
        if not is_authorized(user_id):
            await query.answer("🔒 Access required", show_alert=True)
            return
            
        if data == "ana_back":
            ANALYTICS_CONTEXT.pop(user_id, None)
            keyboard = [
                [InlineKeyboardButton("🔥 Most Issued Books", callback_data="ana_most")],
                [InlineKeyboardButton("🧑‍🎓 Top Readers", callback_data="ana_readers")],
                [InlineKeyboardButton("⏰ Overdue List", callback_data="ana_overdue")],
                [InlineKeyboardButton("🔙 Main Menu", callback_data="nav_menu")]
            ]
            await query.message.edit_text("📊 *Advanced Analytics*\n\nSelect a report to view:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return

        if data == "ana_prev":
            if user_id in ANALYTICS_CONTEXT:
                ANALYTICS_CONTEXT[user_id]["page"] -= 1
                await send_analytics_page(update, context, user_id)
            return

        if data == "ana_next":
            if user_id in ANALYTICS_CONTEXT:
                ANALYTICS_CONTEXT[user_id]["page"] += 1
                await send_analytics_page(update, context, user_id)
            return

        # Initial analytics call
        ana_type = data # e.g., ana_most
        await log_user_action(update.effective_user, "View Analytics", f"Type: {ana_type}")
        ANALYTICS_CONTEXT[user_id] = {"type": ana_type, "page": 1}
        await send_analytics_page(update, context, user_id)
        return

    # Presentations callbacks
    if data.startswith("pres_dl_"):
        filename = data[8:]  # Remove "pres_dl_" prefix
        await download_presentation(update, context, filename)
        return

    if data == "pres_list":
        await show_presentations_list(update, context)
        return

    # 2. Navigation Actions mechanism (State transitions via button)
    if data == "nav_menu":
        SEARCH_CONTEXT.pop(user_id, None)  # Clear search context
        ANALYTICS_CONTEXT.pop(user_id, None)  # Clear analytics context
        await show_main_menu(update, context)
        set_user_state(user_id, CHOOSING)
        
    elif data == "page_prev":
        if user_id in SEARCH_CONTEXT:
            SEARCH_CONTEXT[user_id]["page"] -= 1
            await send_search_page(update, context, user_id, SEARCH_CONTEXT[user_id]["page"])
            
    elif data == "page_next":
        if user_id in SEARCH_CONTEXT:
            SEARCH_CONTEXT[user_id]["page"] += 1
            await send_search_page(update, context, user_id, SEARCH_CONTEXT[user_id]["page"])
            
    elif data == "nav_search":
        msg = await send_and_track_message(update, context, text="🔍 *Search Books*\n\nPlease enter *Book Code* or *Title*:", reply_markup=ReplyKeyboardRemove(), delay=PROMPT_TTL_SECONDS)
        store_prompt(user_id, msg.message_id if msg else None)
        set_user_state(user_id, SEARCHING_BOOK)
        
    elif data == "nav_status":
        msg = await send_and_track_message(update, context, text="📖 *Check Status*\n\nPlease enter the *Book Code*:", reply_markup=ReplyKeyboardRemove(), delay=PROMPT_TTL_SECONDS)
        store_prompt(user_id, msg.message_id if msg else None)
        set_user_state(user_id, CHECKING_STATUS)
        
    elif data == "nav_student":
        # Check auth again just in case
        if not is_authorized(user_id):
            await send_and_track_message(update, context, text="🔒 *Access Required*\n\nStudent details require authorization.\nPlease request access first.")
            set_user_state(user_id, CHOOSING)
            return
        msg = await send_and_track_message(update, context, text="👤 *Student Profile*\n\nPlease enter *Student ID* or *Name*:", reply_markup=ReplyKeyboardRemove(), delay=PROMPT_TTL_SECONDS)
        store_prompt(user_id, msg.message_id if msg else None)
        set_user_state(user_id, STUDENT_DETAILS)
        
    elif data == "nav_history":
        if not is_authorized(user_id):
            await send_and_track_message(update, context, text="🔒 *Access Required*\n\nIssue history requires authorization.\nPlease request access first.")
            set_user_state(user_id, CHOOSING)
            return
        msg = await send_and_track_message(update, context, text="🕘 *Reading History*\n\nPlease enter the *Book Code*:", reply_markup=ReplyKeyboardRemove(), delay=PROMPT_TTL_SECONDS)
        store_prompt(user_id, msg.message_id if msg else None)
        set_user_state(user_id, ISSUE_HISTORY)

    # 4. Bot Users Management
    elif data == "page_user_prev":
        if user_id in USER_PAGINATION_CONTEXT:
            USER_PAGINATION_CONTEXT[user_id]["page"] -= 1
            await send_bot_users_page(update, context, user_id, USER_PAGINATION_CONTEXT[user_id]["page"])
            
    elif data == "page_user_next":
        if user_id in USER_PAGINATION_CONTEXT:
            USER_PAGINATION_CONTEXT[user_id]["page"] += 1
            await send_bot_users_page(update, context, user_id, USER_PAGINATION_CONTEXT[user_id]["page"])

    elif data.startswith("view_user_"):
        target_id = int(data.split("_")[2])
        await show_user_details(update, context, target_id)

    elif data.startswith("role_"):
        # Format: role_{action}_{target_id}
        parts = data.split("_")
        action = parts[1]
        target_id = int(parts[2])
        
        if action == "approve":
            await confirm_action(update, context, f"Approve user {target_id}?", f"conf_approve_{target_id}")
        elif action == "block":
            await confirm_action(update, context, f"Block user {target_id}?", f"conf_block_{target_id}")
        elif action == "change":
            await show_role_options(update, context, target_id)

    elif data.startswith("conf_"):
        parts = data.split("_")
        action = parts[1]
        target_id = int(parts[2])
        
        new_role = "Approved" if action == "approve" else "Blocked"
        await update_user_role_api(update, context, target_id, new_role)

    elif data.startswith("setrole_"):
        parts = data.split("_")
        role = parts[1]
        target_id = int(parts[2])
        await update_user_role_api(update, context, target_id, role)

# --- UNIFIED HANDLERS ---

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unified message handler that routes based on user state."""
    user = update.effective_user
    user_id = user.id
    await upsert_bot_user(user)

    # --- AUTO DB IMPORT: If admin sends a .db file, import it instantly ---
    if update.message.document and is_admin(user_id):
        doc = update.message.document
        if doc.file_name.endswith('.db'):
            await handle_db_file_upload(update, context)
            return

    current_state = get_user_state(user_id)
    
    msg_text = update.message.text or ""
    logger.info(f"Message from user {user_id} in state {current_state}: {msg_text[:50]}")
    
    # Clean Chat: If we are in an input state, clean up the previous menu tap, prompt, and current input
    if current_state in [SEARCHING_BOOK, CHECKING_STATUS, STUDENT_DETAILS, ISSUE_HISTORY, BROWSING_PRESENTATIONS]:
        await run_clean_chat(update, context)
    elif current_state == CHOOSING:
        # Store the menu button tap ID and schedule its deletion
        store_menu_tap(user_id, update.message.message_id)
        await schedule_delete(context, update.effective_chat.id, update.message.message_id, QUICK_DELETE_SECONDS, show_vanish=True)

    try:
        if current_state == CHOOSING:
            await handle_choice(update, context)
        elif current_state == SEARCHING_BOOK:
            await handle_search_book(update, context)
        elif current_state == CHECKING_STATUS:
            await handle_book_status(update, context)
        elif current_state == STUDENT_DETAILS:
            await handle_student_details(update, context)
        elif current_state == ISSUE_HISTORY:
            await handle_issue_history(update, context)
        elif current_state == BROWSING_PRESENTATIONS:
            await handle_presentation_input(update, context)
        elif current_state == ADMIN_RESET_USER:
            await handle_admin_reset_user(update, context)
        elif current_state == ADMIN_USER_HISTORY:
            try:
                target_id = int(update.message.text.strip())
                await show_user_history(update, context, target_id, 1)
            except ValueError:
                await send_and_track_message(update, context, text="❌ *Invalid Input*\n\nPlease enter a valid numeric User ID.")
                await show_admin_dashboard(update, context)
        else:
            # Unknown state, reset to CHOOSING
            logger.warning(f"Unknown state {current_state} for user {user_id}, resetting to CHOOSING")
            set_user_state(user_id, CHOOSING)
            await show_main_menu(update, context)
    except Exception as e:
        logger.error(f"Error in handle_message for user {user_id}: {e}", exc_info=True)
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nAn unexpected error occurred. Please try again.")
        set_user_state(user_id, CHOOSING)

# --- RUNTIME GUARD ---
BOT_RUNNING = False

def init_bot():
    """Main function to initialize the bot application."""
    application = ApplicationBuilder().token(TOKEN).build()

    # Add simple handlers instead of ConversationHandler
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('cancel', cancel))
    application.add_handler(CommandHandler('presentations', cmd_presentations))
    application.add_handler(MessageHandler((filters.TEXT | filters.Document.ALL) & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    return application

async def send_bot_users_page(update: Update, context: ContextTypes.DEFAULT_TYPE, admin_id: int, page: int):
    """Displays a paginated list of bot users."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/get_bot_users", json={"page": page, "page_size": PAGE_SIZE})
            res_data = response.json()
            
        if res_data["status"] != "ok":
            await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to fetch bot users.")
            return

        users = res_data["data"]["users"]
        total_pages = res_data["data"]["total_pages"]
        
        msg = f"👥 *Registered Users*\n\nPage {page} of {total_pages}\n\n"
        keyboard = []
        
        for u in users:
            masked_id = f"...{str(u['chat_id'])[-4:]}"
            msg += f"👤 *{u['name']}* ({u['role']})\nID: `{masked_id}` | Joined: {u['joined_at'][:10]}\n\n"
            keyboard.append([InlineKeyboardButton(f"🔍 View {u['name']}", callback_data=f"view_user_{u['chat_id']}")])
            
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("⏮ Prev", callback_data="page_user_prev"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("⏭ Next", callback_data="page_user_next"))
        if nav_row:
            keyboard.append(nav_row)
            
        keyboard.append([InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="ana_back")])
        
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await send_and_track_message(update, context, text=msg, reply_markup=InlineKeyboardMarkup(keyboard), is_result=True)
            
    except Exception as e:
        logger.error(f"Error in send_bot_users_page: {e}")
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to load bot users.")

async def show_user_details(update: Update, context: ContextTypes.DEFAULT_TYPE, target_id: int):
    """Shows full details and management actions for a user."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/get_user_details", json={"chat_id": target_id})
            res_data = response.json()
            
        if res_data["status"] != "ok":
            await update.callback_query.answer("❌ User not found", show_alert=True)
            return
            
        u = res_data["data"]
        msg = (
            f"👤 *User Details*\n\n"
            f"📛 *Name:* {u['name']}\n"
            f"🔖 *Username:* @{u['username'] if u['username'] else 'N/A'}\n"
            f"🆔 *Chat ID:* `{u['chat_id']}`\n"
            f"🎭 *Role:* {u['role']}\n"
            f"📅 *Joined:* {u['joined_at']}\n"
            f"🕒 *Last Active:* {u['last_active']}\n"
            f"✅ *Approved By:* {u['approved_by'] if u['approved_by'] else 'N/A'}"
        )
        
        keyboard = [
            [InlineKeyboardButton("✅ Approve", callback_data=f"role_approve_{target_id}"),
             InlineKeyboardButton("⛔ Block", callback_data=f"role_block_{target_id}")],
            [InlineKeyboardButton("🔁 Change Role", callback_data=f"role_change_{target_id}")],
            [InlineKeyboardButton("🔙 Back to List", callback_data="page_user_back")]
        ]
        
        await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error in show_user_details: {e}")
        await update.callback_query.answer("❌ Failed to load details")

async def confirm_action(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, callback_data: str):
    keyboard = [
        [InlineKeyboardButton("✅ Yes, Confirm", callback_data=callback_data)],
        [InlineKeyboardButton("❌ Cancel", callback_data="page_user_back")]
    ]
    await update.callback_query.edit_message_text(f"⚠️ *Confirm Action*\n\n{text}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def show_role_options(update: Update, context: ContextTypes.DEFAULT_TYPE, target_id: int):
    keyboard = [
        [InlineKeyboardButton("Admin", callback_data=f"setrole_Admin_{target_id}")],
        [InlineKeyboardButton("Approved", callback_data=f"setrole_Approved_{target_id}")],
        [InlineKeyboardButton("Basic", callback_data=f"setrole_Basic_{target_id}")],
        [InlineKeyboardButton("Blocked", callback_data=f"setrole_Blocked_{target_id}")],
        [InlineKeyboardButton("🔙 Cancel", callback_data=f"view_user_{target_id}")]
    ]
    await update.callback_query.edit_message_text("🎭 *Select New Role*\n\nChoose a role for this user:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def update_user_role_api(update: Update, context: ContextTypes.DEFAULT_TYPE, target_id: int, role: str):
    try:
        admin_id = update.effective_user.id
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/update_user_role", json={
                "chat_id": target_id,
                "role": role,
                "admin_id": admin_id
            })
            res_data = response.json()
            
        if res_data["status"] == "ok":
            await update.callback_query.answer(f"✅ Role updated to {role}")
            await log_admin_action(admin_id, "Change Role", target_id, f"New role: {role}")
            # Update in-memory set and persist to file
            if role == "Approved":
                APPROVED_USERS.add(target_id)
            elif role in ("Blocked", "Basic"):
                APPROVED_USERS.discard(target_id)
            _save_approved_users()
                
            await show_user_details(update, context, target_id)
        else:
            await update.callback_query.answer("❌ Failed to update role")
    except Exception as e:
        logger.error(f"Error in update_user_role_api: {e}")
        await update.callback_query.answer("❌ API Error")

async def show_admin_health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows system health dashboard."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{API_BASE}/health")
            data = response.json()

        status = "🟢 Running" if data["status"] == "ok" else "🔴 Down"
        db_status = "🟢 Present" if data["database_present"] else "🔴 Missing"

        msg = (
            f"📊 *System Health*\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🤖 *Bot:* 🟢 Running\n"
            f"⚙️ *Backend:* {status}\n"
            f"📁 *Database:* {db_status}\n"
            f"🕒 *Last Check:* {data['timestamp']}"
        )

        keyboard = [
            [InlineKeyboardButton("🔄 Refresh", callback_data="admin_health")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_dash")]
        ]

        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await send_and_track_message(update, context, text=msg, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in show_admin_health: {e}")
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to fetch system health.")

async def show_admin_logs(update: Update, context: ContextTypes.DEFAULT_TYPE, level: str, page: int):
    """Shows in-memory logs with pagination."""
    logs = in_memory_logs.get_logs(level)
    logs.reverse()

    page_size = 10
    total_pages = (len(logs) + page_size - 1) // page_size if logs else 1
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    page_logs = logs[start_idx:end_idx]

    msg = f"📜 *Logs* — `{level}` ({page}/{total_pages})\n━━━━━━━━━━━━━━━\n\n"
    if not page_logs:
        msg += "_No logs found._"
    else:
        for l in page_logs:
            msg += f"`{l['timestamp']}`\n{l['message'][:100]}\n\n"

    keyboard = []
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"log_{level}_{page-1}"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton("➡️", callback_data=f"log_{level}_{page+1}"))
    if nav_row:
        keyboard.append(nav_row)

    keyboard.append([
        InlineKeyboardButton("❌ Errors", callback_data="log_ERROR_1"),
        InlineKeyboardButton("⚠️ Warnings", callback_data="log_WARNING_1")
    ])
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="admin_dash")])
    
    if update.callback_query:
        await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await send_and_track_message(update, context, text=msg, reply_markup=InlineKeyboardMarkup(keyboard))

async def show_admin_audit(update: Update, context: ContextTypes.DEFAULT_TYPE, filter_type: str, page: int):
    """Shows admin action audit log."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/get_admin_actions", json={"page": page, "filter": filter_type})
            res_data = response.json()

        if res_data["status"] != "ok":
            await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to fetch audit log.")
            return

        actions = res_data["data"]
        msg = f"🛡 *Audit Trail* — {filter_type.capitalize()} ({page})\n━━━━━━━━━━━━━━━\n\n"

        if not actions:
            msg += "_No actions recorded._"
        else:
            for a in actions:
                target = f" → `{a['target_user_id']}`" if a['target_user_id'] else ""
                msg += f"`{a['created_at']}`\n*{a['action']}*{target}\n{a['details']}\n\n"

        keyboard = []
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"audit_{filter_type}_{page-1}"))
        if len(actions) == 10:
            nav_row.append(InlineKeyboardButton("➡️", callback_data=f"audit_{filter_type}_{page+1}"))
        if nav_row:
            keyboard.append(nav_row)

        keyboard.append([
            InlineKeyboardButton("All", callback_data="audit_all_1"),
            InlineKeyboardButton("Access", callback_data="audit_access_1"),
            InlineKeyboardButton("Resets", callback_data="audit_reset_1")
        ])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="admin_dash")])
        
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await send_and_track_message(update, context, text=msg, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in show_admin_audit: {e}")
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to load audit log.")

async def show_user_history(update: Update, context: ContextTypes.DEFAULT_TYPE, target_id: int, page: int = 1):
    """Shows action history for a specific user."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/get_user_actions", json={"user_id": target_id, "page": page})
            res_data = response.json()
            
        if res_data["status"] != "ok":
            await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to fetch user action history.")
            return

        actions = res_data["data"]
        msg = f"📜 *User Action History*\n\n👤 User: `{target_id}`\n\n"
        
        if not actions:
            msg += "_No actions found for this user._"
        else:
            for a in actions:
                msg += f"🕒 `{a['created_at']}`\n⚡ *{a['action']}*\n📝 {a['details']}\n\n"
                
        keyboard = []
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("⏮ Prev", callback_data=f"uhist_{target_id}_{page-1}"))
        if len(actions) == 10:
            nav_row.append(InlineKeyboardButton("⏭ Next", callback_data=f"uhist_{target_id}_{page+1}"))
        if nav_row:
            keyboard.append(nav_row)
            
        keyboard.append([InlineKeyboardButton("🔙 Back to Dashboard", callback_data="admin_dash")])
        
        await send_and_track_message(update, context, text=msg, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in show_user_history: {e}")
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to load user action history.")

async def handle_admin_reset_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Resets a user's session and state."""
    admin_id = update.effective_user.id
    try:
        target_id = int(update.message.text.strip())
        
        # 1. Clear State
        USER_STATES.pop(target_id, None)
        SEARCH_CONTEXT.pop(target_id, None)
        USER_PAGINATION_CONTEXT.pop(target_id, None)
        ANALYTICS_CONTEXT.pop(target_id, None)
        
        # 2. Cancel Deletion Tasks
        for key in list(DELETION_TASKS.keys()):
            if key[0] == target_id:
                task = DELETION_TASKS.pop(key)
                task.cancel()
        
        # 3. Notify User
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="🔄 *Session Reset*\n\nYour session has been reset by an administrator.\nPlease use /start to begin a new session.",
                parse_mode='Markdown'
            )
        except Exception:
            pass # User might have blocked the bot
            
        # 4. Log Action
        await log_admin_action(admin_id, "Reset User Session", target_id, "Cleared state and cancelled tasks.")
        
        await send_and_track_message(update, context, text=f"✅ *User Reset*\n\nUser `{target_id}` has been reset successfully.")
        await show_admin_dashboard(update, context)
        
    except ValueError:
        await send_and_track_message(update, context, text="❌ *Invalid Input*\n\nPlease enter a valid numeric User ID.")
    except Exception as e:
        logger.error(f"Error in handle_admin_reset_user: {e}")
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to reset user session.")

async def handle_db_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Exports the database file to the admin."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return

    if not os.path.exists(DB_PATH):
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nDatabase file not found.")
        return

    try:
        await update.effective_chat.send_action(ChatAction.UPLOAD_DOCUMENT)
        with open(DB_PATH, 'rb') as f:
            await context.bot.send_document(
                chat_id=user_id,
                document=f,
                filename=f"library_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db",
                caption="✅ *Database Exported Successfully*\n\nYour backup has been generated."
            )
        await log_admin_action(user_id, "DB_EXPORT", details=f"File: {DB_PATH}")
    except Exception as e:
        logger.error(f"Error exporting database: {e}")
        await send_and_track_message(update, context, text="⚠️ *Error*\n\nFailed to export database.")

async def handle_db_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Backup the database to GitHub Releases (cloud backup)."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return

    await update.callback_query.answer("☁️ Backing up to cloud...")
    await send_and_track_message(update, context, text="☁️ *Backing Up Database*\n\nUploading to cloud storage...")

    try:
        import threading
        result = [None]
        def _do_backup():
            result[0] = backup_db_to_github()
        t = threading.Thread(target=_do_backup)
        t.start()
        t.join(timeout=120)

        if result[0]:
            await send_and_track_message(update, context, text="✅ *Backup Complete*\n\nDatabase has been backed up to cloud storage.\nData will survive server restarts.")
            await log_admin_action(user_id, "DB_BACKUP", details="Manual cloud backup")
        else:
            await send_and_track_message(update, context, text="⚠️ *Backup Failed*\n\nCould not backup to cloud. Check that GITHUB_TOKEN is set.")
    except Exception as e:
        logger.error(f"DB backup error: {e}")
        await send_and_track_message(update, context, text="⚠️ *Backup Failed*\n\nAn error occurred during backup.")

async def handle_db_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processes the uploaded database file. Admin-only, auto-imports instantly."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return

    doc = update.message.document
    if not doc.file_name.endswith('.db'):
        await send_and_track_message(update, context, text="⚠️ *Invalid File*\n\nPlease upload a `.db` file.")
        return

    if doc.file_size > 50 * 1024 * 1024: # 50MB limit
        await send_and_track_message(update, context, text="⚠️ *File Too Large*\n\nMaximum file size is 50MB.")
        return

    temp_path = "temp_import.db"
    backup_path = f"{DB_PATH}.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # 1. Download file
        await update.effective_chat.send_action(ChatAction.TYPING)
        new_file = await context.bot.get_file(doc.file_id)
        await new_file.download_to_drive(temp_path)

        # 2. Validate SQLite and Tables
        conn = sqlite3.connect(temp_path)
        cursor = conn.cursor()
        required_tables = {'books', 'members', 'transactions'}
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = {row[0] for row in cursor.fetchall()}
        conn.close()

        if not required_tables.issubset(existing_tables):
            missing = required_tables - existing_tables
            os.remove(temp_path)
            await send_and_track_message(update, context, text=f"⚠️ *Invalid Schema*\n\nThe database is missing required tables: {', '.join(missing)}")
            return

        # 3. Atomic Replace with Backup
        if os.path.exists(DB_PATH):
            shutil.copy2(DB_PATH, backup_path)
        
        try:
            shutil.move(temp_path, DB_PATH)
            await send_and_track_message(update, context, text="✅ *Import Complete*\n\nDatabase has been replaced successfully.\nLibrary data has been updated.")
            await log_admin_action(user_id, "DB_IMPORT", details=f"File: {doc.file_name}, Size: {doc.file_size}")
            set_user_state(user_id, CHOOSING)
            # Auto-backup to GitHub Releases so data survives Railway restarts
            try:
                import threading
                threading.Thread(target=backup_db_to_github, daemon=True).start()
            except Exception as be:
                logger.error(f"DB auto-backup failed: {be}")
        except Exception as e:
            # Rollback
            if os.path.exists(backup_path):
                shutil.move(backup_path, DB_PATH)
            raise e

    except Exception as e:
        logger.error(f"Error importing database: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        await send_and_track_message(update, context, text="⚠️ *Import Failed*\n\nFailed to import database. Rollback performed if possible.")

async def send_analytics_page(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Fetches and displays a specific page of analytics."""
    context_data = ANALYTICS_CONTEXT.get(user_id)
    if not context_data:
        return

    ana_type = context_data["type"]
    page = context_data["page"]
    
    query = update.callback_query
    await query.message.edit_text("⏳ _Generating report, please wait..._", parse_mode='Markdown')

    try:
        endpoint = {
            "ana_most": "analytics_most_issued",
            "ana_readers": "analytics_top_readers",
            "ana_overdue": "analytics_overdue"
        }.get(ana_type)
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(f"{API_BASE}/{endpoint}", json={
                "page": page,
                "page_size": ANALYTICS_PAGE_SIZE
            })
            res_data = response.json()
        
        if res_data["status"] != "ok":
            await query.message.edit_text(f"⚠️ *Error*\n\n{res_data.get('message', 'An unknown error occurred.')}")
            return

        result_data = res_data["data"]
        items = result_data["items"]
        total_pages = result_data["total_pages"]
        total_count = result_data["total_count"]

        if not items:
            msg = "📊 *Analytics Report*\n\nNo data available for this category."
        else:
            if ana_type == "ana_most":
                msg = f"🔥 *Most Issued Books*\nTotal: {total_count} | Page: {page}/{total_pages}\n\n"
                for i, item in enumerate(items, 1 + (page-1)*ANALYTICS_PAGE_SIZE):
                    msg += f"{i}) `{item['id']}` — {item['title']} ({item['count']} issues)\n"
            elif ana_type == "ana_readers":
                msg = f"🧑‍🎓 *Top Readers*\nTotal: {total_count} | Page: {page}/{total_pages}\n\n"
                for i, item in enumerate(items, 1 + (page-1)*ANALYTICS_PAGE_SIZE):
                    msg += f"{i}) {item['name']} ({item['count']} books)\n"
            elif ana_type == "ana_overdue":
                msg = f"⏰ *Overdue List*\nTotal: {total_count} | Page: {page}/{total_pages}\n\n"
                for i, item in enumerate(items, 1 + (page-1)*ANALYTICS_PAGE_SIZE):
                    msg += f"{i}) {item['title']} — {item['name']} (Due: {item['due_date']})\n"

        keyboard = []
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("⏮ Prev", callback_data="ana_prev"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("⏭ Next", callback_data="ana_next"))
        
        if nav_row:
            keyboard.append(nav_row)
            
        keyboard.append([InlineKeyboardButton("🔙 Back to Analytics", callback_data="ana_back")])
        keyboard.append([InlineKeyboardButton("🏠 Main Menu", callback_data="nav_menu")])
        
        await query.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Analytics error: {e}")
        await query.message.edit_text("⚠️ *Error*\n\nFailed to generate report. Please try again.")

# ── PRESENTATIONS ────────────────────────────────────────────

PRESENTATIONS_GITHUB_REPO = os.getenv("PRESENTATIONS_GITHUB_REPO", GITHUB_REPO)

# Cache for presentations list (avoids re-fetching on every number input)
_PRESENTATIONS_CACHE = {}  # {user_id: [list of presentations]}

async def _github_fetch_file(filepath):
    """Fetch a file from GitHub via API with retry (bypasses CDN cache of raw URLs)."""
    api_url = f"https://api.github.com/repos/{PRESENTATIONS_GITHUB_REPO}/contents/{filepath}"
    headers = {}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(api_url, headers=headers)
            if response.status_code == 200:
                import base64
                data = response.json()
                content = base64.b64decode(data['content']).decode('utf-8')
                return content, response.status_code
            if response.status_code == 403:
                # Rate limited — wait and retry
                await asyncio.sleep(2)
                continue
            logger.error(f"GitHub API error for {filepath}: {response.status_code}")
            return None, response.status_code
        except Exception as e:
            logger.error(f"GitHub API attempt {attempt+1} failed for {filepath}: {e}")
            if attempt < 2:
                await asyncio.sleep(1)
    return None, 500

async def _github_fetch_file_bytes(filepath):
    """Fetch a file as bytes from GitHub via API with retry (bypasses CDN cache).

    For files >1MB the Contents API omits the base64 ``content`` field.
    In that case we fall back to the ``download_url`` (raw.githubusercontent.com).
    """
    api_url = f"https://api.github.com/repos/{PRESENTATIONS_GITHUB_REPO}/contents/{filepath}"
    headers = {}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(api_url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                # Small file — base64 content is returned inline
                if "content" in data and data.get("encoding") != "none":
                    content = base64.b64decode(data["content"])
                    return content, 200
                # Large file (>1MB) — use download_url instead
                if "download_url" in data:
                    dl_url = data["download_url"]
                    async with httpx.AsyncClient(timeout=60.0) as dl_client:
                        dl_resp = await dl_client.get(dl_url, headers=headers)
                    if dl_resp.status_code == 200:
                        return dl_resp.content, 200
                    logger.error(f"download_url fetch failed: {dl_resp.status_code}")
                    return None, dl_resp.status_code
                logger.error(f"No content or download_url for {filepath}")
                return None, 200
            if response.status_code == 403:
                await asyncio.sleep(2)
                continue
            logger.error(f"GitHub API error for {filepath}: {response.status_code}")
            return None, response.status_code
        except Exception as e:
            logger.error(f"GitHub API attempt {attempt+1} failed for {filepath}: {e}")
            if attempt < 2:
                await asyncio.sleep(1)
    return None, 500

async def cmd_presentations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /presentations command."""
    await show_presentations_list(update, context)

async def show_presentations_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fetch all presentations and show as a numbered list. User types number to download."""
    user_id = update.effective_user.id
    try:
        content, status = await _github_fetch_file("presentations_index.json")

        if status != 200 or not content:
            msg = (
                "📂 PRESENTATIONS\n\n"
                "No study materials available yet.\n"
                "Check back later!"
            )
            keyboard = [[InlineKeyboardButton("Back to Menu", callback_data="nav_menu")]]
            if update.callback_query:
                await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await send_and_track_message(update, context, text=msg, reply_markup=InlineKeyboardMarkup(keyboard), is_result=True, delay=45)
            return

        presentations = json.loads(content)
        if not presentations:
            msg = (
                "📂 PRESENTATIONS\n\n"
                "No study materials available yet.\n"
                "Check back later!"
            )
            keyboard = [[InlineKeyboardButton("Back to Menu", callback_data="nav_menu")]]
            if update.callback_query:
                await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await send_and_track_message(update, context, text=msg, reply_markup=InlineKeyboardMarkup(keyboard), is_result=True, delay=45)
            return

        # Cache for this user
        _PRESENTATIONS_CACHE[user_id] = presentations

        # Number emojis
        num_emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

        # Month names
        months = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        def format_date(date_str):
            try:
                parts = date_str.split("-")
                day = int(parts[2])
                month = months[int(parts[1])]
                year = parts[0]
                return f"{day:02d} {month} {year}"
            except Exception:
                return date_str

        # Build list
        msg = "🎓 PRESENTATIONS\n"
        msg += "━━━━━━━━━━━━━━━━━━━━\n"

        for i, pres in enumerate(presentations, 1):
            topic = pres.get('topic', 'Untitled')
            presenter = pres.get('presenter', 'Unknown')
            event_date = pres.get('event_date', '')

            num = num_emojis[i-1] if i <= len(num_emojis) else f"{i}."

            msg += f"\n{num}  {topic}\n"
            msg += f"   👤 : {presenter}\n"
            msg += f"   🗓️ : {format_date(event_date)}\n"

            if i < len(presentations):
                msg += "\n - - - - - - - - - - - -\n"

        msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"📚 Available : {len(presentations)} files\n"
        msg += f"👉 Reply with 1–{len(presentations)} to download."

        keyboard = [[InlineKeyboardButton("Back to Menu", callback_data="nav_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=reply_markup)
        else:
            await send_and_track_message(update, context, text=msg, reply_markup=reply_markup, is_result=True, delay=45)

        set_user_state(user_id, BROWSING_PRESENTATIONS)

    except Exception as e:
        logger.error(f"Presentations error: {e}")
        error_msg = "Error loading presentations. Please try again."
        keyboard = [[InlineKeyboardButton("Back to Menu", callback_data="nav_menu")]]
        if update.callback_query:
            await update.callback_query.edit_message_text(error_msg, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await send_and_track_message(update, context, text=error_msg, reply_markup=InlineKeyboardMarkup(keyboard), delay=45)

async def handle_presentation_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle user typing a number to download a presentation."""
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    # Check if user typed a number
    if not text.isdigit():
        await send_and_track_message(update, context, text="Please type a number (e.g. 1) to download, or type /menu to go back.", delay=3)
        return

    num = int(text)
    cache = _PRESENTATIONS_CACHE.get(user_id, [])

    if not cache:
        await send_and_track_message(update, context, text="Session expired. Type /presentations to reload the list.", delay=3)
        set_user_state(user_id, CHOOSING)
        return

    if num < 1 or num > len(cache):
        await send_and_track_message(update, context, text=f"Invalid number. Choose between 1 and {len(cache)}.", delay=3)
        return

    pres = cache[num - 1]
    filename = pres.get('file_name', '')
    topic = pres.get('topic', 'Untitled')
    presenter = pres.get('presenter', 'Unknown')

    # Send loading message
    loading_msg = await send_and_track_message(update, context, text=f"Downloading: {topic}...", delay=3)

    try:
        # Get file path from github_url or construct it
        github_url = pres.get('github_url', '')
        if github_url and "/presentations/" in github_url:
            filepath = "presentations/" + github_url.split("/presentations/")[1]
        else:
            filepath = f"presentations/{filename}"

        file_bytes, status = await _github_fetch_file_bytes(filepath)

        if status != 200 or not file_bytes:
            await send_and_track_message(update, context, text="File not found on server. Try again later.")
            return

        ext = filename.split('.')[-1]
        display_name = f"{topic} - {presenter}.{ext}"

        caption = (
            f"📎 {topic}\n\n"
            f"By: {presenter}\n"
            f"Date: {pres.get('event_date', '')}\n\n"
            f"——————————————\n"
            f"© Markhins Central Library"
        )

        await update.message.reply_document(
            document=file_bytes,
            filename=display_name,
            caption=caption
        )

    except Exception as e:
        logger.error(f"Presentation download error: {e}")
        await send_and_track_message(update, context, text="Download failed. Please try again.")

async def download_presentation(update: Update, context: ContextTypes.DEFAULT_TYPE, filename: str):
    """Download a presentation via inline button callback."""
    query = update.callback_query
    await query.answer("Downloading file...")

    try:
        # Fetch index to get metadata
        index_content, _ = await _github_fetch_file("presentations_index.json")

        github_url = None
        topic = filename
        presenter = "Unknown"
        event_date = ""

        if index_content:
            for pres in json.loads(index_content):
                if pres.get('file_name', '') == filename:
                    github_url = pres.get('github_url', '')
                    topic = pres.get('topic', filename)
                    presenter = pres.get('presenter', 'Unknown')
                    event_date = pres.get('event_date', '')
                    break

        # Get file via API
        filepath = f"presentations/{filename}"
        file_bytes, status = await _github_fetch_file_bytes(filepath)

        if status != 200 or not file_bytes:
            await query.message.reply_text("File not found on server.")
            return

        ext = filename.split('.')[-1]
        display_name = f"{topic} - {presenter}.{ext}"

        caption = (
            f"📎 {topic}\n\n"
            f"By: {presenter}\n"
            f"Date: {event_date}\n\n"
            f"——————————————\n"
            f"© Markhins Central Library"
        )

        await query.message.reply_document(
            document=file_bytes,
            filename=display_name,
            caption=caption
        )

    except Exception as e:
        logger.error(f"Presentation download error: {e}")
        await query.message.reply_text("Download failed. Please try again.")

if __name__ == '__main__':
    if not BOT_RUNNING:
        BOT_RUNNING = True
        logger.info("Initializing bot system...")
        app_instance = init_bot()
        
        logger.info("Bot started and waiting for messages...")
        # drop_pending_updates=True prevents 409 Conflict errors during redeploys/restarts
        # allowed_updates=Update.ALL_TYPES ensures all update types are handled correctly
        app_instance.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
    else:
        logger.warning("Bot is already running. Skipping duplicate startup.")
