import os
import asyncio
import logging
from datetime import datetime
from typing import Set, Dict, Any
import httpx
import base64

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

# --- CONFIGURATION ---
TOKEN = "8284783402:AAHkRaxmBOpJ4jYUzboH4cK3XQoRt2iK5Ow"
ADMIN_CHAT_ID = 8291437833
APPROVED_USERS: Set[int] = {ADMIN_CHAT_ID}  # Admin is always approved
PORT = int(os.environ.get("PORT", "8080"))
API_BASE = os.environ.get("API_BASE", f"http://127.0.0.1:{PORT}")

# --- LOGGING ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- MANUAL STATE MANAGEMENT ---
USER_STATES = {}  # {user_id: state_name}
SEARCH_CONTEXT = {}  # {user_id: {"term": str, "page": int}}
USER_PAGINATION_CONTEXT = {}  # {user_id: {"page": int}}
PAGE_SIZE = 5

# State constants
CHOOSING = "CHOOSING"
SEARCHING_BOOK = "SEARCHING_BOOK"
CHECKING_STATUS = "CHECKING_STATUS"
STUDENT_DETAILS = "STUDENT_DETAILS"
ISSUE_HISTORY = "ISSUE_HISTORY"

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

# --- SECURITY HANDLER ---
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_CHAT_ID

def is_authorized(user_id: int) -> bool:
    return is_admin(user_id) or user_id in APPROVED_USERS

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
        await update.message.reply_text(
            "✅ You already have full access to all features.",
            parse_mode='Markdown'
        )
        set_user_state(user_id, CHOOSING)
        return
    
    # Notify User
    await update.message.reply_text(
        "🔐 *Access Request*\n\n"
        "Sending your request to the admin...\n"
        "You will be notified once it's reviewed.",
        parse_mode='Markdown'
    )
    
    # Build admin notification message
    first_name = user.first_name or "Unknown"
    last_name = user.last_name or ""
    full_name = f"{first_name} {last_name}".strip()
    username = f"@{user.username}" if user.username else "Not set"
    
    msg = (
        f"🔐 *New Access Request*\n\n"
        f"👤 *Name:* {full_name}\n"
        f"🔖 *Username:* {username}\n"
        f"🆔 *User ID:* `{user_id}`\n\n"
        f"Approve this user?"
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
        photos = await user.get_profile_photos(limit=1)
        if photos.total_count > 0:
            await context.bot.send_photo(
                chat_id=ADMIN_CHAT_ID,
                photo=photos.photos[0][0].file_id,
                caption=msg,
                parse_mode='Markdown',
                reply_markup=markup
            )
            logger.info(f"Access request sent to admin for user {user_id} (with photo)")
        else:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=msg,
                parse_mode='Markdown',
                reply_markup=markup
            )
            logger.info(f"Access request sent to admin for user {user_id} (no photo)")
    except Exception as e:
        logger.error(f"Failed to send admin request for user {user_id}: {e}")
        await update.message.reply_text(
            "⚠️ Failed to send request. Please try again later.",
            parse_mode='Markdown'
        )
        set_user_state(user_id, CHOOSING)
        return

    await update.message.reply_text(
        "✅ *Request Sent Successfully*\n\n"
        "The admin will review your request.\n"
        "You will be notified once a decision is made.",
        parse_mode='Markdown'
    )
    set_user_state(user_id, CHOOSING)

# --- BOT HANDLERS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for the bot."""
    user = update.effective_user
    logger.info(f"Start command received from user {user.id} ({user.full_name})")
    
    try:
        set_user_state(user.id, CHOOSING)
        await show_main_menu(update, context)
        logger.info(f"Main menu sent to user {user.id}")
    except Exception as e:
        logger.error(f"Error in start handler: {e}", exc_info=True)
        await update.message.reply_text("Sorry, an error occurred. Please try again.")
        clear_user_state(user.id)

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
            ["👥 Bot Users", "❌ Exit"]
        ]
        msg = "👋 *Welcome to the Library Bot*\n_Please select an action below:_"
    else:
        # Limited menu for public users (not approved yet)
        keyboard = [
            ["🔍 Find a Book", "📖 Check Status"],
            ["📊 Library Stats"],
            ["🔐 Request Access"],
            ["❌ Exit"]
        ]
        msg = (
            "👋 *Welcome to the Library Bot*\n\n"
            "📚 You can search books and view library stats.\n\n"
            "🔒 For student profiles and history, please request access."
        )
    
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)
    
    if update.message:
        await update.message.reply_text(msg, reply_markup=reply_markup, parse_mode='Markdown')
    elif update.callback_query:
        await update.callback_query.message.reply_text(msg, reply_markup=reply_markup, parse_mode='Markdown')

async def handle_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Routes the user choice from the main menu."""
    text = update.message.text
    user_id = update.effective_user.id

    # PUBLIC FEATURES (no authorization required)
    if text == "🔍 Find a Book":
        await update.message.reply_text("🔎 Please enter the *Book Code* or *Name* to search:", parse_mode='Markdown', reply_markup=ReplyKeyboardRemove())
        set_user_state(user_id, SEARCHING_BOOK)
        
    elif text == "📖 Check Status":
        await update.message.reply_text("📖 Please enter the *Book Code* to check status:", parse_mode='Markdown', reply_markup=ReplyKeyboardRemove())
        set_user_state(user_id, CHECKING_STATUS)
        
    elif text == "📊 Library Stats":
        await handle_book_count(update, context)
        set_user_state(user_id, CHOOSING)
    
    elif text == "🔐 Request Access":
        if is_authorized(user_id):
            await update.message.reply_text("✅ You already have access to all features.", parse_mode='Markdown')
            set_user_state(user_id, CHOOSING)
        else:
            await request_admin_approval(update, context)
    
    elif text == "📊 Advanced Analytics":
        if not is_admin(user_id):
            await update.message.reply_text("🔒 *Admin Only*\nThis section is restricted to administrators.", parse_mode='Markdown')
            set_user_state(user_id, CHOOSING)
            return
        
        keyboard = [
            [InlineKeyboardButton("🔥 Most Issued Books", callback_data="ana_most")],
            [InlineKeyboardButton("🧑‍🎓 Top Readers", callback_data="ana_readers")],
            [InlineKeyboardButton("⏰ Overdue List", callback_data="ana_overdue")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="nav_menu")]
        ]
        await update.message.reply_text(
            "📊 *Advanced Analytics*\nSelect a report to view:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        set_user_state(user_id, CHOOSING)

    elif text == "👥 Bot Users":
        if not is_admin(user_id):
            await update.message.reply_text("🔒 *Admin Only*\nThis section is restricted to administrators.", parse_mode='Markdown')
            set_user_state(user_id, CHOOSING)
            return
        
        USER_PAGINATION_CONTEXT[user_id] = {"page": 1}
        await send_bot_users_page(update, context, user_id, 1)
    
    elif text == "❌ Exit":
        await handle_exit(update, context)
    
    # RESTRICTED FEATURES (authorization required)
    elif text == "👤 Student Profile":
        if not is_authorized(user_id):
            await update.message.reply_text(
                "🔒 *Access Restricted*\n\n"
                "This feature requires admin approval.\n\n"
                "Please use 🔐 *Request Access* button to get approved.",
                parse_mode='Markdown'
            )
            set_user_state(user_id, CHOOSING)
            return
        await update.message.reply_text("👤 Please enter the *Student ID* or *Name*:", parse_mode='Markdown', reply_markup=ReplyKeyboardRemove())
        set_user_state(user_id, STUDENT_DETAILS)
        
    elif text == "🕘 Reading History":
        if not is_authorized(user_id):
            await update.message.reply_text(
                "🔒 *Access Restricted*\n\n"
                "This feature requires admin approval.\n\n"
                "Please use 🔐 *Request Access* button to get approved.",
                parse_mode='Markdown'
            )
            set_user_state(user_id, CHOOSING)
            return
        await update.message.reply_text("🕘 Please enter the *Book Code* to view history:", parse_mode='Markdown', reply_markup=ReplyKeyboardRemove())
        set_user_state(user_id, ISSUE_HISTORY)
    
    else:
        await update.message.reply_text("⚠️ Unknown option. Please use the menu buttons.")
        set_user_state(user_id, CHOOSING)

async def handle_search_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processes search book request and initializes pagination."""
    user_id = update.effective_user.id
    term = update.message.text.strip()
    
    # Initialize search context
    SEARCH_CONTEXT[user_id] = {"term": term, "page": 1}
    
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
            msg = f"Error: {data.get('message', 'Unknown error')}"
            if update.callback_query:
                await update.callback_query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)
            return
        
        books = data["data"]["books"]
        total_pages = data["data"]["total_pages"]
        total_count = data["data"]["total_count"]
        
        if not books:
            msg = "No books found matching that term."
            if update.callback_query:
                await update.callback_query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)
            return

        # Format results into a single professional message
        message_text = f"📢 *Library Notice*\n\n📘 *Search Results for:* `{term}`\n"
        message_text += f"🔢 *Total Found:* {total_count}\n"
        message_text += f"📄 *Page:* {page}/{total_pages}\n\n"
        
        for book in books:
            status_icon = "✅" if book["available"] > 0 else "❌"
            message_text += (
                f"📚 *Code:* `{book['id']}`\n"
                f"📘 *Title:* {book['title']}\n"
                f"📊 *Status:* {status_icon} {'Available' if book['available'] > 0 else 'Issued'}\n"
                f"-------------------\n"
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
            await update.message.reply_text(
                message_text, 
                reply_markup=reply_markup, 
                parse_mode='Markdown'
            )
            
    except Exception as e:
        logger.error(f"Error in send_search_page: {e}")
        error_msg = "Failed to fetch results. Please try again."
        if update.callback_query:
            await update.callback_query.edit_message_text(error_msg)
        else:
            await update.message.reply_text(error_msg)
    
    set_user_state(user_id, CHOOSING)

async def handle_book_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processes book status request."""
    user_id = update.effective_user.id
    book_id = update.message.text.strip().upper()
    
    await update.effective_chat.send_action(ChatAction.TYPING)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/book_status", json={"book_id": book_id})
            data = response.json()
        
        if data["status"] != "ok":
            await update.message.reply_text(f"Book code not found.")
            return
        
        book = data["data"]
        state_text = "✅ Available" if book["available"] > 0 else "❌ Issued"
        
        msg = (
            f"📢 *Library Notice*\n\n"
            f"📖 *Book Status Report*\n\n"
            f"📚 *Book Code:* `{book['id']}`\n"
            f"📘 *Title:* {book['title']}\n"
            f"📊 *Status:* {state_text}\n"
        )
        
        if book["available"] == 0 and "issued_to" in book:
            issued = book["issued_to"]
            msg += (
                f"\n👤 *Issued To:* {issued['name']} ({issued['batch']or 'N/A'})\n"
                f"📅 *Issue Date:* {issued['issue_date']}\n"
                f"📅 *Due Date:* {issued['due_date']}\n\n"
                f"⚠️ _Please ensure timely return._"
            )
        
        await update.message.reply_text(msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error fetching book status: {e}")
        await update.message.reply_text("Failed to fetch book status. Please try again.")
    
    # Inline Navigation
    keyboard = [
        [InlineKeyboardButton("📖 Check Another", callback_data="nav_status"),
         InlineKeyboardButton("🔙 Main Menu", callback_data="nav_menu")]
    ]
    await update.message.reply_text("Choose an action:", reply_markup=InlineKeyboardMarkup(keyboard))
    set_user_state(user_id, CHOOSING)

async def handle_student_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processes student details request."""
    user_id = update.effective_user.id
    student_id = update.message.text.strip()
    
    await update.effective_chat.send_action(ChatAction.TYPING)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/student_details", json={"student_id": student_id})
            data = response.json()
        
        if data["status"] != "ok":
            await update.message.reply_text("Student not found.")
            await show_main_menu(update, context)
            set_user_state(user_id, CHOOSING)
            return
        
        student = data["data"]
        
        # 1. Photo Handling
        if student["has_photo"] and student["photo"]:
            try:
                photo_bytes = base64.b64decode(student["photo"])
                await update.message.reply_photo(photo=photo_bytes)
            except Exception as e:
                logger.error(f"Error sending student photo: {e}")

        # 2. Construct Message
        issued_books = student["issued"]
        returned_books = student["returned"]
        
        if not issued_books and not returned_books:
            await update.message.reply_text(
                f"📢 *Library Notice*\n\n👤 *Student Profile*\n\n"
                f"📛 *Name:* {student['name']}\n🏫 *Batch:* {student['batch']}\n\n"
                f"No records found for this student.",
                parse_mode='Markdown'
            )
        else:
            msg = (
                f"📢 *Library Notice*\n\n"
                f"👤 *Student Profile*\n\n"
                f"📛 *Name:* {student['name']}\n"
                f"🏫 *Batch:* {student['batch']}\n\n"
            )
            
            msg += "📚 *Currently Issued:*\n"
            if issued_books:
                for book in issued_books:
                    msg += f"- `{book['id']}` – {book['title']} (Issued: {book['issue_date']})\n"
            else:
                msg += "- None\n"
                
            msg += "\n📜 *Returned Books:*\n"
            if returned_books:
                for book in returned_books:
                    msg += f"- `{book['id']}` – {book['title']} ({book['issue_date']} → {book['return_date']})\n"
            else:
                msg += "- None\n"
                
            await update.message.reply_text(msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error fetching student details: {e}")
        await update.message.reply_text("Failed to fetch student details. Please try again.")
    
    # Inline Navigation - Smart Actions
    keyboard = [
        [InlineKeyboardButton("🕘 View History", callback_data="nav_history"),
         InlineKeyboardButton("📚 Search Another", callback_data="nav_student")],
        [InlineKeyboardButton("🔙 Main Menu", callback_data="nav_menu")]
    ]
    await update.message.reply_text("Choose an action:", reply_markup=InlineKeyboardMarkup(keyboard))
    set_user_state(user_id, CHOOSING)

async def handle_issue_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processes issue history for a book."""
    user_id = update.effective_user.id
    book_id = update.message.text.strip().upper()
    
    await update.effective_chat.send_action(ChatAction.TYPING)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/issue_history", json={"book_id": book_id})
            data = response.json()
        
        if data["status"] != "ok":
            await update.message.reply_text("Failed to fetch history.")
            return
        
        history = data["data"]["history"]
        
        if not history:
            await update.message.reply_text("No transaction history found for this book code.")
        else:
            msg = (
                f"📢 *Library Notice*\n\n"
                f"🕘 *Transaction History*\n"
                f"_Last 5 transactions for_ `{book_id}`\n\n"
            )
            for trans in history:
                ret_text = trans["return_date"] if trans["return_date"] else "Not Returned"
                msg += (
                    f"👤 *{trans['name']}*\n"
                    f"📅 *Issued:* {trans['issue_date']}\n"
                    f"📅 *Returned:* {ret_text}\n"
                    f"---\n"
                )
            await update.message.reply_text(msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error fetching issue history: {e}")
        await update.message.reply_text("Failed to fetch history. Please try again.")

    keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="nav_menu")]]
    await update.message.reply_text("Choose an action:", reply_markup=InlineKeyboardMarkup(keyboard))
    set_user_state(user_id, CHOOSING)

async def handle_book_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays aggregate book counts."""
    # This handler can be triggered text command or callback
    # If callback, update.message might be None
    message_func = update.message.reply_text if update.message else update.callback_query.message.reply_text
    
    await update.effective_chat.send_action(ChatAction.TYPING)
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{API_BASE}/library_stats")
            data = response.json()
        
        if data["status"] != "ok":
            await message_func("Error fetching book counts.")
            return
        
        stats = data["data"]
        msg = (
            f"📢 *Library Notice*\n\n"
            f"📊 *Library Analytics*\n\n"
            f"📚 *Total Unique Books:* {stats['total_books']}\n"
            f"✅ *Available Copies:* {stats['available_copies']}\n"
            f"📖 *Currently Issued:* {stats['issued_books']}\n\n"
            f"_Data accurate as of {stats['timestamp']}_"
        )
        await message_func(msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error fetching library stats: {e}")
        await message_func("Failed to fetch library stats. Please try again.")
        
    # If called from menu (update.message present), show menu again? 
    # Or strict button nav? Let's show menu if it was a text choice.
    if update.message:
        await show_main_menu(update, context)

async def handle_exit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Exits the conversation."""
    user_id = update.effective_user.id
    await update.message.reply_text("Session closed. Use /start to begin again.", reply_markup=ReplyKeyboardRemove())
    clear_user_state(user_id)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancels and ends the conversation."""
    user_id = update.effective_user.id
    await update.message.reply_text("Operation cancelled.", reply_markup=ReplyKeyboardRemove())
    await show_main_menu(update, context)
    set_user_state(user_id, CHOOSING)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles all inline button clicks."""
    query = update.callback_query
    await query.answer()
    
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
            logger.info(f"Admin approved user {target_id}")
            
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
                        "✅ *Your access has been approved.*\n\n"
                        "You can now use all features including:\n"
                        "• 👤 Student Profile\n"
                        "• 🕘 Reading History\n\n"
                        "Use /start to see the updated menu."
                    ),
                    parse_mode='Markdown'
                )
                logger.info(f"Notified user {target_id} of approval")
            except Exception as e:
                logger.error(f"Failed to notify user {target_id}: {e}")
            
            await query.answer("✅ User approved", show_alert=False)
                
        elif data.startswith("decline_"):
            logger.info(f"Admin declined user {target_id}")
            
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
                        "❌ *Your access request was declined.*\n\n"
                        "Contact library admin for more information."
                    ),
                    parse_mode='Markdown'
                )
                logger.info(f"Notified user {target_id} of decline")
            except Exception as e:
                logger.error(f"Failed to notify user {target_id}: {e}")
            
            await query.answer("❌ User declined", show_alert=False)
        
        return

    # 3. Analytics Actions
    if data.startswith("ana_"):
        if not is_admin(user_id):
            await query.answer("🔒 Admin only", show_alert=True)
            return
            
        await query.message.edit_text("⏳ _Generating report..._", parse_mode='Markdown')
        
        try:
            endpoint = {
                "ana_most": "analytics_most_issued",
                "ana_readers": "analytics_top_readers",
                "ana_overdue": "analytics_overdue"
            }.get(data)
            
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(f"{API_BASE}/{endpoint}")
                res_data = response.json()
            
            if res_data["status"] != "ok":
                await query.message.edit_text(f"❌ Error: {res_data.get('message', 'Unknown error')}")
                return

            items = res_data["data"]
            if not items:
                msg = "📊 *Analytics Report*\n\nNo data found for this category."
            else:
                if data == "ana_most":
                    msg = "🔥 *Most Issued Books (Top 10)*\n\n"
                    for i, item in enumerate(items, 1):
                        msg += f"{i}) `{item['id']}` — {item['title']} ({item['count']} issues)\n"
                elif data == "ana_readers":
                    msg = "🧑‍🎓 *Top Readers (Top 10)*\n\n"
                    for i, item in enumerate(items, 1):
                        msg += f"{i}) {item['name']} ({item['count']} books)\n"
                elif data == "ana_overdue":
                    msg = "⏰ *Overdue List (Top 10)*\n\n"
                    for i, item in enumerate(items, 1):
                        msg += f"{i}) {item['title']} — {item['name']} (Due: {item['due_date']})\n"
            
            keyboard = [[InlineKeyboardButton("🔙 Back to Analytics", callback_data="ana_back")],
                        [InlineKeyboardButton("🏠 Main Menu", callback_data="nav_menu")]]
            await query.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Analytics error: {e}")
            await query.message.edit_text("❌ Failed to generate report. Please try again.")
        return

    if data == "ana_back":
        keyboard = [
            [InlineKeyboardButton("🔥 Most Issued Books", callback_data="ana_most")],
            [InlineKeyboardButton("🧑‍🎓 Top Readers", callback_data="ana_readers")],
            [InlineKeyboardButton("⏰ Overdue List", callback_data="ana_overdue")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="nav_menu")]
        ]
        await query.message.edit_text("📊 *Advanced Analytics*\nSelect a report to view:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    # 2. Navigation Actions mechanism (State transitions via button)
    if data == "nav_menu":
        SEARCH_CONTEXT.pop(user_id, None)  # Clear search context
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
        await query.message.reply_text("🔎 Please enter *Book Code* or *Name*:", parse_mode='Markdown', reply_markup=ReplyKeyboardRemove())
        set_user_state(user_id, SEARCHING_BOOK)
        
    elif data == "nav_status":
        await query.message.reply_text("📖 Enter *Book Code*:", parse_mode='Markdown', reply_markup=ReplyKeyboardRemove())
        set_user_state(user_id, CHECKING_STATUS)
        
    elif data == "nav_student":
        # Check auth again just in case
        if not is_authorized(user_id):
            await query.message.reply_text("🚫 details restricted.")
            set_user_state(user_id, CHOOSING)
            return
        await query.message.reply_text("👤 Enter *Student ID* or *Name*:", parse_mode='Markdown', reply_markup=ReplyKeyboardRemove())
        set_user_state(user_id, STUDENT_DETAILS)
        
    elif data == "nav_history":
        if not is_authorized(user_id):
            await query.message.reply_text("🚫 history restricted.")
            set_user_state(user_id, CHOOSING)
            return
        await query.message.reply_text("🕘 Enter *Book Code*:", parse_mode='Markdown', reply_markup=ReplyKeyboardRemove())
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
    
    current_state = get_user_state(user_id)
    
    logger.info(f"Message from user {user_id} in state {current_state}: {update.message.text[:50]}")
    
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
        else:
            # Unknown state, reset to CHOOSING
            logger.warning(f"Unknown state {current_state} for user {user_id}, resetting to CHOOSING")
            set_user_state(user_id, CHOOSING)
            await show_main_menu(update, context)
    except Exception as e:
        logger.error(f"Error in handle_message for user {user_id}: {e}", exc_info=True)
        await update.message.reply_text("Sorry, an error occurred. Please try again.")
        set_user_state(user_id, CHOOSING)

# --- RUNTIME GUARD ---
BOT_RUNNING = False

def init_bot():
    """Main function to initialize the bot application."""
    application = ApplicationBuilder().token(TOKEN).build()

    # Add simple handlers instead of ConversationHandler
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('cancel', cancel))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    return application

async def send_bot_users_page(update: Update, context: ContextTypes.DEFAULT_TYPE, admin_id: int, page: int):
    """Displays a paginated list of bot users."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{API_BASE}/get_bot_users", json={"page": page, "page_size": PAGE_SIZE})
            res_data = response.json()
            
        if res_data["status"] != "ok":
            await update.effective_message.reply_text("❌ Error fetching users.")
            return

        users = res_data["data"]["users"]
        total_pages = res_data["data"]["total_pages"]
        
        msg = f"👥 *Bot Users* (Page {page}/{total_pages})\n\n"
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
            await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            
    except Exception as e:
        logger.error(f"Error in send_bot_users_page: {e}")
        await update.effective_message.reply_text("❌ Failed to load users.")

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
            f"🔖 *Username:* @{u['username'] if u['username'] else 'None'}\n"
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
    await update.callback_query.edit_message_text(f"⚠️ *Confirmation*\n\n{text}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def show_role_options(update: Update, context: ContextTypes.DEFAULT_TYPE, target_id: int):
    keyboard = [
        [InlineKeyboardButton("Admin", callback_data=f"setrole_Admin_{target_id}")],
        [InlineKeyboardButton("Approved", callback_data=f"setrole_Approved_{target_id}")],
        [InlineKeyboardButton("Basic", callback_data=f"setrole_Basic_{target_id}")],
        [InlineKeyboardButton("Blocked", callback_data=f"setrole_Blocked_{target_id}")],
        [InlineKeyboardButton("🔙 Cancel", callback_data=f"view_user_{target_id}")]
    ]
    await update.callback_query.edit_message_text("🎭 *Select New Role*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

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
            # If approved, update the in-memory set for immediate effect
            if role == "Approved":
                APPROVED_USERS.add(target_id)
            elif role == "Blocked" and target_id in APPROVED_USERS:
                APPROVED_USERS.remove(target_id)
                
            await show_user_details(update, context, target_id)
        else:
            await update.callback_query.answer("❌ Failed to update role")
    except Exception as e:
        logger.error(f"Error in update_user_role_api: {e}")
        await update.callback_query.answer("❌ API Error")

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
