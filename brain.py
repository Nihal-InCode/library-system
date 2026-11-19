import os
import sqlite3
import asyncio
import hmac
import logging
from datetime import datetime
import time
from typing import Set, Dict, Any, List, Tuple
from flask import Flask, request, jsonify
from flask_cors import CORS
import base64
import requests as _req

# --- CONFIGURATION ---
DB_PATH = os.environ.get("LIBRARY_DB_PATH", "").strip() or "islamic_library.db"
IS_RAILWAY = any(os.environ.get(v) for v in ("RAILWAY_ENVIRONMENT", "RAILWAY_PROJECT_ID", "RAILWAY_SERVICE_NAME"))
if IS_RAILWAY and not os.environ.get("LIBRARY_DB_PATH", "").strip():
    raise RuntimeError("LIBRARY_DB_PATH must be configured on Railway; refusing to use a local database copy")
IMAGES_DIR = "images"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO = "Nihal-InCode/library-system"
DB_RELEASE_TAG = "db-backup-latest"

# --- LOGGING ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
def get_db_connection():
    """Create a new SQLite connection for each request."""
    return sqlite3.connect(DB_PATH, timeout=10)

def query_db(query, params=()):
    """Execute a SELECT query and return results."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return []
    finally:
        conn.close()

# --- IMAGE HANDLING ---
def get_student_image_base64(student_id: str) -> str:
    """Get student image as base64 string."""
    for ext in [".jpg", ".png", ".JPG", ".PNG"]:
        path = os.path.join(IMAGES_DIR, f"{student_id}{ext}")
        if os.path.exists(path):
            try:
                with open(path, "rb") as img_file:
                    return base64.b64encode(img_file.read()).decode('utf-8')
            except Exception as e:
                logger.error(f"Error reading student image {student_id}: {e}")
                break
    return ""

# --- FLASK APP ---
app = Flask(__name__)
CORS(app)

# --- GITHUB RELEASES BACKUP / RESTORE ---

def backup_db_to_github():
    """Push the current DB to a GitHub Release (does NOT trigger Railway webhooks)."""
    if not GITHUB_TOKEN:
        logger.warning("GITHUB_TOKEN not set — DB backup to GitHub skipped")
        return False
    if not os.path.exists(DB_PATH) or os.path.getsize(DB_PATH) == 0:
        logger.warning("DB file missing or empty — backup skipped")
        return False

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    api = f"https://api.github.com/repos/{GITHUB_REPO}"

    try:
        # 1. Get HEAD sha (needed to create a lightweight tag)
        r = _req.get(f"{api}/git/refs/heads/main", headers=headers, timeout=10)
        if r.status_code != 200:
            logger.error(f"backup_db: can't get HEAD sha: {r.status_code}")
            return False
        head_sha = r.json()["object"]["sha"]

        # 2. Delete old release + tag if they exist
        r = _req.get(f"{api}/releases/tags/{DB_RELEASE_TAG}", headers=headers, timeout=10)
        if r.status_code == 200:
            rel = r.json()
            for asset in rel.get("assets", []):
                _req.delete(f"{api}/releases/assets/{asset['id']}", headers=headers, timeout=10)
            _req.delete(f"{api}/releases/{rel['id']}", headers=headers, timeout=10)
        _req.delete(f"{api}/git/refs/tags/{DB_RELEASE_TAG}", headers=headers, timeout=10)

        # 3. Create lightweight tag pointing to HEAD
        _req.post(f"{api}/git/refs", json={
            "ref": f"refs/tags/{DB_RELEASE_TAG}",
            "sha": head_sha,
        }, headers=headers, timeout=10)

        # 4. Create a new release
        r = _req.post(f"{api}/releases", json={
            "tag_name": DB_RELEASE_TAG,
            "name": "Database Backup",
            "body": f"Auto-backup at {datetime.now().isoformat()}",
            "draft": False,
            "prerelease": False,
        }, headers=headers, timeout=10)
        if r.status_code not in (200, 201):
            logger.error(f"backup_db: can't create release: {r.status_code} {r.text[:200]}")
            return False

        release = r.json()
        upload_tpl = release["upload_url"]  # {...?name,label}

        # 5. Upload the .db file as a release asset
        with open(DB_PATH, "rb") as fh:
            db_bytes = fh.read()

        asset_url = upload_tpl.replace("{?name,label}", f"?name={DB_PATH}")
        r = _req.post(
            asset_url,
            data=db_bytes,
            headers={
                "Authorization": f"token {GITHUB_TOKEN}",
                "Content-Type": "application/octet-stream",
            },
            timeout=120,
        )
        if r.status_code in (200, 201):
            logger.info(f"Database backed up to GitHub Release ({len(db_bytes)} bytes)")
            return True
        logger.error(f"backup_db: upload failed: {r.status_code}")
        return False
    except Exception as e:
        logger.error(f"backup_db exception: {e}")
        return False


def restore_db_from_github():
    """Download the DB from GitHub Releases if the local file is missing/empty."""
    if os.path.exists(DB_PATH) and os.path.getsize(DB_PATH) > 1024:
        logger.info("Database already exists locally — skipping restore")
        return True
    if not GITHUB_TOKEN:
        logger.warning("GITHUB_TOKEN not set — cannot restore DB from GitHub")
        return False

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    api = f"https://api.github.com/repos/{GITHUB_REPO}"

    try:
        r = _req.get(f"{api}/releases/tags/{DB_RELEASE_TAG}", headers=headers, timeout=10)
        if r.status_code != 200:
            logger.warning(f"No DB backup found on GitHub (status {r.status_code})")
            return False

        assets = r.json().get("assets", [])
        if not assets:
            logger.warning("DB backup release has no assets")
            return False

        dl_url = assets[0]["browser_download_url"]
        r = _req.get(dl_url, headers=headers, timeout=120)
        if r.status_code == 200:
            with open(DB_PATH, "wb") as fh:
                fh.write(r.content)
            logger.info(f"Database restored from GitHub Release ({len(r.content)} bytes)")
            return True
        logger.error(f"restore_db: download failed: {r.status_code}")
        return False
    except Exception as e:
        logger.error(f"restore_db exception: {e}")
        return False

# --- ENDPOINTS ---

@app.route('/search_book', methods=['POST'])
def search_book():
    """Search for books by code or name with optional pagination."""
    try:
        data = request.get_json()
        term = data.get('term', '').strip()
        page = data.get('page')  # Optional
        page_size = data.get('page_size', 5)
        norm_term = term.upper()
        
        # 1. Get total count for pagination
        count_query = "SELECT COUNT(*) FROM books WHERE id = ? OR title LIKE ?"
        total_count = query_db(count_query, (norm_term, f"%{term}%"))[0][0]
        
        # 2. Build search query
        query = "SELECT id, title, author, category, available_copies FROM books WHERE id = ? OR title LIKE ?"
        params = [norm_term, f"%{term}%"]
        
        if page is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([page_size, (page - 1) * page_size])
            
        results = query_db(query, tuple(params))
        
        books = []
        for res in results:
            id_val, title, author, category, available = res
            books.append({
                "id": id_val,
                "title": title,
                "author": author or "N/A",
                "category": category or "N/A",
                "available": available,
                "status": "Available" if available > 0 else "Issued"
            })
        
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        
        return jsonify({
            "status": "ok",
            "data": {
                "books": books, 
                "count": len(books),
                "total_count": total_count,
                "total_pages": total_pages,
                "current_page": page or 1
            }
        })
    except Exception as e:
        logger.error(f"Error in search_book: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/book_status', methods=['POST'])
def book_status():
    """Get book status and current issuer."""
    try:
        data = request.get_json()
        book_id = data.get('book_id', '').strip().upper()
        
        query = "SELECT title, available_copies FROM books WHERE id = ?"
        book = query_db(query, (book_id,))
        
        if not book:
            return jsonify({"status": "error", "message": "Book not found"})
        
        title, available = book[0]
        response = {
            "status": "ok",
            "data": {
                "id": book_id,
                "title": title,
                "available": available,
                "status": "Available" if available > 0 else "Issued"
            }
        }
        
        if available == 0:
            # Find who has it
            t_query = """
                SELECT m.name, m.batch, t.issue_date, t.due_date 
                FROM transactions t 
                JOIN members m ON t.member_id = m.student_id 
                WHERE t.book_id = ? AND t.status = 'issued' 
                ORDER BY t.issue_date DESC LIMIT 1
            """
            issued_info = query_db(t_query, (book_id,))
            if issued_info:
                name, batch, issue_date, due_date = issued_info[0]
                response["data"]["issued_to"] = {
                    "name": name,
                    "batch": batch or "N/A",
                    "issue_date": issue_date,
                    "due_date": due_date
                }
        
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error in book_status: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/student_details', methods=['POST'])
def student_details():
    """Get complete student profile with history."""
    try:
        data = request.get_json()
        student_id = data.get('student_id', '').strip()
        
        # Fetch student details
        query = "SELECT name, batch FROM members WHERE student_id = ?"
        member = query_db(query, (student_id,))
        
        if not member:
            return jsonify({"status": "error", "message": "Student not found"})
        
        name, batch = member[0]
        
        # Get photo
        photo_base64 = get_student_image_base64(student_id)
        
        # Currently Issued (return_date IS NULL)
        issued_query = """
            SELECT b.id, b.title, t.issue_date 
            FROM transactions t 
            JOIN books b ON t.book_id = b.id 
            WHERE t.member_id = ? AND t.return_date IS NULL
            ORDER BY t.issue_date DESC
        """
        issued_books = query_db(issued_query, (student_id,))
        
        # Returned Books (return_date IS NOT NULL)
        returned_query = """
            SELECT b.id, b.title, t.issue_date, t.return_date 
            FROM transactions t 
            JOIN books b ON t.book_id = b.id 
            WHERE t.member_id = ? AND t.return_date IS NOT NULL
            ORDER BY t.return_date DESC
        """
        returned_books = query_db(returned_query, (student_id,))
        
        # Format data
        issued = [{"id": bid, "title": title, "issue_date": idate} for bid, title, idate in issued_books]
        returned = [{"id": bid, "title": title, "issue_date": idate, "return_date": rdate} for bid, title, idate, rdate in returned_books]
        
        return jsonify({
            "status": "ok",
            "data": {
                "student_id": student_id,
                "name": name,
                "batch": batch or "N/A",
                "photo": photo_base64,
                "issued": issued,
                "returned": returned,
                "has_photo": bool(photo_base64)
            }
        })
    except Exception as e:
        logger.error(f"Error in student_details: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/issue_history', methods=['POST'])
def issue_history():
    """Get transaction history for a book."""
    try:
        data = request.get_json()
        book_id = data.get('book_id', '').strip().upper()
        
        query = """
            SELECT m.name, t.issue_date, t.return_date 
            FROM transactions t 
            JOIN members m ON t.member_id = m.student_id 
            WHERE t.book_id = ? 
            ORDER BY t.issue_date DESC LIMIT 5
        """
        history = query_db(query, (book_id,))
        
        transactions = []
        for name, issue, ret in history:
            transactions.append({
                "name": name,
                "issue_date": issue,
                "return_date": ret if ret else None
            })
        
        return jsonify({
            "status": "ok",
            "data": {
                "book_id": book_id,
                "history": transactions,
                "count": len(transactions)
            }
        })
    except Exception as e:
        logger.error(f"Error in issue_history: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/library_stats', methods=['GET'])
def library_stats():
    """Get library statistics."""
    try:
        query = """
            SELECT 
                (SELECT COUNT(*) FROM books) as total,
                (SELECT SUM(available_copies) FROM books) as available,
                (SELECT COUNT(*) FROM transactions WHERE status = 'issued') as issued
        """
        counts = query_db(query)
        
        if counts:
            total, available, issued = counts[0]
            return jsonify({
                "status": "ok",
                "data": {
                    "total_books": total,
                    "available_copies": available or 0,
                    "issued_books": issued or 0,
                    "timestamp": datetime.now().strftime("%d-%m-%Y %I:%M %p")
                }
            })
        else:
            return jsonify({"status": "error", "message": "Failed to fetch stats"})
    except Exception as e:
        logger.error(f"Error in library_stats: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/analytics_most_issued', methods=['POST'])
def analytics_most_issued():
    """Get most issued books with pagination."""
    try:
        data = request.get_json() or {}
        page = data.get('page', 1)
        page_size = data.get('page_size', 10)
        offset = (page - 1) * page_size

        # Get total count
        count_query = "SELECT COUNT(DISTINCT book_id) FROM transactions"
        total_count = query_db(count_query)[0][0]

        query = """
            SELECT b.id, b.title, COUNT(t.book_id) as issue_count 
            FROM transactions t 
            JOIN books b ON t.book_id = b.id 
            GROUP BY t.book_id 
            ORDER BY issue_count DESC 
            LIMIT ? OFFSET ?
        """
        results = query_db(query, (page_size, offset))
        books = [{"id": r[0], "title": r[1], "count": r[2]} for r in results]
        
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        
        return jsonify({
            "status": "ok", 
            "data": {
                "items": books,
                "total_count": total_count,
                "total_pages": total_pages,
                "current_page": page
            }
        })
    except Exception as e:
        logger.error(f"Error in analytics_most_issued: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/analytics_top_readers', methods=['POST'])
def analytics_top_readers():
    """Get top members by issue count with pagination."""
    try:
        data = request.get_json() or {}
        page = data.get('page', 1)
        page_size = data.get('page_size', 10)
        offset = (page - 1) * page_size

        # Get total count
        count_query = "SELECT COUNT(DISTINCT member_id) FROM transactions"
        total_count = query_db(count_query)[0][0]

        query = """
            SELECT m.name, COUNT(t.member_id) as issue_count 
            FROM transactions t 
            JOIN members m ON t.member_id = m.student_id 
            GROUP BY t.member_id 
            ORDER BY issue_count DESC 
            LIMIT ? OFFSET ?
        """
        results = query_db(query, (page_size, offset))
        readers = [{"name": r[0], "count": r[1]} for r in results]
        
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1

        return jsonify({
            "status": "ok", 
            "data": {
                "items": readers,
                "total_count": total_count,
                "total_pages": total_pages,
                "current_page": page
            }
        })
    except Exception as e:
        logger.error(f"Error in analytics_top_readers: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/analytics_overdue', methods=['POST'])
def analytics_overdue():
    """Get list of overdue transactions with pagination."""
    try:
        data = request.get_json() or {}
        page = data.get('page', 1)
        page_size = data.get('page_size', 10)
        
        query = """
            SELECT b.title, m.name, t.due_date 
            FROM transactions t 
            JOIN books b ON t.book_id = b.id 
            JOIN members m ON t.member_id = m.student_id 
            WHERE t.return_date IS NULL AND t.status = 'issued'
            ORDER BY t.due_date ASC
        """
        results = query_db(query)
        
        overdue_all = []
        now = datetime.now()
        for title, name, due_str in results:
            try:
                for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
                    try:
                        due_date = datetime.strptime(due_str, fmt)
                        if due_date < now:
                            overdue_all.append({"title": title, "name": name, "due_date": due_str})
                        break
                    except ValueError:
                        continue
            except Exception:
                continue
        
        total_count = len(overdue_all)
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        
        # Paginate the list
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_items = overdue_all[start_idx:end_idx]
                
        return jsonify({
            "status": "ok", 
            "data": {
                "items": paginated_items,
                "total_count": total_count,
                "total_pages": total_pages,
                "current_page": page
            }
        })
    except Exception as e:
        logger.error(f"Error in analytics_overdue: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- DATABASE INITIALIZATION ---
def init_db():
    """Create all required tables if they don't exist."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        # Core library tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS books (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                author TEXT,
                category TEXT,
                available_copies INTEGER DEFAULT 1
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS members (
                student_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                batch TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                book_id TEXT,
                member_id TEXT,
                issue_date TEXT,
                due_date TEXT,
                return_date TEXT,
                status TEXT DEFAULT 'issued'
            )
        """)
        # Bot user tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bot_users (
                chat_id INTEGER PRIMARY KEY,
                name TEXT,
                username TEXT,
                role TEXT DEFAULT 'Basic',
                joined_at TEXT,
                last_active TEXT,
                approved_by INTEGER
            )
        """)
        # Audit tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bot_user_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                name TEXT,
                username TEXT,
                action TEXT,
                details TEXT,
                created_at TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bot_admin_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                action TEXT,
                target_user_id INTEGER,
                details TEXT,
                created_at TEXT
            )
        """)
        conn.commit()
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
    finally:
        conn.close()

# --- BOT USER ENDPOINTS ---

@app.route('/upsert_user', methods=['POST'])
def upsert_user():
    """Create or update a bot user record."""
    try:
        data = request.get_json()
        chat_id = data.get('chat_id')
        name = data.get('name')
        username = data.get('username')
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if user exists
        cursor.execute("SELECT chat_id FROM bot_users WHERE chat_id = ?", (chat_id,))
        user = cursor.fetchone()
        
        if user:
            cursor.execute("""
                UPDATE bot_users 
                SET name = ?, username = ?, last_active = ? 
                WHERE chat_id = ?
            """, (name, username, now, chat_id))
        else:
            cursor.execute("""
                INSERT INTO bot_users (chat_id, name, username, role, joined_at, last_active)
                VALUES (?, ?, ?, 'Basic', ?, ?)
            """, (chat_id, name, username, now, now))
            
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        logger.error(f"Error in upsert_user: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/get_bot_users', methods=['POST'])
def get_bot_users():
    """Get paginated list of bot users."""
    try:
        data = request.get_json()
        page = data.get('page', 1)
        page_size = data.get('page_size', 5)
        
        count_query = "SELECT COUNT(*) FROM bot_users"
        total_count = query_db(count_query)[0][0]
        
        query = """
            SELECT name, chat_id, role, joined_at 
            FROM bot_users 
            ORDER BY joined_at DESC 
            LIMIT ? OFFSET ?
        """
        results = query_db(query, (page_size, (page - 1) * page_size))
        
        users = []
        for r in results:
            users.append({
                "name": r[0],
                "chat_id": r[1],
                "role": r[2],
                "joined_at": r[3]
            })
            
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        
        return jsonify({
            "status": "ok",
            "data": {
                "users": users,
                "total_count": total_count,
                "total_pages": total_pages,
                "current_page": page
            }
        })
    except Exception as e:
        logger.error(f"Error in get_bot_users: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/get_user_details', methods=['POST'])
def get_user_details():
    """Get full details for a specific bot user."""
    try:
        data = request.get_json()
        chat_id = data.get('chat_id')
        
        query = "SELECT * FROM bot_users WHERE chat_id = ?"
        result = query_db(query, (chat_id,))
        
        if not result:
            return jsonify({"status": "error", "message": "User not found"})
            
        r = result[0]
        return jsonify({
            "status": "ok",
            "data": {
                "chat_id": r[0],
                "name": r[1],
                "username": r[2],
                "role": r[3],
                "joined_at": r[4],
                "last_active": r[5],
                "approved_by": r[6]
            }
        })
    except Exception as e:
        logger.error(f"Error in get_user_details: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/update_user_role', methods=['POST'])
def update_user_role():
    """Update a user's role."""
    try:
        data = request.get_json()
        chat_id = data.get('chat_id')
        new_role = data.get('role')
        admin_id = data.get('admin_id')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE bot_users 
            SET role = ?, approved_by = ? 
            WHERE chat_id = ?
        """, (new_role, admin_id if new_role == 'Approved' else None, chat_id))
        conn.commit()
        conn.close()
        
        return jsonify({"status": "ok"})
    except Exception as e:
        logger.error(f"Error in update_user_role: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "database_present": os.path.exists(DB_PATH),
        "db_path": DB_PATH,
        "port": int(os.environ.get("PORT", 5000))
    })

@app.route('/log_user_action', methods=['POST'])
def log_user_action():
    """Log a user action to the audit trail."""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        name = data.get('name')
        username = data.get('username')
        action = data.get('action')
        details = data.get('details', '')
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO bot_user_actions (user_id, name, username, action, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, name, username, action, details, now))
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        logger.error(f"Error logging user action: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/get_user_actions', methods=['POST'])
def get_user_actions():
    """Retrieve paginated user actions."""
    try:
        data = request.get_json()
        page = data.get('page', 1)
        page_size = data.get('page_size', 10)
        user_id = data.get('user_id') # Optional filter
        
        query = "SELECT * FROM bot_user_actions"
        params = []
        if user_id:
            query += " WHERE user_id = ?"
            params.append(user_id)
        
        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([page_size, (page - 1) * page_size])
        
        results = query_db(query, tuple(params))
        actions = []
        for r in results:
            actions.append({
                "id": r[0],
                "user_id": r[1],
                "name": r[2],
                "username": r[3],
                "action": r[4],
                "details": r[5],
                "created_at": r[6]
            })
            
        return jsonify({"status": "ok", "data": actions})
    except Exception as e:
        logger.error(f"Error getting user actions: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/log_admin_action', methods=['POST'])
def log_admin_action():
    """Log an admin action to the audit trail."""
    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        action = data.get('action')
        target_user_id = data.get('target_user_id')
        details = data.get('details', '')
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO bot_admin_actions (admin_id, action, target_user_id, details, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (admin_id, action, target_user_id, details, now))
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        logger.error(f"Error logging admin action: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/get_admin_actions', methods=['POST'])
def get_admin_actions():
    """Retrieve paginated admin actions."""
    try:
        data = request.get_json()
        page = data.get('page', 1)
        page_size = data.get('page_size', 10)
        filter_type = data.get('filter') # Optional filter
        
        query = "SELECT * FROM bot_admin_actions"
        params = []
        if filter_type == 'access':
            query += " WHERE action IN ('Approve User', 'Decline User', 'Change Role')"
        elif filter_type == 'reset':
            query += " WHERE action = 'Reset User Session'"
            
        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([page_size, (page - 1) * page_size])
        
        results = query_db(query, tuple(params))
        actions = []
        for r in results:
            actions.append({
                "id": r[0],
                "admin_id": r[1],
                "action": r[2],
                "target_user_id": r[3],
                "details": r[4],
                "created_at": r[5]
            })
            
        return jsonify({"status": "ok", "data": actions})
    except Exception as e:
        logger.error(f"Error getting admin actions: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ============================================================
# --- EXTERNAL READ-ONLY API (Attendance integration) ---
# Auth: X-API-Key header compared against the LIBRARY_API_KEY env var.
# GET-only. The Telegram bot talks to brain.py over localhost on the
# legacy routes, so it is NOT affected by this guard.
# ============================================================
EXTERNAL_API_PREFIX = "/api/external/"
def _external_query_db(query, params=()):
    """Execute an external API SELECT and fail visibly instead of masking DB errors as empty data."""
    conn = get_db_connection()
    try:
        return conn.execute(query, params).fetchall()
    finally:
        conn.close()

# All transactions that are still out (used by stats / status / overdue)
_OPEN_TX_QUERY = """
    SELECT t.id, t.book_id, t.member_id, t.issue_date, t.due_date, t.status, t.return_date,
           b.title, m.name, m.batch
    FROM transactions t
    LEFT JOIN books b ON b.id = t.book_id
    LEFT JOIN members m ON m.student_id = t.member_id
    WHERE t.status = 'issued' AND t.return_date IS NULL
"""


@app.before_request
def guard_external_api():
    """Require a valid X-API-Key on /api/external/* only."""
    if not request.path.startswith(EXTERNAL_API_PREFIX):
        return None
    configured_key = os.environ.get("LIBRARY_API_KEY", "").strip()
    if not configured_key:
        # Fail closed: never serve external data without a configured secret.
        logger.warning("External API request blocked: LIBRARY_API_KEY is not configured")
        return jsonify({"status": "error", "message": "External API is disabled"}), 503
    provided_key = request.headers.get("X-API-Key", "").strip()
    if not provided_key or not hmac.compare_digest(provided_key.encode("utf-8"), configured_key.encode("utf-8")):
        # Never log the provided key - only the route that was rejected.
        logger.warning("External API unauthorized: %s %s", request.method, request.path)
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
    return None


# --- EXTERNAL API HELPERS ---
DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y",
    "%d/%m/%Y",
)


def parse_date(value):
    """Parse any date/datetime string used across the project.
    Returns a datetime, or None for NULL / empty / unrecognised values."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def is_open_transaction(status, return_date) -> bool:
    """Currently out on loan: status='issued' and no return date."""
    return str(status or "").strip().lower() == "issued" and not str(return_date or "").strip()


def is_overdue_transaction(due_date, status, return_date) -> bool:
    """True only when the item is still out AND due_date parses AND it is past due.
    NULL / empty / unparseable due dates are NEVER treated as overdue."""
    if not is_open_transaction(status, return_date):
        return False
    due = parse_date(due_date)
    if due is None:
        return False
    return due < datetime.now()


def _int_arg(name, default, min_value=None, max_value=None):
    """Read an integer query parameter, clamped to a safe range."""
    raw = request.args.get(name, None)
    try:
        value = int(str(raw).strip()) if raw is not None else default
    except (TypeError, ValueError):
        value = default
    if min_value is not None and value < min_value:
        value = min_value
    if max_value is not None and value > max_value:
        value = max_value
    return value


def _like_escape(term: str) -> str:
    """Escape LIKE wildcards supplied by the client."""
    return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _load_open_transactions():
    """Rows of _OPEN_TX_QUERY plus computed overdue flag, sorted overdue-first."""
    rows = _external_query_db(_OPEN_TX_QUERY)
    records = []
    for row in rows:
        (tid, book_id, member_id, issue_date, due_date, status, return_date,
         title, member_name, batch) = row
        records.append({
            "transaction_id": tid,
            "book_id": book_id,
            "book_title": title or "Unknown",
            "member_id": member_id,
            "member_name": member_name or "Unknown",
            "member_batch": batch,
            "issue_date": issue_date,
            "due_date": due_date,
            "overdue": is_overdue_transaction(due_date, status, return_date),
            "_due": parse_date(due_date),
        })
    # Overdue first, then earliest due date (NULL due dates last)
    records.sort(key=lambda r: (0 if r["overdue"] else 1, r["_due"] or datetime.max, str(r["issue_date"] or "")))
    return records


# --- EXTERNAL ENDPOINT 1: STATISTICS ---
@app.route('/api/external/library/stats', methods=['GET'])
def external_library_stats():
    """Library-wide statistics + currently issued list."""
    try:
        now = datetime.now()
        today = now.date()

        totals = _external_query_db("""
            SELECT COUNT(*),
                   COALESCE(SUM(total_copies), 0),
                   COALESCE(SUM(available_copies), 0)
            FROM books
        """)
        total_titles, total_copies, available_copies = totals[0] if totals else (0, 0, 0)

        member_rows = _external_query_db("SELECT COUNT(*) FROM members")
        registered_members = member_rows[0][0] if member_rows else 0

        open_records = _load_open_transactions()

        issues_today = 0
        for (value,) in _external_query_db("SELECT issue_date FROM transactions WHERE issue_date IS NOT NULL"):
            parsed = parse_date(value)
            if parsed is not None and parsed.date() == today:
                issues_today += 1

        returns_today = 0
        for (value,) in _external_query_db("SELECT return_date FROM transactions WHERE return_date IS NOT NULL"):
            parsed = parse_date(value)
            if parsed is not None and parsed.date() == today:
                returns_today += 1

        issued_list = [{k: v for k, v in rec.items() if k != "_due"} for rec in open_records[:50]]
        overdue_count = sum(1 for rec in open_records if rec["overdue"])

        return jsonify({
            "status": "ok",
            "data": {
                "total_titles": total_titles,
                "total_copies": total_copies,
                "available_copies": available_copies,
                "currently_issued": len(open_records),
                "currently_issued_titles": len({r["book_id"] for r in open_records}),
                "overdue_books": overdue_count,
                "registered_members": registered_members,
                "issues_today": issues_today,
                "returns_today": returns_today,
                "currently_issued_list": issued_list,
                "currently_issued_list_count": len(issued_list),
                "last_updated": now.strftime("%Y-%m-%d %H:%M:%S"),
            }
        })
    except Exception as e:
        logger.error(f"Error in external_library_stats: {e}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500


# --- EXTERNAL ENDPOINT 2: BOOK SEARCH ---
@app.route('/api/external/library/books', methods=['GET'])
def external_books():
    """Search books by id / title / author / category with pagination and filters."""
    try:
        term = (request.args.get('q') or request.args.get('term') or '').strip()
        page = _int_arg('page', 1, min_value=1)
        page_size = _int_arg('page_size', 20, min_value=1, max_value=100)
        status_filter = (request.args.get('status') or '').strip().lower()
        category = (request.args.get('category') or '').strip()

        # Book IDs with open transactions (computed once).
        open_records = _load_open_transactions()
        open_counts = {}
        overdue_book_ids = set()
        for rec in open_records:
            open_counts[rec["book_id"]] = open_counts.get(rec["book_id"], 0) + 1
            if rec["overdue"]:
                overdue_book_ids.add(rec["book_id"])
        open_book_ids = set(open_counts)

        conditions = []
        params = []

        if term:
            like = f"%{_like_escape(term)}%"
            conditions.append(
                "(LOWER(b.id) = LOWER(?)"
                " OR LOWER(b.id) LIKE ? ESCAPE '\\'"
                " OR LOWER(b.title) LIKE ? ESCAPE '\\'"
                " OR LOWER(COALESCE(b.author, '')) LIKE ? ESCAPE '\\'"
                " OR LOWER(COALESCE(b.category, '')) LIKE ? ESCAPE '\\'"
                " OR LOWER(COALESCE(b.shelf, '')) LIKE ? ESCAPE '\\'"
                " OR LOWER(COALESCE(b.row, '')) LIKE ? ESCAPE '\\'"
                " OR LOWER(COALESCE(b.area, '')) LIKE ? ESCAPE '\\')"
            )
            params.extend([term, like, like, like, like, like, like, like])

        if category:
            conditions.append("LOWER(COALESCE(b.category, '')) = LOWER(?)")
            params.append(category)

        if status_filter == "available":
            conditions.append("b.available_copies > 0")
        elif status_filter == "issued":
            if open_book_ids:
                ordered_open_ids = sorted(open_book_ids)
                conditions.append("b.id IN (%s)" % ",".join("?" * len(ordered_open_ids)))
                params.extend(ordered_open_ids)
            else:
                conditions.append("1 = 0")
        elif status_filter == "overdue":
            if overdue_book_ids:
                ordered_overdue_ids = sorted(overdue_book_ids)
                conditions.append("b.id IN (%s)" % ",".join("?" * len(ordered_overdue_ids)))
                params.extend(ordered_overdue_ids)
            else:
                conditions.append("1 = 0")
        elif status_filter:
            return jsonify({
                "status": "error",
                "message": "Invalid status filter. Use: available, issued or overdue."
            }), 400

        where_sql = (" WHERE " + " AND ".join(conditions)) if conditions else ""

        count_rows = _external_query_db(f"SELECT COUNT(*) FROM books b{where_sql}", tuple(params))
        total_count = count_rows[0][0] if count_rows else 0

        data_params = list(params) + [page_size, (page - 1) * page_size]
        rows = _external_query_db(
            f"""
            SELECT b.id, b.title, b.author, b.category,
                   b.total_copies, b.available_copies, b.shelf, b.row, b.area
            FROM books b
            {where_sql}
            ORDER BY b.title COLLATE NOCASE ASC, b.id ASC
            LIMIT ? OFFSET ?
            """,
            tuple(data_params),
        )

        overdue_ids = overdue_book_ids

        books = []
        for book_id, title, author, category_val, total, available, shelf, row, area in rows:
            is_overdue_book = book_id in overdue_ids
            books.append({
                "id": book_id,
                "title": title,
                "author": author or "N/A",
                "category": category_val or "N/A",
                "total_copies": total if total is not None else 0,
                "available_copies": available if available is not None else 0,
                "currently_issued_copies": open_counts.get(book_id, 0),
                "status": "available" if (available or 0) > 0 else "issued",
                "overdue": is_overdue_book,
                "shelf": shelf,
                "row": row,
                "area": area,
            })

        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1

        return jsonify({
            "status": "ok",
            "data": {
                "books": books,
                "count": len(books),
                "total_count": total_count,
                "total_pages": total_pages,
                "current_page": page,
                "page_size": page_size,
            }
        })
    except Exception as e:
        logger.error(f"Error in external_books: {e}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500


# --- EXTERNAL ENDPOINT 3: CATEGORIES ---
@app.route('/api/external/library/books/categories', methods=['GET'])
def external_book_categories():
    """Distinct book categories with counts."""
    try:
        rows = _external_query_db("""
            SELECT CASE WHEN category IS NULL OR TRIM(category) = ''
                        THEN 'Uncategorized' ELSE TRIM(category) END AS cat,
                   COUNT(*),
                   COALESCE(SUM(total_copies), 0),
                   COALESCE(SUM(available_copies), 0)
            FROM books
            GROUP BY cat
            ORDER BY cat COLLATE NOCASE ASC
        """)
        categories = [
            {"category": cat, "titles": titles, "total_copies": copies, "available_copies": available}
            for cat, titles, copies, available in rows
        ]
        return jsonify({
            "status": "ok",
            "data": {"categories": categories, "total_count": len(categories)}
        })
    except Exception as e:
        logger.error(f"Error in external_book_categories: {e}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500


# --- EXTERNAL ENDPOINT 4: BOOK STATUS ---
@app.route('/api/external/library/books/status', methods=['GET'])
def external_books_status():
    """Availability summary, or the status of one book (?book_id=...)."""
    try:
        book_id = (request.args.get('book_id') or '').strip()
        include_holder = (request.args.get('include_holder') or '').strip().lower() in ("1", "true", "yes")

        if book_id:
            rows = _external_query_db(
                "SELECT id, title, author, category, total_copies, available_copies "
                "FROM books WHERE id = ? COLLATE NOCASE",
                (book_id,),
            )
            if not rows:
                return jsonify({"status": "error", "message": "Book not found"}), 404
            book_id, title, author, category, total, available = rows[0]

            open_rows = _external_query_db(_OPEN_TX_QUERY + " AND t.book_id = ? COLLATE NOCASE ORDER BY t.issue_date DESC",
                                 (book_id,))
            overdue = any(is_overdue_transaction(r[4], r[5], r[6]) for r in open_rows)

            data = {
                "id": book_id,
                "title": title,
                "author": author or "N/A",
                "category": category or "N/A",
                "total_copies": total if total is not None else 0,
                "available_copies": available if available is not None else 0,
                "status": "available" if (available or 0) > 0 else "issued",
                "overdue": overdue,
                "open_copies": len(open_rows),
            }

            if include_holder and open_rows:
                holder = open_rows[0]  # (tid, book_id, member_id, issue, due, status, ret, title, name, batch)
                data["current_holder"] = {
                    "member_id": holder[2],
                    "member_name": holder[8] or "Unknown",
                    "member_batch": holder[9],
                    "issue_date": holder[3],
                    "due_date": holder[4],
                }

            return jsonify({"status": "ok", "data": data})

        # Aggregate view
        totals = _external_query_db("""
            SELECT COUNT(*),
                   SUM(CASE WHEN available_copies > 0 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN available_copies <= 0 THEN 1 ELSE 0 END)
            FROM books
        """)
        total_titles, available_titles, issued_titles = totals[0] if totals else (0, 0, 0)

        open_records = _load_open_transactions()
        return jsonify({
            "status": "ok",
            "data": {
                "total_titles": total_titles,
                "available_titles": available_titles or 0,
                "issued_titles": issued_titles or 0,
                "open_copies": len(open_records),
                "overdue_copies": sum(1 for r in open_records if r["overdue"]),
                "open_without_due_date": sum(1 for r in open_records if r["_due"] is None),
                "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        })
    except Exception as e:
        logger.error(f"Error in external_books_status: {e}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500


# --- EXTERNAL ENDPOINT 5: MEMBER SEARCH ---
@app.route('/api/external/library/members/search', methods=['GET'])
def external_member_search():
    """Search members by student ID or name. Returns minimal profile data only."""
    try:
        term = (request.args.get('q') or request.args.get('term') or '').strip()
        limit = _int_arg('limit', 20, min_value=1, max_value=100)

        if not term:
            return jsonify({
                "status": "ok",
                "data": {"members": [], "total_count": 0, "message": "Empty search term"}
            })

        like = f"%{_like_escape(term)}%"
        count_rows = _external_query_db(
            "SELECT COUNT(*) FROM members "
            "WHERE LOWER(student_id) LIKE ? ESCAPE '\\' OR LOWER(name) LIKE ? ESCAPE '\\'",
            (like, like),
        )
        total_count = count_rows[0][0] if count_rows else 0

        rows = _external_query_db(
            """
            SELECT student_id, name, batch, type
            FROM members
            WHERE LOWER(student_id) LIKE ? ESCAPE '\\' OR LOWER(name) LIKE ? ESCAPE '\\'
            ORDER BY CASE WHEN LOWER(student_id) = LOWER(?) THEN 0 ELSE 1 END,
                     name COLLATE NOCASE ASC
            LIMIT ?
            """,
            (like, like, term, limit),
        )

        members = [
            {"student_id": sid, "name": name, "batch": batch, "type": mtype or "student"}
            for sid, name, batch, mtype in rows
        ]
        return jsonify({
            "status": "ok",
            "data": {"members": members, "count": len(members), "total_count": total_count}
        })
    except Exception as e:
        logger.error(f"Error in external_member_search: {e}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500


# --- EXTERNAL ENDPOINT 6: MEMBER HISTORY ---
@app.route('/api/external/library/members/<string:student_id>/history', methods=['GET'])
def external_member_history(student_id):
    """Borrowing summary + complete issue/return history for one member."""
    try:
        student_id = (student_id or "").strip()
        member = _external_query_db(
            "SELECT student_id, name, batch, type FROM members WHERE student_id = ? COLLATE NOCASE",
            (student_id,),
        )
        if not member:
            return jsonify({"status": "error", "message": "Member not found"}), 404

        sid, name, batch, mtype = member[0]

        rows = _external_query_db(
            """
            SELECT t.id, t.book_id, t.issue_date, t.due_date, t.return_date, t.status,
                   b.title, b.author
            FROM transactions t
            LEFT JOIN books b ON b.id = t.book_id
            WHERE t.member_id = ? COLLATE NOCASE
            ORDER BY t.id DESC
            """,
            (sid,),
        )

        history_items = []
        held_items = []
        held_count = 0
        overdue_count = 0
        for tid, book_id, issue_date, due_date, return_date, status, title, author in rows:
            open_tx = is_open_transaction(status, return_date)
            overdue = is_overdue_transaction(due_date, status, return_date)
            item = {
                "transaction_id": tid,
                "book_id": book_id,
                "book_title": title or "Unknown",
                "book_author": author or "N/A",
                "issue_date": issue_date,
                "due_date": due_date,
                "return_date": return_date,
                "status": status or "",
                "state": "held" if open_tx else "returned",
                "is_overdue": overdue,
            }
            if open_tx:
                held_count += 1
                if overdue:
                    overdue_count += 1
                held_items.append(item)
            history_items.append(item)

        borrowed_count = len(history_items)
        returned_count = borrowed_count - held_count

        # Pagination is optional. Without it, return the complete history.
        page_raw = request.args.get('page')
        page_size_raw = request.args.get('page_size')
        if page_raw is None and page_size_raw is None:
            page, page_size = 1, max(len(history_items), 1)
        else:
            page = _int_arg('page', 1, min_value=1)
            page_size = _int_arg('page_size', 100, min_value=1, max_value=500)

        total_count = len(history_items)
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        start = (page - 1) * page_size
        page_items = history_items[start:start + page_size]

        data = {
            "student_id": sid,
            "name": name,
            "batch": batch,
            "type": mtype or "student",
            "summary": {
                "borrowed_count": borrowed_count,
                "returned_count": returned_count,
                "currently_held_count": held_count,
                "overdue_count": overdue_count,
            },
            "currently_held": held_items,
            "history": page_items,
            "total_count": total_count,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": page_size,
            "truncated": False,
        }
        return jsonify({"status": "ok", "data": data})
    except Exception as e:
        logger.error(f"Error in external_member_history: {e}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500


# --- EXTERNAL ENDPOINT 7: RECENT ACTIVITY ---
@app.route('/api/external/library/transactions/recent', methods=['GET'])
def external_recent_transactions():
    """Latest issue and return events (newest first)."""
    try:
        limit = _int_arg('limit', 20, min_value=1, max_value=100)

        rows = _external_query_db("""
            SELECT t.id, t.book_id, t.member_id, t.issue_date, t.due_date, t.return_date, t.status,
                   b.title, m.name
            FROM transactions t
            LEFT JOIN books b ON b.id = t.book_id
            LEFT JOIN members m ON m.student_id = t.member_id
            ORDER BY t.id DESC
            LIMIT 500
        """)

        events = []
        for tid, book_id, member_id, issue_date, due_date, return_date, status, title, member_name in rows:
            returned = bool(str(return_date or "").strip()) or str(status or "").strip().lower() == "returned"
            activity_raw = return_date if returned else issue_date
            events.append((
                parse_date(activity_raw) or datetime.min,
                tid or 0,
                {
                    "transaction_id": tid,
                    "event": "return" if returned else "issue",
                    "activity_date": activity_raw,
                    "book": {"id": book_id, "title": title or "Unknown"},
                    "member": {"student_id": member_id, "name": member_name or "Unknown"},
                    "issue_date": issue_date,
                    "due_date": due_date,
                    "return_date": return_date,
                    "status": status or "",
                },
            ))

        events.sort(key=lambda e: (e[0], e[1]), reverse=True)
        items = [e[2] for e in events[:limit]]

        return jsonify({
            "status": "ok",
            "data": {"items": items, "count": len(items), "limit": limit}
        })
    except Exception as e:
        logger.error(f"Error in external_recent_transactions: {e}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500


# --- EXTERNAL ENDPOINT 8: OVERDUE ---
@app.route('/api/external/library/transactions/overdue', methods=['GET'])
def external_overdue_transactions():
    """Open (never returned) transactions past their due date.
    NULL / unparseable due dates are reported separately, never counted as overdue."""
    try:
        page = _int_arg('page', 1, min_value=1)
        page_size = _int_arg('page_size', 20, min_value=1, max_value=100)
        now = datetime.now()

        overdue_all = []
        missing_due = 0
        for rec in _load_open_transactions():
            if rec["_due"] is None:
                missing_due += 1
                continue
            if rec["_due"] >= now:
                continue
            overdue_all.append({
                "transaction_id": rec["transaction_id"],
                "book": {"id": rec["book_id"], "title": rec["book_title"]},
                "member": {"student_id": rec["member_id"], "name": rec["member_name"]},
                "issue_date": rec["issue_date"],
                "due_date": rec["due_date"],
                "days_overdue": (now - rec["_due"]).days,
            })

        total_count = len(overdue_all)
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        start = (page - 1) * page_size
        page_items = overdue_all[start:start + page_size]

        return jsonify({
            "status": "ok",
            "data": {
                "items": page_items,
                "count": len(page_items),
                "total_count": total_count,
                "missing_due_date_count": missing_due,
                "total_pages": total_pages,
                "current_page": page,
                "page_size": page_size,
                "last_updated": now.strftime("%Y-%m-%d %H:%M:%S"),
            }
        })
    except Exception as e:
        logger.error(f"Error in external_overdue_transactions: {e}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500



# --- ERROR HANDLERS ---
@app.errorhandler(404)
def not_found(error):
    return jsonify({"status": "error", "message": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"status": "error", "message": "Internal server error"}), 500

# --- MAIN ---
if __name__ == '__main__':
    init_db()

    if not os.path.exists(IMAGES_DIR):
        os.makedirs(IMAGES_DIR)
        logger.info(f"Created images directory: {IMAGES_DIR}")

    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Python Brain Backend starting on port {port}...")
    app.run(host='0.0.0.0', port=port, debug=False)
