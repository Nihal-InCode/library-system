import os
import sqlite3
import asyncio
import logging
from datetime import datetime
import time
from typing import Set, Dict, Any, List, Tuple
from flask import Flask, request, jsonify
from flask_cors import CORS
import base64

# --- CONFIGURATION ---
DB_PATH = "islamic_library.db"
IMAGES_DIR = "images"

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
    """Get top 10 most issued books."""
    try:
        query = """
            SELECT b.id, b.title, COUNT(t.book_id) as issue_count 
            FROM transactions t 
            JOIN books b ON t.book_id = b.id 
            GROUP BY t.book_id 
            ORDER BY issue_count DESC LIMIT 10
        """
        results = query_db(query)
        data = [{"id": r[0], "title": r[1], "count": r[2]} for r in results]
        return jsonify({"status": "ok", "data": data})
    except Exception as e:
        logger.error(f"Error in analytics_most_issued: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/analytics_top_readers', methods=['POST'])
def analytics_top_readers():
    """Get top 10 members by issue count."""
    try:
        query = """
            SELECT m.name, COUNT(t.member_id) as issue_count 
            FROM transactions t 
            JOIN members m ON t.member_id = m.student_id 
            GROUP BY t.member_id 
            ORDER BY issue_count DESC LIMIT 10
        """
        results = query_db(query)
        data = [{"name": r[0], "count": r[1]} for r in results]
        return jsonify({"status": "ok", "data": data})
    except Exception as e:
        logger.error(f"Error in analytics_top_readers: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/analytics_overdue', methods=['POST'])
def analytics_overdue():
    """Get list of overdue transactions."""
    try:
        # Assuming date format is DD-MM-YYYY or YYYY-MM-DD. 
        # For SQLite comparison to work reliably on text, we'd ideally want YYYY-MM-DD.
        # However, we'll fetch all active issues and filter in Python for safety 
        # if the DB format is non-standard, or use a simple date string comparison.
        today = datetime.now().strftime("%Y-%m-%d")
        
        query = """
            SELECT b.title, m.name, t.due_date 
            FROM transactions t 
            JOIN books b ON t.book_id = b.id 
            JOIN members m ON t.member_id = m.student_id 
            WHERE t.return_date IS NULL AND t.status = 'issued'
            ORDER BY t.due_date ASC
        """
        results = query_db(query)
        
        # Filter overdue in Python to be safe with various date formats
        overdue = []
        now = datetime.now()
        for title, name, due_str in results:
            try:
                # Try common formats
                for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
                    try:
                        due_date = datetime.strptime(due_str, fmt)
                        if due_date < now:
                            overdue.append({"title": title, "name": name, "due_date": due_str})
                        break
                    except ValueError:
                        continue
            except Exception:
                continue
                
        return jsonify({"status": "ok", "data": overdue[:10]})
    except Exception as e:
        logger.error(f"Error in analytics_overdue: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- DATABASE INITIALIZATION ---
def init_bot_users_db():
    """Initialize the bot_users table if it doesn't exist."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
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
        conn.commit()
    except Exception as e:
        logger.error(f"Error initializing bot_users table: {e}")
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
        "timestamp": datetime.now().isoformat(),
        "database": os.path.exists(DB_PATH)
    })

# --- ERROR HANDLERS ---
@app.errorhandler(404)
def not_found(error):
    return jsonify({"status": "error", "message": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"status": "error", "message": "Internal server error"}), 500

# --- MAIN ---
if __name__ == '__main__':
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file NOT FOUND at: {DB_PATH}")
        exit(1)
    
    init_bot_users_db()
    
    if not os.path.exists(IMAGES_DIR):
        os.makedirs(IMAGES_DIR)
        logger.info(f"Created images directory: {IMAGES_DIR}")
    
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Python Brain Backend starting on port {port}...")
    app.run(host='0.0.0.0', port=port, debug=False)
