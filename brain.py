import os
import sqlite3
import asyncio
import logging
from datetime import datetime
import time
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
    """Search for books by code or name."""
    try:
        data = request.get_json()
        term = data.get('term', '').strip()
        norm_term = term.upper()
        
        query = "SELECT id, title, author, category, available_copies FROM books WHERE id = ? OR title LIKE ?"
        results = query_db(query, (norm_term, f"%{term}%"))
        
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
        
        return jsonify({
            "status": "ok",
            "data": {"books": books, "count": len(books)}
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
    
    if not os.path.exists(IMAGES_DIR):
        os.makedirs(IMAGES_DIR)
        logger.info(f"Created images directory: {IMAGES_DIR}")
    
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Python Brain Backend starting on port {port}...")
    app.run(host='0.0.0.0', port=port, debug=False)
