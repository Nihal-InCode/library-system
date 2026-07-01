# 📚 Markhins Library Management System

A professional, high-end Library Management System built with **Python 3** and **PySide6 (Qt Quick/QML)**. Featuring a luxurious Midnight Indigo glassmorphic design and real-time Telegram notifications.

---

## 🛠 Prerequisites & Downloads

To run this application, you need to have the following installed on your system:

### 1. Python 3.10+
Install the latest version of Python. 
- **Download:** [python.org/downloads](https://www.python.org/downloads/)
- **Note:** During installation on Windows, make sure to check the box **"Add Python to PATH"**.

### 2. Required Libraries (dependencies)
Open your terminal (Command Prompt or PowerShell) in the project folder and run the following command to install all necessary packages:

```bash
# Install UI dependencies
pip install PySide6

# Install Backend & Notification dependencies
pip install Flask Flask-CORS Werkzeug python-telegram-bot httpx
```

*(Alternatively, you can run: `pip install -r requirements.txt` followed by `pip install -r qt_ui/requirements.txt`)*

---

## 🚀 How to Run

### Option 1: Desktop UI (Recommended)
This starts the professional management dashboard.
1. Navigate to the project folder.
2. Open a terminal and run:
   ```bash
   python qt_ui/main.py
   ```

### Option 2: Telegram Bot
If you want to run the Telegram integration separately:
1. Open a terminal and run:
   ```bash
   python telegram_bot.py
   ```

---

## ✨ Key Features
- **Modern Dashboard:** High-end glassmorphic UI with Midnight Indigo gradients.
- **Smart Search:** Instant search for books and members with auto-focus.
- **Transaction History:** Detailed logs and analytics for issued/returned books.
- **Real-time Notifications:** Telegram integration for due alerts and updates.
- **Fresh Start:** Quick-reset feature for transactions while maintaining core data.

## 👥 Developers
- **Nihal**
- **Ansah**
- **Swamad**
*(12th Batch Team)*

---
Designed with ❤️ for Markhins Central Library.