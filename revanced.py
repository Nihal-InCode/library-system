
import customtkinter as ctk
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
from tkinter import messagebox
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import tkinter.ttk as ttk
import time
from tkinter import filedialog
from PIL import Image, ImageTk, ImageDraw, ImageFont
import webbrowser
from tkinter import Canvas
import shutil
import platform
import math
import tkinter.font as tkFont
import threading
import requests
import os, time
from urllib.parse import quote
from PIL import Image, ImageDraw, ImageFont, ImageOps

class GlassButton(ctk.CTkFrame):
    """Liquid Glass Button with hover effects"""
    
    def __init__(self, parent, text, command, width=200, height=60, category="default", **kwargs):
        super().__init__(parent, **kwargs)
        
        self.command = command
        self.width = width
        self.height = height
        self.category = category
        self.is_hovering = False
        self.animation_step = 0
        self.original_width = width
        self.original_height = height
        
        # Category-based color tints (very subtle overlays on glass)
        self.color_schemes = {
            "books": {
                "normal": ("#1E3A5F", "#2C5282"),      # Blue tint
                "hover": ("#2B4C7C", "#3A6BA5"),       # Lighter blue
                "border": "#4A90E2",                   # Blue border
                "text": "#E8F4FD"                      # Light blue text
            },
            "students": {
                "normal": ("#1A4E2E", "#2D6A4F"),      # Green tint
                "hover": ("#266545", "#3A8B5E"),       # Lighter green
                "border": "#52C41A",                   # Green border
                "text": "#E8FDF5"                      # Light green text
            },
            "analytics": {
                "normal": ("#4A1B6B", "#6B3AA0"),      # Purple tint
                "hover": ("#5B2A7C", "#7C4AB1"),       # Lighter purple
                "border": "#9254DE",                   # Purple border
                "text": "#F4E8FF"                      # Light purple text
            },
            "settings": {
                "normal": ("#4A5568", "#6B7280"),      # Grey tint
                "hover": ("#5B6A7C", "#7C8B9A"),       # Lighter grey
                "border": "#8C9196",                   # Grey border
                "text": "#F3F4F6"                      # Light grey text
            },
            "danger": {
                "normal": ("#7F1D1D", "#991B1B"),      # Soft red tint
                "hover": ("#9B2C2C", "#B91C1C"),       # Lighter red
                "border": "#EF4444",                   # Red border
                "text": "#FEE2E2"                      # Light red text
            },
            "default": {
                "normal": ("#1E293B", "#2D3748"),      # Original glass
                "hover": ("#374151", "#4B5563"),       # Lighter glass
                "border": "#6B7280",                   # Original border
                "text": "#F3F4F6"                      # Light text
            }
        }
        
        # Get color scheme for category
        scheme = self.color_schemes.get(category, self.color_schemes["default"])
        self.normal_bg = scheme["normal"]
        self.hover_bg = scheme["hover"]
        self.normal_border = scheme["border"]
        self.hover_border = scheme["border"]
        self.text_color = scheme["text"]
        
        # Configure glass appearance
        self.configure(
            fg_color=self.normal_bg,
            border_width=2,
            border_color=self.normal_border,
            corner_radius=18,  # Curved rectangle (not pill, not sharp)
            width=width,
            height=height
        )
        
        # Create label for text directly on glass
        self.label = ctk.CTkLabel(
            self,
            text=text,
            font=ctk.CTkFont(family="Segoe UI", size=16, weight="bold"),
            text_color=self.text_color
        )
        self.label.pack(expand=True, padx=15, pady=12)  # Increased internal padding
        
        # Bind hover events with smooth animation
        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)
        self.label.bind("<Enter>", self._on_enter)
        self.label.bind("<Leave>", self._on_leave)
        
        # Bind click
        self.bind("<Button-1>", self._on_click)
        self.label.bind("<Button-1>", self._on_click)
    
    def _animate_transition(self, target_bg, target_border):
        """Smooth color transition between states"""
        steps = 12  # Smooth steps for color transition
        for i in range(steps + 1):
            progress = i / steps
            delay = i * 15  # 15ms per frame for liquid smoothness
            self.after(delay, lambda p=progress: self._update_colors(p, target_bg, target_border))
    
    def _update_colors(self, progress, target_bg, target_border):
        """Update colors during animation"""
        if progress >= 1.0:
            self.configure(fg_color=target_bg, border_color=target_border)
        else:
            # Smooth color interpolation
            if progress > 0.5:
                self.configure(fg_color=target_bg, border_color=target_border)
            else:
                self.configure(fg_color=self.normal_bg, border_color=self.normal_border)
    
    def _on_enter(self, event=None):
        """Handle mouse enter - liquid glass hover with color brightening"""
        if not self.is_hovering:
            self.is_hovering = True
            # Animate: color brighten only (no scale)
            self._animate_transition(self.hover_bg, self.hover_border)
    
    def _on_leave(self, event=None):
        """Handle mouse leave - smooth return to normal state"""
        if self.is_hovering:
            self.is_hovering = False
            # Animate: color dim only (no scale)
            self._animate_transition(self.normal_bg, self.normal_border)
    
    def _on_click(self, event=None):
        """Handle button click with press effect"""
        if self.command:
            # Brief press animation with color flash
            press_color = self.hover_bg
            self.configure(fg_color=press_color)
            self.after(80, lambda: self.configure(fg_color=self.hover_bg if self.is_hovering else self.normal_bg))
            self.command()


# === WhatsApp CONFIG ===
# Batches fetched from DB: BS1, BS2, BS3, BS4, BS5, HS1, HS2, HSU1, HSU2

# ------------------ Teachers Mapping ------------------ #
# 9 batches mapped to 9 teachers (all numbers same for now)
# Folder where student photos are stored
STUDENT_PHOTO_DIR = "images"

BATCH_TEACHERS = {
    "BS1": 0,
    "BS2": 0,
    "BS3": 8291437833,
    "BS4": 0,
    "BS5": 0,
    "HS1": 0,
    "HS2": 0,
    "HSU1": 0,
    "HSU2": 0,
}


def get_teacher_chat_id(batch: str) -> int:
    """Return teacher Telegram chat id for given batch"""
    return BATCH_TEACHERS.get(batch, 0)
#SANITISE

# ------------------ WhatsApp Setup ------------------ #
# ------------------ WhatsApp Setup ------------------ #
from typing import Optional

# ------------------ WhatsApp Setup ------------------ #

def send_telegram_message(chat_id: int, text: str, image_path: Optional[str] = None):
    bot_token = "8284783402:AAHkRaxmBOpJ4jYUzboH4cK3XQoRt2iK5Ow"
    if not bot_token:
        print("⚠️ TELEGRAM_BOT_TOKEN is not set")
        return False

    if not chat_id:
        print("⚠️ Telegram chat_id not configured")
        return False

    try:
        if image_path and os.path.exists(image_path):
            url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
            with open(image_path, "rb") as f:
                files = {"photo": f}
                data = {
                    "chat_id": chat_id,
                    "caption": text,
                    "parse_mode": "HTML",
                }
                resp = requests.post(url, data=data, files=files, timeout=30)
        else:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            data = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
            }
            resp = requests.post(url, data=data, timeout=30)

        if resp.status_code != 200:
            print("⚠️ Telegram send error:", resp.text)
            return False
        return True
    except Exception as e:
        print("⚠️ Telegram send error:", e)
        return False



class IslamicLibraryApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        # Font management
        self.font_arabic = tkFont.Font(size=16)   # no family → uses default system Arabic
        self.font_malayalam = tkFont.Font(family="Noto Sans Malayalam Thin", size=14, weight="bold")
        self.font_kannada = tkFont.Font(family="Noto Serif Kannada Thin", size=14, weight="bold")
        self.font_english = tkFont.Font(family="Beckman FREE", size=14, weight="bold")

        # Standard UI Fonts
        self.font_28_bold = ctk.CTkFont(family="Arial", size=28, weight="bold")
        self.font_26_bold = ctk.CTkFont(family="Arial", size=26, weight="bold")
        self.font_24_bold = ctk.CTkFont(family="Arial", size=24, weight="bold")
        self.font_20_bold = ctk.CTkFont(family="Arial", size=20, weight="bold")
        self.font_18_bold = ctk.CTkFont(family="Arial", size=18, weight="bold")
        self.font_16_bold = ctk.CTkFont(family="Arial", size=16, weight="bold")
        self.font_16 = ctk.CTkFont(family="Arial", size=16)
        self.font_14_bold = ctk.CTkFont(family="Arial", size=14, weight="bold")
        self.font_14 = ctk.CTkFont(family="Arial", size=14)
        self.font_14_italic = ctk.CTkFont(family="Arial", size=14, slant="italic")
        self.font_13_bold = ctk.CTkFont(family="Arial", size=13, weight="bold")
        self.font_12_bold = ctk.CTkFont(family="Arial", size=12, weight="bold")
        self.font_12 = ctk.CTkFont(family="Arial", size=12)
        self.font_11 = ctk.CTkFont(family="Arial", size=11)
        self.font_10 = ctk.CTkFont(family="Arial", size=10)

        # In-memory image cache for performance
        self._image_cache = {}
        
        # App Config
        self.title(" Islamic Library Management")
        self.geometry("1300x750")
        self.state('zoomed')  # Force maximize
        self.minsize(1000, 600)
        self.resizable(True, True)
        self.update() # Force layout update
        ctk.set_appearance_mode("dark")
        self.configure(fg_color="#0F172A")  # Background
        
        self.app_start_time = datetime.now()
        self.total_running_time = timedelta(0)
        self.running_time_update_interval = 1000  # Update every second
        self.running_time_label = None
        
        # Modern color scheme
        self.primary_color = "#1E293B"
        self.secondary_color = "#334155"
        self.accent_color = "#38BDF8"
        self.success_color = "#22C55E"
        self.warning_color = "#38BDF8" 
        self.danger_color = "#EF4444"
        self.light_text = "#F8FAFC"
        self.dark_text = "#94A3B8"
        
        # Watermark setup
        self.alpha_step = 0
        self.fading_in = True
        self.fade_colors = [
            "#111111", "#222222", "#333333", "#444444", "#555555", "#666666",
            "#777777", "#888888", "#999999", "#AAAAAA", "#BBBBBB", "#CCCCCC",
            "#DDDDDD", "#EEEEEE", "#FFFFDD"  # soft white-yellow
        ]

        self.watermark_label = ctk.CTkLabel(
            self,
            text="12th batch presents",
            font=self.font_18_bold,
            text_color=self.fade_colors[0],
            anchor="center"
        )
        self.watermark_label.place(x=20, y=20)
        
        self.after(300, self.show_login)
        self.init_db()
        self.current_view = None
        self.view_stack = []
        self.current_librarian = None  # Track current librarian
        self.current_username = None
        self.attendance_id = None
        self.admin_mode = False
        self.app_start_time = datetime.now()
        
        # View frame cache for performance
        self._view_frames = {}
        self._current_view_key = None

        # Handle window closing event
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        

        # Treeview style (row height + colors)
        style = ttk.Style()
        style.configure(
            "Treeview",
            rowheight=45,              # Increased row height for better readability
            background="#1e1e1e",      # Dark background
            fieldbackground="#1e1e1e",
            foreground="white"         # White text
        )
        style.configure(
            "Treeview.Heading",
            font=self.font_12_bold,
            foreground="white",
            background="#2e2e2e"
        )

      
        # Configure Treeview row styles
        
       # self.books_table.tag_configure("arabic", font=self.font_arabic, foreground="green")
        #self.books_table.tag_configure("malayalam", font=self.font_malayalam, foreground="blue")
         #self.books_table.tag_configure("english", font=self.font_english, foreground="black")




    def center_window(self, window, width, height):
        """Center a window on the screen"""
        screen_width = window.winfo_screenwidth()
        screen_height = window.winfo_screenheight()
        x = (screen_width // 2) - (width // 2)
        y = (screen_height // 2) - (height // 2)
        window.geometry(f"{width}x{height}+{x}+{y}")

    def create_popup_window(self, title, width, height):
        """Create a professional-looking popup window"""
        popup = ctk.CTkToplevel(self)
        popup.title(title)
        popup.geometry(f"{width}x{height}")
        self.center_window(popup, width, height)
        popup.grab_set()
        popup.configure(fg_color="#2c3e50")
        
        # Add a header frame with title
        header_frame = ctk.CTkFrame(popup, fg_color=self.primary_color, height=50)
        header_frame.pack(fill="x", pady=(0, 10))
        header_frame.pack_propagate(False)
        
        title_label = ctk.CTkLabel(
            header_frame,
            text=title,
            font=self.font_20_bold,
            text_color=self.light_text
        )
        title_label.pack(expand=True)
        
        return popup

    def on_closing(self):
        """Handle window closing event"""
        # Stop running time updates
        if hasattr(self, 'running_time_label') and self.running_time_label:
            self.running_time_label = None
        
        if self.attendance_id:
            conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
            c = conn.cursor()
            logout_time = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
            c.execute("UPDATE attendance SET logout_time = ?, duration = julianday('now') - julianday(login_time) WHERE id = ?", 
                     (logout_time, self.attendance_id))
            conn.commit()
            conn.close()
        self.destroy()
    def init_db(self):
        """Initialize database with proper schema"""
        conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
        c = conn.cursor()
        c.execute("PRAGMA journal_mode=WAL;")
        
        # Create tables with all required columns
        c.execute('''CREATE TABLE IF NOT EXISTS books
                     (id TEXT PRIMARY KEY,
                      title TEXT NOT NULL,
                      author TEXT,
                      category TEXT,
                      total_copies INTEGER DEFAULT 1,
                      available_copies INTEGER DEFAULT 1,
                      shelf TEXT,
                      row TEXT,
                      area TEXT,
                      added_date TEXT DEFAULT CURRENT_TIMESTAMP)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS members
                     (student_id TEXT PRIMARY KEY,
                      name TEXT NOT NULL,
                      batch TEXT,
                      join_date TEXT DEFAULT CURRENT_TIMESTAMP,
                      type TEXT DEFAULT 'student')''')  # Added type column
        
        c.execute('''CREATE TABLE IF NOT EXISTS transactions
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      book_id TEXT,
                      member_id TEXT,
                      issue_date TEXT,
                      due_date TEXT,
                      return_date TEXT,
                      status TEXT DEFAULT 'issued',
                      librarian TEXT,
                      FOREIGN KEY(book_id) REFERENCES books(id),
                      FOREIGN KEY(member_id) REFERENCES members(student_id))''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS librarians
                     (username TEXT PRIMARY KEY,
                      password TEXT NOT NULL,
                      full_name TEXT)''')
        
        # Create attendance table
        c.execute('''CREATE TABLE IF NOT EXISTS attendance
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      librarian_id TEXT NOT NULL,
                      login_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                      logout_time DATETIME,
                      duration REAL,
                      books_issued INTEGER DEFAULT 0,
                      FOREIGN KEY(librarian_id) REFERENCES librarians(username))''')
        
        # Create ratings table
        c.execute('''CREATE TABLE IF NOT EXISTS ratings
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      book_id TEXT NOT NULL,
                      member_id TEXT NOT NULL,
                      rating INTEGER,  -- 1 for like, 0 for dislike
                      timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                      FOREIGN KEY(book_id) REFERENCES books(id),
                      FOREIGN KEY(member_id) REFERENCES members(student_id))''')
        
        # Insert librarian data if not exists
        librarians = [
            ('ansah', 'markhinss', 'Ansah'),
            ('nihal', 'olavilam', 'Nihal'),
            ('samad', 'kannur', 'Samad'),
            ('shammas', 'kasarkod', 'Shammas'),
            ('shinan', 'kambibane', 'Shinan'),
            ('sinan', 'orakk', 'Sinan'),
            ('hamdan', 'girraffe', 'Hamdan'),
            ('rouf', 'ghatta', 'Rouf'),
            ('kasim', 'kodagu', 'Kasim')
        ]
        
        c.executemany('''INSERT OR IGNORE INTO librarians (username, password, full_name)
                         VALUES (?, ?, ?)''', librarians)
        
        # Pre-populate teachers
        teachers = [
            ('JN', 'JAFAR AHMAD NURANI', 'teacher'),
            ('JM', 'JUNAID KHALEEL NURANI', 'teacher'),
            ('HN', 'HABEEBULLAH NURANI', 'teacher'),
            ('SS', 'SADIQUE SAQAFI', 'teacher'),
            ('AU', 'NASAR AHSANI', 'teacher'),
            ('MS', 'MUJEEB SAQAFI', 'teacher'),
            ('FQ', 'FAYIS QADIRI', 'teacher'),
            ('YQ', 'YASEEN QADIRI', 'teacher'),
            ('JN', 'JUNAID NURANI', 'teacher'),
            ('MQ', 'MAHROOF QADIRI', 'teacher'),
            ('BB', 'BILAL QADIRI', 'teacher'),
            ('MA', 'MUSSTAQ AHMED QADIRI', 'teacher')
        ]
        
        for teacher in teachers:
            c.execute('''INSERT OR IGNORE INTO members (student_id, name, type)
                         VALUES (?, ?, ?)''', teacher)
        
        conn.commit()
        conn.close()

    def format_time_12h(self, time_str):
        """Convert 24h time to 12h format"""
        if not time_str:
            return "N/A"
        
        try:
            # Handle different datetime formats
            if " " in time_str:
                # Try different formats
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %I:%M:%S %p"):
                    try:
                        dt = datetime.strptime(time_str, fmt)
                        return dt.strftime("%Y-%m-%d %I:%M:%S %p")
                    except ValueError:
                        continue
                return time_str  # Return original if no format matched
            else:
                dt = datetime.strptime(time_str, "%Y-%m-%d")
                return dt.strftime("%Y-%m-%d")
        except:
            return time_str

    def calculate_running_time(self):
        """Calculate total running time since app start"""
        return datetime.now() - self.app_start_time
    
    def format_running_time(self, delta):
        """Format running time to human-readable format"""
        total_seconds = int(delta.total_seconds())
        days, remainder = divmod(total_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m {seconds}s"
        elif hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    def update_running_time_display(self):
        """Update the running time display in analytics tab"""
        if self.running_time_label and self.current_view == "analytics":
            running_time = self.calculate_running_time()
            formatted_time = self.format_running_time(running_time)
            self.running_time_label.configure(text=formatted_time)
            
            # Schedule next update
            self.after(1000, self.update_running_time_display)

    def format_duration(self, seconds):
        """Format duration in seconds to a human-readable format"""
        if not seconds:
            return "N/A"
        
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"

    def show_login(self):
        self.state('zoomed') # Ensure maximized
        # === TOP FRAME FOR LOGIN ===
        self.login_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.login_frame.pack(expand=True, pady=20)
    
        # Logo
        try:
            logo_image = self.get_cached_image("logo.png", size=(360, 275))
            if logo_image:
                logo_label = ctk.CTkLabel(self.login_frame, image=logo_image, text="")
                logo_label.image = logo_image
                logo_label.pack(pady=(0, 0))
        except Exception as e:
            print("Logo not found:", e)
    
        # Title
        title_label = ctk.CTkLabel(
            self.login_frame,
            text="🕌 MARKHINS CENTRAL LIBRARY\nLogin to Continue",
            font=self.font_24_bold,
            text_color=self.light_text
        )
        title_label.pack(pady=20)
    
        # Username
        ctk.CTkLabel(self.login_frame, text="Username:", font=self.font_16).pack(pady=(10, 0))
        self.username_entry = ctk.CTkEntry(self.login_frame, font=self.font_16)
        self.username_entry.pack(pady=5)
        
        try:
            self.username_entry.focus_set()
        except:
            pass
        # Password
        ctk.CTkLabel(self.login_frame, text="Password:", font=self.font_16).pack(pady=(10, 0))
        self.password_entry = ctk.CTkEntry(self.login_frame, show="*", font=self.font_16)
        self.password_entry.pack(pady=5)
    
        # Login Button
        login_btn = ctk.CTkButton(
            self.login_frame,
            text="Login",
            command=self.authenticate,
            fg_color=self.primary_color,
            hover_color=self.secondary_color,
            width=200,
            height=40,
            font=self.font_16
        )
        login_btn.pack(pady=20)
    
        # Admin Login Button
        admin_btn = ctk.CTkButton(
            self.login_frame,
            text="Admin Login",
            command=self.authenticate_admin,
            fg_color=self.secondary_color,
            hover_color=self.primary_color,
            width=200,
            height=35,
            font=self.font_14
        )
        admin_btn.pack(pady=10)
    
        # === FOOTER AT BOTTOM OF WINDOW ===
        footer_frame = ctk.CTkFrame(
            self,
            fg_color="#2C3E50",
            corner_radius=30,   # Curved footer background
            height=120
        )
        footer_frame.pack(side="bottom", fill="x", padx=20, pady=10)
        footer_frame.pack_propagate(False)
    
        # Line 1 → Slogan (with heart image instead of emoji)
        slogan_frame = ctk.CTkFrame(footer_frame, fg_color="transparent")
        slogan_frame.pack(pady=(8, 2))
    
        slogan_text1 = ctk.CTkLabel(
            slogan_frame,
            text="Built with",
            text_color="#ECF0F1",
            font=self.font_13_bold
        )
        slogan_text1.pack(side="left", padx=3)
    
        # Heart as an image
        try:
            heart_icon = self.get_cached_image("icons/heart.png", size=(20, 20))
            if heart_icon:
                heart_label = ctk.CTkLabel(slogan_frame, image=heart_icon, text="")
                heart_label.image = heart_icon
                heart_label.pack(side="left", padx=2)
        except:
            pass
    
        slogan_text2 = ctk.CTkLabel(
            slogan_frame,
            text="by 12th Batch",
            text_color="#ECF0F1",
            font=self.font_13_bold
        )
        slogan_text2.pack(side="left", padx=3)

        
        # Line 2 → Social Links (icons only)
        links_frame = ctk.CTkFrame(footer_frame, fg_color="transparent")
        links_frame.pack(pady=2)
    
            # Email icon
        try:
            email_icon = self.get_cached_image("icons/email.png", size=(24, 24))
            if email_icon:
                email_btn = ctk.CTkLabel(
                    links_frame,
                    image=email_icon,
                    text="",
                    cursor="hand2"
                )
                email_btn.image = email_icon
                email_btn.pack(side="left", padx=12)
        except Exception as e:
            print("Email icon missing:", e)
    
        # Instagram icon
        try:
            insta_icon = self.get_cached_image("icons/instagram.png", size=(24, 24))
            if insta_icon:
                insta_btn = ctk.CTkLabel(
                    links_frame,
                    image=insta_icon,
                    text="",
                    cursor="hand2"
                )
                insta_btn.image = insta_icon
                insta_btn.pack(side="left", padx=12)
        except Exception as e:
            print("Instagram icon missing:", e)
    
        # WhatsApp icon
        try:
            wa_icon = self.get_cached_image("icons/whatsapp.png", size=(24, 24))
            if wa_icon:
                wa_btn = ctk.CTkLabel(
                    links_frame,
                    image=wa_icon,
                    text="",
                    cursor="hand2"
                )
                wa_btn.image = wa_icon
                wa_btn.pack(side="left", padx=12)
        except Exception as e:
            print("WhatsApp icon missing:", e)
    
        # Line 3 → Version
        version = ctk.CTkLabel(
            footer_frame,
            text="Version 1.2.0",
            text_color="#999999",
            font=self.font_11
        )
        version.pack(pady=(3, 6))
    
        # === Link Handlers ===
        def open_email(event):
            subject = "MARKHINS LIBRARY"
            body = "Hi Nihal,\n\nI have a question regarding Markhins Library App..."
            url = f"https://mail.google.com/mail/?view=cm&fs=1&to=nihalch.in@gmail.com&su={subject}&body={body}"
            webbrowser.open(url)       
        
        def open_instagram(event):
            webbrowser.open("https://www.instagram.com/turathu_taibah_official/")
    
        def open_whatsapp(event):
            msg = "Hello Nihal, I would like to know more about the Markhins Library App."
            url = f"https://wa.me/918123312736?text={msg}"
            webbrowser.open(url)
        if "email_btn" in locals():
            email_btn.bind("<Button-1>", open_email)
        if "insta_btn" in locals():
            insta_btn.bind("<Button-1>", open_instagram)
        if "wa_btn" in locals():
            wa_btn.bind("<Button-1>", open_whatsapp)
    # Save reference so we can remove later
        self.footer_frame = footer_frame


    
        # Keyboard Navigation
        self.username_entry.bind("<Return>", lambda e: self.password_entry.focus_set())
        self.username_entry.bind("<Down>", lambda e: self.password_entry.focus_set())
        self.password_entry.bind("<Return>", lambda e: self.authenticate())
        self.password_entry.bind("<Up>", lambda e: self.username_entry.focus_set())
    
        # Watermark animation
        self.after(300, self.animate_fade_watermark)


    
    def authenticate_admin(self):
        password = ctk.CTkInputDialog(text="Enter Admin Password:", title="Admin Login").get_input()
        if password == "600093":
            self.admin_mode = True
            self.authenticate()
        else:
            messagebox.showerror("Access Denied", "Invalid admin password")

    def animate_fade_watermark(self):
        """Fade in/out the watermark label without moving corners"""
        if not hasattr(self, 'login_frame') or not self.login_frame.winfo_exists():
            return

        if self.fading_in:
            self.alpha_step += 1
            if self.alpha_step >= len(self.fade_colors) - 1:
                self.alpha_step = len(self.fade_colors) - 1
                self.fading_in = False
        else:
            self.alpha_step -= 1
            if self.alpha_step <= 0:
                self.alpha_step = 0
                self.fading_in = True

        self.watermark_label.configure(text_color=self.fade_colors[self.alpha_step])
        self.after(80, self.animate_fade_watermark)

    def authenticate(self):
        # 🔹 If admin_mode is set, bypass DB and login directly
        if hasattr(self, "admin_mode") and self.admin_mode:
            self.current_librarian = "Administrator"
            self.current_username = "admin"
            
            # Maximize and clear root window
            self.state('zoomed')
            self.update() # Force update
            for widget in self.winfo_children():
                widget.destroy()
                
            self.create_widgets()
            self.update_clock()
            self.after(100, self.fullscreen_launch)
            return

        # 🔹 Normal librarian login from database
        username = self.username_entry.get()
        password = self.password_entry.get()

        def run_auth():
            try:
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                c = conn.cursor()
                c.execute("SELECT full_name FROM librarians WHERE username=? AND password=?", (username, password))
                librarian = c.fetchone()
                
                if librarian:
                    full_name = librarian[0]
                    # Record login
                    login_time = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
                    c.execute("INSERT INTO attendance (librarian_id, login_time) VALUES (?, ?)", (username, login_time))
                    attendance_id = c.lastrowid
                    conn.commit()
                    self.after(0, lambda: self._on_auth_success(full_name, username, attendance_id))
                else:
                    self.after(0, self._on_auth_failure)
                conn.close()
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", f"Database error: {str(e)}"))

        threading.Thread(target=run_auth, daemon=True).start()

    def _on_auth_success(self, full_name, username, attendance_id):
        self.current_librarian = full_name
        self.current_username = username
        self.attendance_id = attendance_id
        
        # Maximize and clear root window entirely
        self.state('zoomed')
        self.update() # Force update
        for widget in self.winfo_children():
            widget.destroy()
            
        self.create_widgets()
        self.update_clock()
        self.after(100, self.fullscreen_launch)

    def _on_auth_failure(self):
        messagebox.showerror("Login Failed", "Invalid username or password")
        self.password_entry.delete(0, 'end')


    def create_widgets(self):
        """Create all UI elements"""
        # Reset view cache to avoid invalid window paths after login/logout
        self._view_frames = {}
        self._current_view_key = None

        # Header Frame
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(pady=10, fill="x")
        
        # === Header with Logo + Title ===
        try:
            logo_img = self.get_cached_image("logo1.png", size=(161, 135))
            if logo_img:
                logo_label = ctk.CTkLabel(self.header_frame, image=logo_img, text="")
                logo_label.image = logo_img
                logo_label.pack(side="left", padx=20, pady=5)
        except Exception as e:
            print("⚠️ Logo not found:", e)
        
        # Title (centered)
        self.title_label = ctk.CTkLabel(
            self.header_frame,
            text="MARKHINS CENTRAL LIBRARY\nاقرأ باسم ربك الذي خلق",
            font=self.font_26_bold,
            text_color=self.light_text,
            anchor="center"
        )
        self.title_label.pack(side="left", expand=True, padx=20, pady=5)
             
        # Navigation Buttons - 2 rows
        self.nav_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.nav_frame.pack(pady=10)
        
        # First row of buttons
        buttons_row1 = [
            ("📖 Add Book", self.add_book, self.primary_color),
            ("✏️ Edit Book", self.edit_book, self.secondary_color),
            ("👤 Add Member", self.add_member, self.primary_color),
            ("🗑️ Delete Book", self.delete_book, self.danger_color),
            ("📚 Issue Book", self.issue_book, self.success_color),
            ("🔄 Return Book", self.return_book, self.accent_color),
            ("ℹ️ About", self.show_about, self.secondary_color),
        ]
        
        for i, (text, command, color) in enumerate(buttons_row1):
            btn = ctk.CTkButton(
                self.nav_frame,
                text=text,
                command=command,
                fg_color=color,
                hover_color=self.secondary_color,
                font=self.font_16,
                corner_radius=10,
                width=150,
                height=45
            )
            btn.grid(row=0, column=i, padx=4, pady=5)
        
        # Second row of buttons
        buttons_row2 = [
            ("🔍 Search Books", self.search_book, self.secondary_color),
            ("👥 View Members", self.view_members, self.primary_color),
            ("📜 View Issued", self.view_issued_books, self.accent_color),
            ("📜 View History", self.view_history, self.secondary_color),
            ("📊 Analytics", self.show_analytics, self.primary_color),
            ("🏠 Home", self.show_home, self.secondary_color),
            ("👤 Search Member", self.search_member, self.accent_color),
            ("🔎 Check Status", self.check_book_status, self.secondary_color)
        ]
        
        for i, (text, command, color) in enumerate(buttons_row2):
            btn = ctk.CTkButton(
                self.nav_frame,
                text=text,
                command=command,
                fg_color=color,
                hover_color=self.secondary_color,
                font=self.font_16,
                corner_radius=10,
                width=150,
                height=45
            )
            btn.grid(row=1, column=i, padx=4, pady=5)
        
        # Admin button if in admin mode
        if self.admin_mode:
            admin_btn = ctk.CTkButton(
                self.nav_frame,
                text="🔒 Admin Panel",
                command=self.admin_panel,
                fg_color=self.danger_color,
                hover_color=self.secondary_color,
                font=self.font_16_bold,
                corner_radius=10,
                width=150,
                height=45
            )
            admin_btn.grid(row=1, column=len(buttons_row2), padx=4, pady=5)
        
        # Main Content Frame
        self.content_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.content_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        # Load initial book list
        self.show_home()
        
    def _switch_view(self, view_key=None):
        """Switch between cached views by key, safely clearing non-cached content"""
        self.update_idletasks() # Refresh layout before switching
        
        # Manage top navigation visibility to prevent duplicates on Dashboard
        if hasattr(self, 'header_frame') and self.header_frame.winfo_exists():
            if view_key == "home":
                self.header_frame.pack_forget()
                self.nav_frame.pack_forget()
            else:
                # Ensure they are repacked at the top for other screens
                self.header_frame.pack_forget()
                self.nav_frame.pack_forget()
                self.content_frame.pack_forget()
                
                self.header_frame.pack(side="top", fill="x", pady=(10, 0))
                self.nav_frame.pack(side="top", pady=10)
                self.content_frame.pack(fill="both", expand=True, padx=20, pady=10)

        # 1. Hide all cached frames safely
        for frame in self._view_frames.values():
            try:
                if frame.winfo_exists():
                    frame.pack_forget()
            except:
                pass
        
        # 2. Destroy non-cached widgets in content_frame
        for widget in self.content_frame.winfo_children():
            if widget not in self._view_frames.values():
                widget.destroy()
        
        # 3. Show requested frame
        if view_key and view_key in self._view_frames:
            try:
                self._view_frames[view_key].pack(fill="both", expand=True)
                self._current_view_key = view_key
                return self._view_frames[view_key]
            except Exception as e:
                print(f"Error packing cached frame {view_key}: {e}")
                del self._view_frames[view_key] # Remove broken frame
        
        self._current_view_key = None
        return None
        

        
        
    # =========================
    # WhatsApp Helper Methods (Selenium Version)
    # =========================
    def _get_teacher_contact_for_student(self, student_id):
        """Return (teacher_name, phone, batch, student_name) for the student's batch; None if missing."""
        try:
            conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
            c = conn.cursor()
            c.execute("SELECT batch, name FROM members WHERE student_id=?", (student_id,))
            row = c.fetchone()
            if not row:
                return None
            batch, student_name = row
            chat_id = get_teacher_chat_id(batch)  # from global mapping
            return "Class Teacher", chat_id, batch, student_name
        except Exception:
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass
            #SANITISE


    def _resolve_student_photo_path(self, student_id):
        """Find a student photo by trying common extensions. Returns path or None."""
        base = os.path.join(STUDENT_PHOTO_DIR, str(student_id))
        for ext in (".jpg", ".jpeg", ".png"):
            p = base + ext
            if os.path.exists(p):
                return p
        return None

    def _format_issue_caption(self, student_name, batch, book_title, due_date):
        formatted_due = datetime.strptime(due_date, '%Y-%m-%d').strftime('%d-%b-%Y')
        return (
            "📢 *Library Notice*\n\n"
            "📚 *Book Issued Successfully*\n\n"
            f"👤 *Student:* {student_name} ({batch})\n"
            f"📘 *Book:* {book_title}\n"
            f"📅 *Due Date:* {formatted_due}\n\n"
            "🙏 _Please remind the student to return the book on or before the due date._"
        )
    
    def _format_return_caption(self, student_name, batch, book_title, rating=None):
        return_date = datetime.now().strftime('%d-%b-%Y')
        msg = (
            "📢 *Library Notice*\n\n"
            "✅ *Book Returned*\n\n"
            f"👤 *Student:* {student_name} ({batch})\n"
            f"📘 *Book:* {book_title}\n"
            f"📅 *Returned On:* {return_date}\n\n"
            "🙏 _Thank you for your cooperation._"
        )
        if rating and rating > 0:
            msg += f"\n⭐ *Rating:* {rating}/5"
        return msg

    def _notify_teacher_issue(self, student_id, book_title, due_date):
        """Notify class teacher when a student issues a book"""
        info = self._get_teacher_contact_for_student(student_id)
        if not info:
            return
        teacher_name, chat_id, batch, student_name = info
        caption = self._format_issue_caption(student_name, batch, book_title, due_date)
        img = self._resolve_student_photo_path(student_id)

        # Send to class teacher
        send_telegram_message(chat_id, caption, image_path=img)

    def _notify_teacher_return(self, student_id, book_title, rating=None):
        """Notify class teacher when a student returns a book"""
        info = self._get_teacher_contact_for_student(student_id)
        if not info:
            return
        teacher_name, chat_id, batch, student_name = info
        caption = self._format_return_caption(student_name, batch, book_title, rating)
        img = self._resolve_student_photo_path(student_id)

        # Send to class teacher
        send_telegram_message(chat_id, caption, image_path=img)

            
            

    def fullscreen_launch(self):
        """Force the app into fullscreen/maximized state"""
        self.state('zoomed')  # Standard maximize for Windows
        try:
            self.attributes('-fullscreen', True) # Attempt true fullscreen
        except:
            pass # Fallback to zoomed state which is already set
        self.bind("<Escape>", lambda event: self.attributes('-fullscreen', False))

    def update_clock(self):
        """Update clock display - safely handles widget recreation"""
        now = datetime.now()
        time_str = now.strftime("%I:%M:%S %p")  # 12-hour format
        date_str = now.strftime("%a, %d %b %Y")
        
        # Check if clock_label exists and belongs to the current header_frame
        if hasattr(self, 'clock_label') and self.clock_label.winfo_exists():
            try:
                self.clock_label.configure(text=f"{time_str}\n{date_str}\nLibrarian: {self.current_librarian}")
            except:
                # If configure fails (e.g. widget becoming invalid), recreate it
                self.clock_label = None
        
        if not hasattr(self, 'clock_label') or self.clock_label is None or not self.clock_label.winfo_exists():
            if hasattr(self, 'header_frame') and self.header_frame.winfo_exists():
                self.clock_label = ctk.CTkLabel(
                    self.header_frame,
                    text=f"{time_str}\n{date_str}\nLibrarian: {self.current_librarian}",
                    font=self.font_14,
                    text_color=self.light_text
                )
                self.clock_label.pack(side="right", padx=20)
        
        self.after(1000, self.update_clock)

    def show_custom_popup(self, member_name, book_title, due_date, member_id):
        popup = self.create_popup_window("Book Issued", 500, 400)
        
        ctk.CTkLabel(
            popup,
            text="✅ Book Issued Successfully!",
            font=self.font_18_bold,
            text_color=self.success_color
        ).pack(pady=(15, 10))
        
        # Member photo
        photo_frame = ctk.CTkFrame(popup, fg_color="transparent")
        photo_frame.pack(pady=10)
        
        img = self.get_member_image(member_id, size=(150, 150))
        img_label = ctk.CTkLabel(photo_frame, image=img, text="")
        img_label.pack()
    
        ctk.CTkLabel(popup, text=f"👤 Member: {member_name}", font=self.font_16).pack(pady=5)
        ctk.CTkLabel(popup, text=f"📘 Book: {book_title}", font=self.font_16).pack(pady=5)
        ctk.CTkLabel(popup, text=f"📅 Due Date: {due_date}", font=self.font_16).pack(pady=5)
    
        ctk.CTkButton(popup, text="OK", command=popup.destroy, font=self.font_14).pack(pady=20)


    



    def get_cached_image(self, path, size):
        """Get image from in-memory cache or load from disk"""
        cache_key = (path, size)
        if cache_key in self._image_cache:
            return self._image_cache[cache_key]
        
        try:
            img = Image.open(path)
            ctk_img = ctk.CTkImage(img, size=size)
            self._image_cache[cache_key] = ctk_img
            return ctk_img
        except Exception as e:
            print(f"Error loading image {path}: {e}")
            return None

    def get_member_image(self, member_id, size=(60, 60)):
        """Get member image from cache or generate thumbnail"""
        cache_key = (f"member_{member_id}", size)
        if cache_key in self._image_cache:
            return self._image_cache[cache_key]

        os.makedirs("images/cache", exist_ok=True)  # Ensure cache folder exists
        thumb_path = f"images/cache/{member_id}_{size[0]}x{size[1]}.png"
    
        # ✅ Use cached thumbnail if available
        if os.path.exists(thumb_path):
            try:
                img = Image.open(thumb_path)
                ctk_img = ctk.CTkImage(img, size=size)
                self._image_cache[cache_key] = ctk_img
                return ctk_img
            except:
                pass  # If broken, regenerate
    
        # 🔍 Find original image
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif']
        image_path = None
        for ext in image_extensions:
            test_path = f"images/{member_id}{ext}"
            if os.path.exists(test_path):
                image_path = test_path
                break
    
        if image_path:
            try:
                # Crop + resize to square
                img = Image.open(image_path)
                img = ImageOps.fit(img, size, Image.Resampling.LANCZOS)
                img.save(thumb_path)  # ✅ Save for future use
                ctk_img = ctk.CTkImage(img, size=size)
                self._image_cache[cache_key] = ctk_img
                return ctk_img
            except Exception as e:
                print(f"Error processing image {image_path}: {e}")
    
        # ❌ No image → placeholder (also cached)
        img = Image.new('RGB', size, color='#2c3e50')
        draw = ImageDraw.Draw(img)
    
        initial = str(member_id)[0].upper() if member_id else "?"
        font_size = min(size) // 2
        try:
            font = ImageFont.truetype("arial.ttf", font_size)
        except:
            try:
                font = ImageFont.truetype("Arial", font_size)
            except:
                font = ImageFont.load_default()
    
        text_bbox = draw.textbbox((0, 0), initial, font=font)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]
        x = (size[0] - text_width) // 2
        y = (size[1] - text_height) // 2
        draw.text((x, y), initial, fill="white", font=font)
        img.save(thumb_path)  # ✅ Save placeholder too
        
        ctk_img = ctk.CTkImage(img, size=size)
        self._image_cache[cache_key] = ctk_img
        return ctk_img

    
    #font detecto
    def get_font_for_text(self, text):
        """Return font tuple depending on script (Arabic / Malayalam / Other)"""
        # Arabic Unicode range: 0600–06FF
        if any('\u0600' <= ch <= '\u06FF' for ch in text):
            return ("Amiri Quran", 20, "bold")   # Arabic font
    
        # Malayalam Unicode range: 0D00–0D7F
        if any('\u0D00' <= ch <= '\u0D7F' for ch in text):
            return ("Noto Serif Malayalam Thin", 20, "bold")  # Malayalam font
    
        # Default (English, etc.)
        return ("Beckman FREE", 16, "bold")#english font
        
    
    
    def search_member(self):
        """Search member by ID - Redesigned"""
        # Track view for back button
        if self.current_view != "search_member":
            self.view_stack.append(self.current_view)
        self.current_view = "search_member"

        if "search_member" not in self._view_frames:
            frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
            self._view_frames["search_member"] = frame
            
            # Header Bar
            header = ctk.CTkFrame(frame, fg_color=self.primary_color, corner_radius=15, height=60)
            header.pack(fill="x", pady=10)
            
            # Back Button
            ctk.CTkButton(header, text="⬅️ Back", command=self.go_back,
                         fg_color=self.secondary_color, hover_color=self.primary_color,
                         width=80, height=35).pack(side="left", padx=15, pady=10)
            
            ctk.CTkLabel(header, text="Search Member", font=self.font_24_bold, text_color=self.light_text).pack(side="left", padx=10)
            
            # Search Section (Centered)
            search_container = ctk.CTkFrame(frame, fg_color=self.secondary_color, corner_radius=15)
            search_container.pack(fill="x", pady=20, padx=50)
            
            input_box = ctk.CTkFrame(search_container, fg_color="transparent")
            input_box.pack(pady=20)
            
            ctk.CTkLabel(input_box, text="Enter Student ID", font=self.font_16, text_color=self.light_text).pack(pady=(0, 5))
            
            self._member_search_entry = ctk.CTkEntry(input_box, font=self.font_16, width=300, height=40, placeholder_text="e.g. 1234")
            self._member_search_entry.pack(side="left", padx=10)
            
            ctk.CTkButton(input_box, text="🔍 Search", command=self._perform_member_search,
                         fg_color=self.accent_color, hover_color=self.primary_color,
                         width=120, height=40, font=self.font_16_bold).pack(side="left", padx=10)
            
            self._member_search_entry.bind("<Return>", lambda e: self._perform_member_search())
            
            # Results Section
            self._member_results_scroll = ctk.CTkScrollableFrame(frame, fg_color="transparent")
            self._member_results_scroll.pack(fill="both", expand=True, padx=10, pady=10)

        self._switch_view("search_member")
        
        # Auto-focus search box
        if hasattr(self, "_member_search_entry"):
            self._member_search_entry.delete(0, 'end')
            self.after(100, self._member_search_entry.focus)
        
        # Clear previous results
        for widget in self._member_results_scroll.winfo_children():
            widget.destroy()

    def _perform_member_search(self):
        member_id = self._member_search_entry.get().strip()
        if not member_id:
            messagebox.showerror("Error", "Please enter a member ID")
            return
        
        results_frame = self._member_results_scroll
        for widget in results_frame.winfo_children():
            widget.destroy()
        
        loading_label = ctk.CTkLabel(results_frame, text="Searching...", font=self.font_16)
        loading_label.pack(pady=20)

        def run_search():
            try:
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                c = conn.cursor()
                
                # Get member details
                c.execute('''SELECT name, batch, type, join_date 
                            FROM members WHERE student_id=?''', (member_id,))
                member = c.fetchone()
                
                if not member:
                    self.after(0, lambda: self._display_no_member(results_frame, loading_label))
                    conn.close()
                    return
                
                name, batch, member_type, join_date = member
                
                # Get book history
                c.execute('''SELECT b.title, t.issue_date, t.return_date 
                            FROM transactions t
                            JOIN books b ON t.book_id = b.id
                            WHERE t.member_id=?
                            ORDER BY t.issue_date DESC''', (member_id,))
                history = c.fetchall()
                
                # Get ratings
                c.execute('''SELECT b.title, r.rating 
                            FROM ratings r
                            JOIN books b ON r.book_id = b.id
                            WHERE r.member_id=?
                            ORDER BY r.timestamp DESC''', (member_id,))
                ratings = c.fetchall()
                
                conn.close()
                
                self.after(0, lambda: self._display_member_results(results_frame, loading_label, member_id, name, batch, member_type, join_date, history, ratings))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", f"Failed to search member: {str(e)}"))

        threading.Thread(target=run_search, daemon=True).start()

    def _display_no_member(self, results_frame, loading_label):
        loading_label.destroy()
        ctk.CTkLabel(results_frame, text="Member not found", font=self.font_16).pack(pady=50)

    def _display_member_results(self, results_frame, loading_label, member_id, name, batch, member_type, join_date, history, ratings):
        loading_label.destroy()
        
        # Card Container for Member Info
        card = ctk.CTkFrame(results_frame, fg_color=self.primary_color, corner_radius=15)
        card.pack(fill="x", pady=10, padx=10)
        
        # Layout: Photo Left, Details Right
        content_box = ctk.CTkFrame(card, fg_color="transparent")
        content_box.pack(fill="x", padx=20, pady=20)
        
        # 1. Photo
        try:
            img = self.get_member_image(member_id, size=(120, 120))
            if img:
                img_label = ctk.CTkLabel(content_box, image=img, text="")
                img_label.image = img
                img_label.pack(side="left", padx=(0, 30), anchor="n")
        except:
             ctk.CTkLabel(content_box, text="👤", font=("Arial", 60)).pack(side="left", padx=(0, 30))
        
        # 2. Details
        details_col = ctk.CTkFrame(content_box, fg_color="transparent")
        details_col.pack(side="left", fill="both", expand=True)
        
        ctk.CTkLabel(details_col, text=name, font=self.font_24_bold, text_color=self.accent_color).pack(anchor="w", pady=(0, 5))
        ctk.CTkLabel(details_col, text=f"Student ID: {member_id}", font=self.font_16_bold, text_color=self.light_text).pack(anchor="w")
        ctk.CTkLabel(details_col, text=f"Batch: {batch}", font=self.font_16, text_color=self.dark_text).pack(anchor="w")
        ctk.CTkLabel(details_col, text=f"Type: {member_type.upper()}", font=self.font_14_bold, text_color=self.secondary_color).pack(anchor="w", pady=(5, 0))
        ctk.CTkLabel(details_col, text=f"Joined: {join_date}", font=self.font_14, text_color=self.dark_text).pack(anchor="w")

        # 3. History Section
        if history:
            ctk.CTkLabel(results_frame, text="📜 Book History", font=self.font_20_bold, text_color=self.accent_color).pack(anchor="w", padx=10, pady=(20, 10))
            
            history_card = ctk.CTkFrame(results_frame, fg_color=self.secondary_color, corner_radius=10)
            history_card.pack(fill="x", padx=10)
            
            # Header Row
            header = ctk.CTkFrame(history_card, fg_color=self.primary_color, height=40)
            header.pack(fill="x", padx=2, pady=2)
            ctk.CTkLabel(header, text="Book Title", font=self.font_14_bold, width=300, anchor="w").pack(side="left", padx=10)
            ctk.CTkLabel(header, text="Status", font=self.font_14_bold, width=100, anchor="center").pack(side="left", padx=10)
            ctk.CTkLabel(header, text="Date", font=self.font_14_bold, width=150, anchor="w").pack(side="left", padx=10)

            for title, issue_date, return_date in history:
                row = ctk.CTkFrame(history_card, fg_color="transparent")
                row.pack(fill="x", pady=2, padx=5)
                
                status = "Returned" if return_date else "Issued"
                status_color = self.success_color if return_date else self.warning_color
                date_fmt = self.format_time_12h(issue_date)

                ctk.CTkLabel(row, text=title, font=self.get_font_for_text(title), width=300, anchor="w").pack(side="left", padx=10)
                ctk.CTkLabel(row, text=status, text_color=status_color, font=self.font_12_bold, width=100).pack(side="left", padx=10)
                ctk.CTkLabel(row, text=date_fmt, font=self.font_12, width=150, anchor="w").pack(side="left", padx=10)

        # 4. Ratings Section
        if ratings:
            ctk.CTkLabel(results_frame, text="⭐ Book Ratings", font=self.font_20_bold, text_color="#FFD700").pack(anchor="w", padx=10, pady=(20, 10))
            
            ratings_card = ctk.CTkFrame(results_frame, fg_color=self.secondary_color, corner_radius=10)
            ratings_card.pack(fill="x", padx=10)

            for title, rating in ratings:
                r_row = ctk.CTkFrame(ratings_card, fg_color="transparent")
                r_row.pack(fill="x", pady=5, padx=10)
                
                ctk.CTkLabel(r_row, text=title, font=self.get_font_for_text(title), anchor="w").pack(side="left", fill="x", expand=True)
                stars = "⭐" * rating
                ctk.CTkLabel(r_row, text=stars, font=("Arial", 16), text_color="#FFD700").pack(side="right")
        
    
    def check_book_status(self):
        """Check book status by ID"""
        status_window = self.create_popup_window("Check Book Status", 600, 400)
        
        # Book ID
        ctk.CTkLabel(status_window, text="Book ID:", font=self.font_16).pack(pady=(10, 0))
        book_id_entry = ctk.CTkEntry(status_window, font=self.font_16)
        book_id_entry.pack(pady=5)
        try:
            book_id_entry.focus_set()
        except:
            pass
        
        def check_status():
            input_code = book_id_entry.get().strip().upper()
            book_id = input_code
            if not book_id:
                messagebox.showerror("Error", "Please enter a book ID")
                return
            
            # Clear previous results
            for widget in results_frame.winfo_children():
                widget.destroy()
            
            # Show loading
            loading_label = ctk.CTkLabel(results_frame, text="Checking status...", font=self.font_16)
            loading_label.pack(pady=20)

            def run_check():
                try:
                    conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                    c = conn.cursor()
                    
                    # Get book details
                    c.execute('''SELECT title, author, available_copies, total_copies 
                                FROM books WHERE id=?''', (book_id,))
                    book = c.fetchone()
                    
                    if not book:
                        self.after(0, lambda: self._display_no_book(results_frame, loading_label))
                        conn.close()
                        return
                    
                    title, author, available, total = book
                    
                    # Check if book is currently issued to anyone
                    c.execute('''SELECT m.name, m.student_id, t.issue_date, t.due_date 
                                FROM transactions t
                                JOIN members m ON t.member_id = m.student_id
                                WHERE t.book_id=? AND t.status='issued' 
                                ORDER BY t.issue_date DESC''', (book_id,))
                    issued_to = c.fetchall()
                    
                    conn.close()
                    self.after(0, lambda: self._display_book_status(results_frame, loading_label, title, author, available, total, issued_to))
                except Exception as e:
                    self.after(0, lambda: messagebox.showerror("Error", f"Failed to check book status: {str(e)}"))

            threading.Thread(target=run_check, daemon=True).start()

        # Search button
        search_btn = ctk.CTkButton(
            status_window,
            text="🔍 Check Status",
            command=check_status,
            fg_color=self.secondary_color,
            hover_color=self.primary_color,
            font=self.font_16
        )
        search_btn.pack(pady=10)
        
        # Bind Enter key to search
        book_id_entry.bind("<Return>", lambda event: check_status())

        # Results frame with scroll
        results_frame = ctk.CTkScrollableFrame(status_window)
        results_frame.pack(fill="both", expand=True, padx=10, pady=10)

    def _display_no_book(self, results_frame, loading_label):
        loading_label.destroy()
        ctk.CTkLabel(results_frame, text="Book not found", font=self.font_16).pack(pady=50)

    def _display_book_status(self, results_frame, loading_label, title, author, available, total, issued_to):
        loading_label.destroy()
        
        # Display book info
        info_frame = ctk.CTkFrame(results_frame, fg_color="transparent")
        info_frame.pack(pady=10, fill="x")
        
        ctk.CTkLabel(info_frame, text=f"Title: {title}", font=self.font_18_bold).pack(anchor="w")
        ctk.CTkLabel(info_frame, text=f"Author: {author}", font=self.font_16).pack(anchor="w")
        
        status_color = self.success_color if available > 0 else self.danger_color
        status_text = f"Status: {available}/{total} copies available"
        ctk.CTkLabel(info_frame, text=status_text, font=self.font_16, text_color=status_color).pack(anchor="w")
        
        if issued_to:
            issued_frame = ctk.CTkFrame(results_frame)
            issued_frame.pack(fill="x", padx=10, pady=10)
            
            ctk.CTkLabel(issued_frame, text="Currently issued to:", font=self.font_16_bold).pack(anchor="w")
            
            for name, student_id, issue_date, due_date in issued_to:
                member_frame = ctk.CTkFrame(issued_frame, fg_color="transparent")
                member_frame.pack(fill="x", pady=5)
                
                # Display member image and info in a row
                member_row = ctk.CTkFrame(member_frame, fg_color="transparent")
                member_row.pack(fill="x")
                
                # Display member image
                img = self.get_member_image(student_id, size=(40, 40))
                img_label = ctk.CTkLabel(member_row, image=img, text="")
                img_label.pack(side="left", padx=10)
                
                # Display member info
                info_label_text = f"{name} ({student_id})\nIssued: {self.format_time_12h(issue_date)}\nDue: {self.format_time_12h(due_date)}"
                ctk.CTkLabel(member_row, text=info_label_text, font=self.font_14, justify="left").pack(side="left", padx=5)
        
    
    def show_home(self):
        """Show the modern home dashboard"""
        # 🔹 Destroy login footer if it exists
        if hasattr(self, "footer_frame") and self.footer_frame.winfo_exists():
            self.footer_frame.destroy()
    
        self.view_stack = []  
        self.current_view = "home"
        
        if "home" not in self._view_frames:
            # Main container for home screen
            frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
            self._view_frames["home"] = frame
            
            # Use a scrollable frame for the dashboard content
            self._home_scroll = ctk.CTkScrollableFrame(frame, fg_color="transparent")
            self._home_scroll.pack(fill="both", expand=True, padx=10, pady=10)
            
            # Sub-container for content
            self._home_content = ctk.CTkFrame(self._home_scroll, fg_color="transparent")
            self._home_content.pack(fill="both", expand=True)

        self._switch_view("home")
        
        # Build/Refresh footer
        self.footer_frame = ctk.CTkFrame(
            self,
            fg_color=self.primary_color,
            corner_radius=12,
            height=30
        )
        self.footer_frame.pack(side="bottom", fill="x")
        self.footer_frame.pack_propagate(False)
    
        footer_text = ctk.CTkLabel(
            self.footer_frame,
            text="Markhins Central Library — Powered by Antigravity Intelligence | v1.5.0",
            text_color=self.light_text,
            font=self.font_11
        )
        footer_text.pack(expand=True, pady=2)

        # Clear dynaminc content and build dashboard
        for widget in self._home_content.winfo_children():
            widget.destroy()
            
        self._build_home_dashboard()

    def _build_home_dashboard(self):
        # 1. Header Section
        header = ctk.CTkFrame(self._home_content, fg_color=self.primary_color, corner_radius=15)
        header.pack(fill="x", pady=(0, 20))
        
        title_box = ctk.CTkFrame(header, fg_color="transparent")
        title_box.pack(side="left", padx=30, pady=25)
        
        ctk.CTkLabel(title_box, text="Markhins Central Library", font=self.font_28_bold, text_color=self.accent_color).pack(anchor="w")
        ctk.CTkLabel(title_box, text="\"Read in the name of your Lord who created\"", 
                     font=self.font_16, text_color=self.dark_text).pack(anchor="w")
        
        time_frame = ctk.CTkFrame(header, fg_color="transparent")
        time_frame.pack(side="right", padx=30)
        
        self.home_clock_label = ctk.CTkLabel(
            time_frame, 
            text=datetime.now().strftime("%d %b %Y | %I:%M:%S %p"), 
            font=self.font_18_bold, 
            text_color=self.accent_color
        )
        self.home_clock_label.pack()
        
        # Start the clock update loop
        self._update_home_clock()

        # 2. KPI Cards Row
        kpi_frame = ctk.CTkFrame(self._home_content, fg_color="transparent")
        kpi_frame.pack(fill="x", pady=10)
        
        self.home_kpi_cards = {}
        labels = [
            ("Total Books", "📚"),
            ("Currently Issued", "📖"),
            ("Overdue Books", "⚠️"),
            ("Active Members", "👤")
        ]
        
        for i, (label, icon) in enumerate(labels):
            card = ctk.CTkFrame(kpi_frame, fg_color=self.primary_color, corner_radius=15, height=130)
            card.grid(row=0, column=i, padx=10, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)
            
            ctk.CTkLabel(card, text=f"{icon} {label}", font=self.font_14, text_color=self.dark_text).pack(pady=(20, 5))
            val_label = ctk.CTkLabel(card, text="...", font=self.font_28_bold, text_color=self.light_text)
            val_label.pack(pady=5)
            self.home_kpi_cards[label] = val_label

        # 3. LIQUID GLASS HOME BUTTONS SECTION
        qa_label = ctk.CTkLabel(self._home_content, text="⚡ Quick Actions", font=self.font_20_bold, text_color=self.light_text)
        qa_label.pack(anchor="w", padx=10, pady=(30, 10))
        
        # Glass container for buttons
        glass_container = ctk.CTkFrame(self._home_content, fg_color="transparent")
        glass_container.pack(fill="x", pady=5)
        
        # Create liquid glass buttons with modern layout
        primary_actions = [
            ("📚 Books", self.search_book, "books"),
            ("👤 Students", self.view_members, "students"), 
            ("🔍 Search", self.search_book, "default"),
            ("📊 Analytics", self.show_analytics, "analytics"),
            ("⚙️ Settings", self.show_about, "settings"),
            ("🕘 History", self.view_history, "default")
        ]
        
        # Create 2x3 grid layout for glass buttons
        for i, (text, command, category) in enumerate(primary_actions):
            row = i // 3
            col = i % 3
            
            # Create glass button with liquid effect and category tint
            glass_btn = GlassButton(
                glass_container,
                text=text,
                command=command,
                width=320,  # Much larger and touch-friendly
                height=130, # Much larger and touch-friendly
                category=category  # Apply category-based tint
            )
            glass_btn.grid(row=row, column=col, padx=15, pady=15, sticky="nsew")
            glass_container.grid_columnconfigure(col, weight=1)
            glass_container.grid_rowconfigure(row, weight=1)

        # 4. Secondary Actions with smaller glass buttons
        secondary_label = ctk.CTkLabel(self._home_content, text="📋 Management Tools", font=self.font_18_bold, text_color=self.light_text)
        secondary_label.pack(anchor="w", padx=10, pady=(25, 10))
        
        secondary_container = ctk.CTkFrame(self._home_content, fg_color="transparent")
        secondary_container.pack(fill="x", pady=5)
        
        secondary_actions = [
            ("📖 Add Book", self.add_book, "books"),
            ("🔄 Return Book", self.return_book, "books"),
            ("✏️ Edit Book", self.edit_book, "books"),
            ("🗑️ Delete", self.delete_book, "danger"),
            ("👤 Add Member", self.add_member, "students"),
            ("📜 Issue Book", self.issue_book, "books")
        ]
        
        # Create smaller glass buttons in single row with category tints
        for i, (text, command, category) in enumerate(secondary_actions):
            glass_btn = GlassButton(
                secondary_container,
                text=text,
                command=command,
                width=220,  # Larger and touch-friendly
                height=100, # Larger and touch-friendly
                category=category  # Apply category-based tint
            )
            glass_btn.pack(side="left", padx=8, pady=10, expand=True, fill="x")

        # 5. Maintenance & App Info Section
        maint_frame = ctk.CTkFrame(self._home_content, fg_color="transparent")
        maint_frame.pack(fill="x", pady=(20, 10))
        
        maint_actions = [
            ("✏️ Edit Book", self.edit_book, self.secondary_color),
            ("🗑️ Delete Book", self.delete_book, self.danger_color),
            ("👤 Search Member", self.search_member, self.accent_color),
            ("ℹ️ About", self.show_about, self.secondary_color)
        ]
        
        if self.admin_mode:
            maint_actions.append(("🔒 Admin Panel", self.admin_panel, self.danger_color))

        for i, (text, cmd, color) in enumerate(maint_actions):
            btn = ctk.CTkButton(maint_frame, text=text, command=cmd, fg_color=color, height=40, font=self.font_14, corner_radius=10)
            btn.grid(row=0, column=i, padx=5, sticky="ew")
            maint_frame.grid_columnconfigure(i, weight=1)

        # 6. Info Panel (Recent Activity / Quote)
        info_row = ctk.CTkFrame(self._home_content, fg_color="transparent")
        info_row.pack(fill="both", expand=True, pady=30)
        
        # Library Spotlight
        spot_panel = ctk.CTkFrame(info_row, fg_color=self.primary_color, corner_radius=20)
        spot_panel.pack(side="left", fill="both", expand=True, padx=10)
        
        ctk.CTkLabel(spot_panel, text="🌟 Library Spotlight", font=self.font_18_bold, text_color=self.accent_color).pack(pady=20)
        quote = "\"The best of you are those who learn the Qur'an and teach it.\""
        ctk.CTkLabel(spot_panel, text=quote, font=self.font_16, text_color=self.light_text, wraplength=400).pack(pady=10, padx=20)
        ctk.CTkLabel(spot_panel, text="- Prophet Muhammad (ﷺ)", font=("Arial", 12, "italic"), text_color=self.dark_text).pack(pady=5)
        
        # Recent Activity
        recent_panel = ctk.CTkFrame(info_row, fg_color=self.primary_color, corner_radius=20)
        recent_panel.pack(side="right", fill="both", expand=True, padx=10)
        
        ctk.CTkLabel(recent_panel, text="🔄 Recent Activity", font=self.font_18_bold, text_color=self.accent_color).pack(pady=20)
        self.home_activity_label = ctk.CTkLabel(recent_panel, text="Fetching latest transactions...", font=self.font_14, text_color=self.dark_text)
        self.home_activity_label.pack(pady=10, padx=20)

        # Start background thread to fetch stats
        threading.Thread(target=self._update_home_dashboard_stats, daemon=True).start()

    def _update_home_dashboard_stats(self):
        try:
            conn = sqlite3.connect("islamic_library.db", timeout=10)
            c = conn.cursor()
            
            c.execute("SELECT COUNT(*) FROM books")
            total_books = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM transactions WHERE status='issued'")
            issued_books = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM transactions WHERE status='issued' AND due_date < datetime('now')")
            overdue_books = c.fetchone()[0]
            
            c.execute("SELECT COUNT(DISTINCT member_id) FROM transactions")
            active_members = c.fetchone()[0]
            
            # Fetch last interaction
            c.execute('''SELECT b.title, m.name FROM transactions t 
                        JOIN books b ON t.book_id = b.id 
                        JOIN members m ON t.member_id = m.student_id 
                        ORDER BY t.issue_date DESC LIMIT 1''')
            last_tx = c.fetchone()
            
            conn.close()
            
            def update_ui():
                if hasattr(self, "home_kpi_cards"):
                    self.home_kpi_cards["Total Books"].configure(text=str(total_books))
                    self.home_kpi_cards["Currently Issued"].configure(text=str(issued_books))
                    self.home_kpi_cards["Overdue Books"].configure(text=str(overdue_books))
                    self.home_kpi_cards["Active Members"].configure(text=str(active_members))
                    
                if hasattr(self, "home_activity_label") and last_tx:
                    self.home_activity_label.configure(text=f"Last Action:\n{last_tx[1]} issued\n\"{last_tx[0]}\"", text_color=self.light_text)
            
            self.after(0, update_ui)
        except:
            pass

    def _update_home_clock(self):
        """Update the dashboard clock every second"""
        if self.current_view == "home" and hasattr(self, 'home_clock_label') and self.home_clock_label.winfo_exists():
            now = datetime.now()
            time_str = now.strftime("%d %b %Y | %I:%M:%S %p")
            self.home_clock_label.configure(text=time_str)
            self.after(1000, self._update_home_clock)
        
    def show_about(self):
        """Show About page with app information and acknowledgements - optimized with view caching"""
        # Track view for back button
        if self.current_view != "about":
            self.view_stack.append(self.current_view)
        self.current_view = "about"
        
        if "about" in self._view_frames:
            self._switch_view("about")
            return

        frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
        self._view_frames["about"] = frame
        self._switch_view("about")
        
        # Add back button
        ctk.CTkButton(frame, text="⬅️ Back", command=self.go_back, 
                     fg_color=self.secondary_color, hover_color=self.primary_color, 
                     width=100, height=35, font=self.font_14).pack(anchor="nw", padx=10, pady=5)
        
        # Create a scrollable frame for the content
        scroll_frame = ctk.CTkScrollableFrame(frame)
        scroll_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        # Title
        title_label = ctk.CTkLabel(
            scroll_frame,
            text="ABOUT\n Markhins Library App",
            font=self.font_24_bold,
            text_color=self.accent_color
        )
        title_label.pack(pady=10)
        
        # Logo
        try:
            logo_img = self.get_cached_image("logo1.png", size=(180, 150))
            if logo_img:
                logo_label = ctk.CTkLabel(scroll_frame, image=logo_img, text="")
                logo_label.image = logo_img
                logo_label.pack(pady=10)
        except Exception as e:
            print(f"Logo not found: {e}")
        
        # App info
        app_info = ctk.CTkLabel(
            scroll_frame,
            text=" Markhins Central Library\nVersion 1.2.0",
            font=("Arial", 18),
            text_color=self.light_text
        )
        app_info.pack(pady=5)
        
  

        
        # Developer info
        dev_info = ctk.CTkLabel(
            scroll_frame,
            text="Developed by\n Nihal , Ansah , Swamad\n& Team 12th Batch",
            font=self.font_16_bold,
            text_color="#FFD700"  # Golden color
        )
        dev_info.pack(pady=10)
        
        # Acknowledgements
        acknowledgement_text = (
            "🌟 Special Acknowledgement\n\n"
            "We extend our heartfelt gratitude to\n"
            "Junaid Usthad , Sadique Usthad\n"
            "and other Faculties  for their unwavering guidance, continuous\n"
            "support, and encouragement throughout this journey.\n\n"
            "Their mentorship has been instrumental in taking this\n"
            "project from its inception to its successful completion."
        )
        
        acknowledgement = ctk.CTkLabel(
            scroll_frame,
            text=acknowledgement_text,
            font=self.font_14_italic,
            text_color=self.light_text,
            justify="center"
        )
        acknowledgement.pack(pady=15, padx=20)
        
        # --- Support via GPay ---
        try:
            gpay_img = self.get_cached_image("icons/gpay.png", size=(28, 28))
            support_btn = ctk.CTkButton(
                scroll_frame,
                text="Support via GPay",
                image=gpay_img,
                compound="left",
                command=lambda: webbrowser.open(
                    "upi://pay?pa=8123312736@upi&pn=Nihal&cu=INR"
                ),
                fg_color="white",        # White background (official style)
                text_color="black",      # Black text for visibility
                hover_color="#f0f0f0",   # Light gray hover
                width=220,
                height=40,
                font=self.font_14_bold
            )
            support_btn.pack(pady=20)
        
        except Exception as e:
            print(f"GPay icon not found: {e}")
            # Fallback button without image
            support_btn = ctk.CTkButton(
                scroll_frame,
                text="Support via GPay",
                command=lambda: webbrowser.open(
                    "upi://pay?pa=8123312736@upi&pn=Nihal&cu=INR"
                ),
                fg_color="white",
                text_color="black",
                hover_color="#f0f0f0",
                width=220,
                height=40,
                font=self.font_14_bold
            )
            support_btn.pack(pady=20)
        
                
        # --- Developer Contact (icons row, same as login screen) ---
        contact_label = ctk.CTkLabel(
            scroll_frame,
            text="📬 Contact Developer",
            font=self.font_16_bold,
            text_color=self.accent_color
        )
        contact_label.pack(pady=10)
        
        links_frame = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        links_frame.pack(pady=5)
        
        # Email icon
        try:
            email_icon = self.get_cached_image("icons/email.png", size=(32, 32))
            if email_icon:
                email_btn = ctk.CTkLabel(
                    links_frame,
                    image=email_icon,
                    text="",
                    cursor="hand2"
                )
                email_btn.image = email_icon
                email_btn.pack(side="left", padx=20)
            email_btn.bind(
                "<Button-1>",
                lambda e: webbrowser.open(
                    "mailto:nihalch.in@gmail.com?subject=MARKHINS LIBRARY"
                )
            )
        except:
            pass
        
        # Instagram icon
        try:
            insta_icon = self.get_cached_image("icons/instagram.png", size=(32, 32))
            if insta_icon:
                insta_btn = ctk.CTkLabel(
                    links_frame,
                    image=insta_icon,
                    text="",
                    cursor="hand2"
                )
                insta_btn.image = insta_icon
                insta_btn.pack(side="left", padx=20)
            insta_btn.bind(
                "<Button-1>",
                lambda e: webbrowser.open(
                    "https://www.instagram.com/turathu_taibah_official/"
                )
            )
        except:
            pass
        
        # WhatsApp icon
        try:
            wa_icon = self.get_cached_image("icons/whatsapp.png", size=(32, 32))
            if wa_icon:
                wa_btn = ctk.CTkLabel(
                    links_frame,
                    image=wa_icon,
                    text="",
                    cursor="hand2"
                )
                wa_btn.image = wa_icon
                wa_btn.pack(side="left", padx=20)
            wa_btn.bind(
                "<Button-1>",
                lambda e: webbrowser.open(
                    "https://wa.me/918123312736?text=Hello Nihal, I would like to know more about the Markhins Library App."
                )
            )
        except:
            pass

        # --- Slogan (same as login screen footer) ---
        slogan_frame = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        slogan_frame.pack(pady=15)
        
        slogan_text1 = ctk.CTkLabel(
            slogan_frame,
            text="Built with",
            text_color="#ECF0F1",
            font=self.font_13_bold
        )
        slogan_text1.pack(side="left", padx=3)
        
        # Heart icon
        try:
            heart_icon = self.get_cached_image("icons/heart.png", size=(20, 20))
            if heart_icon:
                heart_label = ctk.CTkLabel(slogan_frame, image=heart_icon, text="")
                heart_label.image = heart_icon
                heart_label.pack(side="left", padx=2)
        except:
            pass
        
        slogan_text2 = ctk.CTkLabel(
            slogan_frame,
            text="by 12th Batch",
            text_color="#ECF0F1",
            font=self.font_13_bold
        )
        slogan_text2.pack(side="left", padx=3)


        # Footer text
        footer_text = ctk.CTkLabel(
            scroll_frame,
            text="© 2025 Markhins Central Library. All Rights Reserved.",
            font=self.font_12,
            text_color="#AAAAAA"
        )
        footer_text.pack(pady=10)
               
    def go_back(self):
        """Go back to previous view"""
        if self.view_stack:
            prev_view = self.view_stack.pop()
            if prev_view == "home":
                self.show_home()
            elif prev_view == "members":
                self.view_members()
            elif prev_view == "issued":
                self.view_issued_books()
            elif prev_view == "history":
                self.view_history()
            elif prev_view == "analytics":
                self.show_analytics()
            elif prev_view == "admin":
                self.admin_panel()
    
    def add_back_button(self, parent=None):
        """Add back button to content frame or specific parent"""
        target = parent if parent else self.content_frame
        back_btn = ctk.CTkButton(
            target,
            text="⬅️ Back",
            command=self.go_back,
            fg_color=self.secondary_color,
            hover_color=self.primary_color,
            width=100,
            height=35,
            font=self.font_14
        )
        back_btn.pack(anchor="nw", padx=10, pady=10)
    
    def add_export_button(self, export_func, color="#1a6f8c", parent=None):
        """Add export button to content frame or specific parent"""
        target = parent if parent else self.content_frame
        export_btn = ctk.CTkButton(
            target,
            text="📤 Export to Excel",
            command=export_func,
            fg_color=color,
            hover_color=self.secondary_color,
            width=200,
            height=35,
            font=self.font_14
        )
        export_btn.pack(pady=5)
    
    def load_books_table(self):
        """Load and display books in a table - optimized with view caching"""
        if "books" not in self._view_frames:
            frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
            self._view_frames["books"] = frame
            
            # Persistent parts
            self._books_back_holder = ctk.CTkFrame(frame, fg_color="transparent")
            self._books_back_holder.pack(fill="x")
            
            ctk.CTkLabel(frame, text="📚 Library Books", font=self.font_24_bold, text_color=self.accent_color).pack(pady=10)
            
            ctk.CTkButton(frame, text="➕ Add New Book", command=self.add_book, fg_color=self.primary_color,
                          hover_color=self.secondary_color, width=200, height=35, font=self.font_14).pack(pady=5)
            
            ctk.CTkButton(frame, text="📤 Export to Excel", command=self.export_books_to_excel, 
                          fg_color=self.primary_color, hover_color=self.secondary_color, width=200, height=35, font=self.font_14).pack(pady=5)
            
            self._books_table_container = ctk.CTkFrame(frame)
            self._books_table_container.pack(fill="both", expand=True, padx=20, pady=10)

        frame = self._switch_view("books")
        
        # Manage back button visibility
        for widget in self._books_back_holder.winfo_children():
            widget.destroy()
        if self.current_view != "home":
             ctk.CTkButton(self._books_back_holder, text="⬅️ Back", command=self.go_back, 
                           fg_color=self.secondary_color, hover_color=self.primary_color, 
                           width=100, height=35, font=self.font_14).pack(anchor="nw", padx=10, pady=5)

        # Clear dynaminc container and show loading
        for widget in self._books_table_container.winfo_children():
            widget.destroy()
            
        loading_label = ctk.CTkLabel(self._books_table_container, text="Loading books...", font=self.font_16)
        loading_label.pack(pady=50)

        def fetch_data():
            try:
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                df = pd.read_sql("SELECT id, title, author, category, available_copies, shelf, row, area FROM books", conn)
                conn.close()
                self.after(0, lambda: self._populate_books_table_ui(df, self._books_table_container, loading_label))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", f"Failed to load books: {str(e)}"))

        threading.Thread(target=fetch_data, daemon=True).start()

    def _populate_books_table_ui(self, df, table_frame, loading_label):
        loading_label.destroy()
        if df.empty:
            label = ctk.CTkLabel(table_frame, text="No books found in the library", font=self.font_16)
            label.pack(pady=50)
            return
        
        # Apply custom Treeview style (dark background + white text)
        style = ttk.Style()
        style.configure("Treeview",
                        background="#2E2E2E",      # dark grey background
                        foreground="white",        # white text
                        rowheight=35,
                        fieldbackground="#2E2E2E")
        style.configure("Treeview.Heading",
                        font=self.font_12_bold,
                        foreground="white",
                        background="#444444")
                
        
        # Create Treeview
        self.books_table = ttk.Treeview(table_frame, columns=list(df.columns), show="headings")
        
        # Arabic → Noto Naskh Arabic
        self.books_table.tag_configure("arabic", font=self.font_arabic, foreground="white")
        
        # Malayalam → Noto Sans Malayalam Thin
        self.books_table.tag_configure("malayalam", font=("Noto Sans Malayalam Thin", 14, "bold"), foreground="white")
        
        # Kannada → Noto Serif Kannada Thin
        self.books_table.tag_configure("kannada", font=("Noto Serif Kannada Thin", 14, "bold"), foreground="white")
        
        # English → Beckman FREE
        self.books_table.tag_configure("english", font=("Beckman FREE", 14, "bold"), foreground="white")

        
        # Add columns
        for col in df.columns:
            self.books_table.heading(col, text=col.replace("_", " ").title())
            self.books_table.column(col, width=100, anchor="center")
        
        # Set specific widths
        self.books_table.column("id", width=100)
        self.books_table.column("title", width=250)
        self.books_table.column("author", width=150)
        self.books_table.column("category", width=120)
        self.books_table.column("available_copies", width=80)
        self.books_table.column("shelf", width=80)
        self.books_table.column("row", width=80)
        self.books_table.column("area", width=120)
        
        # Add data
        for _, row in df.iterrows():
            title = str(row["title"])  # book title
            
            if any('\u0600' <= ch <= '\u06FF' for ch in title):   # Arabic range
                tag = "arabic"
            elif any('\u0D00' <= ch <= '\u0D7F' for ch in title): # Malayalam range
                tag = "malayalam"
            elif any('\u0C80' <= ch <= '\u0CFF' for ch in title): # Kannada range
                tag = "kannada"
            else:
                tag = "english"
            
            self.books_table.insert("", "end", values=tuple(row), tags=(tag,))

        
        # Add scrollbar
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.books_table.yview)
        self.books_table.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        
        self.books_table.pack(fill="both", expand=True)

        
        # Style the treeview
        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview", 
                      background="#2a2a2a", 
                      foreground="white",
                      rowheight=30,
                      fieldbackground="#2a2a2a",
                      bordercolor="#3a3a3a",
                      borderwidth=0)
        style.configure("Treeview.Heading", 
                      background=self.primary_color, 
                      foreground="white",
                      relief="flat")
        style.map("Treeview", background=[('selected', self.success_color)])
    
    def export_books_to_excel(self):
        """Export books list to Excel"""
        conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
        try:
            df = pd.read_sql("SELECT * FROM books", conn)
            
            if df.empty:
                messagebox.showwarning("Warning", "No books to export")
                return
            
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                title="Save books list as"
            )
            
            if file_path:
                df.to_excel(file_path, index=False)
                messagebox.showinfo("Success", "Books exported to Excel successfully!")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to export: {str(e)}")
        finally:
            conn.close()
    
    def add_book(self):
        """Show add book form"""
        self._switch_view()
        

        # Track view for back button
        if self.current_view != "add_book":
            self.view_stack.append(self.current_view)
        self.current_view = "add_book"
        
        # Add back button
        self.add_back_button()
        
        # Title
        title_label = ctk.CTkLabel(
            self.content_frame,
            text="📖 Add New Book",
            font=self.font_24_bold,
            text_color=self.accent_color)
        title_label.pack(pady=10)
        
        # Form frame
        form_frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
        form_frame.pack(pady=10)
        
        # Book details with ID field first
        fields = [
            ("ID (alphanumeric)", "id"),
            ("Title", "title"),
            ("Author", "author"),
            ("Category", "category"),
            ("Shelf", "shelf"),
            ("Row", "row"),
            ("Area", "area")
        ]
        
        self.book_entries = {}
        for i, (label_text, field_name) in enumerate(fields):
            row = i // 2
            column = i % 2
            
            label = ctk.CTkLabel(form_frame, text=f"{label_text}:", font=self.font_16)
            label.grid(row=row, column=column*2, padx=10, pady=5, sticky="e")
            
            entry = ctk.CTkEntry(form_frame, font=self.font_16)
            entry.grid(row=row, column=column*2+1, padx=10, pady=5, sticky="w")
            self.book_entries[field_name] = entry
        
               
        # Focus on first field
        try:
            self.book_entries['id'].focus_set()
        except:
            pass
                
        # Submit button
        submit_btn = ctk.CTkButton(
            self.content_frame,
            text="Save Book",
            command=self.save_book,
            fg_color=self.primary_color,
            hover_color=self.secondary_color,
            width=200,
            height=40,
            font=self.font_16
        )
        submit_btn.pack(pady=20)
        
        # Bind Enter key to save
        for entry in self.book_entries.values():
            entry.bind("<Return>", lambda event: self.save_book())
    
    def save_book(self):
        """Save new book to database"""
        conn = None
        try:
            # Get values from entries
            book_data = {
                'id': self.book_entries['id'].get().strip().upper(),
                'title': self.book_entries['title'].get().strip(),
                'author': self.book_entries['author'].get().strip(),
                'category': self.book_entries['category'].get().strip(),
                'shelf': self.book_entries['shelf'].get().strip(),
                'row': self.book_entries['row'].get().strip(),
                'area': self.book_entries['area'].get().strip()
            }
            
            # Validate required fields
            if not book_data['id']:
                messagebox.showerror("Error", "Book ID is required")
                return
            if not book_data['title']:
                messagebox.showerror("Error", "Title is required")
                return
            
            # Insert into database
            conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
            c = conn.cursor()
            
            c.execute('''INSERT INTO books 
                        (id, title, author, category, shelf, row, area)
                        VALUES (?, ?, ?, ?, ?, ?, ?)''',
                     (book_data['id'], book_data['title'], book_data['author'], 
                      book_data['category'], book_data['shelf'], 
                      book_data['row'], book_data['area']))
            
            conn.commit()
            messagebox.showinfo("Success", "Book added successfully!")
            self.show_home()
            
        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint failed: books.id" in str(e):
                messagebox.showerror("Error", "Book ID already exists")
            else:
                messagebox.showerror("Error", f"Database error: {str(e)}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to add book: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    def edit_book(self):
        """Edit an existing book's data"""
        edit_window = self.create_popup_window("Edit Book", 600, 400)
        
        # Book ID
        ctk.CTkLabel(edit_window, text="Book ID to Edit:", font=self.font_16).pack(pady=(10, 0))
        book_id_entry = ctk.CTkEntry(edit_window, font=self.font_16)
        book_id_entry.pack(pady=5)
        book_id_entry.focus_set()  # Auto-focus
        
        def load_book_data():
            book_id = book_id_entry.get().strip()
            if not book_id:
                messagebox.showerror("Error", "Please enter a book ID")
                return
            
            conn = None
            try:
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                c = conn.cursor()
                
                # Get book data
                c.execute('''SELECT title, author, category, total_copies, available_copies, shelf, row, area 
                            FROM books WHERE id=?''', (book_id,))
                book_data = c.fetchone()
                
                if not book_data:
                    messagebox.showerror("Error", "Book not found")
                    return
                
                # Destroy the ID entry and button
                book_id_entry.destroy()
                load_btn.destroy()
                
                # Create form to edit book data
                fields = [
                    ("Title", "title", book_data[0]),
                    ("Author", "author", book_data[1]),
                    ("Category", "category", book_data[2]),
                    ("Total Copies", "total_copies", book_data[3]),
                    ("Available Copies", "available_copies", book_data[4]),
                    ("Shelf", "shelf", book_data[5]),
                    ("Row", "row", book_data[6]),
                    ("Area", "area", book_data[7])
                ]
                
                # Form frame with scroll
                form_frame = ctk.CTkFrame(edit_window)
                form_frame.pack(fill="both", expand=True, padx=10, pady=10)
                
                canvas = Canvas(form_frame)
                scrollbar = ttk.Scrollbar(form_frame, orient="vertical", command=canvas.yview)
                scrollable_frame = ctk.CTkFrame(canvas)
                
                scrollable_frame.bind(
                    "<Configure>",
                    lambda e: canvas.configure(
                        scrollregion=canvas.bbox("all")
                    )
                )
                
                canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
                canvas.configure(yscrollcommand=scrollbar.set)
                
                canvas.pack(side="left", fill="both", expand=True)
                scrollbar.pack(side="right", fill="y")
                
                # Enable mouse wheel scrolling
                def _on_mousewheel(event):
                    canvas.yview_scroll(int(-1*(event.delta/120)), "units")
                    
                canvas.bind_all("<MouseWheel>", _on_mousewheel)
                
                self.edit_book_entries = {}
                for i, (label_text, field_name, default_value) in enumerate(fields):
                    row = i
                    
                    label = ctk.CTkLabel(scrollable_frame, text=f"{label_text}:", font=self.font_16)
                    label.grid(row=row, column=0, padx=10, pady=5, sticky="e")
                    
                    entry = ctk.CTkEntry(scrollable_frame, font=self.font_16)
                    entry.insert(0, str(default_value))
                    entry.grid(row=row, column=1, padx=10, pady=5, sticky="w")
                    self.edit_book_entries[field_name] = entry
                
                # Focus on first field
                # Focus on first field
                try:
                    self.edit_book_entries['title'].focus_set()
                except:
                    pass
                                
                                # Update button to save changes
                save_btn = ctk.CTkButton(
                    edit_window,
                    text="💾 Save Changes",
                    command=lambda: save_book_changes(book_id),
                    fg_color=self.success_color,
                    hover_color=self.secondary_color,
                    width=200,
                    height=40,
                    font=self.font_16
                )
                save_btn.pack(pady=10)
                
                # Bind Enter key to save
                for entry in self.edit_book_entries.values():
                    entry.bind("<Return>", lambda event: save_book_changes(book_id))
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load book data: {str(e)}")
            finally:
                if conn:
                    conn.close()
        
        def save_book_changes(book_id):
            """Save edited book data to database"""
            conn = None
            try:
                # Get values from entries
                book_data = {
                    'title': self.edit_book_entries['title'].get().strip(),
                    'author': self.edit_book_entries['author'].get().strip(),
                    'category': self.edit_book_entries['category'].get().strip(),
                    'total_copies': int(self.edit_book_entries['total_copies'].get()),
                    'available_copies': int(self.edit_book_entries['available_copies'].get()),
                    'shelf': self.edit_book_entries['shelf'].get().strip(),
                    'row': self.edit_book_entries['row'].get().strip(),
                    'area': self.edit_book_entries['area'].get().strip()
                }
                
                # Validate required fields
                if not book_data['title']:
                    messagebox.showerror("Error", "Title is required")
                    return
                
                # Update database
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                c = conn.cursor()
                
                c.execute('''UPDATE books SET 
                            title=?, author=?, category=?, 
                            total_copies=?, available_copies=?, 
                            shelf=?, row=?, area=?
                            WHERE id=?''',
                         (book_data['title'], book_data['author'], 
                          book_data['category'], book_data['total_copies'], 
                          book_data['available_copies'], book_data['shelf'], 
                          book_data['row'], book_data['area'], book_id))
                
                conn.commit()
                messagebox.showinfo("Success", "Book updated successfully!")
                edit_window.destroy()
                self.load_books_table()
                
            except ValueError:
                messagebox.showerror("Error", "Copies values must be numbers")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to update book: {str(e)}")
            finally:
                if conn:
                    conn.close()
        
        # Initial load button
        load_btn = ctk.CTkButton(
            edit_window,
            text="🔍 Load Book Data",
            command=load_book_data,
            fg_color=self.primary_color,
            hover_color=self.secondary_color,
            width=200,
            height=40,
            font=self.font_16
        )
        load_btn.pack(pady=20)
        
        # Bind Enter key to load
        book_id_entry.bind("<Return>", lambda event: load_book_data())
    
    def add_member(self):
        """Show add member form"""
        self._switch_view()
        
        # Track view for back button
        if self.current_view != "add_member":
            self.view_stack.append(self.current_view)
        self.current_view = "add_member"
        
        # Add back button
        self.add_back_button()
        
        # Title
        title_label = ctk.CTkLabel(
            self.content_frame,
            text="👤 Add New Member",
            font=self.font_24_bold,
            text_color=self.accent_color)
        title_label.pack(pady=10)
        
        # Form frame
        form_frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
        form_frame.pack(pady=10)
        
        # Member details
        fields = [
            ("Student ID", "student_id"),
            ("Name", "name"),
            ("Batch", "batch"),
            ("Type (student/teacher)", "type")
        ]
        
        self.member_entries = {}
        for i, (label_text, field_name) in enumerate(fields):
            label = ctk.CTkLabel(form_frame, text=f"{label_text}:", font=self.font_16)
            label.grid(row=i, column=0, padx=10, pady=5, sticky="e")
            
            if field_name == "type":
                type_combo = ctk.CTkComboBox(form_frame, values=["student", "teacher"], font=self.font_16)
                type_combo.set("student")
                type_combo.grid(row=i, column=1, padx=10, pady=5, sticky="w")
                self.member_entries[field_name] = type_combo
            else:
                entry = ctk.CTkEntry(form_frame, font=self.font_16)
                entry.grid(row=i, column=1, padx=10, pady=5, sticky="w")
                self.member_entries[field_name] = entry
        
        # Focus on first field
        self.member_entries['student_id'].focus_set()
        
        # Submit button
        submit_btn = ctk.CTkButton(
            self.content_frame,
            text="Save Member",
            command=self.save_member,
            fg_color=self.primary_color,
            hover_color=self.secondary_color,
            width=200,
            height=40,
            font=self.font_16
        )
        submit_btn.pack(pady=20)
        
        # Bind Enter key to save
        for entry in self.member_entries.values():
            if isinstance(entry, ctk.CTkEntry):
                entry.bind("<Return>", lambda event: self.save_member())
    
    def save_member(self):
        """Save new member to database"""
        conn = None
        try:
            # Get values from entries
            member_data = {
                'student_id': self.member_entries['student_id'].get().strip(),
                'name': self.member_entries['name'].get().strip(),
                'batch': self.member_entries['batch'].get().strip(),
                'type': self.member_entries['type'].get()
            }
            
            # Validate required fields
            if not member_data['student_id'] or not member_data['name']:
                messagebox.showerror("Error", "Student ID and Name are required")
                return
            
            # Insert into database
            conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
            c = conn.cursor()
            
            c.execute('''INSERT INTO members (student_id, name, batch, type) 
                        VALUES (?, ?, ?, ?)''',
                     (member_data['student_id'], member_data['name'], member_data['batch'], member_data['type']))
            
            conn.commit()
            messagebox.showinfo("Success", "Member added successfully!")
            self.show_home()
            
        except sqlite3.IntegrityError:
            messagebox.showerror("Error", "Student ID must be unique")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to add member: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    def delete_book(self):
        """Delete a book from the library"""
        delete_window = self.create_popup_window("Delete Book", 400, 200)
        
        # Book ID
        ctk.CTkLabel(delete_window, text="Book ID:", font=self.font_16).pack(pady=(10, 0))
        book_id_entry = ctk.CTkEntry(delete_window, font=self.font_16)
        book_id_entry.pack(pady=5)
        book_id_entry.focus_set()  # Auto-focus
        
        def process_delete():
            book_id = book_id_entry.get().strip()
            
            if not book_id:
                messagebox.showerror("Error", "Book ID is required")
                return
            
            conn = None
            try:
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                c = conn.cursor()
                
                # Check if book exists
                c.execute("SELECT title FROM books WHERE id=?", (book_id,))
                book = c.fetchone()
                
                if not book:
                    messagebox.showerror("Error", "Book not found")
                    return
                
                # Check if book is currently issued
                c.execute("SELECT 1 FROM transactions WHERE book_id=? AND status='issued' LIMIT 1", (book_id,))
                if c.fetchone():
                    messagebox.showerror("Error", "Cannot delete book - it is currently issued")
                    return
                
                # Confirm deletion
                if not messagebox.askyesno("Confirm", f"Delete book: {book[0]}?"):
                    return
                
                # Delete book and related transactions/history
                c.execute("DELETE FROM transactions WHERE book_id=?", (book_id,))
                c.execute("DELETE FROM books WHERE id=?", (book_id,))
                conn.commit()
                
                messagebox.showinfo("Success", "Book deleted successfully!")
                delete_window.destroy()
                self.load_books_table()
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to delete book: {str(e)}")
            finally:
                if conn:
                    conn.close()
        
        ctk.CTkButton(
            delete_window,
            text="Delete Book",
            command=process_delete,
            fg_color=self.danger_color,
            hover_color=self.secondary_color,
            font=self.font_16).pack(pady=10)
        
        # Bind Enter key to delete
        book_id_entry.bind("<Return>", lambda event: process_delete())
    
    def delete_member(self, student_id):
        """Delete a member from the library"""
        conn = None
        try:
            conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
            c = conn.cursor()
            
            # Check if member has any issued books
            c.execute("SELECT 1 FROM transactions WHERE member_id=? AND status='issued' LIMIT 1", (student_id,))
            if c.fetchone():
                messagebox.showerror("Error", "Cannot delete member - they have issued books")
                return
            
            # Get member name for confirmation
            c.execute("SELECT name FROM members WHERE student_id=?", (student_id,))
            member_name = c.fetchone()[0]
            
            # Confirm deletion
            if not messagebox.askyesno("Confirm", f"Delete member: {member_name}?"):
                return
            
            # Delete member and related transactions
            c.execute("DELETE FROM transactions WHERE member_id=?", (student_id,))
            c.execute("DELETE FROM members WHERE student_id=?", (student_id,))
            conn.commit()
            
            messagebox.showinfo("Success", "Member deleted successfully!")
            self.view_members()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete member: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    def issue_book(self):
        """Issue a book to a member"""
        issue_window = self.create_popup_window("Issue Book", 500, 300)
    
        # Book ID
        ctk.CTkLabel(issue_window, text="Book ID:", font=self.font_16).pack(pady=(10, 0))
        book_id_entry = ctk.CTkEntry(issue_window, font=self.font_16)
        book_id_entry.pack(pady=5)
        book_id_entry.focus_set()  # Auto-focus
    
        # Member ID
        ctk.CTkLabel(issue_window, text="Student ID:", font=self.font_16).pack(pady=(10, 0))
        student_id_entry = ctk.CTkEntry(issue_window, font=self.font_16)
        student_id_entry.pack(pady=5)
    
        # Due Date Display
        due_date = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%d")
        ctk.CTkLabel(issue_window, text=f"Due Date: {due_date}", font=self.font_16).pack(pady=10)
    
        def process_issue():
            input_code = book_id_entry.get().strip().upper()
            book_id = input_code
            student_id = student_id_entry.get().strip()
    
            if not book_id or not student_id:
                messagebox.showerror("Error", "Both fields are required")
                return
    
            conn = None
            try:
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                c = conn.cursor()
    
                # Check book availability and get title
                c.execute("SELECT title, available_copies FROM books WHERE id=?", (book_id,))
                result = c.fetchone()
    
                if not result:
                    messagebox.showerror("Error", "Book not found")
                    return
    
                book_title, available = result
                if available < 1:
                    messagebox.showerror("Error", "No copies available")
                    return
    
                # Get member info
                c.execute("SELECT name FROM members WHERE student_id=?", (student_id,))
                member = c.fetchone()
    
                if not member:
                    messagebox.showerror("Error", "Member not found")
                    return
    
                member_name = member[0]
    
                # Insert into transactions
                issue_date = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
                c.execute('''INSERT INTO transactions 
                             (book_id, member_id, issue_date, due_date, librarian)
                             VALUES (?, ?, ?, ?, ?)''',
                          (book_id, student_id, issue_date, due_date, self.current_librarian))
    
                # Update book stock
                c.execute("UPDATE books SET available_copies = available_copies - 1 WHERE id=?", (book_id,))
                
                # Update attendance record with books issued count
                c.execute("UPDATE attendance SET books_issued = books_issued + 1 WHERE id = ?", (self.attendance_id,))
    
                conn.commit()

                # ✅ WhatsApp notification to teacher & library number
                self._notify_teacher_issue(student_id, book_title, due_date)
    
                # Show detailed confirmation with member photo
                self.show_custom_popup(member_name, book_title, due_date, student_id)
                issue_window.destroy()
                self.load_books_table()
    
            except Exception as e:
                messagebox.showerror("Error", f"Failed to issue book: {str(e)}")
    
            finally:
                if conn:
                    conn.close()

    
        # Issue Button
        ctk.CTkButton(
            issue_window,
            text="Issue Book",
            command=process_issue,
            fg_color=self.success_color,
            hover_color=self.secondary_color,
            font=self.font_16).pack(pady=10)
    
        # Bind Enter to both entries
        student_id_entry.bind("<Return>", lambda event: process_issue())
        book_id_entry.bind("<Return>", lambda event: process_issue())
    
    def return_book(self):
        """Return a book with rating option"""
        return_window = self.create_popup_window("Return Book", 500, 350)
        
        # Book ID
        ctk.CTkLabel(return_window, text="Book ID:", font=self.font_16).pack(pady=(10, 0))
        book_id_entry = ctk.CTkEntry(return_window, font=self.font_16)
        book_id_entry.pack(pady=5)
        book_id_entry.focus_set()  # Auto-focus
        
        # Student ID
        ctk.CTkLabel(return_window, text="Student ID:", font=self.font_16).pack(pady=(10, 0))
        student_id_entry = ctk.CTkEntry(return_window, font=self.font_16)
        student_id_entry.pack(pady=5)
        
        # Rating frame
        rating_frame = ctk.CTkFrame(return_window, fg_color="transparent")
        rating_frame.pack(pady=10)
        
        self.rating_var = ctk.IntVar(value=0)  # 0 = no rating, 1–5 = stars
        
        ctk.CTkLabel(rating_frame, text="Rate this book:", font=self.font_14).pack(side="left", padx=5)
        
        self.stars = []
        
        def update_stars(rating):
            self.rating_var.set(rating)
            for i, btn in enumerate(self.stars, start=1):
                if i <= rating:
                    btn.configure(text="⭐", fg_color=self.accent_color)  # highlighted
                else:
                    btn.configure(text="☆", fg_color=self.secondary_color)  # empty
        
        for i in range(1, 6):  # Create 5 star buttons
            star_btn = ctk.CTkButton(
                rating_frame,
                text="☆",
                command=lambda r=i: update_stars(r),
                fg_color=self.secondary_color,
                width=40,
                height=30,
                font=self.font_16
            )
            star_btn.pack(side="left", padx=2)
            self.stars.append(star_btn)

        
        def process_return():
            input_code = book_id_entry.get().strip().upper()
            book_id = input_code
            student_id = student_id_entry.get().strip()
            rating = self.rating_var.get()
            
            if not book_id or not student_id:
                messagebox.showerror("Error", "Both fields are required")
                return
            
            conn = None
            try:
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                c = conn.cursor()
                
                # Check if transaction exists
                c.execute('''SELECT id FROM transactions 
                            WHERE book_id=? AND member_id=? AND status='issued' 
                            ORDER BY issue_date DESC LIMIT 1''',
                         (book_id, student_id))
                transaction = c.fetchone()
                
                if not transaction:
                    messagebox.showerror("Error", "No active issue found for this book and member")
                    return
                
                # Update transaction
                return_date = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
                c.execute('''UPDATE transactions 
                            SET return_date=?, status='returned' 
                            WHERE id=?''', 
                         (return_date, transaction[0]))
                
                # Update book availability
                c.execute("UPDATE books SET available_copies = available_copies + 1 WHERE id=?", (book_id,))
                
                # Save rating if provided
                if rating > 0:
                    c.execute('''INSERT INTO ratings (book_id, member_id, rating)
                                 VALUES (?, ?, ?)''', (book_id, student_id, rating))
                
                conn.commit()

                # ✅ WhatsApp notification to teacher & library number
                c.execute("SELECT title FROM books WHERE id=?", (book_id,))
                book_title = c.fetchone()[0]
                self._notify_teacher_return(student_id, book_title, rating)


                messagebox.showinfo("Success", f"Book returned successfully on {return_date}!")
                return_window.destroy()
                self.load_books_table()
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to return book: {str(e)}")
            finally:
                if conn:
                    conn.close()
        
        ctk.CTkButton(
            return_window,
            text="Return Book",
            command=process_return,
            fg_color=self.accent_color,
            hover_color=self.secondary_color,
            font=self.font_16).pack(pady=10)
        
        # Bind Enter key to return
        student_id_entry.bind("<Return>", lambda event: process_return())
        book_id_entry.bind("<Return>", lambda event: process_return())

    
    def view_members(self):
        """View all library members - optimized with view caching"""
        # Track view for back button
        if self.current_view != "members":
            self.view_stack.append(self.current_view)
        self.current_view = "members"
        
        if "members" not in self._view_frames:
            frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
            self._view_frames["members"] = frame
            
            # Persistent parts
            ctk.CTkButton(frame, text="⬅️ Back", command=self.go_back, 
                         fg_color=self.secondary_color, hover_color=self.primary_color, 
                         width=100, height=35, font=self.font_14).pack(anchor="nw", padx=10, pady=5)
            
            ctk.CTkLabel(frame, text="👥 Library Members", font=self.font_24_bold, text_color=self.accent_color).pack(pady=10)
            
            ctk.CTkButton(frame, text="➕ Add New Member", command=self.add_member, fg_color=self.primary_color,
                          hover_color=self.secondary_color, width=200, height=35, font=self.font_14).pack(pady=5)
            
            self.add_export_button(self.export_members_to_excel, self.primary_color, parent=frame)
            
            # Use a persistent container for the scroll frame
            self._members_scroll_container = ctk.CTkFrame(frame, fg_color="transparent")
            self._members_scroll_container.pack(fill="both", expand=True, padx=20, pady=10)

        self._switch_view("members")
        
        # Clear dynaminc container and show loading
        for widget in self._members_scroll_container.winfo_children():
            widget.destroy()
            
        scroll_frame = ctk.CTkScrollableFrame(self._members_scroll_container)
        scroll_frame.pack(fill="both", expand=True)
        
        loading_label = ctk.CTkLabel(scroll_frame, text="Loading members...", font=self.font_16)
        loading_label.pack(pady=50)

        def fetch_data():
            try:
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                # Query to get members and their issued book count
                df = pd.read_sql('''
                    SELECT 
                        m.student_id, 
                        m.name, 
                        COALESCE(m.batch, '') AS batch, 
                        COALESCE(m.join_date, '') AS join_date, 
                        COALESCE(m.type, 'student') AS type,
                        (SELECT COUNT(*) FROM transactions 
                         WHERE member_id = m.student_id AND status = 'issued') AS issued_books 
                    FROM members m
                ''', conn)
                conn.close()
                self.after(0, lambda: self._populate_members_ui(df, scroll_frame, loading_label))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", f"Failed to load members: {str(e)}"))

        threading.Thread(target=fetch_data, daemon=True).start()

    def _populate_members_ui(self, df, scroll_frame, loading_label):
        loading_label.destroy()
        if df.empty:
            label = ctk.CTkLabel(scroll_frame, text="No members found", font=self.font_16)
            label.pack(pady=50)
            return
        
        # Create member cards
        for _, row in df.iterrows():
            card_frame = ctk.CTkFrame(scroll_frame, corner_radius=10)
            card_frame.pack(fill="x", pady=5)
            
            # Student image
            img = self.get_member_image(row['student_id'])
            img_label = ctk.CTkLabel(card_frame, image=img, text="")
            img_label.pack(side="left", padx=10, pady=10)
            
            # Member details
            details_frame = ctk.CTkFrame(card_frame, fg_color="transparent")
            details_frame.pack(side="left", fill="x", expand=True, padx=10, pady=10)
            
            ctk.CTkLabel(
                details_frame, 
                text=row['name'],
                font=self.font_18_bold).pack(anchor="w")
            
            ctk.CTkLabel(
                details_frame, 
                text=f"ID: {row['student_id']} | Type: {row['type']}",
                font=self.font_16).pack(anchor="w")
            
            if row['batch']:
                ctk.CTkLabel(
                    details_frame, 
                    text=f"Batch: {row['batch']} | Books Issued: {row['issued_books']}",
                    font=self.font_14).pack(anchor="w")
            else:
                ctk.CTkLabel(
                    details_frame, 
                    text=f"Books Issued: {row['issued_books']}",
                    font=self.font_14).pack(anchor="w")
            
            # Action buttons
            button_frame = ctk.CTkFrame(card_frame, fg_color="transparent")
            button_frame.pack(side="right", padx=10, pady=10)
            
            history_btn = ctk.CTkButton(
                button_frame,
                text="View History",
                command=lambda sid=row['student_id']: self.view_member_history(sid),
                fg_color=self.accent_color,
                hover_color=self.secondary_color,
                width=120,
                height=30,
                font=self.font_12
            )
            history_btn.pack(side="left", padx=5)
            
            delete_btn = ctk.CTkButton(
                button_frame,
                text="Delete",
                command=lambda sid=row['student_id']: self.delete_member(sid),
                fg_color=self.danger_color,
                hover_color=self.secondary_color,
                width=100,
                height=30,
                font=self.font_12
            )
            delete_btn.pack(side="left", padx=5)
    
    def view_member_history(self, student_id):
        """View history for selected member"""
        # Create history window
        history_window = self.create_popup_window(f"Member History - {student_id}", 1200, 700)
        
        # Title frame and labels (initial state)
        title_frame = ctk.CTkFrame(history_window, fg_color="transparent")
        title_frame.pack(fill="x", padx=20, pady=10)
        
        # Table frame with scroll
        table_frame = ctk.CTkScrollableFrame(history_window)
        table_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        # Loading indicator
        loading_label = ctk.CTkLabel(table_frame, text="Fetching history records...", font=self.font_16)
        loading_label.pack(pady=50)

        def fetch_history():
            try:
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                c = conn.cursor()
                
                # Get member name
                c.execute("SELECT name FROM members WHERE student_id=?", (student_id,))
                member_row = c.fetchone()
                member_name = member_row[0] if member_row else "Unknown"
                
                # Get history with book details
                query = '''SELECT 
                            t.issue_date as date, 
                            b.title as book_title,
                            t.due_date,
                            t.return_date,
                            t.status,
                            t.librarian
                           FROM transactions t
                           JOIN books b ON t.book_id = b.id
                           WHERE t.member_id = ?
                           ORDER BY t.issue_date DESC'''
                
                df = pd.read_sql(query, conn, params=(student_id,))
                conn.close()
                
                self.after(0, lambda: self._populate_history_ui(df, student_id, member_name, title_frame, table_frame, loading_label))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", f"Failed to load history: {str(e)}"))

        threading.Thread(target=fetch_history, daemon=True).start()

    def _populate_history_ui(self, df, student_id, member_name, title_frame, table_frame, loading_label):
        loading_label.destroy()
        
        # Update title with member info
        img = self.get_member_image(student_id, size=(80, 80))
        img_label = ctk.CTkLabel(title_frame, image=img, text="")
        img_label.pack(side="left", padx=10)
        
        ctk.CTkLabel(
            title_frame,
            text=f"📜 History for {member_name} ({student_id})",
            font=self.font_20_bold,
            text_color=self.accent_color).pack(side="left", fill="x", expand=True)

        if df.empty:
            label = ctk.CTkLabel(table_frame, text="No history records found", font=self.font_16)
            label.pack(pady=50)
            return
        
        # Create Treeview
        history_tree = ttk.Treeview(table_frame, columns=list(df.columns), show="headings")
        
        # Add columns
        for col in df.columns:
            history_tree.heading(col, text=col.replace("_", " ").title())
            history_tree.column(col, width=150, anchor="center")
        
        # Set specific widths
        history_tree.column("date", width=150)
        history_tree.column("book_title", width=300)
        history_tree.column("due_date", width=150)
        history_tree.column("return_date", width=150)
        history_tree.column("status", width=100)
        history_tree.column("librarian", width=150)
        
        # Add data with formatted dates
        for _, row in df.iterrows():
            formatted_values = []
            for i, value in enumerate(row):
                if i in [0, 2, 3]:  # Date columns
                    formatted_values.append(self.format_time_12h(value))
                else:
                    formatted_values.append(value)
            history_tree.insert("", "end", values=tuple(formatted_values))
        
        # Style the treeview
        style = ttk.Style(self) # Changed from ttk.Style() to ttk.Style(self) for consistency
        style.theme_use("default")
        style.configure("Treeview",
                        rowheight=40,
                        background="#1e1e1e",
                        fieldbackground="#1e1e1e",
                        foreground="white")
        style.configure("Treeview.Heading",
                        font=self.font_12_bold,
                        foreground="white",
                        background="#2e2e2e")
        
        # Scrollbar for treeview
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=history_tree.yview)
        history_tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        
        history_tree.pack(fill="both", expand=True)
    
    def export_members_to_excel(self):
        """Export members list to Excel"""
        conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
        try:
            df = pd.read_sql("SELECT * FROM members", conn)
            
            if df.empty:
                messagebox.showwarning("Warning", "No members to export")
                return
            
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                title="Save members list as"
            )
            
            if file_path:
                df.to_excel(file_path, index=False)
                messagebox.showinfo("Success", "Members exported to Excel successfully!")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to export: {str(e)}")
        finally:
            conn.close()
    
    def view_issued_books(self):
        """View all currently issued books - optimized with view caching"""
        # Track view for back button
        if self.current_view != "issued":
            self.view_stack.append(self.current_view)
        self.current_view = "issued"
        
        if "issued" not in self._view_frames:
            frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
            self._view_frames["issued"] = frame
            
            # Persistent parts
            ctk.CTkButton(frame, text="⬅️ Back", command=self.go_back, 
                         fg_color=self.secondary_color, hover_color=self.primary_color, 
                         width=100, height=35, font=self.font_14).pack(anchor="nw", padx=10, pady=5)
            
            ctk.CTkLabel(frame, text="📜 Currently Issued Books", font=self.font_24_bold, text_color=self.accent_color).pack(pady=10)
            
            ctk.CTkButton(frame, text="⚠️ View Overdue Books", command=self.view_overdue_books, 
                          fg_color=self.warning_color, hover_color=self.secondary_color, width=200, height=35, font=self.font_14).pack(pady=5)
            
            self.add_export_button(self.export_issued_to_excel, self.accent_color, parent=frame)
            
            self._issued_scroll_container = ctk.CTkFrame(frame, fg_color="transparent")
            self._issued_scroll_container.pack(fill="both", expand=True, padx=20, pady=10)

        self._switch_view("issued")
        
        # Clear dynaminc container and show loading
        for widget in self._issued_scroll_container.winfo_children():
            widget.destroy()
            
        scroll_frame = ctk.CTkScrollableFrame(self._issued_scroll_container)
        scroll_frame.pack(fill="both", expand=True)
        
        loading_label = ctk.CTkLabel(scroll_frame, text="Loading issued books...", font=self.font_16)
        loading_label.pack(pady=50)

        def fetch_issued():
            try:
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                # Get issued books with member and book details
                query = '''SELECT t.id, b.title, m.name, m.student_id, 
                                  t.issue_date, t.due_date, t.librarian,
                                  julianday(t.due_date) - julianday('now') as days_remaining
                           FROM transactions t
                           JOIN books b ON t.book_id = b.id
                           JOIN members m ON t.member_id = m.student_id
                           WHERE t.status='issued'
                           ORDER BY t.due_date'''
                df = pd.read_sql(query, conn)
                conn.close()
                self.after(0, lambda: self._populate_issued_ui(df, scroll_frame, loading_label))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", f"Failed to load issued books: {str(e)}"))

        threading.Thread(target=fetch_issued, daemon=True).start()

    def _populate_issued_ui(self, df, scroll_frame, loading_label):
        loading_label.destroy()
        if df.empty:
            label = ctk.CTkLabel(scroll_frame, text="No books currently issued", font=self.font_16)
            label.pack(pady=50)
            return
        
        # Create issued book cards
        for _, row in df.iterrows():
            card_frame = ctk.CTkFrame(scroll_frame, corner_radius=10)
            card_frame.pack(fill="x", pady=5)
            
            # Book icon
            book_icon = ctk.CTkLabel(
                card_frame, 
                text="📚",
                font=("Arial", 24),
                width=40
            )
            book_icon.pack(side="left", padx=10, pady=10)
            
            # Book details
            book_frame = ctk.CTkFrame(card_frame, fg_color="transparent")
            book_frame.pack(side="left", fill="x", expand=True, padx=10, pady=10)
            
            ctk.CTkLabel(
                book_frame, 
                text=row['title'],
                font=self.font_18_bold).pack(anchor="w")
            
            issue_date_fmt = self.format_time_12h(row['issue_date'])
            due_date_fmt = self.format_time_12h(row['due_date'])
            ctk.CTkLabel(
                book_frame, 
                text=f"Issued: {issue_date_fmt} | Due: {due_date_fmt}",
                font=self.font_16).pack(anchor="w")
            
            # Member details
            member_frame = ctk.CTkFrame(card_frame, fg_color="transparent")
            member_frame.pack(side="left", fill="x", expand=True, padx=10, pady=10)
            
            # Member image
            img = self.get_member_image(row['student_id'], size=(40, 40))
            img_label = ctk.CTkLabel(member_frame, image=img, text="")
            img_label.pack(side="left", padx=10)
            
            ctk.CTkLabel(
                member_frame, 
                text=row['name'],
                font=self.font_16_bold).pack(anchor="w")
            
            ctk.CTkLabel(
                member_frame, 
                text=f"ID: {row['student_id']} | Librarian: {row['librarian']}",
                font=self.font_14).pack(anchor="w")
            
            # Status indicator
            status_frame = ctk.CTkFrame(card_frame, fg_color="transparent")
            status_frame.pack(side="right", padx=10, pady=10)
            
            days_remaining = int(row['days_remaining'])
            if days_remaining < 0:
                status_text = "OVERDUE"
                status_color = self.danger_color
            else:
                status_text = f"{days_remaining} days remaining"
                status_color = self.success_color if days_remaining > 3 else self.warning_color
            
            ctk.CTkLabel(
                status_frame,
                text=status_text,
                font=self.font_16_bold,
                text_color=status_color
            ).pack()
    
    def export_issued_to_excel(self):
        """Export issued books list to Excel"""
        conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
        try:
            query = '''SELECT b.title, m.name, m.student_id, 
                              t.issue_date, t.due_date, t.librarian,
                              CASE 
                                  WHEN t.due_date < datetime('now') THEN 'OVERDUE'
                                  ELSE 'On Time'
                              END as status
                       FROM transactions t
                       JOIN books b ON t.book_id = b.id
                       JOIN members m ON t.member_id = m.student_id
                       WHERE t.status='issued'
                       ORDER BY t.due_date'''
            
            df = pd.read_sql(query, conn)
            
            if df.empty:
                messagebox.showwarning("Warning", "No issued books to export")
                return
            
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                title="Save issued books list as"
            )
            
            if file_path:
                df.to_excel(file_path, index=False)
                messagebox.showinfo("Success", "Issued books exported to Excel successfully!")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to export: {str(e)}")
        finally:
            conn.close()
    
    def view_overdue_books(self):
        """View only overdue books - optimized with view caching"""
        # Track view for back button (optional since it's a sub-view)
        if self.current_view != "overdue":
             self.view_stack.append(self.current_view)
        self.current_view = "overdue"

        if "overdue" not in self._view_frames:
            frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
            self._view_frames["overdue"] = frame
            
            # Persistent parts
            ctk.CTkButton(frame, text="⬅️ Back", command=self.go_back, 
                         fg_color=self.secondary_color, hover_color=self.primary_color, 
                         width=100, height=35, font=self.font_14).pack(anchor="nw", padx=10, pady=5)
            
            ctk.CTkLabel(frame, text="⚠️ Overdue Books", font=self.font_24_bold, text_color=self.warning_color).pack(pady=10)
            
            self.add_export_button(self.export_overdue_to_excel, self.warning_color, parent=frame)
            
            self._overdue_scroll_container = ctk.CTkFrame(frame, fg_color="transparent")
            self._overdue_scroll_container.pack(fill="both", expand=True, padx=20, pady=10)

        self._switch_view("overdue")
        
        # Clear dynaminc container and show loading
        for widget in self._overdue_scroll_container.winfo_children():
            widget.destroy()
            
        scroll_frame = ctk.CTkScrollableFrame(self._overdue_scroll_container)
        scroll_frame.pack(fill="both", expand=True)
        
        loading_label = ctk.CTkLabel(scroll_frame, text="Checking for overdue books...", font=self.font_16)
        loading_label.pack(pady=50)

        def fetch_overdue():
            try:
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                # Get overdue books
                query = '''SELECT t.id, b.title, m.name, m.student_id, 
                                  t.issue_date, t.due_date, t.librarian,
                                  julianday('now') - julianday(t.due_date) as days_overdue
                           FROM transactions t
                           JOIN books b ON t.book_id = b.id
                           JOIN members m ON t.member_id = m.student_id
                           WHERE t.status='issued' AND t.due_date < datetime('now')
                           ORDER BY t.due_date'''
                df = pd.read_sql(query, conn)
                conn.close()
                self.after(0, lambda: self._populate_overdue_ui(df, scroll_frame, loading_label))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", f"Failed to load overdue books: {str(e)}"))

        threading.Thread(target=fetch_overdue, daemon=True).start()

    def _populate_overdue_ui(self, df, scroll_frame, loading_label):
        loading_label.destroy()
        if df.empty:
            label = ctk.CTkLabel(scroll_frame, text="No overdue books found", font=self.font_16)
            label.pack(pady=50)
            return
        
        # Create overdue book cards
        for _, row in df.iterrows():
            card_frame = ctk.CTkFrame(scroll_frame, corner_radius=10)
            card_frame.pack(fill="x", pady=5)
            
            # Warning icon
            warning_icon = ctk.CTkLabel(
                card_frame, 
                text="⚠️",
                font=("Arial", 24),
                width=40
            )
            warning_icon.pack(side="left", padx=10, pady=10)
            
            # Book details
            book_frame = ctk.CTkFrame(card_frame, fg_color="transparent")
            book_frame.pack(side="left", fill="x", expand=True, padx=10, pady=10)
            
            ctk.CTkLabel(
                book_frame, 
                text=row['title'],
                font=self.font_18_bold).pack(anchor="w")
            
            issue_date_fmt = self.format_time_12h(row['issue_date'])
            due_date_fmt = self.format_time_12h(row['due_date'])
            ctk.CTkLabel(
                book_frame, 
                text=f"Issued: {issue_date_fmt} | Due: {due_date_fmt}",
                font=self.font_16).pack(anchor="w")
            
            # Member details
            member_frame = ctk.CTkFrame(card_frame, fg_color="transparent")
            member_frame.pack(side="left", fill="x", expand=True, padx=10, pady=10)
            
            # Member image
            img = self.get_member_image(row['student_id'], size=(40, 40))
            img_label = ctk.CTkLabel(member_frame, image=img, text="")
            img_label.pack(side="left", padx=10)
            
            ctk.CTkLabel(
                member_frame, 
                text=row['name'],
                font=self.font_16_bold).pack(anchor="w")
            
            ctk.CTkLabel(
                member_frame, 
                text=f"ID: {row['student_id']} | Librarian: {row['librarian']}",
                font=self.font_14).pack(anchor="w")
            
            # Overdue status
            status_frame = ctk.CTkFrame(card_frame, fg_color="transparent")
            status_frame.pack(side="right", padx=10, pady=10)
            
            days_overdue = int(row['days_overdue'])
            ctk.CTkLabel(
                status_frame,
                text=f"{days_overdue} days overdue",
                font=self.font_16_bold,
                text_color=self.danger_color
            ).pack()
    
    def export_overdue_to_excel(self):
        """Export overdue books list to Excel"""
        conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
        try:
            query = '''SELECT b.title, m.name, m.student_id, 
                              t.issue_date, t.due_date, t.librarian,
                              julianday('now') - julianday(t.due_date) as days_overdue
                       FROM transactions t
                       JOIN books b ON t.book_id = b.id
                       JOIN members m ON t.member_id = m.student_id
                       WHERE t.status='issued' AND t.due_date < datetime('now')
                       ORDER BY t.due_date'''
            
            df = pd.read_sql(query, conn)
            
            if df.empty:
                messagebox.showwarning("Warning", "No overdue books to export")
                return
            
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                title="Save overdue books list as"
            )
            
            if file_path:
                df.to_excel(file_path, index=False)
                messagebox.showinfo("Success", "Overdue books exported to Excel successfully!")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to export: {str(e)}")
        finally:
            conn.close()
    
    def view_history(self):
        """View complete transaction history - Redesigned UI"""
        # Track view for back button
        if self.current_view != "history":
            self.view_stack.append(self.current_view)
        self.current_view = "history"
        
        if "history" not in self._view_frames:
            frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
            self._view_frames["history"] = frame
            
            # --- 1. Top Header Bar ---
            header_frame = ctk.CTkFrame(frame, fg_color="transparent")
            header_frame.pack(fill="x", pady=(0, 15))
            
            # Back Button (Left)
            ctk.CTkButton(header_frame, text="⬅️ Back", command=self.go_back, 
                         fg_color=self.secondary_color, hover_color=self.primary_color, 
                         width=80, height=35, font=self.font_14).pack(side="left")

            # Title & Subtitle (Left, next to back button)
            title_box = ctk.CTkFrame(header_frame, fg_color="transparent")
            title_box.pack(side="left", padx=15)
            
            ctk.CTkLabel(title_box, text="Issue & Return History", font=self.font_24_bold, text_color=self.accent_color).pack(anchor="w")
            ctk.CTkLabel(title_box, text="Complete transaction log", font=self.font_12, text_color=self.light_text).pack(anchor="w")

            # --- 2. Filters Section (Card Style) ---
            filter_card = ctk.CTkFrame(frame, fg_color=self.primary_color, corner_radius=12)
            filter_card.pack(fill="x", pady=(0, 15))
            
            # Container for filter controls
            filter_box = ctk.CTkFrame(filter_card, fg_color="transparent")
            filter_box.pack(padx=20, pady=15, fill="x")
            
            # Variables
            self.history_period = ctk.StringVar(value="all")
            self.batch_filter = ctk.StringVar(value="All")
            
            # Row 1: Filters Layout
            # We used grid layout here for nice alignment: [Period Label] [Period Options] ... [Batch Label] [Batch Combo]
            
            # Period Filter
            ctk.CTkLabel(filter_box, text="Date Range:", font=self.font_14_bold, text_color=self.light_text).pack(side="left", padx=(0, 10))
            
            periods = [("All Time", "all"), ("Last 7 Days", "7days"), ("Last 30 Days", "30days")]
            for text, val in periods:
                ctk.CTkRadioButton(filter_box, text=text, variable=self.history_period, value=val, 
                                   command=self.update_history_view, font=self.font_14,
                                   fg_color=self.accent_color, hover_color=self.secondary_color).pack(side="left", padx=10)

            # Separator / Spacer
            ctk.CTkFrame(filter_box, fg_color=self.secondary_color, width=2, height=30).pack(side="left", padx=20)

            # Batch Filter
            ctk.CTkLabel(filter_box, text="Batch:", font=self.font_14_bold, text_color=self.light_text).pack(side="left", padx=(0, 10))
            
            batches = ["All", "BS5", "BS4", "BS3", "BS2", "BS1", "HS2", "HSU2", "HS1", "HSU1"]
            batch_combo = ctk.CTkComboBox(filter_box, values=batches, variable=self.batch_filter, 
                                         font=self.font_14, width=150,
                                         fg_color=self.secondary_color, button_color=self.accent_color, border_width=0)
            batch_combo.pack(side="left")
            batch_combo.bind("<<ComboboxSelected>>", lambda event: self.update_history_view())
            
            # Clear / Apply buttons (Visual only, as logic is instant)
            # Added a 'Refresh' button which effectively re-triggers update
            ctk.CTkButton(filter_box, text="🔄 Refresh", command=self.update_history_view,
                          fg_color=self.secondary_color, hover_color=self.primary_color,
                          width=100, height=30, font=self.font_14).pack(side="right")


            # --- 3. History Table Section (Main Area) ---
            table_card = ctk.CTkFrame(frame, fg_color=self.primary_color, corner_radius=12)
            table_card.pack(fill="both", expand=True, pady=(0, 15))
            
            # Internal padding frame for the table
            self.history_table_frame = ctk.CTkFrame(table_card, fg_color="transparent")
            self.history_table_frame.pack(fill="both", expand=True, padx=15, pady=15)

            # --- 4. Footer Actions ---
            footer_frame = ctk.CTkFrame(frame, fg_color="transparent")
            footer_frame.pack(fill="x", pady=(0, 5))
            
            # Export Button (Right Aligned)
            ctk.CTkButton(footer_frame, text="📤 Export to Excel", command=self.export_history_to_excel,
                         fg_color="#107C10", hover_color="#0A550A", # Excel Green
                         width=160, height=40, font=self.font_14_bold).pack(side="right")
        
        self._switch_view("history")
        self.update_history_view()

    
    def export_history_to_excel(self):
        """Export transaction history to Excel"""
        period = self.history_period.get()
        batch = self.batch_filter.get()
        
        # Determine date filter
        date_filter = ""
        if period == "7days":
            date_filter = "AND date(t.issue_date) >= date('now', '-7 days')"
        elif period == "30days":
            date_filter = "AND date(t.issue_date) >= date('now', '-30 days')"
        
        # Determine batch filter
        batch_filter = ""
        if batch != "All":
            batch_filter = f"AND m.batch = '{batch}'"
        
        conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
        try:
            query = f'''SELECT 
                        t.issue_date as issue_date, 
                        b.title as book_title, 
                        m.name as member_name, 
                        m.student_id, 
                        m.batch,
                        t.due_date,
                        t.return_date,
                        t.librarian
                       FROM transactions t
                       JOIN books b ON t.book_id = b.id
                       JOIN members m ON t.member_id = m.student_id
                       WHERE 1=1 {date_filter} {batch_filter}
                       ORDER BY t.issue_date DESC'''
            
            df = pd.read_sql(query, conn)
            
            if df.empty:
                messagebox.showwarning("Warning", "No history records to export")
                return
            
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                title="Save transaction history as"
            )
            
            if file_path:
                df.to_excel(file_path, index=False)
                messagebox.showinfo("Success", "Transaction history exported to Excel successfully!")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to export: {str(e)}")
        finally:
            conn.close()
    
    def update_history_view(self):
        """Update history table based on selected period and batch"""
        # Clear previous table
        for widget in self.history_table_frame.winfo_children():
            widget.destroy()
        
        period = self.history_period.get()
        batch = self.batch_filter.get()
        
        # Determine date filter
        date_filter = ""
        if period == "7days":
            date_filter = "AND date(t.issue_date) >= date('now', '-7 days')"
        elif period == "30days":
            date_filter = "AND date(t.issue_date) >= date('now', '-30 days')"
        
        # Determine batch filter
        batch_filter = ""
        if batch != "All":
            batch_filter = f"AND m.batch = '{batch}'"
        
        conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
        try:
            # Get history with book and member details
            query = f'''SELECT 
                        t.issue_date as issue_date, 
                        b.title as book_title, 
                        m.name as member_name, 
                        m.student_id, 
                        m.batch,
                        t.due_date,
                        t.return_date,
                        t.librarian
                       FROM transactions t
                       JOIN books b ON t.book_id = b.id
                       JOIN members m ON t.member_id = m.student_id
                       WHERE 1=1 {date_filter} {batch_filter}
                       ORDER BY t.issue_date DESC'''
            
            df = pd.read_sql(query, conn)
            
            if df.empty:
                label = ctk.CTkLabel(self.history_table_frame, text="No history records found", font=self.font_16)
                label.pack(pady=50)
                return
            
            # Create Treeview
            tree = ttk.Treeview(self.history_table_frame, columns=list(df.columns), show="headings")
            
            # Add columns
            for col in df.columns:
                tree.heading(col, text=col.replace("_", " ").title())
                tree.column(col, width=100, anchor="center")
            
            # Set specific widths
            tree.column("issue_date", width=150)
            tree.column("book_title", width=250)
            tree.column("member_name", width=150)
            tree.column("student_id", width=100)
            tree.column("batch", width=80)
            tree.column("due_date", width=120)
            tree.column("return_date", width=150)
            tree.column("librarian", width=150)
            
            # Add data with formatted dates
            for _, row in df.iterrows():
                formatted_values = []
                for i, value in enumerate(row):
                    if i in [0, 5, 6]:  # Date columns
                        formatted_values.append(self.format_time_12h(value))
                    else:
                        formatted_values.append(value)
                tree.insert("", "end", values=tuple(formatted_values))
            
            # Add scrollbar
            scrollbar = ttk.Scrollbar(self.history_table_frame, orient="vertical", command=tree.yview)
            tree.configure(yscrollcommand=scrollbar.set)
            scrollbar.pack(side="right", fill="y")
            
            tree.pack(fill="both", expand=True)
            
            # Style the treeview
            style = ttk.Style()
            style.theme_use("default")
            
            # Configure colors to match the dark theme request
            # Background: #1E293B (Primary from request, matches card background)
            # Text: #F8FAFC (Text Primary)
            # Header: #334155 (Secondary)
            
            style.configure("Treeview", 
                          background="#1E293B", 
                          foreground="#F8FAFC",
                          rowheight=40,
                          fieldbackground="#1E293B",
                          bordercolor="#334155",
                          borderwidth=0)
                          
            style.configure("Treeview.Heading", 
                          background="#334155", 
                          foreground="#F8FAFC",
                          font=self.font_14_bold,
                          relief="flat")
                          
            style.map("Treeview", 
                      background=[('selected', self.accent_color)], # #38BDF8
                      foreground=[('selected', '#000000')])
            
        finally:
            conn.close()
    
    def search_book(self):
        """Search for books by title, author, or category"""
        search_window = self.create_popup_window("Search Books", 600, 500)
        
        # Search frame
        search_frame = ctk.CTkFrame(search_window)
        search_frame.pack(pady=10, padx=10, fill="x")
        
        # Search term
        ctk.CTkLabel(search_frame, text="Search Term:", font=self.font_16).pack(pady=(10, 0))
        search_entry = ctk.CTkEntry(search_frame, font=self.font_16)
        search_entry.pack(pady=5, fill="x", padx=10)
        search_entry.focus_set()  # Auto-focus
        
        # Search type
        ctk.CTkLabel(search_frame, text="Search By:", font=self.font_16).pack(pady=(10, 0))
        search_type = ctk.StringVar(value="title")
        search_type_frame = ctk.CTkFrame(search_frame, fg_color="transparent")
        search_type_frame.pack()
        
        ctk.CTkRadioButton(search_type_frame, text="Title", variable=search_type, value="title", font=self.font_16).pack(side="left", padx=5)
        ctk.CTkRadioButton(search_type_frame, text="Author", variable=search_type, value="author", font=self.font_16).pack(side="left", padx=5)
        ctk.CTkRadioButton(search_type_frame, text="Category", variable=search_type, value="category", font=self.font_16).pack(side="left", padx=5)
        ctk.CTkRadioButton(search_type_frame, text="ID", variable=search_type, value="id", font=self.font_16).pack(side="left", padx=5)
        
        # Search button
        search_btn = ctk.CTkButton(
            search_frame,
            text="🔍 Search",
            command=lambda: perform_search(),
            fg_color=self.secondary_color,
            hover_color=self.primary_color,
            font=self.font_16
        )
        search_btn.pack(pady=10)
        
        # Bind Enter key to search
        search_entry.bind("<Return>", lambda event: perform_search())
        
        # Results frame
        results_frame = ctk.CTkFrame(search_window)
        results_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        def perform_search():
            term = search_entry.get().strip()
            if not term:
                messagebox.showerror("Error", "Please enter a search term")
                return
            
            search_by = search_type.get()
            if search_by == "id":
                term = term.upper()
            
            # Clear previous results
            for widget in results_frame.winfo_children():
                widget.destroy()
            
            # Show loading
            loading_label = ctk.CTkLabel(results_frame, text="Searching...", font=self.font_16)
            loading_label.pack(pady=20)

            def run_search():
                try:
                    conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                    if search_by == "id":
                        # Exact match for ID
                        query = "SELECT id, title, author, category, available_copies FROM books WHERE id = ?"
                        params = (term,)
                    else:
                        # Partial match for other fields
                        query = f"SELECT id, title, author, category, available_copies FROM books WHERE {search_by} LIKE ?"
                        params = (f"%{term}%",)
                    
                    df = pd.read_sql(query, conn, params=params)
                    conn.close()
                    self.after(0, lambda: self._display_search_results(df, results_frame, loading_label))
                except Exception as e:
                    self.after(0, lambda: messagebox.showerror("Error", f"Search failed: {str(e)}"))

            threading.Thread(target=run_search, daemon=True).start()

    def _display_search_results(self, df, results_frame, loading_label):
        loading_label.destroy()
        if df.empty:
            label = ctk.CTkLabel(results_frame, text="No books found", font=self.font_16)
            label.pack(pady=50)
            return
        
        # Create Treeview
        tree = ttk.Treeview(results_frame, columns=list(df.columns), show="headings")
        
        # Add columns
        for col in df.columns:
            tree.heading(col, text=col.replace("_", " ").title())
            tree.column(col, width=100, anchor="center")
        
        # Set specific widths
        tree.column("id", width=100)
        tree.column("title", width=200)
        tree.column("author", width=150)
        tree.column("category", width=120)
        tree.column("available_copies", width=80)
        
        # Add data
        for _, row in df.iterrows():
            tree.insert("", "end", values=tuple(row))
        
        # Add scrollbar
        scrollbar = ttk.Scrollbar(results_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        
        tree.pack(fill="both", expand=True)

        # Style the treeview
        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview", 
                      background="#2a2a2a", 
                      foreground="white",
                      rowheight=40,
                      fieldbackground="#2a2a2a",
                      bordercolor="#3a3a3a",
                      borderwidth=0)
        style.configure("Treeview.Heading", 
                      background=self.secondary_color, 
                      foreground="white",
                      relief="flat")
        style.map("Treeview", background=[('selected', self.success_color)])
    
    def show_analytics(self):
        """Show library analytics dashboard - optimized with view caching"""
        # Track view for back button
        if self.current_view != "analytics":
            self.view_stack.append(self.current_view)
        self.current_view = "analytics"
        
        if "analytics" not in self._view_frames:
            frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
            self._view_frames["analytics"] = frame
            
            # Persistent parts
            ctk.CTkButton(frame, text="⬅️ Back", command=self.go_back, 
                         fg_color=self.secondary_color, hover_color=self.primary_color, 
                         width=100, height=35, font=self.font_14).pack(anchor="nw", padx=10, pady=5)
            
            ctk.CTkLabel(frame, text="📊 Library Analytics", font=self.font_24_bold, text_color=self.accent_color).pack(pady=10)
            
            self._analytics_scroll = ctk.CTkScrollableFrame(frame)
            self._analytics_scroll.pack(fill="both", expand=True, padx=20, pady=10)
            
            # Enable mouse wheel scrolling
            def _on_mousewheel(event):
                self._analytics_scroll._parent_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
            self._analytics_scroll.bind("<MouseWheel>", _on_mousewheel)

        self._switch_view("analytics")
        
        # Clear dynamic content in scroll frame
        for widget in self._analytics_scroll.winfo_children():
            widget.destroy()
            
        analytics_frame = ctk.CTkFrame(self._analytics_scroll, fg_color="transparent")
        analytics_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        # ========== Running time section (live updating) ==========
        running_time_frame = ctk.CTkFrame(analytics_frame, fg_color=self.primary_color, corner_radius=10)
        running_time_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            running_time_frame, 
            text="⏰ App Running Time",
            font=self.font_20_bold).pack(pady=10)
        
        # Create running time label (live updating)
        running_time = self.calculate_running_time()
        formatted_time = self.format_running_time(running_time)
        self.running_time_label = ctk.CTkLabel(
            running_time_frame, 
            text=formatted_time,
            font=self.font_28_bold,
            text_color=self.accent_color)
        self.running_time_label.pack(pady=10)
        
        # Start updating running time display
        self.update_running_time_display()
        # ==========================================================
        
        # Loading indicator for DB data
        loading_label = ctk.CTkLabel(analytics_frame, text="Calculating analytics...", font=self.font_16)
        loading_label.pack(pady=50)

        def fetch_analytics():
            try:
                conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
                
                # Today's attendance
                query = '''SELECT l.full_name, a.login_time, a.logout_time, 
                                  a.duration, a.books_issued
                           FROM attendance a
                           JOIN librarians l ON a.librarian_id = l.username
                           WHERE date(a.login_time) = date('now')
                           ORDER BY a.login_time DESC'''
                df_attendance = pd.read_sql(query, conn)
                
                # Stats
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM books")
                total_books = c.fetchone()[0]
                
                c.execute("SELECT COUNT(*) FROM members")
                total_members = c.fetchone()[0]
                
                c.execute("SELECT SUM(available_copies) FROM books")
                available_books = c.fetchone()[0] or 0
                
                c.execute("SELECT COUNT(*) FROM transactions WHERE status='issued'")
                issued_books = c.fetchone()[0]
                
                c.execute("SELECT COUNT(*) FROM transactions WHERE status='issued' AND due_date < datetime('now')")
                overdue_books = c.fetchone()[0]
                
                # Extended Stats
                c.execute("SELECT COUNT(DISTINCT member_id) FROM transactions")
                active_members = c.fetchone()[0]
                inactive_members = total_members - active_members

                # Chart Data
                df_batch = pd.read_sql('''SELECT m.batch, COUNT(*) as count FROM transactions t 
                                        JOIN members m ON t.member_id = m.student_id GROUP BY m.batch''', conn)
                df_monthly = pd.read_sql('''SELECT strftime('%Y-%m', issue_date) as month, COUNT(*) as count 
                                          FROM transactions GROUP BY month ORDER BY month''', conn)
                c.execute('''SELECT SUM(CASE WHEN return_date <= due_date THEN 1 ELSE 0 END), 
                            SUM(CASE WHEN return_date > due_date THEN 1 ELSE 0 END) 
                            FROM transactions WHERE return_date IS NOT NULL''')
                res = c.fetchone()
                discipline = res if res and res[0] is not None else (0, 0)

                # Table Data
                df_most = pd.read_sql('''SELECT b.id as "Book Code", b.title as "Title", COUNT(*) as "Issues" 
                                       FROM transactions t JOIN books b ON t.book_id = b.id 
                                       GROUP BY b.id, b.title ORDER BY "Issues" DESC LIMIT 10''', conn)
                df_least = pd.read_sql('''SELECT b.id as "Book Code", b.title as "Title", COUNT(*) as "Issues" 
                                        FROM transactions t JOIN books b ON t.book_id = b.id 
                                        GROUP BY b.id, b.title ORDER BY "Issues" ASC LIMIT 10''', conn)
                df_teachers = pd.read_sql('''SELECT m.name as "Teacher Name", COUNT(*) as "Activity" 
                                           FROM transactions t JOIN members m ON t.member_id = m.student_id 
                                           WHERE m.type = 'teacher' GROUP BY m.student_id, m.name 
                                           ORDER BY "Activity" DESC''', conn)
                df_rated = pd.read_sql('''SELECT b.title as "Book Name", AVG(r.rating) as "Rating" 
                                        FROM ratings r JOIN books b ON r.book_id = b.id 
                                        GROUP BY b.id, b.title ORDER BY "Rating" DESC LIMIT 10''', conn)

                conn.close()

                # --- Data Sanitization for Matplotlib ---
                # Ensure categorical columns don't have None/NaN which crashes Matplotlib
                if not df_batch.empty:
                    df_batch['batch'] = df_batch['batch'].fillna('Unknown').astype(str)
                if not df_monthly.empty:
                    df_monthly['month'] = df_monthly['month'].fillna('Unknown').astype(str)
                # Ensure table data is also clean
                df_most = df_most.fillna('N/A')
                df_least = df_least.fillna('N/A')
                df_teachers = df_teachers.fillna('N/A')
                df_rated = df_rated.fillna(0)
                # ----------------------------------------

                self.after(0, lambda: self._populate_analytics_ui(
                    analytics_frame, loading_label, df_attendance, total_books, total_members, 
                    available_books, issued_books, overdue_books, active_members, inactive_members,
                    df_batch, df_monthly, discipline, df_most, df_least, df_teachers, df_rated
                ))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", f"Failed to load analytics: {str(e)}"))

        threading.Thread(target=fetch_analytics, daemon=True).start()

    def _populate_analytics_ui(self, analytics_frame, loading_label, df_attendance, total_books, total_members, 
                             available_books, issued_books, overdue_books, active_members, inactive_members,
                             df_batch, df_monthly, discipline, df_most, df_least, df_teachers, df_rated):
        loading_label.destroy()
        
        # === 1. TOP STATS ROW ===
        stats_frame = ctk.CTkFrame(analytics_frame, fg_color="transparent")
        stats_frame.pack(fill="x", pady=(0, 20))
        
        kpis = [
            ("Total Books", total_books, self.primary_color),
            ("Issued Books", issued_books, self.accent_color),
            ("Overdue", overdue_books, self.danger_color),
            ("Active Members", active_members, self.success_color)
        ]
        
        for i, (label, value, color) in enumerate(kpis):
            card = ctk.CTkFrame(stats_frame, fg_color=self.primary_color, corner_radius=15)
            card.grid(row=0, column=i, padx=10, sticky="ew")
            stats_frame.grid_columnconfigure(i, weight=1)
            
            ctk.CTkLabel(card, text=str(value), font=self.font_28_bold, text_color=color).pack(pady=(15, 0))
            ctk.CTkLabel(card, text=label, font=self.font_14, text_color=self.dark_text).pack(pady=(0, 15))


        # === 2. CHARTS SECTION (2-Column Grid) ===
        charts_container = ctk.CTkFrame(analytics_frame, fg_color="transparent")
        charts_container.pack(fill="x", pady=10)
        charts_container.grid_columnconfigure((0, 1), weight=1)

        # Chart 1: Batch-wise Usage
        batch_card = ctk.CTkFrame(charts_container, fg_color=self.primary_color, corner_radius=15)
        batch_card.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        ctk.CTkLabel(batch_card, text="📊 Batch-wise Usage", font=self.font_16_bold, text_color=self.light_text).pack(pady=10)
        
        if not df_batch.empty:
            fig_batch = Figure(figsize=(5, 3.5), dpi=80, facecolor=self.primary_color)
            ax_batch = fig_batch.add_subplot(111)
            ax_batch.bar(df_batch['batch'], df_batch['count'], color=self.accent_color)
            ax_batch.set_facecolor(self.primary_color)
            ax_batch.tick_params(colors=self.light_text, labelsize=8)
            for spine in ax_batch.spines.values(): spine.set_color(self.secondary_color)
            
            canvas_batch = FigureCanvasTkAgg(fig_batch, master=batch_card)
            canvas_batch.draw()
            canvas_batch.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
        else:
             ctk.CTkLabel(batch_card, text="No Data Available", text_color=self.dark_text).pack(expand=True)

        # Chart 2: Monthly Trend
        trend_card = ctk.CTkFrame(charts_container, fg_color=self.primary_color, corner_radius=15)
        trend_card.grid(row=0, column=1, padx=10, pady=10, sticky="nsew")
        ctk.CTkLabel(trend_card, text="📈 Monthly Issue Trend", font=self.font_16_bold, text_color=self.light_text).pack(pady=10)
        
        if not df_monthly.empty:
            fig_trend = Figure(figsize=(5, 3.5), dpi=80, facecolor=self.primary_color)
            ax_trend = fig_trend.add_subplot(111)
            ax_trend.plot(df_monthly['month'], df_monthly['count'], marker='o', color='#F59E0B') # Amber
            ax_trend.set_facecolor(self.primary_color)
            ax_trend.tick_params(colors=self.light_text, labelsize=8)
            for spine in ax_trend.spines.values(): spine.set_color(self.secondary_color)

            canvas_trend = FigureCanvasTkAgg(fig_trend, master=trend_card)
            canvas_trend.draw()
            canvas_trend.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
        else:
             ctk.CTkLabel(trend_card, text="No Data Available", text_color=self.dark_text).pack(expand=True)

        # Chart 3: Return Discipline (Full Width or Grid)
        disc_card = ctk.CTkFrame(charts_container, fg_color=self.primary_color, corner_radius=15)
        disc_card.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")
        ctk.CTkLabel(disc_card, text="🥧 Return Discipline", font=self.font_16_bold, text_color=self.light_text).pack(pady=10)
        
        on_time, late = discipline
        if on_time + late > 0:
            fig_pie = Figure(figsize=(5, 3.5), dpi=80, facecolor=self.primary_color)
            ax_pie = fig_pie.add_subplot(111)
            ax_pie.pie([on_time, late], labels=['On-time', 'Late'], autopct='%1.1f%%', 
                      colors=[self.success_color, self.danger_color], textprops={'color': self.light_text})
            
            canvas_pie = FigureCanvasTkAgg(fig_pie, master=disc_card)
            canvas_pie.draw()
            canvas_pie.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
        else:
             ctk.CTkLabel(disc_card, text="No Data Available", text_color=self.dark_text).pack(expand=True)


        # === 3. TABLES SECTION (Full Width Cards) ===
        tables_container = ctk.CTkFrame(analytics_frame, fg_color="transparent")
        tables_container.pack(fill="x", pady=20)

        def add_styled_table(title, df):
            card = ctk.CTkFrame(tables_container, fg_color=self.primary_color, corner_radius=15)
            card.pack(fill="x", pady=10, padx=10)
            
            ctk.CTkLabel(card, text=title, font=self.font_18_bold, text_color=self.accent_color).pack(anchor="w", padx=20, pady=(15, 10))
            
            if df.empty:
                ctk.CTkLabel(card, text="No Data Available", text_color=self.dark_text).pack(pady=20)
                return

            # Table Container
            table_frame = ctk.CTkFrame(card, fg_color="transparent")
            table_frame.pack(fill="x", padx=20, pady=(0, 20))
            
            # Use Treeview with custom style
            style = ttk.Style()
            style.configure("Analytics.Treeview", background=self.primary_color, fieldbackground=self.primary_color, foreground=self.light_text, borderwidth=0, rowheight=30)
            style.configure("Analytics.Treeview.Heading", background=self.secondary_color, foreground=self.light_text, font=("Arial", 12, "bold"))
            
            columns = list(df.columns)
            tree = ttk.Treeview(table_frame, columns=columns, show="headings", style="Analytics.Treeview", height=min(len(df), 10))
            
            for col in columns:
                tree.heading(col, text=col)
                tree.column(col, anchor="center")
            
            for _, row in df.iterrows():
                tree.insert("", "end", values=list(row))
                
            tree.pack(fill="x")

        add_styled_table("🔥 Most Issued Books", df_most)
        add_styled_table("🧊 Least Issued Books", df_least)
        add_styled_table("👨‍🏫 Teacher Activity", df_teachers)
        add_styled_table("⭐ Top Rated Books", df_rated)

    
    def create_issues_chart(self, parent, time_period):
        """Create a chart of book issues for the specified time period"""
        conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
        
        # Determine date filter based on time period
        if time_period == "today":
            date_filter = "AND date(issue_date) = date('now')"
            group_by = "strftime('%H', issue_date)"
            x_label = "Hour of Day"
        elif time_period == "week":
            date_filter = "AND date(issue_date) >= date('now', '-7 days')"
            group_by = "date(issue_date)"
            x_label = "Day"
        elif time_period == "month":
            date_filter = "AND date(issue_date) >= date('now', '-30 days')"
            group_by = "date(issue_date)"
            x_label = "Day"
        else:  # all time
            date_filter = ""
            group_by = "strftime('%Y-%m', issue_date)"
            x_label = "Month"
        
        try:
            query = f'''SELECT {group_by} as period, COUNT(*) as count 
                       FROM transactions 
                       WHERE 1=1 {date_filter}
                       GROUP BY period 
                       ORDER BY period'''
            
            df = pd.read_sql(query, conn)
            
            if df.empty:
                ctk.CTkLabel(parent, text="No data available", font=self.font_16).pack(pady=50)
                return
            
            # Create figure
            fig = Figure(figsize=(8, 5), dpi=100)
            ax = fig.add_subplot(111)
            
            # Plot data
            if time_period == "today":
                # Ensure we have all hours of the day
                hours = [f"{i:02d}" for i in range(24)]
                counts = [0] * 24
                
                for _, row in df.iterrows():
                    hour = int(row['period'])
                    counts[hour] = row['count']
                
                ax.bar(hours, counts)
                ax.set_xlabel(x_label)
                ax.set_ylabel('Number of Books Issued')
                ax.set_title('Books Issued Today by Hour')
                
            else:
                ax.bar(df['period'], df['count'])
                ax.set_xlabel(x_label)
                ax.set_ylabel('Number of Books Issued')
                ax.set_title(f'Books Issued - {time_period.capitalize()}')
                ax.tick_params(axis='x', rotation=45)
            
            # Add to canvas
            canvas = FigureCanvasTkAgg(fig, master=parent)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
            
        except Exception as e:
            ctk.CTkLabel(parent, text=f"Error creating chart: {str(e)}", font=self.font_16).pack(pady=50)
        finally:
            conn.close()
    
    def admin_panel(self):
        """Admin control panel"""
        self._switch_view()
        
        # Track view for back button
        if self.current_view != "admin":
            self.view_stack.append(self.current_view)
        self.current_view = "admin"
        
        # Add back button
        self.add_back_button()
        
        # Title
        title_label = ctk.CTkLabel(
            self.content_frame,
            text="🔒 ADMIN PANEL",
            font=self.font_24_bold,
            text_color=self.danger_color)
        title_label.pack(pady=20)
        
        # Theme selection
        theme_frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
        theme_frame.pack(pady=10)
        
        ctk.CTkLabel(theme_frame, text="Appearance Mode:", font=self.font_16).pack(side="left", padx=10)
        
        self.theme_var = ctk.StringVar(value=ctk.get_appearance_mode())
        theme_combo = ctk.CTkComboBox(theme_frame, 
                                     values=["Light", "Dark", "System"], 
                                     variable=self.theme_var,
                                     command=self.change_theme,
                                     font=self.font_16)
        theme_combo.pack(side="left", padx=10)
        
        # Storage info
        storage_frame = ctk.CTkFrame(self.content_frame)
        storage_frame.pack(fill="x", padx=50, pady=20)
        
        # Get storage info
        db_size = os.path.getsize('islamic_library.db') / (1024 * 1024)  # in MB
        images_size = sum(os.path.getsize(f) for f in os.listdir('images') if os.path.isfile(f)) / (1024 * 1024) if os.path.exists('images') else 0
        total_size = db_size + images_size
        
        # Display storage info
        ctk.CTkLabel(storage_frame, text="Storage Usage:", font=self.font_16_bold).pack(pady=5)
        
        info_text = f"""
        Database: {db_size:.2f} MB
        Images: {images_size:.2f} MB
        Total: {total_size:.2f} MB
        """
        
        ctk.CTkLabel(storage_frame, text=info_text, font=self.font_14).pack(pady=10)
        
        # Clear data button
        clear_btn = ctk.CTkButton(
            storage_frame,
            text="Clear All Data",
            command=self.confirm_clear_data,
            fg_color=self.danger_color,
            hover_color=self.secondary_color,
            font=self.font_14
        )
        clear_btn.pack(pady=10)
        
        # Edit data section
        edit_frame = ctk.CTkFrame(self.content_frame)
        edit_frame.pack(fill="both", expand=True, padx=50, pady=20)
        
        ctk.CTkLabel(edit_frame, text="Edit Data:", font=self.font_16_bold).pack(pady=5)
        
        # Buttons to edit different sections
        buttons = [
            ("Edit Books", self.show_home),
            ("Edit Members", self.view_members),
            ("Edit Transactions", self.view_history),
            ("Edit Ratings", self.show_ratings)
        ]
        
        for text, command in buttons:
            btn = ctk.CTkButton(
                edit_frame,
                text=text,
                command=command,
                fg_color=self.primary_color,
                hover_color=self.secondary_color,
                width=200,
                height=40,
                font=self.font_14
            )
            btn.pack(pady=5)
    
    def show_ratings(self):
        """Show book ratings for moderation"""
        self._switch_view()
        
        # Track view for back button
        if self.current_view != "ratings":
            self.view_stack.append(self.current_view)
        self.current_view = "ratings"
        # Add back button
        self.add_back_button()
        
        # Title
        title_label = ctk.CTkLabel(
            self.content_frame,
            text="⭐ Book Ratings",
            font=self.font_20_bold,
            text_color=self.accent_color)
        title_label.pack(pady=10)
        
        # Table frame
        table_frame = ctk.CTkFrame(self.content_frame)
        table_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        conn = sqlite3.connect("islamic_library.db", timeout=10, check_same_thread=False)
        try:
            # Get ratings with book and member details
            query = '''SELECT r.id, b.title, m.name, 
                              CASE WHEN r.rating = 1 THEN '👍 Like' ELSE '👎 Dislike' END as rating,
                              r.timestamp
                       FROM ratings r
                       JOIN books b ON r.book_id = b.id
                       JOIN members m ON r.member_id = m.student_id
                       ORDER BY r.timestamp DESC'''
            
            df = pd.read_sql(query, conn)
            
            if df.empty:
                label = ctk.CTkLabel(table_frame, text="No ratings found", font=self.font_16)
                label.pack(pady=50)
                return
            
            # Create Treeview
            tree = ttk.Treeview(table_frame, columns=list(df.columns), show="headings")
            
            # Add columns
            for col in df.columns:
                tree.heading(col, text=col.replace("_", " ").title())
                tree.column(col, width=150, anchor="center")
            
            # Set specific widths
            tree.column("id", width=50)
            tree.column("title", width=250)
            tree.column("name", width=150)
            tree.column("rating", width=100)
            tree.column("timestamp", width=200)
            
            # Add data with formatted dates
            for _, row in df.iterrows():
                formatted_values = list(row)
                formatted_values[4] = self.format_time_12h(row['timestamp'])  # Format timestamp
                tree.insert("", "end", values=tuple(formatted_values))
            
            # Add scrollbar
            scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
            tree.configure(yscrollcommand=scrollbar.set)
            scrollbar.pack(side="right", fill="y")
            
            tree.pack(fill="both", expand=True)
            
            # Style the treeview
            style = ttk.Style()
            style.theme_use("default")
            style.configure("Treeview", 
                          background="#2a2a2a", 
                          foreground="white",
                          rowheight=40,
                          fieldbackground="#2a2a2a",
                          bordercolor="#3a3a3a",
                          borderwidth=0)
            style.configure("Treeview.Heading", 
                          background=self.secondary_color, 
                          foreground="white",
                          relief="flat")
            style.map("Treeview", background=[('selected', self.success_color)])
            
            # Delete rating button
            def delete_rating():
                selected = tree.focus()
                if not selected:
                    return
                
                item = tree.item(selected)
                rating_id = item['values'][0]
                
                if messagebox.askyesno("Confirm", "Delete this rating?"):
                    c = conn.cursor()
                    c.execute("DELETE FROM ratings WHERE id=?", (rating_id,))
                    conn.commit()
                    messagebox.showinfo("Success", "Rating deleted!")
                    self.show_ratings()
            
            delete_btn = ctk.CTkButton(
                self.content_frame,
                text="Delete Selected Rating",
                command=delete_rating,
                fg_color=self.danger_color,
                hover_color=self.secondary_color,
                font=self.font_14
            )
            delete_btn.pack(pady=10)
            
        finally:
            conn.close()
    
    def change_theme(self, choice):
        """Change app theme"""
        ctk.set_appearance_mode(choice)
    
    def confirm_clear_data(self):
        """Confirm before clearing all data"""
        if messagebox.askyesno("Confirm", "WARNING: This will delete ALL data. Continue?"):
            self.clear_all_data()
    
    def clear_all_data(self):
        """Clear all app data"""
        try:
            # Close database connection if open
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
            
            # Delete database file
            if os.path.exists('islamic_library.db'):
                os.remove('islamic_library.db')
            
            # Delete images directory
            if os.path.exists('images'):
                shutil.rmtree('images')
            
            # Reinitialize database
            self.init_db()
            
            messagebox.showinfo("Success", "All data has been cleared. The app will now restart.")
            
            # Restart the app
            self.destroy()
            app = IslamicLibraryApp()
            app.mainloop()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to clear data: {str(e)}")

if __name__ == "__main__":
    app = IslamicLibraryApp()
    app.mainloop()