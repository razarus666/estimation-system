"""
DBè¨­è¨: å¨ãã¼ãã«å®ç¾©
é»æ°è¨­åç©ç®ã»è¦ç©Webãµã¼ãã¹
"""
import sqlite3
import os
import bcrypt
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "data/estimation.db")


def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    # === ã¦ã¼ã¶ã¼ ===
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        full_name TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'pending',
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        approved_at TEXT,
        approved_by INTEGER,
        FOREIGN KEY (approved_by) REFERENCES users(id)
    )""")

    # === æ¡ä»¶ ===
    c.execute("""CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        client_name TEXT,
        created_by INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        status TEXT NOT NULL DEFAULT 'active',
        FOREIGN KEY (created_by) REFERENCES users(id)
    )""")

    # === ã¢ããã­ã¼ããã¡ã¤ã« ===
    c.execute("""CREATE TABLE IF NOT EXISTS project_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        file_type TEXT NOT NULL,
        original_name TEXT NOT NULL,
        stored_path TEXT NOT NULL,
        file_size INTEGER,
        uploaded_by INTEGER NOT NULL,
        uploaded_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (project_id) REFERENCES projects(id),
        FOREIGN KEY (uploaded_by) REFERENCES users(id)
    )""")

    # === ç©ç®ãã¹ã¿ ===
    c.execute("""CREATE TABLE IF NOT EXISTS estimate_master (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_page TEXT,
        category_no TEXT,
        field_category TEXT,
        material_name TEXT,
        spec_summary TEXT,
        remarks TEXT,
        construction_method TEXT,
        unit TEXT,
        material_unit_price REAL DEFAULT 0,
        material_cost REAL DEFAULT 0,
        labor_cost REAL DEFAULT 0,
        expense_cost REAL DEFAULT 0,
        composite_unit_price REAL DEFAULT 0,
        removal_productivity REAL DEFAULT 0,
        removal_cost REAL DEFAULT 0,
        normalized_name TEXT,
        normalized_spec TEXT,
        normalized_method TEXT,
        match_key TEXT,
        source_text TEXT,
        master_version INTEGER DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
    )""")

    # === ææãªã¹ãï¼æ¡ä»¶ãã¨ï¼ ===
    c.execute("""CREATE TABLE IF NOT EXISTS material_list (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        row_no INTEGER,
        material_name TEXT,
        spec TEXT,
        size TEXT,
        quantity REAL DEFAULT 0,
        unit TEXT,
        construction_method TEXT,
        field_category TEXT,
        drawing_ref TEXT,
        remarks TEXT,
        FOREIGN KEY (project_id) REFERENCES projects(id)
    )""")

    # === ç§åçµæ ===
    c.execute("""CREATE TABLE IF NOT EXISTS match_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        material_id INTEGER NOT NULL,
        candidate_rank INTEGER DEFAULT 1,
        master_id INTEGER,
        match_type TEXT,
        confidence REAL DEFAULT 0,
        reason TEXT,
        is_adopted INTEGER DEFAULT 0,
        master_name TEXT,
        master_spec TEXT,
        master_method TEXT,
        master_unit TEXT,
        composite_unit_price REAL DEFAULT 0,
        removal_productivity REAL DEFAULT 0,
        source_page TEXT,
        field_category TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (project_id) REFERENCES projects(id),
        FOREIGN KEY (material_id) REFERENCES material_list(id),
        FOREIGN KEY (master_id) REFERENCES estimate_master(id)
    )""")

    # === è¦ç©æç´° ===
    c.execute("""CREATE TABLE IF NOT EXISTS estimate_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        row_no INTEGER,
        field_category TEXT,
        material_name TEXT,
        spec TEXT,
        construction_method TEXT,
        unit TEXT,
        quantity REAL DEFAULT 0,
        composite_unit_price REAL DEFAULT 0,
        amount REAL DEFAULT 0,
        productivity REAL DEFAULT 0,
        productivity_total REAL DEFAULT 0,
        source_pdf TEXT,
        source_page TEXT,
        match_type TEXT,
        confidence REAL DEFAULT 0,
        match_reason TEXT,
        remarks TEXT,
        is_manual_added INTEGER DEFAULT 0,
        material_id INTEGER,
        master_id INTEGER,
        FOREIGN KEY (project_id) REFERENCES projects(id)
    )""")

    # === æä¿®æ­£å±¥æ­´ ===
    c.execute("""CREATE TABLE IF NOT EXISTS edit_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        detail_id INTEGER NOT NULL,
        column_name TEXT NOT NULL,
        old_value TEXT,
        new_value TEXT,
        edited_by INTEGER NOT NULL,
        edited_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (project_id) REFERENCES projects(id),
        FOREIGN KEY (edited_by) REFERENCES users(id)
    )""")

    # === å­¦ç¿è¾æ¸ ===
    c.execute("""CREATE TABLE IF NOT EXISTS learning_dictionary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        input_name TEXT NOT NULL,
        canonical_name TEXT NOT NULL,
        input_spec TEXT,
        canonical_spec TEXT,
        input_method TEXT,
        canonical_method TEXT,
        confidence REAL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'candidate',
        confirmed_by INTEGER,
        confirmed_at TEXT,
        source_project_id INTEGER,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (confirmed_by) REFERENCES users(id)
    )""")

    # === è¦ç©å±éè¨­å® ===
    c.execute("""CREATE TABLE IF NOT EXISTS estimate_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        setting_key TEXT UNIQUE NOT NULL,
        setting_value TEXT,
        description TEXT,
        updated_by INTEGER,
        updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
    )""")

    # === ãã¹ã¿æ´æ°å±¥æ­´ ===
    c.execute("""CREATE TABLE IF NOT EXISTS master_update_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        action TEXT NOT NULL,
        source_file TEXT,
        records_added INTEGER DEFAULT 0,
        records_updated INTEGER DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'pending',
        updated_by INTEGER,
        updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (updated_by) REFERENCES users(id)
    )""")

    # === ç£æ»ã­ã° ===
    c.execute("""CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        action TEXT NOT NULL,
        entity_type TEXT,
        entity_id TEXT,
        level TEXT DEFAULT 'INFO',
        details TEXT,
        ip_address TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (user_id) REFERENCES users(id)
    )""")

    # === ã¨ã©ã¼ã­ã° ===
    c.execute("""CREATE TABLE IF NOT EXISTS error_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        error_type TEXT,
        error_message TEXT,
        traceback TEXT,
        url TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
    )""")

    # === åæè¨­å®ãã¼ã¿ ===
    settings_defaults = [
        ("company_name", "", "èªç¤¾å"),
        ("company_address", "", "èªç¤¾ä½æ"),
        ("company_tel", "", "èªç¤¾é»è©±çªå·"),
        ("company_fax", "", "èªç¤¾FAX"),
        ("labor_unit_price", "25000", "å·¥äºåä¾¡ï¼å´ååä¾¡ï¼å/äººå·¥"),
        ("estimate_title", "é»æ°è¨­åå·¥äº å¾¡è¦ç©æ¸", "è¦ç©æ¸ã¿ã¤ãã«"),
        ("estimate_conditions", "1. æ¬è¦ç©ã¯æ¦ç®ã§ã\n2. æå¹æé: è¦ç©æ¥ãã30æ¥é", "è¦ç©æ¡ä»¶"),
        ("auto_adopt_threshold", "0.75", "èªåæ¡ç¨ãããå¤"),
        ("fuzzy_threshold", "0.50", "ããã¾ãç§åãããå¤"),
        ("max_candidates", "5", "æå¤§åè£æ°"),
    ]
    for key, val, desc in settings_defaults:
        c.execute("""INSERT OR IGNORE INTO estimate_settings
            (setting_key, setting_value, description) VALUES (?,?,?)""",
            (key, val, desc))

    conn.commit()
    conn.close()


def create_admin_user(email, password, full_name):
    conn = get_db()
    pw_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    try:
        conn.execute("""INSERT INTO users (email, password_hash, full_name, role, is_active, approved_at)
            VALUES (?, ?, ?, 'admin', 1, datetime('now','localtime'))""",
            (email, pw_hash, full_name))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def add_audit_log(user_id, action, entity_type="", entity_id="", level="INFO", details="", ip=""):
    try:
        conn = get_db()
        conn.execute("""INSERT INTO audit_log
            (user_id, action, entity_type, entity_id, level, details, ip_address)
            VALUES (?,?,?,?,?,?,?)""",
            (user_id, action, entity_type, entity_id, level, details, ip))
        conn.commit()
        conn.close()
    except Exception:
        pass


def add_error_log(user_id, error_type, error_message, traceback_str="", url=""):
    try:
        conn = get_db()
        conn.execute("""INSERT INTO error_log
            (user_id, error_type, error_message, traceback, url)
            VALUES (?,?,?,?,?)""",
            (user_id, error_type, error_message, traceback_str, url))
        conn.commit()
        conn.close()
    except Exception:
        pass
