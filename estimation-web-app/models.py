"""
DB設計: 全テーブル定義
電気設備積算・見積Webサービス
SQLite / PostgreSQL 両対応（db_compat経由）
"""
import os
import bcrypt
from datetime import datetime
from db_compat import get_connection, USE_PG, get_db_type

# 後方互換: sqlite3.IntegrityErrorの代わりに汎用Exceptionを使用
try:
    import sqlite3
    _INTEGRITY_ERRORS = (sqlite3.IntegrityError,)
except ImportError:
    _INTEGRITY_ERRORS = (Exception,)

if USE_PG:
    try:
        import psycopg2
        _INTEGRITY_ERRORS = _INTEGRITY_ERRORS + (psycopg2.IntegrityError,)
    except ImportError:
        pass


def get_db():
    """DB接続を返す（SQLite/PostgreSQL自動判定）"""
    return get_connection()


def init_db():
    conn = get_db()
    c = conn.cursor()

    # === ユーザー ===
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
        avatar_path TEXT,
        phone TEXT,
        department TEXT,
        last_login_at TEXT,
        FOREIGN KEY (approved_by) REFERENCES users(id)
    )""")

    # Migration: add new columns if missing from existing DB
    _safe_add_column(c, conn, "users", "avatar_path", "TEXT")
    _safe_add_column(c, conn, "users", "phone", "TEXT")
    _safe_add_column(c, conn, "users", "department", "TEXT")
    _safe_add_column(c, conn, "users", "last_login_at", "TEXT")

    # === 案件 ===
    c.execute("""CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        client_name TEXT,
        location TEXT,
        created_by INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        status TEXT NOT NULL DEFAULT 'active',
        FOREIGN KEY (created_by) REFERENCES users(id)
    )""")
    _safe_add_column(c, conn, "projects", "location", "TEXT")

    # === アップロードファイル ===
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

    # === 積算マスタ ===
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

    # === 材料リスト（案件ごと） ===
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

    # === 照合結果 ===
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

    # === 見積明細 ===
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

    # === 手修正履歴 ===
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

    # === 学習辞書 ===
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

    # === 見積共通設定 ===
    c.execute("""CREATE TABLE IF NOT EXISTS estimate_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        setting_key TEXT UNIQUE NOT NULL,
        setting_value TEXT,
        description TEXT,
        updated_by INTEGER,
        updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
    )""")

    # === 案件別見積設定 ===
    c.execute("""CREATE TABLE IF NOT EXISTS project_estimate_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        company_name TEXT,
        company_address TEXT,
        company_tel TEXT,
        company_fax TEXT,
        labor_unit_price REAL DEFAULT 25000,
        estimate_title TEXT,
        estimate_conditions TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (project_id) REFERENCES projects(id)
    )""")

    # === マスタ更新履歴 ===
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

    # === 監査ログ ===
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

    # === エラーログ ===
    c.execute("""CREATE TABLE IF NOT EXISTS error_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        error_type TEXT,
        error_message TEXT,
        traceback TEXT,
        url TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
    )""")

    # === 共有ファイル ===
    c.execute("""CREATE TABLE IF NOT EXISTS shared_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        original_name TEXT NOT NULL,
        stored_path TEXT NOT NULL,
        file_type TEXT NOT NULL DEFAULT 'other',
        file_size INTEGER DEFAULT 0,
        uploaded_by INTEGER NOT NULL,
        uploaded_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (project_id) REFERENCES projects(id),
        FOREIGN KEY (uploaded_by) REFERENCES users(id)
    )""")

    # === 図面拾い出しアイテム ===
    c.execute("""CREATE TABLE IF NOT EXISTS blueprint_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        file_id INTEGER NOT NULL,
        page_number INTEGER DEFAULT 1,
        material_name TEXT,
        spec TEXT,
        quantity REAL DEFAULT 0,
        unit TEXT,
        construction_method TEXT,
        field_category TEXT,
        confidence REAL DEFAULT 0,
        match_type TEXT DEFAULT 'manual',
        reason TEXT,
        master_id INTEGER,
        is_adopted INTEGER DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (project_id) REFERENCES projects(id),
        FOREIGN KEY (file_id) REFERENCES project_files(id)
    )""")

    # === PDF解析テキストキャッシュ ===
    c.execute("""CREATE TABLE IF NOT EXISTS pdf_page_text (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id INTEGER NOT NULL,
        page_number INTEGER NOT NULL,
        text_content TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (file_id) REFERENCES project_files(id)
    )""")

    # === メール送信ログ ===
    c.execute("""CREATE TABLE IF NOT EXISTS email_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        to_email TEXT NOT NULL,
        to_name TEXT,
        subject TEXT NOT NULL,
        email_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        error_message TEXT,
        triggered_by INTEGER,
        related_user_id INTEGER,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (triggered_by) REFERENCES users(id),
        FOREIGN KEY (related_user_id) REFERENCES users(id)
    )""")

    # === 初期設定データ ===
    settings_defaults = [
        ("company_name", "", "自社名"),
        ("company_address", "", "自社住所"),
        ("company_tel", "", "自社電話番号"),
        ("company_fax", "", "自社FAX"),
        ("labor_unit_price", "25000", "工事単価（労務単価）円/人工"),
        ("estimate_title", "電気設備工事 御見積書", "見積書タイトル"),
        ("estimate_conditions", "1. 本見積は概算です\n2. 有効期限: 見積日より30日間", "見積条件"),
        ("auto_adopt_threshold", "0.75", "自動採用しきい値"),
        ("fuzzy_threshold", "0.50", "あいまい照合しきい値"),
        ("max_candidates", "5", "最大候補数"),
    ]
    for key, val, desc in settings_defaults:
        c.execute("""INSERT OR IGNORE INTO estimate_settings
            (setting_key, setting_value, description) VALUES (?,?,?)""",
            (key, val, desc))

    conn.commit()
    conn.close()


def _safe_add_column(cursor, conn, table, column, col_type):
    """既存テーブルにカラムを安全に追加（存在済みなら無視）"""
    try:
        if USE_PG:
            # PostgreSQL: information_schemaで存在チェック
            cursor.execute(
                """SELECT 1 FROM information_schema.columns
                   WHERE table_name = %s AND column_name = %s""",
                (table, column)
            )
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                conn.commit()
        else:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    except Exception:
        pass


def create_admin_user(email, password, full_name):
    conn = get_db()
    pw_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    try:
        conn.execute("""INSERT INTO users (email, password_hash, full_name, role, is_active, approved_at)
            VALUES (?, ?, ?, 'admin', 1, datetime('now','localtime'))""",
            (email, pw_hash, full_name))
        conn.commit()
        return True
    except _INTEGRITY_ERRORS:
        return False
    except Exception:
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
