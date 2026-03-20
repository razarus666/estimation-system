"""
Render.comããã­ã¤æã®åæåã¹ã¯ãªãã
ååèµ·åæã«DBã®åæåã¨ãã¹ã¿ãã¼ã¿ã®ã»ããã¢ãããè¡ã
"""
import os
import shutil
import sqlite3

DB_PATH = os.environ.get('DB_PATH', 'data/estimation.db')
SEED_DB = os.path.join(os.path.dirname(__file__), 'seed_data', 'estimation.db')


def ensure_db():
    """æ°¸ç¶ãã£ã¹ã¯ã«DBããªããã°ã·ã¼ãDBãã³ãã¼ãã¦åæå"""
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    # Upload folder
    upload_dir = os.environ.get('UPLOAD_FOLDER', 'uploads')
    os.makedirs(upload_dir, exist_ok=True)

    if not os.path.exists(DB_PATH):
        # ååããã­ã¤: ã·ã¼ãDBãã³ãã¼
        if os.path.exists(SEED_DB):
            shutil.copy2(SEED_DB, DB_PATH)
            print(f"[startup] ã·ã¼ãDBãã³ãã¼: {SEED_DB} -> {DB_PATH}")
        else:
            print(f"[startup] ã·ã¼ãDBãªããç©ºDBãä½æãã¾ã")

        # ãã¼ãã«ä½æï¼ã·ã¼ãDBã«ãªãè¿½å ãã¼ãã«ãè£å®ï¼
        from models import init_db, create_admin_user
        init_db()
        create_admin_user('admin@system.local', 'admin123', 'ç®¡çè')
        print("[startup] DBåæåå®äºãç®¡çèã¦ã¼ã¶ã¼ä½ææ¸")
    else:
        # æ¢å­DB: ãã¼ãã«ãè¶³ããªããã°è£e®
        from models import init_db
        init_db()
        print(f"[startup] æ¢å­DBç¢ºèªæ¸: {DB_PATH}")

    # DBçµ±è¨æå ±
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM estimate_master")
    master_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM users")
    user_count = c.fetchone()[0]
    conn.close()
    print(f"[startup] ãã¹ã¿: {master_count}ä»¶, ã¦ã¼ã¶ã¼: {user_count}å")


if __name__ == '__main__':
    ensure_db()
