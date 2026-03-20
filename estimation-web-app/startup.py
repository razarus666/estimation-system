"""
Render.comデプロイ時の初期化スクリプト
初回起動時にDBの初期化とマスタデータのセットアップを行う
"""
import os
import shutil
import sqlite3

DB_PATH = os.environ.get('DB_PATH', 'data/estimation.db')
SEED_DB = os.path.join(os.path.dirname(__file__), 'seed_data', 'estimation.db')


def ensure_db():
    """永続ディスクにDBがなければシードDBをコピーして初期化"""
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    # Upload folder
    upload_dir = os.environ.get('UPLOAD_FOLDER', 'uploads')
    os.makedirs(upload_dir, exist_ok=True)

    if not os.path.exists(DB_PATH):
        # 初回デプロイ: シードDBをコピー
        if os.path.exists(SEED_DB):
            shutil.copy2(SEED_DB, DB_PATH)
            print(f"[startup] シードDBをコピー: {SEED_DB} -> {DB_PATH}")
        else:
            print(f"[startup] シードDBなし。空DBを作成します")

        # テーブル作成（シードDBにない追加テーブルを補完）
        from models import init_db, create_admin_user
        init_db()
        create_admin_user('admin@system.local', 'admin123', '管理者')
        print("[startup] DB初期化完了、管理者ユーザー作成済")
    else:
        # 既存DB: テーブルが足りなければ補完
        from models import init_db
        init_db()
        print(f"[startup] 既存DB確認済: {DB_PATH}")

    # DB統計情報
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM estimate_master")
    master_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM users")
    user_count = c.fetchone()[0]
    conn.close()
    print(f"[startup] マスタ: {master_count}件, ユーザー: {user_count}名")


if __name__ == '__main__':
    ensure_db()
