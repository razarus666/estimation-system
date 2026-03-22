"""
Render.comデプロイ時の初期化スクリプト
初回起動時にDBの初期化とマスタデータのセットアップを行う
SQLite / PostgreSQL 両対応
"""
import os
import shutil

DB_PATH = os.environ.get('DB_PATH', 'data/estimation.db')
SEED_DB = os.path.join(os.path.dirname(__file__), 'seed_data', 'estimation.db')
DATABASE_URL = os.environ.get('DATABASE_URL', '')


def ensure_db():
    """永続ディスクにDBがなければシードDBをコピーして初期化"""
    use_pg = bool(DATABASE_URL)

    if not use_pg:
        # SQLiteモード: ディレクトリ作成
        db_dir = os.path.dirname(DB_PATH)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

    # Upload folder
    upload_dir = os.environ.get('UPLOAD_FOLDER', 'uploads')
    os.makedirs(upload_dir, exist_ok=True)

    if use_pg:
        # PostgreSQLモード: テーブル作成のみ
        from models import init_db, create_admin_user
        init_db()
        admin_email = os.environ.get('ADMIN_INIT_EMAIL', 'admin@system.local')
        admin_pass = os.environ.get('ADMIN_INIT_PASSWORD', '')
        admin_name = os.environ.get('ADMIN_INIT_NAME', '管理者')
        if admin_pass:
            created = create_admin_user(admin_email, admin_pass, admin_name)
            if created:
                print(f"[startup] PostgreSQL: 管理者ユーザー作成済")
            else:
                print(f"[startup] PostgreSQL: 管理者ユーザー既存")
        else:
            print("[startup] PostgreSQL: ADMIN_INIT_PASSWORD未設定、管理者作成スキップ")
        print(f"[startup] PostgreSQL接続確認済")

    elif not os.path.exists(DB_PATH):
        # SQLite初回デプロイ: シードDBをコピー
        if os.path.exists(SEED_DB):
            shutil.copy2(SEED_DB, DB_PATH)
            print(f"[startup] シードDBをコピー: {SEED_DB} -> {DB_PATH}")
        else:
            print(f"[startup] シードDBなし。空DBを作成します")

        # テーブル作成（シードDBにない追加テーブルを補完）
        from models import init_db, create_admin_user
        init_db()
        admin_email = os.environ.get('ADMIN_INIT_EMAIL', 'admin@system.local')
        admin_pass = os.environ.get('ADMIN_INIT_PASSWORD', 'admin123')
        admin_name = os.environ.get('ADMIN_INIT_NAME', '管理者')
        create_admin_user(admin_email, admin_pass, admin_name)
        print("[startup] DB初期化完了、管理者ユーザー作成済")
    else:
        # 既存DB: テーブルが足りなければ補完
        from models import init_db
        init_db()
        print(f"[startup] 既存DB確認済: {DB_PATH}")

    # DB統計情報
    try:
        from models import get_db
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM estimate_master")
        master_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users")
        user_count = c.fetchone()[0]
        conn.close()
        print(f"[startup] マスタ: {master_count}件, ユーザー: {user_count}名")
    except Exception as e:
        print(f"[startup] DB統計取得エラー: {e}")


if __name__ == '__main__':
    ensure_db()
