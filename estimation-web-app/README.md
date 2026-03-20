# 電気設備積算・見積Webサービス

電気設備工事の材料リストから自動で積算マスタと照合し、見積明細を生成するWebアプリケーションです。

## 機能一覧

- **ユーザー管理**: 管理者承認制のログインシステム
- **プロジェクト管理**: 案件ごとにファイル・材料・見積を管理
- **AI自動照合**: 4段階マッチング（完全一致→正規化→名称→あいまい）
- **学習辞書**: 材料名の別名を学習し照合精度を向上
- **Excel出力**: 見積明細・照合結果・修正履歴をExcelエクスポート
- **管理パネル**: ユーザー承認、マスタ管理、監査ログ、エラーログ

## デプロイ方法（Render.com）

### 1. GitHubにリポジトリを作成
1. [github.com](https://github.com) にアカウントを作成
2. 右上の「+」→「New repository」をクリック
3. Repository name に `estimation-system` と入力
4. 「Create repository」をクリック
5. 「uploading an existing file」リンクをクリック
6. このフォルダ内の全ファイルをドラッグ＆ドロップ
7. 「Commit changes」をクリック

### 2. Render.comでデプロイ
1. [render.com](https://render.com) にアカウントを作成
2. ダッシュボード → 「New +」 → 「Web Service」をクリック
3. 「Connect a repository」でGitHubアカウントを接続
4. `estimation-system` リポジトリを選択
5. 以下を設定:
   - **Name**: `estimation-system`
   - **Runtime**: `Python`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120`
6. 「Advanced」を開き、環境変数を追加:
   - `FLASK_SECRET_KEY` → 「Generate」ボタン
   - `DB_PATH` → `/opt/render/project/data/estimation.db`
   - `UPLOAD_FOLDER` → `/opt/render/project/data/uploads`
7. 「Add Disk」をクリック:
   - **Name**: `estimation-data`
   - **Mount Path**: `/opt/render/project/data`
   - **Size**: `1 GB`
8. 「Create Web Service」をクリック

### 3. 初期ログイン
- URL: Render.comダッシュボードに表示されるURL
- 管理者メール: `admin@system.local`
- 管理者パスワード: `admin123`
- ⚠️ ログイン後すぐに管理者パスワードを変更してください

## 技術情報

- Python 3.11 / Flask 3.1
- SQLite（永続ディスク）
- 積算マスタ: 8,938件
- Bootstrap 5 レスポンシブUI
