import os
import uuid
import csv
import json
from datetime import datetime
from functools import wraps
from io import BytesIO

import bcrypt
import pdfplumber
from flask import (
    Flask, render_template, request, redirect, url_for, session, jsonify, send_file
)
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from werkzeug.utils import secure_filename

from models import (
    get_db, init_db, create_admin_user, add_audit_log, add_error_log
)
from matching_engine import run_project_matching, load_master_data

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', os.getenv('SECRET_KEY', os.urandom(24).hex()))
app.config['UPLOAD_FOLDER'] = os.getenv('UPLOAD_FOLDER', os.path.join(os.path.dirname(__file__), 'uploads'))
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size
ALLOWED_EXTENSIONS = {'pdf', 'xlsx', 'xls', 'csv', 'tsv', 'shd', 'str', 'txt', 'mdb', 'rak'}

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Create upload folder
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Initialize database (startup handles seed DB copy on Render)
from startup import ensure_db
ensure_db()


class User(UserMixin):
    """User class for Flask-Login"""
    def __init__(self, user_id, email, full_name, role, active):
        self.id = user_id
        self.email = email
        self.full_name = full_name
        self.role = role
        self._active = bool(active)

    @property
    def is_active(self):
        return self._active

    def is_admin(self):
        return self.role == 'admin'

    def is_approved(self):
        return self.role != 'pending'


@login_manager.user_loader
def load_user(user_id):
    """Load user from database"""
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        'SELECT id, email, full_name, role, is_active FROM users WHERE id = ?',
        (user_id,)
    )
    user_data = cursor.fetchone()
    if user_data:
        return User(user_data[0], user_data[1], user_data[2], user_data[3], user_data[4])
    return None


def admin_required(f):
    """Decorator for admin-only routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user or not current_user.is_authenticated:
            return redirect(url_for('login'))
        if not current_user.is_admin():
            add_audit_log(
                current_user.id,
                'UNAUTHORIZED_ACCESS',
                'route',
                request.path,
                'WARNING',
                f'Unauthorized admin access attempt: {request.path}',
                request.remote_addr
            )
            return render_template('error.html', error='管理者権限が必要です'), 403
        return f(*args, **kwargs)
    return decorated_function


def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def get_user_ip():
    """Get client IP address"""
    return request.headers.get('X-Forwarded-For', request.remote_addr).split(',')[0].strip()


def extract_pdf_text(file_path):
    """Extract text from PDF using pdfplumber"""
    try:
        text_content = []
        with pdfplumber.open(file_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text() or ""
                text_content.append({
                    'page': page_num,
                    'text': text
                })
        return text_content
    except Exception as e:
        raise Exception(f'PDF解析エラー: {str(e)}')


def parse_material_list_excel(file_path):
    """Parse material list from Excel file"""
    try:
        from openpyxl import load_workbook

        wb = load_workbook(file_path)
        ws = wb.active

        # Find header row by looking for '名称' or 'name'
        header_row = None
        for row_idx, row in enumerate(ws.iter_rows(values_only=True), 1):
            row_lower = [str(v).lower() if v else '' for v in row]
            if any(name in row_lower for name in ['名称', 'name', 'material']):
                header_row = row_idx
                break

        if not header_row:
            raise Exception('ヘッダー行が見つかりません')

        # Get headers
        headers = []
        for cell in ws[header_row]:
            headers.append(str(cell.value).strip() if cell.value else '')

        # Expected columns (Japanese)
        column_mapping = {
            '行番号': 'row_no',
            '名称': 'material_name',
            '規格': 'spec',
            'サイズ': 'size',
            '数量': 'quantity',
            '単位': 'unit',
            '施工条件': 'construction_method',
            '分野': 'field_category',
            '図面参照': 'drawing_ref',
            '備考': 'remarks'
        }

        # Map columns
        column_indices = {}
        for col_idx, header in enumerate(headers):
            for jp_name, py_name in column_mapping.items():
                if jp_name.lower() in header.lower() or py_name in header.lower():
                    column_indices[py_name] = col_idx
                    break

        # Parse rows
        materials = []
        for row_idx, row in enumerate(ws.iter_rows(values_only=True), 1):
            if row_idx <= header_row:
                continue
            if all(cell is None for cell in row):
                continue

            material = {}
            for py_name, col_idx in column_indices.items():
                if col_idx < len(row):
                    value = row[col_idx]
                    if py_name == 'quantity':
                        try:
                            material[py_name] = float(value) if value else 0
                        except:
                            material[py_name] = 0
                    else:
                        material[py_name] = str(value).strip() if value else ''
                else:
                    material[py_name] = '' if py_name != 'quantity' else 0

            if material.get('material_name'):
                materials.append(material)

        return materials
    except Exception as e:
        raise Exception(f'Excel解析エラー: {str(e)}')


def parse_material_list_csv(file_path):
    """Parse material list from CSV file"""
    try:
        materials = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise Exception('CSVが空です')

            column_mapping = {
                '行番号': 'row_no',
                '名称': 'material_name',
                '規格': 'spec',
                'サイズ': 'size',
                '数量': 'quantity',
                '単位': 'unit',
                '施工条件': 'construction_method',
                '分野': 'field_category',
                '図面参照': 'drawing_ref',
                '備考': 'remarks'
            }

            for row in reader:
                material = {}
                for py_name in column_mapping.values():
                    material[py_name] = ''

                for jp_name, py_name in column_mapping.items():
                    for csv_col in reader.fieldnames:
                        if jp_name.lower() in csv_col.lower() or py_name in csv_col.lower():
                            value = row.get(csv_col, '')
                            if py_name == 'quantity':
                                try:
                                    material[py_name] = float(value) if value else 0
                                except:
                                    material[py_name] = 0
                            else:
                                material[py_name] = str(value).strip() if value else ''
                            break

                if material.get('material_name'):
                    materials.append(material)

        return materials
    except Exception as e:
        raise Exception(f'CSV解析エラー: {str(e)}')


def parse_material_list_shd(file_path):
    """Parse material list from Adonis IXAS .shd file (Shift-JIS encoded)"""
    try:
        materials = []
        with open(file_path, 'rb') as f:
            # Read as Shift-JIS with error handling
            content = f.read().decode('shift_jis', errors='replace')

        # Split by carriage return and then by tab
        lines = content.split('\r')

        for row_idx, line in enumerate(lines):
            fields = line.split('\t')

            # Skip header row (row 0)
            if row_idx == 0:
                continue

            # Skip if no fields or code (col 0/1) is empty
            if not fields or (len(fields) > 1 and not fields[1].strip()):
                continue

            # Skip category headers (name starts with ◆ or 【)
            if len(fields) > 2 and fields[2]:
                name = fields[2].strip()
                if name.startswith('◆') or name.startswith('【'):
                    continue

            # Extract fields (adjust indices based on .shd format)
            # Typical format: col0=binary, col1=code, col2=name, col3=spec, col4=construction_method, col5=quantity, col6=unit, col7=unit_price
            if len(fields) >= 8:
                material = {
                    'row_no': row_idx,
                    'material_name': fields[2].strip() if len(fields) > 2 else '',
                    'spec': fields[3].strip() if len(fields) > 3 else '',
                    'construction_method': fields[4].strip() if len(fields) > 4 else '',
                    'quantity': 0,
                    'unit': fields[6].strip() if len(fields) > 6 else '',
                    'field_category': '',
                    'size': '',
                    'drawing_ref': '',
                    'remarks': ''
                }

                # Try to parse quantity and unit_price
                if len(fields) > 5:
                    try:
                        material['quantity'] = float(fields[5].strip()) if fields[5].strip() else 0
                    except:
                        material['quantity'] = 0

                if material.get('material_name'):
                    materials.append(material)

        return materials
    except Exception as e:
        raise Exception(f'SHDファイル解析エラー: {str(e)}')


# ==================== AUTH ROUTES ====================

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page"""
    if request.method == 'POST':
        try:
            email = request.form.get('email', '').strip()
            password = request.form.get('password', '').strip()

            if not email or not password:
                return render_template('login.html', error='メールアドレスとパスワードを入力してください'), 400

            db = get_db()
            cursor = db.cursor()
            cursor.execute(
                'SELECT id, email, password_hash, full_name, role, is_active FROM users WHERE email = ?',
                (email,)
            )
            user_data = cursor.fetchone()

            if not user_data:
                add_audit_log(
                    None,
                    'LOGIN_FAILED',
                    'user',
                    email,
                    'WARNING',
                    'ユーザーが見つかりません',
                    get_user_ip()
                )
                return render_template('login.html', error='メールアドレスまたはパスワードが正しくありません'), 401

            user_id, db_email, password_hash, full_name, role, is_active = user_data

            if not is_active:
                add_audit_log(
                    user_id,
                    'LOGIN_FAILED',
                    'user',
                    email,
                    'WARNING',
                    'アカウントが無効化されています',
                    get_user_ip()
                )
                return render_template('login.html', error='アカウントが無効化されています'), 403

            pw_hash_bytes = password_hash.encode('utf-8') if isinstance(password_hash, str) else password_hash
            if not bcrypt.checkpw(password.encode('utf-8'), pw_hash_bytes):
                add_audit_log(
                    user_id,
                    'LOGIN_FAILED',
                    'user',
                    email,
                    'WARNING',
                    'パスワード不正',
                    get_user_ip()
                )
                return render_template('login.html', error='メールアドレスまたはパスワードが正しくありません'), 401

            user = User(user_id, db_email, full_name, role, is_active)
            login_user(user)

            # Record last login time
            cursor.execute(
                'UPDATE users SET last_login_at = ? WHERE id = ?',
                (datetime.utcnow(), user_id)
            )
            db.commit()

            add_audit_log(
                user_id,
                'LOGIN',
                'user',
                email,
                'INFO',
                'ログイン成功',
                get_user_ip()
            )

            # Redirect to pending page if user is pending approval
            if role == 'pending':
                return redirect(url_for('pending_page'))

            return redirect(url_for('dashboard'))

        except Exception as e:
            add_error_log(
                current_user.id if current_user.is_authenticated else None,
                'LOGIN_ERROR',
                str(e),
                str(e),
                request.url
            )
            return render_template('login.html', error='ログイン処理中にエラーが発生しました'), 500

    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    """Register new user"""
    if request.method == 'POST':
        try:
            email = request.form.get('email', '').strip()
            password = request.form.get('password', '').strip()
            full_name = request.form.get('full_name', '').strip()

            if not all([email, password, full_name]):
                return render_template('register.html', error='すべてのフィールドを入力してください'), 400

            if len(password) < 8:
                return render_template('register.html', error='パスワードは8文字以上である必要があります'), 400

            db = get_db()
            cursor = db.cursor()

            cursor.execute('SELECT id FROM users WHERE email = ?', (email,))
            if cursor.fetchone():
                return render_template('register.html', error='このメールアドレスは既に登録されています'), 400

            password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

            cursor.execute(
                '''INSERT INTO users (email, password_hash, full_name, role, is_active, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)''',
                (email, password_hash, full_name, 'pending', True, datetime.utcnow())
            )
            db.commit()

            new_user_id = cursor.lastrowid
            add_audit_log(
                new_user_id,
                'REGISTER',
                'user',
                email,
                'INFO',
                f'ユーザー登録完了（承認待ち）',
                get_user_ip()
            )

            return render_template(
                'register.html',
                success='登録完了しました。管理者の承認をお待ちください。'
            )

        except Exception as e:
            add_error_log(
                current_user.id if current_user.is_authenticated else None,
                'REGISTER_ERROR',
                str(e),
                str(e),
                request.url
            )
            return render_template('register.html', error='登録処理中にエラーが発生しました'), 500

    return render_template('register.html')


@app.route('/logout')
@login_required
def logout():
    """Logout"""
    add_audit_log(
        current_user.id,
        'LOGOUT',
        'user',
        current_user.email,
        'INFO',
        'ログアウト',
        get_user_ip()
    )
    logout_user()
    return redirect(url_for('login'))


# ==================== USER PROFILE ROUTES ====================

@app.route('/profile', methods=['GET', 'POST'])
@login_required
def user_profile():
    """User profile page"""
    try:
        db = get_db()
        cursor = db.cursor()

        if request.method == 'POST':
            # Update profile
            full_name = request.form.get('full_name', '').strip()
            phone = request.form.get('phone', '').strip()
            department = request.form.get('department', '').strip()

            cursor.execute(
                '''UPDATE users
                   SET full_name = ?, phone = ?, department = ?
                   WHERE id = ?''',
                (full_name, phone, department, current_user.id)
            )
            db.commit()

            add_audit_log(
                current_user.id,
                'UPDATE_PROFILE',
                'user',
                current_user.id,
                'INFO',
                'プロフィール更新',
                get_user_ip()
            )

            # Reload user object
            user = load_user(current_user.id)
            login_user(user)

            return render_template('profile.html', user=user, message='プロフィールを更新しました')

        # GET: Show profile page
        cursor.execute(
            'SELECT id, email, full_name, phone, department, avatar_path FROM users WHERE id = ?',
            (current_user.id,)
        )
        user_data = cursor.fetchone()

        return render_template('profile.html', user=user_data)

    except Exception as e:
        add_error_log(
            current_user.id,
            'PROFILE_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='プロフィールの読み込みに失敗しました'), 500


@app.route('/profile/avatar', methods=['POST'])
@login_required
def upload_avatar():
    """Upload user avatar"""
    try:
        if 'avatar' not in request.files:
            return jsonify({'error': 'ファイルが選択されていません'}), 400

        file = request.files['avatar']
        if file.filename == '':
            return jsonify({'error': 'ファイルが選択されていません'}), 400

        # Check file type
        if not file.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif')):
            return jsonify({'error': '画像ファイル（PNG, JPG, GIF）のみサポートしています'}), 400

        # Create user avatar directory
        avatar_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'avatars')
        os.makedirs(avatar_dir, exist_ok=True)

        # Save avatar
        file_ext = file.filename.rsplit('.', 1)[1].lower()
        avatar_filename = f"avatar_{current_user.id}.{file_ext}"
        avatar_path = os.path.join(avatar_dir, avatar_filename)
        file.save(avatar_path)

        # Update database
        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            'UPDATE users SET avatar_path = ? WHERE id = ?',
            (f'avatars/{avatar_filename}', current_user.id)
        )
        db.commit()

        add_audit_log(
            current_user.id,
            'UPLOAD_AVATAR',
            'user',
            current_user.id,
            'INFO',
            'アバター画像アップロード',
            get_user_ip()
        )

        return jsonify({'success': True, 'message': 'アバター画像をアップロードしました'}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'UPLOAD_AVATAR_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'アバターアップロードエラー: {str(e)}'}), 500


# ==================== PENDING PAGE ROUTE ====================

@app.route('/pending')
@login_required
def pending_page():
    """Pending approval page"""
    try:
        add_audit_log(
            current_user.id,
            'VIEW_PENDING_PAGE',
            'user',
            current_user.id,
            'INFO',
            '承認待ちページ表示',
            get_user_ip()
        )
        return render_template('pending.html')
    except Exception as e:
        add_error_log(
            current_user.id,
            'PENDING_PAGE_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='承認待ちページの読み込みに失敗しました'), 500


# ==================== DASHBOARD ROUTES ====================

@app.route('/')
def index():
    """Redirect to dashboard"""
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))


@app.route('/dashboard')
@login_required
def dashboard():
    """Dashboard - show projects list and stats"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Get projects for this user
        cursor.execute(
            '''SELECT id, name, description, client_name, status, created_at, updated_at
               FROM projects
               WHERE created_by = ?
               ORDER BY updated_at DESC''',
            (current_user.id,)
        )
        projects = cursor.fetchall()

        project_list = []
        for project in projects:
            project_list.append({
                'id': project[0],
                'name': project[1],
                'description': project[2],
                'client_name': project[3],
                'status': project[4],
                'created_at': project[5],
                'updated_at': project[6]
            })

        # Compute dashboard statistics
        # Total projects
        total_projects = len(project_list)

        # Total estimate details
        cursor.execute(
            'SELECT COUNT(*) FROM estimate_details WHERE project_id IN (SELECT id FROM projects WHERE created_by = ?)',
            (current_user.id,)
        )
        total_estimates = cursor.fetchone()[0] or 0

        # Estimates needing review (confidence < 0.75)
        cursor.execute(
            '''SELECT COUNT(*) FROM estimate_details ed
               INNER JOIN projects p ON ed.project_id = p.id
               WHERE p.created_by = ? AND ed.confidence < 0.75''',
            (current_user.id,)
        )
        needs_review = cursor.fetchone()[0] or 0

        # Total amount (sum of all estimate amounts)
        cursor.execute(
            '''SELECT COALESCE(SUM(amount), 0) FROM estimate_details ed
               INNER JOIN projects p ON ed.project_id = p.id
               WHERE p.created_by = ?''',
            (current_user.id,)
        )
        total_amount = cursor.fetchone()[0] or 0

        # Total productivity
        cursor.execute(
            '''SELECT COALESCE(SUM(productivity_total), 0) FROM estimate_details ed
               INNER JOIN projects p ON ed.project_id = p.id
               WHERE p.created_by = ?''',
            (current_user.id,)
        )
        total_productivity = cursor.fetchone()[0] or 0

        # Error count (last 7 days)
        cursor.execute(
            '''SELECT COUNT(*) FROM error_log
               WHERE user_id = ? AND datetime(created_at) > datetime('now', '-7 days')''',
            (current_user.id,)
        )
        error_count = cursor.fetchone()[0] or 0

        stats = {
            'total_projects': total_projects,
            'total_estimates': total_estimates,
            'needs_review': needs_review,
            'total_amount': total_amount,
            'total_productivity': total_productivity,
            'error_count': error_count
        }

        add_audit_log(
            current_user.id,
            'VIEW_DASHBOARD',
            'dashboard',
            None,
            'INFO',
            'ダッシュボード表示',
            get_user_ip()
        )

        return render_template('dashboard.html', projects=project_list, stats=stats)

    except Exception as e:
        add_error_log(
            current_user.id,
            'DASHBOARD_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='ダッシュボード読み込みエラーが発生しました'), 500


# ==================== PROJECT ROUTES ====================

@app.route('/projects/new', methods=['GET', 'POST'])
@login_required
def create_project():
    """Create new project"""
    if request.method == 'POST':
        try:
            name = request.form.get('project_name', '').strip()
            description = request.form.get('description', '').strip()
            client_name = request.form.get('client_name', '').strip()
            location = request.form.get('location', '').strip()

            if not name:
                return render_template('project_new.html', error='プロジェクト名は必須です'), 400

            db = get_db()
            cursor = db.cursor()

            cursor.execute(
                '''INSERT INTO projects (name, description, client_name, location, created_by, created_at, updated_at, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                (name, description, client_name, location, current_user.id, datetime.utcnow(), datetime.utcnow(), 'draft')
            )
            db.commit()

            project_id = cursor.lastrowid

            add_audit_log(
                current_user.id,
                'CREATE_PROJECT',
                'project',
                project_id,
                'INFO',
                f'プロジェクト作成: {name}',
                get_user_ip()
            )

            return redirect(url_for('project_detail', project_id=project_id))

        except Exception as e:
            add_error_log(
                current_user.id,
                'CREATE_PROJECT_ERROR',
                str(e),
                str(e),
                request.url
            )
            return render_template('project_new.html', error='プロジェクト作成エラーが発生しました'), 500

    return render_template('project_new.html')


@app.route('/projects/<int:project_id>/edit', methods=['POST'])
@login_required
def edit_project(project_id):
    """Edit project details"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス権限がありません'}), 403

        # Get request data
        data = request.get_json() or request.form

        name = data.get('name', '').strip()
        description = data.get('description', '').strip()
        client_name = data.get('client_name', '').strip()
        location = data.get('location', '').strip()

        if not name:
            return jsonify({'error': 'プロジェクト名は必須です'}), 400

        # Update project
        cursor.execute(
            '''UPDATE projects
               SET name = ?, description = ?, client_name = ?, location = ?, updated_at = ?
               WHERE id = ?''',
            (name, description, client_name, location, datetime.utcnow(), project_id)
        )
        db.commit()

        add_audit_log(
            current_user.id,
            'EDIT_PROJECT',
            'project',
            project_id,
            'INFO',
            f'プロジェクト編集: {name}',
            get_user_ip()
        )

        return jsonify({'success': True, 'message': 'プロジェクトを更新しました'}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'EDIT_PROJECT_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'プロジェクト編集エラー: {str(e)}'}), 500


@app.route('/projects/<int:project_id>/delete', methods=['POST'])
@login_required
def delete_project(project_id):
    """Delete project and all related data"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス権限がありません'}), 403

        # Delete related data
        cursor.execute('DELETE FROM edit_history WHERE project_id = ?', (project_id,))
        cursor.execute('DELETE FROM estimate_details WHERE project_id = ?', (project_id,))
        cursor.execute('DELETE FROM match_results WHERE project_id = ?', (project_id,))
        cursor.execute('DELETE FROM material_list WHERE project_id = ?', (project_id,))
        cursor.execute('DELETE FROM project_files WHERE project_id = ?', (project_id,))
        cursor.execute('DELETE FROM projects WHERE id = ?', (project_id,))

        db.commit()

        # Delete uploaded files from disk
        project_upload_dir = os.path.join(app.config['UPLOAD_FOLDER'], str(project_id))
        if os.path.exists(project_upload_dir):
            import shutil
            shutil.rmtree(project_upload_dir)

        add_audit_log(
            current_user.id,
            'DELETE_PROJECT',
            'project',
            project_id,
            'INFO',
            f'プロジェクト削除: ID {project_id}',
            get_user_ip()
        )

        return jsonify({'success': True, 'message': 'プロジェクトを削除しました'}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'DELETE_PROJECT_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'プロジェクト削除エラー: {str(e)}'}), 500


@app.route('/projects/<int:project_id>')
@login_required
def project_detail(project_id):
    """Project detail page"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute(
            'SELECT id, name, description, client_name, status, created_at, created_by FROM projects WHERE id = ?',
            (project_id,)
        )
        project = cursor.fetchone()

        if not project:
            return render_template('error.html', error='プロジェクトが見つかりません'), 404

        if project[6] != current_user.id and not current_user.is_admin():
            return render_template('error.html', error='アクセス権限がありません'), 403

        # Get project files
        cursor.execute(
            '''SELECT id, file_type, original_name, file_size, uploaded_at
               FROM project_files
               WHERE project_id = ?
               ORDER BY uploaded_at DESC''',
            (project_id,)
        )
        files = cursor.fetchall()

        # Get material list
        cursor.execute(
            '''SELECT id, row_no, material_name, spec, size, quantity, unit, construction_method, field_category
               FROM material_list
               WHERE project_id = ?
               ORDER BY row_no''',
            (project_id,)
        )
        materials = cursor.fetchall()

        # Get estimate details count and totals
        cursor.execute(
            'SELECT COUNT(*), COALESCE(SUM(amount),0), COALESCE(SUM(productivity_total),0) FROM estimate_details WHERE project_id = ?',
            (project_id,)
        )
        est_row = cursor.fetchone()
        estimate_count = est_row[0]
        total_amount = est_row[1]
        total_productivity = est_row[2]

        # Get labor unit price setting
        cursor.execute("SELECT setting_value FROM estimate_settings WHERE setting_key='labor_unit_price'")
        lup_row = cursor.fetchone()
        labor_unit_price = float(lup_row[0]) if lup_row else 25000

        # Get match results for display
        cursor.execute(
            '''SELECT id, material_id, candidate_rank, master_id, match_type,
               confidence, reason, is_adopted, master_name, master_spec,
               master_method, composite_unit_price, removal_productivity, source_page
            FROM match_results WHERE project_id = ? ORDER BY material_id, candidate_rank''',
            (project_id,)
        )
        match_results = cursor.fetchall()

        add_audit_log(
            current_user.id,
            'VIEW_PROJECT',
            'project',
            str(project_id),
            'INFO',
            'プロジェクト詳細表示',
            get_user_ip()
        )

        return render_template(
            'project_detail.html',
            project={
                'id': project[0],
                'name': project[1],
                'description': project[2],
                'client_name': project[3],
                'status': project[4],
                'created_at': project[5]
            },
            files=files,
            materials=materials,
            estimate_count=estimate_count,
            total_amount=total_amount,
            total_productivity=total_productivity,
            labor_unit_price=labor_unit_price,
            match_results=match_results
        )

    except Exception as e:
        add_error_log(
            current_user.id,
            'PROJECT_DETAIL_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='プロジェクト詳細読み込みエラーが発生しました'), 500


@app.route('/projects/<int:project_id>/upload', methods=['POST'])
@login_required
def upload_file(project_id):
    """Upload project file"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス権限がありません'}), 403

        if 'file' not in request.files:
            return jsonify({'error': 'ファイルが選択されていません'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'ファイルが選択されていません'}), 400

        if not allowed_file(file.filename):
            return jsonify({'error': '許可されないファイル形式です（PDF, Excel, CSVのみ）'}), 400

        # Create project upload directory
        project_upload_dir = os.path.join(app.config['UPLOAD_FOLDER'], str(project_id))
        os.makedirs(project_upload_dir, exist_ok=True)

        # Save file
        filename = secure_filename(file.filename)
        file_ext = filename.rsplit('.', 1)[1].lower()
        unique_filename = f"{uuid.uuid4().hex}.{file_ext}"
        file_path = os.path.join(project_upload_dir, unique_filename)
        file.save(file_path)

        file_size = os.path.getsize(file_path)

        # Determine file type and process
        if file_ext == 'pdf':
            file_type = 'estimate_pdf'
            # Extract PDF text
            text_content = extract_pdf_text(file_path)
            stored_path = file_path
        elif file_ext == 'xlsx':
            file_type = 'material_list'
            # Parse Excel and import materials
            materials = parse_material_list_excel(file_path)
            for material in materials:
                cursor.execute(
                    '''INSERT INTO material_list
                       (project_id, row_no, material_name, spec, size, quantity, unit, construction_method, field_category, drawing_ref, remarks)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        project_id,
                        material.get('row_no', ''),
                        material.get('material_name', ''),
                        material.get('spec', ''),
                        material.get('size', ''),
                        material.get('quantity', 0),
                        material.get('unit', ''),
                        material.get('construction_method', ''),
                        material.get('field_category', ''),
                        material.get('drawing_ref', ''),
                        material.get('remarks', '')
                    )
                )
            db.commit()
            stored_path = file_path
        elif file_ext in ('csv', 'tsv', 'txt'):
            file_type = 'material_list'
            # Parse CSV/TSV and import materials
            materials = parse_material_list_csv(file_path)
            for material in materials:
                cursor.execute(
                    '''INSERT INTO material_list
                       (project_id, row_no, material_name, spec, size, quantity, unit, construction_method, field_category, drawing_ref, remarks)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        project_id,
                        material.get('row_no', ''),
                        material.get('material_name', ''),
                        material.get('spec', ''),
                        material.get('size', ''),
                        material.get('quantity', 0),
                        material.get('unit', ''),
                        material.get('construction_method', ''),
                        material.get('field_category', ''),
                        material.get('drawing_ref', ''),
                        material.get('remarks', '')
                    )
                )
            db.commit()
            stored_path = file_path
        elif file_ext == 'shd':
            file_type = 'material_list'
            # Parse SHD (Adonis IXAS) and import materials
            materials = parse_material_list_shd(file_path)
            for material in materials:
                cursor.execute(
                    '''INSERT INTO material_list
                       (project_id, row_no, material_name, spec, size, quantity, unit, construction_method, field_category, drawing_ref, remarks)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        project_id,
                        material.get('row_no', ''),
                        material.get('material_name', ''),
                        material.get('spec', ''),
                        material.get('size', ''),
                        material.get('quantity', 0),
                        material.get('unit', ''),
                        material.get('construction_method', ''),
                        material.get('field_category', ''),
                        material.get('drawing_ref', ''),
                        material.get('remarks', '')
                    )
                )
            db.commit()
            stored_path = file_path
        elif file_ext == 'xls':
            file_type = 'material_list'
            # For .xls files, try to parse as Excel (may need xlrd)
            materials = parse_material_list_excel(file_path)
            for material in materials:
                cursor.execute(
                    '''INSERT INTO material_list
                       (project_id, row_no, material_name, spec, size, quantity, unit, construction_method, field_category, drawing_ref, remarks)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        project_id,
                        material.get('row_no', ''),
                        material.get('material_name', ''),
                        material.get('spec', ''),
                        material.get('size', ''),
                        material.get('quantity', 0),
                        material.get('unit', ''),
                        material.get('construction_method', ''),
                        material.get('field_category', ''),
                        material.get('drawing_ref', ''),
                        material.get('remarks', '')
                    )
                )
            db.commit()
            stored_path = file_path
        else:
            file_type = 'other'
            stored_path = file_path

        # Record in database
        cursor.execute(
            '''INSERT INTO project_files
               (project_id, file_type, original_name, stored_path, file_size, uploaded_by, uploaded_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (project_id, file_type, filename, stored_path, file_size, current_user.id, datetime.utcnow())
        )
        db.commit()

        add_audit_log(
            current_user.id,
            'UPLOAD_FILE',
            'project_file',
            cursor.lastrowid,
            'INFO',
            f'ファイルアップロード: {filename} ({file_type})',
            get_user_ip()
        )

        return jsonify({
            'success': True,
            'message': 'ファイルがアップロードされました',
            'file_id': cursor.lastrowid
        }), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'UPLOAD_FILE_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'ファイルアップロードエラー: {str(e)}'}), 500


@app.route('/projects/<int:project_id>/run-matching', methods=['POST'])
@login_required
def run_matching(project_id):
    """Execute matching engine"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス権限がありません'}), 403

        # Load master data
        load_master_data()

        # Run matching
        results = run_project_matching(project_id, current_user.id)

        # results is a dict, not a list, so get the count directly
        match_count = len(results) if isinstance(results, dict) else 0

        add_audit_log(
            current_user.id,
            'RUN_MATCHING',
            'project',
            project_id,
            'INFO',
            f'マッチング実行完了: {match_count}件',
            get_user_ip()
        )

        # Update project status
        cursor.execute(
            'UPDATE projects SET status = ?, updated_at = ? WHERE id = ?',
            ('matched', datetime.utcnow(), project_id)
        )
        db.commit()

        return jsonify({
            'success': True,
            'message': 'マッチング完了しました',
            'match_count': match_count
        }), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'MATCHING_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'マッチングエラー: {str(e)}'}), 500


@app.route('/projects/<int:project_id>/estimates')
@login_required
def get_estimates(project_id):
    """Get estimate details as JSON"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス権限がありません'}), 403

        cursor.execute(
            '''SELECT id, row_no, field_category, material_name, spec, construction_method, unit,
                      quantity, composite_unit_price, amount, productivity, productivity_total,
                      source_pdf, source_page, match_type, confidence, match_reason, remarks,
                      is_manual_added, material_id, master_id
               FROM estimate_details
               WHERE project_id = ?
               ORDER BY row_no''',
            (project_id,)
        )

        estimates = []
        for row in cursor.fetchall():
            estimates.append({
                'id': row[0],
                'row_no': row[1],
                'field_category': row[2],
                'material_name': row[3],
                'spec': row[4],
                'construction_method': row[5],
                'unit': row[6],
                'quantity': row[7],
                'composite_unit_price': row[8],
                'amount': row[9],
                'productivity': row[10],
                'productivity_total': row[11],
                'source_pdf': row[12],
                'source_page': row[13],
                'match_type': row[14],
                'confidence': row[15],
                'match_reason': row[16],
                'remarks': row[17],
                'is_manual_added': row[18],
                'material_id': row[19],
                'master_id': row[20]
            })

        return jsonify({'estimates': estimates}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'GET_ESTIMATES_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'見積取得エラー: {str(e)}'}), 500


@app.route('/projects/<int:project_id>/estimates/<int:detail_id>/edit', methods=['POST'])
@login_required
def edit_estimate_detail(project_id, detail_id):
    """Edit estimate detail and record history"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス権限がありません'}), 403

        data = request.get_json()
        column_name = data.get('column')
        new_value = data.get('value')

        if not column_name:
            return jsonify({'error': 'カラム名が必須です'}), 400

        # Get old value
        cursor.execute(
            f'SELECT {column_name} FROM estimate_details WHERE id = ? AND project_id = ?',
            (detail_id, project_id)
        )
        result = cursor.fetchone()
        if not result:
            return jsonify({'error': '見積詳細が見つかりません'}), 404

        old_value = result[0]

        # Handle amount recalculation when quantity or composite_unit_price is edited
        if column_name == 'composite_unit_price' or column_name == 'quantity':
            # Get the current values
            cursor.execute(
                'SELECT quantity, composite_unit_price, productivity FROM estimate_details WHERE id = ? AND project_id = ?',
                (detail_id, project_id)
            )
            current_vals = cursor.fetchone()
            if current_vals:
                current_quantity = current_vals[0]
                current_unit_price = current_vals[1]
                current_productivity = current_vals[2]

                # Update the specified column
                if column_name == 'quantity':
                    new_quantity = float(new_value) if new_value else 0
                    new_amount = new_quantity * current_unit_price
                    new_productivity_total = new_quantity * current_productivity if current_productivity else 0
                else:  # composite_unit_price
                    new_unit_price = float(new_value) if new_value else 0
                    new_amount = current_quantity * new_unit_price
                    new_productivity_total = current_quantity * current_productivity if current_productivity else 0

                # Update all three fields
                cursor.execute(
                    '''UPDATE estimate_details
                       SET quantity = ?, composite_unit_price = ?, amount = ?, productivity_total = ?
                       WHERE id = ? AND project_id = ?''',
                    (
                        float(new_quantity) if column_name == 'quantity' else current_quantity,
                        float(new_unit_price) if column_name == 'composite_unit_price' else current_unit_price,
                        new_amount,
                        new_productivity_total,
                        detail_id,
                        project_id
                    )
                )
        elif column_name == 'productivity':
            # When productivity is edited, recalculate productivity_total
            cursor.execute(
                'SELECT quantity FROM estimate_details WHERE id = ? AND project_id = ?',
                (detail_id, project_id)
            )
            qty_result = cursor.fetchone()
            if qty_result:
                quantity = qty_result[0]
                new_productivity = float(new_value) if new_value else 0
                new_productivity_total = quantity * new_productivity

                cursor.execute(
                    '''UPDATE estimate_details
                       SET productivity = ?, productivity_total = ?
                       WHERE id = ? AND project_id = ?''',
                    (new_productivity, new_productivity_total, detail_id, project_id)
                )
            else:
                cursor.execute(
                    f'UPDATE estimate_details SET {column_name} = ? WHERE id = ? AND project_id = ?',
                    (new_value, detail_id, project_id)
                )
        else:
            # Standard update
            cursor.execute(
                f'UPDATE estimate_details SET {column_name} = ? WHERE id = ? AND project_id = ?',
                (new_value, detail_id, project_id)
            )

        db.commit()

        # Record edit history
        cursor.execute(
            '''INSERT INTO edit_history
               (project_id, detail_id, column_name, old_value, new_value, edited_by, edited_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (project_id, detail_id, column_name, str(old_value), str(new_value), current_user.id, datetime.utcnow())
        )
        db.commit()

        add_audit_log(
            current_user.id,
            'EDIT_ESTIMATE',
            'estimate_detail',
            detail_id,
            'INFO',
            f'見積詳細編集: {column_name}',
            get_user_ip()
        )

        return jsonify({
            'success': True,
            'message': '更新しました'
        }), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'EDIT_ESTIMATE_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'編集エラー: {str(e)}'}), 500


@app.route('/projects/<int:project_id>/estimates/add-row', methods=['POST'])
@login_required
def add_estimate_row(project_id):
    """Add manual estimate row"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス権限がありません'}), 403

        data = request.get_json()

        # Get next row number
        cursor.execute(
            'SELECT MAX(row_no) FROM estimate_details WHERE project_id = ?',
            (project_id,)
        )
        result = cursor.fetchone()
        next_row_no = (result[0] or 0) + 1

        cursor.execute(
            '''INSERT INTO estimate_details
               (project_id, row_no, field_category, material_name, spec, construction_method, unit,
                quantity, composite_unit_price, amount, is_manual_added, edited_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (
                project_id,
                next_row_no,
                data.get('field_category', ''),
                data.get('material_name', ''),
                data.get('spec', ''),
                data.get('construction_method', ''),
                data.get('unit', ''),
                data.get('quantity', 0),
                data.get('composite_unit_price', 0),
                data.get('amount', 0),
                True,
                current_user.id
            )
        )
        db.commit()

        new_detail_id = cursor.lastrowid

        add_audit_log(
            current_user.id,
            'ADD_ESTIMATE_ROW',
            'estimate_detail',
            new_detail_id,
            'INFO',
            f'見積行追加（手動）',
            get_user_ip()
        )

        return jsonify({
            'success': True,
            'message': '行を追加しました',
            'detail_id': new_detail_id,
            'row_no': next_row_no
        }), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'ADD_ROW_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'行追加エラー: {str(e)}'}), 500


@app.route('/projects/<int:project_id>/export-excel')
@login_required
def export_excel(project_id):
    """Export project to Excel"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by, name FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス権限がありません'}), 403

        project_name = project[1]

        # Create workbook
        wb = Workbook()

        # Sheet 1: 見積明細
        ws1 = wb.active
        ws1.title = '見積明細'

        headers_1 = [
            '行番号', '分野', '名称', '規格', '施工条件', '単位',
            '数量', '単価', '金額', '生産性', '生産性合計',
            'ソースPDF', 'ページ', 'マッチ種別', '信頼度', 'マッチ理由', '備考'
        ]
        ws1.append(headers_1)

        # Format header
        header_fill = PatternFill(start_color='0070C0', end_color='0070C0', fill_type='solid')
        header_font = Font(color='FFFFFF', bold=True)
        for cell in ws1[1]:
            cell.fill = header_fill
            cell.font = header_font

        cursor.execute(
            '''SELECT row_no, field_category, material_name, spec, construction_method, unit,
                      quantity, composite_unit_price, amount, productivity, productivity_total,
                      source_pdf, source_page, match_type, confidence, match_reason, remarks
               FROM estimate_details
               WHERE project_id = ?
               ORDER BY row_no''',
            (project_id,)
        )

        for row_data in cursor.fetchall():
            ws1.append(list(row_data))

        # Auto-filter
        ws1.auto_filter.ref = f'A1:{get_column_letter(len(headers_1))}1'

        # Sheet 2: 照合結果
        ws2 = wb.create_sheet('照合結果')

        headers_2 = [
            'プロジェクトID', '素材ID', '候補順位', 'マスターID', 'マッチ種別',
            '信頼度', '理由', '採用済', 'マスター名', 'マスター規格', 'マスター施工条件',
            'マスター単位', '単価', '生産性', 'ソースページ', '分野'
        ]
        ws2.append(headers_2)

        for cell in ws2[1]:
            cell.fill = header_fill
            cell.font = header_font

        cursor.execute(
            '''SELECT project_id, material_id, candidate_rank, master_id, match_type,
                      confidence, reason, is_adopted, master_name, master_spec, master_method,
                      master_unit, composite_unit_price, removal_productivity, source_page, field_category
               FROM match_results
               WHERE project_id = ?''',
            (project_id,)
        )

        for row_data in cursor.fetchall():
            ws2.append(list(row_data))

        ws2.auto_filter.ref = f'A1:{get_column_letter(len(headers_2))}1'

        # Sheet 3: 修正履歴
        ws3 = wb.create_sheet('修正履歴')

        headers_3 = [
            'プロジェクトID', '見積詳細ID', 'カラム名', '旧値', '新値', '編集者ID', '編集日時'
        ]
        ws3.append(headers_3)

        for cell in ws3[1]:
            cell.fill = header_fill
            cell.font = header_font

        cursor.execute(
            '''SELECT project_id, detail_id, column_name, old_value, new_value, edited_by, edited_at
               FROM edit_history
               WHERE project_id = ?
               ORDER BY edited_at DESC''',
            (project_id,)
        )

        for row_data in cursor.fetchall():
            ws3.append(list(row_data))

        ws3.auto_filter.ref = f'A1:{get_column_letter(len(headers_3))}1'

        # Save to BytesIO
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        add_audit_log(
            current_user.id,
            'EXPORT_EXCEL',
            'project',
            project_id,
            'INFO',
            'Excel エクスポート',
            get_user_ip()
        )

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'{project_name}_見積.xlsx'
        )

    except Exception as e:
        add_error_log(
            current_user.id,
            'EXPORT_EXCEL_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='Excel エクスポートエラーが発生しました'), 500


@app.route('/projects/<int:project_id>/match-candidates/<int:material_id>')
@login_required
def get_match_candidates(project_id, material_id):
    """Get match candidates for material as JSON"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス権限がありません'}), 403

        cursor.execute(
            '''SELECT id, candidate_rank, master_id, match_type, confidence, reason, is_adopted,
                      master_name, master_spec, master_method, master_unit, composite_unit_price,
                      removal_productivity, source_page, field_category
               FROM match_results
               WHERE project_id = ? AND material_id = ?
               ORDER BY candidate_rank''',
            (project_id, material_id)
        )

        candidates = []
        for row in cursor.fetchall():
            candidates.append({
                'id': row[0],
                'candidate_rank': row[1],
                'master_id': row[2],
                'match_type': row[3],
                'confidence': row[4],
                'reason': row[5],
                'is_adopted': row[6],
                'master_name': row[7],
                'master_spec': row[8],
                'master_method': row[9],
                'master_unit': row[10],
                'composite_unit_price': row[11],
                'removal_productivity': row[12],
                'source_page': row[13],
                'field_category': row[14]
            })

        return jsonify({'candidates': candidates}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'GET_CANDIDATES_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'候補取得エラー: {str(e)}'}), 500


# ==================== PROJECT ESTIMATE SETTINGS ROUTES ====================

@app.route('/projects/<int:project_id>/estimate-settings', methods=['GET', 'POST'])
@login_required
def estimate_settings(project_id):
    """View/edit project-specific estimate settings"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by, name FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス権限がありません'}), 403

        if request.method == 'POST':
            # Update or create project settings
            data = request.get_json() or request.form

            cursor.execute('SELECT id FROM project_estimate_settings WHERE project_id = ?', (project_id,))
            existing = cursor.fetchone()

            settings_data = {
                'company_name': data.get('company_name', ''),
                'company_address': data.get('company_address', ''),
                'company_tel': data.get('company_tel', ''),
                'company_fax': data.get('company_fax', ''),
                'labor_unit_price': float(data.get('labor_unit_price', 25000)) if data.get('labor_unit_price') else 25000,
                'estimate_title': data.get('estimate_title', ''),
                'estimate_conditions': data.get('estimate_conditions', ''),
                'updated_at': datetime.utcnow()
            }

            if existing:
                cursor.execute(
                    '''UPDATE project_estimate_settings
                       SET company_name = ?, company_address = ?, company_tel = ?, company_fax = ?,
                           labor_unit_price = ?, estimate_title = ?, estimate_conditions = ?, updated_at = ?
                       WHERE project_id = ?''',
                    (
                        settings_data['company_name'],
                        settings_data['company_address'],
                        settings_data['company_tel'],
                        settings_data['company_fax'],
                        settings_data['labor_unit_price'],
                        settings_data['estimate_title'],
                        settings_data['estimate_conditions'],
                        settings_data['updated_at'],
                        project_id
                    )
                )
            else:
                cursor.execute(
                    '''INSERT INTO project_estimate_settings
                       (project_id, company_name, company_address, company_tel, company_fax,
                        labor_unit_price, estimate_title, estimate_conditions, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        project_id,
                        settings_data['company_name'],
                        settings_data['company_address'],
                        settings_data['company_tel'],
                        settings_data['company_fax'],
                        settings_data['labor_unit_price'],
                        settings_data['estimate_title'],
                        settings_data['estimate_conditions'],
                        datetime.utcnow(),
                        settings_data['updated_at']
                    )
                )

            db.commit()

            add_audit_log(
                current_user.id,
                'UPDATE_PROJECT_SETTINGS',
                'project_settings',
                project_id,
                'INFO',
                f'案件見積設定更新: プロジェクト {project_id}',
                get_user_ip()
            )

            return jsonify({'success': True, 'message': '設定を保存しました'}), 200

        # GET: Return project settings (or empty if not set)
        cursor.execute(
            '''SELECT company_name, company_address, company_tel, company_fax,
                      labor_unit_price, estimate_title, estimate_conditions
               FROM project_estimate_settings
               WHERE project_id = ?''',
            (project_id,)
        )
        settings = cursor.fetchone()

        if settings:
            return jsonify({
                'company_name': settings[0],
                'company_address': settings[1],
                'company_tel': settings[2],
                'company_fax': settings[3],
                'labor_unit_price': settings[4],
                'estimate_title': settings[5],
                'estimate_conditions': settings[6]
            }), 200
        else:
            # Return empty settings
            return jsonify({
                'company_name': '',
                'company_address': '',
                'company_tel': '',
                'company_fax': '',
                'labor_unit_price': 25000,
                'estimate_title': '',
                'estimate_conditions': ''
            }), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'ESTIMATE_SETTINGS_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'設定の取得に失敗しました: {str(e)}'}), 500


# ==================== ESTIMATE BUILDER ROUTES ====================

@app.route('/projects/<int:project_id>/estimate-builder')
@login_required
def estimate_builder(project_id):
    """Estimate builder page (見積作成画面)"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by, name FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return render_template('error.html', error='アクセス権限がありません'), 403

        # Get estimate details for the project
        cursor.execute(
            '''SELECT id, row_no, field_category, material_name, spec, construction_method, unit,
                      quantity, composite_unit_price, amount, productivity, productivity_total
               FROM estimate_details
               WHERE project_id = ?
               ORDER BY row_no''',
            (project_id,)
        )
        details = cursor.fetchall()

        # Get all settings (global defaults)
        settings = {}
        for row in cursor.execute("SELECT setting_key, setting_value FROM estimate_settings").fetchall():
            settings[row[0]] = row[1]

        # Check for project-specific settings and override defaults if they exist
        cursor.execute(
            'SELECT labor_unit_price FROM project_estimate_settings WHERE project_id = ?',
            (project_id,)
        )
        project_settings = cursor.fetchone()
        if project_settings and project_settings[0]:
            labor_unit_price = float(project_settings[0])
        else:
            labor_unit_price = float(settings.get('labor_unit_price', '25000'))

        # Convert to list of dicts
        details_list = []
        for row in details:
            details_list.append({
                'id': row[0],
                'row_no': row[1],
                'field_category': row[2],
                'material_name': row[3],
                'spec': row[4],
                'construction_method': row[5],
                'unit': row[6],
                'quantity': row[7],
                'composite_unit_price': row[8],
                'amount': row[9],
                'productivity': row[10],
                'productivity_total': row[11]
            })

        add_audit_log(
            current_user.id,
            'VIEW_ESTIMATE_BUILDER',
            'project',
            project_id,
            'INFO',
            '見積作成画面表示',
            get_user_ip()
        )

        # Build project dict for template
        project_dict = {
            'id': project_id,
            'name': project[1],
            'client_name': '',
        }
        # Get full project info
        cursor.execute('SELECT client_name, description FROM projects WHERE id = ?', (project_id,))
        pinfo = cursor.fetchone()
        if pinfo:
            project_dict['client_name'] = pinfo[0] or ''
            project_dict['description'] = pinfo[1] or ''

        return render_template(
            'estimate_builder.html',
            project=project_dict,
            details=details_list,
            settings=settings,
            labor_unit_price=labor_unit_price,
            today=datetime.now().strftime('%Y-%m-%d')
        )

    except Exception as e:
        add_error_log(
            current_user.id,
            'ESTIMATE_BUILDER_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='見積作成画面の読み込みに失敗しました'), 500


@app.route('/projects/<int:project_id>/estimate-builder/save', methods=['POST'])
@login_required
def save_estimate(project_id):
    """Save estimate data from estimate builder"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス稩限がありません'}), 403

        data = request.get_json()
        sections = data.get('sections', [])

        # Process and save sections/rows
        for section in sections:
            rows = section.get('rows', [])
            for row in rows:
                detail_id = row.get('id')
                if detail_id:
                    # Update existing row
                    cursor.execute(
                        '''UPDATE estimate_details
                           SET quantity = ?, composite_unit_price = ?, amount = ?,
                               productivity = ?, productivity_total = ?, remarks = ?
                           WHERE id = ? AND project_id = ?''',
                        (
                            float(row.get('quantity', 0)),
                            float(row.get('composite_unit_price', 0)),
                            float(row.get('amount', 0)),
                            float(row.get('productivity', 0)),
                            float(row.get('productivity_total', 0)),
                            row.get('remarks', ''),
                            detail_id,
                            project_id
                        )
                    )
                else:
                    # Insert new row
                    cursor.execute(
                        '''INSERT INTO estimate_details
                           (project_id, row_no, field_category, material_name, spec,
                            construction_method, unit, quantity, composite_unit_price,
                            amount, productivity, productivity_total, remarks, is_manual_added)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (
                            project_id,
                            row.get('row_no', 0),
                            row.get('field_category', ''),
                            row.get('material_name', ''),
                            row.get('spec', ''),
                            row.get('construction_method', ''),
                            row.get('unit', ''),
                            float(row.get('quantity', 0)),
                            float(row.get('composite_unit_price', 0)),
                            float(row.get('amount', 0)),
                            float(row.get('productivity', 0)),
                            float(row.get('productivity_total', 0)),
                            row.get('remarks', ''),
                            True
                        )
                    )

        db.commit()

        add_audit_log(
            current_user.id,
            'SAVE_ESTIMATE',
            'project',
            project_id,
            'INFO',
            '見積データ保存',
            get_user_ip()
        )

        return jsonify({
            'success': True,
            'message': '見積データを保存しました'
        }), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'SAVE_ESTIMATE_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'保存エラー: {str(e)}'}), 500


@app.route('/projects/<int:project_id>/estimate-builder/export-pdf', methods=['POST'])
@login_required
def export_estimate_pdf(project_id):
    """Export estimate as Excel (PDF generation is complex, so use Excel)"""
    try:
        # For now, just redirect to the existing Excel export endpoint
        return redirect(url_for('export_excel', project_id=project_id))
    except Exception as e:
        add_error_log(
            current_user.id,
            'EXPORT_PDF_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'エクスポートエラー: {str(e)}'}), 500


# ==================== SHARED FILES ROUTES ====================

@app.route('/projects/<int:project_id>/files')
@login_required
def project_files(project_id):
    """Get project files as JSON"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス権限がありません'}), 403

        cursor.execute(
            '''SELECT id, file_type, original_name, file_size, uploaded_at, uploaded_by
               FROM project_files
               WHERE project_id = ?
               ORDER BY uploaded_at DESC''',
            (project_id,)
        )

        files = []
        for row in cursor.fetchall():
            files.append({
                'id': row[0],
                'file_type': row[1],
                'original_name': row[2],
                'file_size': row[3],
                'uploaded_at': row[4],
                'uploaded_by': row[5]
            })

        return jsonify({'files': files}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'GET_FILES_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'ファイル取得エラー: {str(e)}'}), 500


@app.route('/projects/<int:project_id>/files/download/<int:file_id>')
@login_required
def download_file(project_id, file_id):
    """Download a specific file"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return render_template('error.html', error='アクセス権限がありません'), 403

        # Get file info
        cursor.execute(
            'SELECT stored_path, original_name FROM project_files WHERE id = ? AND project_id = ?',
            (file_id, project_id)
        )
        file_info = cursor.fetchone()
        if not file_info:
            return render_template('error.html', error='ファイルが見つかりません'), 404

        stored_path, original_name = file_info

        if not os.path.exists(stored_path):
            return render_template('error.html', error='ファイルが存在しません'), 404

        add_audit_log(
            current_user.id,
            'DOWNLOAD_FILE',
            'project_file',
            file_id,
            'INFO',
            f'ファイルダウンロード: {original_name}',
            get_user_ip()
        )

        return send_file(
            stored_path,
            as_attachment=True,
            download_name=original_name
        )

    except Exception as e:
        add_error_log(
            current_user.id,
            'DOWNLOAD_FILE_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='ファイルダウンロードエラーが発生しました'), 500


# ==================== LEARNING DICTIONARY ROUTES ====================

@app.route('/projects/<int:project_id>/add-to-learning', methods=['POST'])
@login_required
def add_to_learning(project_id):
    """Add corrected match to learning dictionary"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check project ownership
        cursor.execute('SELECT created_by FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        if not project or (project[0] != current_user.id and not current_user.is_admin()):
            return jsonify({'error': 'アクセス権限がありません'}), 403

        data = request.get_json()

        cursor.execute(
            '''INSERT INTO learning_dictionary
               (input_name, canonical_name, input_spec, canonical_spec,
                input_method, canonical_method, status, source_project_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (
                data.get('input_name', ''),
                data.get('canonical_name', ''),
                data.get('input_spec', ''),
                data.get('canonical_spec', ''),
                data.get('input_method', ''),
                data.get('canonical_method', ''),
                'candidate',
                project_id
            )
        )
        db.commit()

        new_entry_id = cursor.lastrowid

        add_audit_log(
            current_user.id,
            'ADD_LEARNING_ENTRY',
            'project',
            project_id,
            'INFO',
            f'学習辞書エントリ追加',
            get_user_ip()
        )

        return jsonify({
            'success': True,
            'message': '学習辞書に追加しました',
            'entry_id': new_entry_id
        }), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'ADD_LEARNING_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'追加エラー: {str(e)}'}), 500


# ==================== ADMIN ROUTES ====================

@app.route('/admin/users')
@admin_required
def admin_users():
    """User management page"""
    try:
        db = get_db()
        cursor = db.cursor()

        cursor.execute(
            '''SELECT id, email, full_name, role, is_active, created_at, approved_at, approved_by, last_login_at
               FROM users
               ORDER BY created_at DESC''',
            ()
        )

        users = []
        for row in cursor.fetchall():
            users.append({
                'id': row[0],
                'email': row[1],
                'full_name': row[2],
                'role': row[3],
                'is_active': row[4],
                'created_at': row[5],
                'approved_at': row[6],
                'approved_by': row[7],
                'last_login_at': row[8]
            })

        add_audit_log(
            current_user.id,
            'VIEW_USER_MANAGEMENT',
            'admin',
            None,
            'INFO',
            'ユーザー管理ページ表示',
            get_user_ip()
        )

        return render_template('admin_users.html', users=users)

    except Exception as e:
        add_error_log(
            current_user.id,
            'ADMIN_USERS_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='ユーザー管理読み込みエラーが発生しました'), 500


@app.route('/admin/users/<int:user_id>/approve', methods=['POST'])
@admin_required
def approve_user(user_id):
    """Approve pending user"""
    try:
        db = get_db()
        cursor = db.cursor()

        cursor.execute(
            'UPDATE users SET role = ?, approved_at = ?, approved_by = ? WHERE id = ? AND role = ?',
            ('user', datetime.utcnow(), current_user.id, user_id, 'pending')
        )
        db.commit()

        add_audit_log(
            current_user.id,
            'APPROVE_USER',
            'user',
            user_id,
            'INFO',
            'ユーザー承認',
            get_user_ip()
        )

        return jsonify({'success': True, 'message': 'ユーザーを承認しました'}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'APPROVE_USER_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'承認エラー: {str(e)}'}), 500


@app.route('/admin/users/<int:user_id>/reject', methods=['POST'])
@admin_required
def reject_user(user_id):
    """Reject pending user"""
    try:
        db = get_db()
        cursor = db.cursor()

        cursor.execute(
            'DELETE FROM users WHERE id = ? AND role = ?',
            (user_id, 'pending')
        )
        db.commit()

        add_audit_log(
            current_user.id,
            'REJECT_USER',
            'user',
            user_id,
            'INFO',
            'ユーザー拒否',
            get_user_ip()
        )

        return jsonify({'success': True, 'message': 'ユーザーを拒否しました'}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'REJECT_USER_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'拒否エラー: {str(e)}'}), 500


@app.route('/admin/users/<int:user_id>/toggle-active', methods=['POST'])
@admin_required
def toggle_user_active(user_id):
    """Toggle user active status"""
    try:
        db = get_db()
        cursor = db.cursor()

        cursor.execute('SELECT is_active FROM users WHERE id = ?', (user_id,))
        result = cursor.fetchone()
        if not result:
            return jsonify({'error': 'ユーザーが見つかりません'}), 404

        new_status = not result[0]
        cursor.execute(
            'UPDATE users SET is_active = ? WHERE id = ?',
            (new_status, user_id)
        )
        db.commit()

        status_text = 'アクティブ化' if new_status else '無効化'
        add_audit_log(
            current_user.id,
            'TOGGLE_USER_ACTIVE',
            'user',
            user_id,
            'INFO',
            f'ユーザー{status_text}',
            get_user_ip()
        )

        return jsonify({'success': True, 'message': f'ユーザーを{status_text}しました', 'is_active': new_status}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'TOGGLE_ACTIVE_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'状態変更エラー: {str(e)}'}), 500


@app.route('/admin/users/<int:user_id>/change-role', methods=['POST'])
@admin_required
def change_user_role(user_id):
    """Change user role"""
    try:
        db = get_db()
        cursor = db.cursor()

        data = request.get_json()
        new_role = data.get('role')

        if new_role not in ['user', 'admin']:
            return jsonify({'error': '無効なロールです'}), 400

        cursor.execute(
            'UPDATE users SET role = ? WHERE id = ?',
            (new_role, user_id)
        )
        db.commit()

        add_audit_log(
            current_user.id,
            'CHANGE_USER_ROLE',
            'user',
            user_id,
            'INFO',
            f'ロール変更: {new_role}',
            get_user_ip()
        )

        return jsonify({'success': True, 'message': 'ロールを変更しました'}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'CHANGE_ROLE_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'ロール変更エラー: {str(e)}'}), 500


@app.route('/admin/users/<int:user_id>/reset-password', methods=['POST'])
@admin_required
def reset_user_password(user_id):
    """Reset user password"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check that user exists and is not the current admin
        if user_id == current_user.id:
            return jsonify({'error': '自分自身のパスワードはこの方法ではリセットできません'}), 400

        cursor.execute('SELECT email FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()
        if not user:
            return jsonify({'error': 'ユーザーが見つかりません'}), 404

        # Generate temporary password
        import string
        import secrets
        temp_password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(12))

        # Hash and update password
        pw_hash = bcrypt.hashpw(temp_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        cursor.execute(
            'UPDATE users SET password_hash = ? WHERE id = ?',
            (pw_hash, user_id)
        )
        db.commit()

        add_audit_log(
            current_user.id,
            'RESET_USER_PASSWORD',
            'user',
            user_id,
            'INFO',
            'ユーザーパスワードリセット',
            get_user_ip()
        )

        return jsonify({
            'success': True,
            'message': 'パスワードをリセットしました',
            'temp_password': temp_password
        }), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'RESET_PASSWORD_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'パスワードリセットエラー: {str(e)}'}), 500


@app.route('/admin/users/<int:user_id>/delete', methods=['POST'])
@admin_required
def delete_user(user_id):
    """Delete user"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Check that user is not the current admin
        if user_id == current_user.id:
            return jsonify({'error': '自分自身を削除することはできません'}), 400

        cursor.execute('SELECT email FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()
        if not user:
            return jsonify({'error': 'ユーザーが見つかりません'}), 404

        # Delete user
        cursor.execute('DELETE FROM users WHERE id = ?', (user_id,))
        db.commit()

        add_audit_log(
            current_user.id,
            'DELETE_USER',
            'user',
            user_id,
            'INFO',
            f'ユーザー削除: {user[0]}',
            get_user_ip()
        )

        return jsonify({'success': True, 'message': 'ユーザーを削除しました'}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'DELETE_USER_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'ユーザー削除エラー: {str(e)}'}), 500


@app.route('/admin/audit-log')
@admin_required
def admin_audit_log():
    """Audit log viewer"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Get filters
        user_id = request.args.get('user_id', '')
        action = request.args.get('action', '')
        level = request.args.get('level', '')

        query = '''SELECT a.id, a.user_id, a.action, a.entity_type, a.entity_id,
                   a.level, a.details, a.created_at, COALESCE(u.full_name,'システム') as user_name
                   FROM audit_log a LEFT JOIN users u ON a.user_id=u.id'''
        params = []

        conditions = []
        if user_id:
            conditions.append('a.user_id = ?')
            params.append(user_id)
        if action:
            conditions.append('a.action LIKE ?')
            params.append(f'%{action}%')
        if level:
            conditions.append('a.level = ?')
            params.append(level)

        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)

        # Pagination
        page = int(request.args.get('page', 1))
        per_page = 50
        count_query = query.replace(
            'SELECT a.id, a.user_id, a.action, a.entity_type, a.entity_id,\n                   a.level, a.details, a.created_at, COALESCE(u.full_name,\'システム\') as user_name\n                   FROM audit_log a LEFT JOIN users u ON a.user_id=u.id',
            'SELECT COUNT(*) FROM audit_log a LEFT JOIN users u ON a.user_id=u.id'
        )
        cursor.execute(count_query, params)
        total = cursor.fetchone()[0]
        total_pages = max(1, (total + per_page - 1) // per_page)

        query += f' ORDER BY a.created_at DESC LIMIT {per_page} OFFSET {(page-1)*per_page}'

        cursor.execute(query, params)

        logs = []
        for row in cursor.fetchall():
            logs.append({
                'id': row[0],
                'user_id': row[1],
                'action': row[2],
                'target': f"{row[3] or ''} {row[4] or ''}".strip(),
                'level': row[5],
                'details': row[6],
                'timestamp': row[7],
                'user_name': row[8]
            })

        add_audit_log(
            current_user.id,
            'VIEW_AUDIT_LOG',
            'admin',
            None,
            'INFO',
            '監査ログ表示',
            get_user_ip()
        )

        return render_template('admin_audit.html', logs=logs, page=page, total_pages=total_pages)

    except Exception as e:
        add_error_log(
            current_user.id,
            'AUDIT_LOG_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='監査ログ読み込みエラーが発生しました'), 500


@app.route('/admin/error-log')
@admin_required
def admin_error_log():
    """Error log viewer"""
    try:
        db = get_db()
        cursor = db.cursor()

        cursor.execute(
            '''SELECT e.id, e.user_id, e.error_type, e.error_message, e.traceback, e.url, e.created_at,
                      COALESCE(u.full_name,'システム') as user_name
               FROM error_log e LEFT JOIN users u ON e.user_id=u.id
               ORDER BY e.created_at DESC
               LIMIT 500'''
        )

        logs = []
        for row in cursor.fetchall():
            logs.append({
                'id': row[0],
                'user_id': row[1],
                'error_type': row[2],
                'message': row[3],
                'traceback': row[4],
                'url': row[5],
                'timestamp': row[6],
                'user_name': row[7]
            })

        add_audit_log(
            current_user.id,
            'VIEW_ERROR_LOG',
            'admin',
            None,
            'INFO',
            'エラーログ表示',
            get_user_ip()
        )

        return render_template('admin_error.html', logs=logs)

    except Exception as e:
        add_error_log(
            current_user.id,
            'ERROR_LOG_VIEW_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='エラーログ読み込みエラーが発生しました'), 500


@app.route('/admin/learning')
@admin_required
def admin_learning():
    """Learning dictionary management"""
    try:
        db = get_db()
        cursor = db.cursor()

        status_filter = request.args.get('status', 'candidate')

        cursor.execute(
            '''SELECT id, input_name, canonical_name, input_spec, canonical_spec, input_method,
                      canonical_method, confidence, status, confirmed_by, confirmed_at, source_project_id
               FROM learning_dictionary
               WHERE status = ?
               ORDER BY confidence DESC''',
            (status_filter,)
        )

        entries = []
        for row in cursor.fetchall():
            entries.append({
                'id': row[0],
                'input_name': row[1],
                'canonical_name': row[2],
                'input_spec': row[3],
                'canonical_spec': row[4],
                'input_method': row[5],
                'canonical_method': row[6],
                'confidence': row[7],
                'status': row[8],
                'confirmed_by': row[9],
                'confirmed_at': row[10],
                'source_project_id': row[11]
            })

        add_audit_log(
            current_user.id,
            'VIEW_LEARNING_DICTIONARY',
            'admin',
            None,
            'INFO',
            f'学習辞書表示 ({status_filter})',
            get_user_ip()
        )

        return render_template('admin_learning.html', entries=entries, status_filter=status_filter)

    except Exception as e:
        add_error_log(
            current_user.id,
            'LEARNING_DICT_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='学習辞書読み込みエラーが発生しました'), 500


@app.route('/admin/learning/<int:entry_id>/confirm', methods=['POST'])
@admin_required
def confirm_learning_entry(entry_id):
    """Confirm learning dictionary entry"""
    try:
        db = get_db()
        cursor = db.cursor()

        cursor.execute(
            '''UPDATE learning_dictionary
               SET status = ?, confirmed_by = ?, confirmed_at = ?
               WHERE id = ?''',
            ('confirmed', current_user.id, datetime.utcnow(), entry_id)
        )
        db.commit()

        add_audit_log(
            current_user.id,
            'CONFIRM_LEARNING_ENTRY',
            'learning_dictionary',
            entry_id,
            'INFO',
            '学習辞書エントリ確認',
            get_user_ip()
        )

        return jsonify({'success': True, 'message': 'エントリを確認しました'}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'CONFIRM_LEARNING_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'確認エラー: {str(e)}'}), 500


@app.route('/admin/learning/<int:entry_id>/reject', methods=['POST'])
@admin_required
def reject_learning_entry(entry_id):
    """Reject learning dictionary entry"""
    try:
        db = get_db()
        cursor = db.cursor()

        cursor.execute(
            '''UPDATE learning_dictionary
               SET status = ?, confirmed_by = ?, confirmed_at = ?
               WHERE id = ?''',
            ('rejected', current_user.id, datetime.utcnow(), entry_id)
        )
        db.commit()

        add_audit_log(
            current_user.id,
            'REJECT_LEARNING_ENTRY',
            'learning_dictionary',
            entry_id,
            'INFO',
            '学習辞書エントリ拒否',
            get_user_ip()
        )

        return jsonify({'success': True, 'message': 'エントリを拒否しました'}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'REJECT_LEARNING_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'拒否エラー: {str(e)}'}), 500


@app.route('/admin/master')
@admin_required
def admin_master():
    """Master data management"""
    try:
        db = get_db()
        cursor = db.cursor()

        # Get latest update log
        cursor.execute(
            '''SELECT id, action, source_file, records_added, records_updated, status, updated_by, updated_at
               FROM master_update_log
               ORDER BY updated_at DESC
               LIMIT 50''',
            ()
        )

        logs = []
        for row in cursor.fetchall():
            logs.append({
                'id': row[0],
                'action': row[1],
                'source_file': row[2],
                'records_added': row[3],
                'records_updated': row[4],
                'status': row[5],
                'updated_by': row[6],
                'updated_at': row[7]
            })

        # Get master data stats
        cursor.execute(
            '''SELECT COUNT(*), COUNT(DISTINCT category_no), COUNT(DISTINCT field_category)
               FROM estimate_master'''
        )
        stats = cursor.fetchone()

        add_audit_log(
            current_user.id,
            'VIEW_MASTER_DATA',
            'admin',
            None,
            'INFO',
            'マスターデータ管理ページ表示',
            get_user_ip()
        )

        return render_template(
            'admin_master.html',
            logs=logs,
            total_records=stats[0] if stats else 0,
            total_categories=stats[1] if stats else 0,
            total_fields=stats[2] if stats else 0
        )

    except Exception as e:
        add_error_log(
            current_user.id,
            'MASTER_DATA_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='マスターデータ読み込みエラーが発生しました'), 500


@app.route('/admin/master/upload', methods=['POST'])
@admin_required
def upload_master_data():
    """Upload master data (Excel, CSV, TSV, SHD, etc.)"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'ファイルが選択されていません'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'ファイルが選択されていません'}), 400

        # Determine file extension
        file_ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else ''

        # Check if file type is supported
        if file_ext not in ('xlsx', 'xls', 'csv', 'tsv', 'shd', 'txt', 'pdf'):
            return jsonify({'error': '.xlsx, .xls, .csv, .tsv, .shd, .txt, .pdf ファイルのみサポートしています'}), 400

        # Save temp file
        temp_filename = f"master_import_{uuid.uuid4().hex}.{file_ext}"
        temp_path = os.path.join(app.config['UPLOAD_FOLDER'], temp_filename)
        file.save(temp_path)

        db = get_db()
        cursor = db.cursor()

        records_added = 0
        records_updated = 0
        rows_data = []

        # Parse file based on type
        if file_ext in ('xlsx', 'xls'):
            from openpyxl import load_workbook
            wb = load_workbook(temp_path)
            ws = wb.active
            # Skip header row
            for row_idx, row in enumerate(ws.iter_rows(values_only=True), 1):
                if row_idx == 1:
                    continue
                if all(cell is None for cell in row):
                    continue
                rows_data.append(row)
        elif file_ext in ('csv', 'tsv', 'txt'):
            # Parse CSV/TSV
            delimiter = '\t' if file_ext == 'tsv' else ','
            with open(temp_path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.reader(f, delimiter=delimiter)
                for row_idx, row in enumerate(reader):
                    if row_idx == 0:  # Skip header
                        continue
                    if not any(row):  # Skip empty rows
                        continue
                    rows_data.append(row)
        elif file_ext == 'shd':
            # Parse SHD file
            with open(temp_path, 'rb') as f:
                content = f.read().decode('shift_jis', errors='replace')
            lines = content.split('\r')
            for row_idx, line in enumerate(lines):
                if row_idx == 0:  # Skip header
                    continue
                fields = line.split('\t')
                if not any(fields):
                    continue
                rows_data.append(fields)
        elif file_ext == 'pdf':
            # For PDF, extract text and skip (not ideal for master data)
            return jsonify({'error': 'PDFファイルはマスターデータアップロードに適していません'}), 400

        # Process rows
        for row_data in rows_data:
            try:
                # Prepare insert data based on expected format
                data = {
                    'source_page': row_data[0] if len(row_data) > 0 else '',
                    'category_no': row_data[1] if len(row_data) > 1 else '',
                    'field_category': row_data[2] if len(row_data) > 2 else '',
                    'material_name': row_data[3] if len(row_data) > 3 else '',
                    'spec_summary': row_data[4] if len(row_data) > 4 else '',
                    'remarks': row_data[5] if len(row_data) > 5 else '',
                    'construction_method': row_data[6] if len(row_data) > 6 else '',
                    'unit': row_data[7] if len(row_data) > 7 else '',
                    'material_unit_price': float(row_data[8]) if len(row_data) > 8 and row_data[8] else 0,
                    'material_cost': float(row_data[9]) if len(row_data) > 9 and row_data[9] else 0,
                    'labor_cost': float(row_data[10]) if len(row_data) > 10 and row_data[10] else 0,
                    'expense_cost': float(row_data[11]) if len(row_data) > 11 and row_data[11] else 0,
                    'composite_unit_price': float(row_data[12]) if len(row_data) > 12 and row_data[12] else 0,
                    'removal_productivity': float(row_data[13]) if len(row_data) > 13 and row_data[13] else 0,
                    'removal_cost': float(row_data[14]) if len(row_data) > 14 and row_data[14] else 0,
                    'master_version': 1,
                }

                # Check if exists
                cursor.execute(
                    'SELECT id FROM estimate_master WHERE category_no = ? AND material_name = ?',
                    (data['category_no'], data['material_name'])
                )
                if cursor.fetchone():
                    records_updated += 1
                else:
                    cursor.execute(
                        '''INSERT INTO estimate_master
                           (source_page, category_no, field_category, material_name, spec_summary, remarks,
                            construction_method, unit, material_unit_price, material_cost, labor_cost,
                            expense_cost, composite_unit_price, removal_productivity, removal_cost, master_version)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        tuple(data.values())
                    )
                    records_added += 1

            except Exception as row_error:
                continue

        db.commit()
        os.remove(temp_path)

        # Record update log
        cursor.execute(
            '''INSERT INTO master_update_log
               (action, source_file, records_added, records_updated, status, updated_by, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)''',
            ('IMPORT', file.filename, records_added, records_updated, 'success', current_user.id, datetime.utcnow())
        )
        db.commit()

        add_audit_log(
            current_user.id,
            'UPLOAD_MASTER_DATA',
            'master_data',
            None,
            'INFO',
            f'マスターデータアップロード: {records_added} 追加, {records_updated} 更新',
            get_user_ip()
        )

        return jsonify({
            'success': True,
            'message': 'マスターデータをアップロードしました',
            'records_added': records_added,
            'records_updated': records_updated
        }), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'UPLOAD_MASTER_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'マスターデータアップロードエラー: {str(e)}'}), 500


@app.route('/admin/settings')
@admin_required
def admin_settings():
    """Estimate settings management"""
    try:
        db = get_db()
        cursor = db.cursor()

        cursor.execute(
            '''SELECT id, setting_key, setting_value, description
               FROM estimate_settings
               ORDER BY setting_key''',
            ()
        )

        settings = {}
        for row in cursor.fetchall():
            settings[row[1]] = {
                'id': row[0],
                'value': row[2],
                'description': row[3]
            }

        add_audit_log(
            current_user.id,
            'VIEW_SETTINGS',
            'admin',
            None,
            'INFO',
            '設定管理ページ表示',
            get_user_ip()
        )

        return render_template('admin_settings.html', settings=settings)

    except Exception as e:
        add_error_log(
            current_user.id,
            'SETTINGS_ERROR',
            str(e),
            str(e),
            request.url
        )
        return render_template('error.html', error='設定読み込みエラーが発生しました'), 500


@app.route('/admin/settings', methods=['POST'])
@admin_required
def update_settings():
    """Update estimate settings"""
    try:
        db = get_db()
        cursor = db.cursor()

        data = request.get_json()

        for setting_key, setting_value in data.items():
            cursor.execute(
                '''UPDATE estimate_settings
                   SET setting_value = ?, updated_by = ?, updated_at = ?
                   WHERE setting_key = ?''',
                (str(setting_value), current_user.id, datetime.utcnow(), setting_key)
            )

        db.commit()

        add_audit_log(
            current_user.id,
            'UPDATE_SETTINGS',
            'admin',
            None,
            'INFO',
            '設定更新',
            get_user_ip()
        )

        return jsonify({'success': True, 'message': '設定を更新しました'}), 200

    except Exception as e:
        add_error_log(
            current_user.id,
            'UPDATE_SETTINGS_ERROR',
            str(e),
            str(e),
            request.url
        )
        return jsonify({'error': f'設定更新エラー: {str(e)}'}), 500


# ==================== ERROR HANDLERS ====================

@app.errorhandler(404)
def not_found(error):
    """404 error handler"""
    return render_template('error.html', error='ページが見つかりません'), 404


@app.errorhandler(500)
def internal_error(error):
    """500 error handler"""
    return render_template('error.html', error='サーバーエラーが発生しました'), 500


@app.errorhandler(403)
def forbidden(error):
    """403 error handler"""
    return render_template('error.html', error='アクセスが禁止されています'), 403


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
