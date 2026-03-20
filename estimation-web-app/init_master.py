"""
積算マスタデータをSQLiteにインポートするスクリプト
既存のestimation.dbから8,938件のマスタを移行
"""
import sqlite3
import os
import re
import sys


def normalize_text(text):
    if not text: return ""
    text = str(text)
    result = []
    for c in text:
        cp = ord(c)
        if 0xFF21 <= cp <= 0xFF3A or 0xFF41 <= cp <= 0xFF5A or 0xFF10 <= cp <= 0xFF19:
            result.append(chr(cp - 0xFEE0))
        elif cp == 0x3000:
            result.append(' ')
        else:
            result.append(c)
    text = ''.join(result)
    text = re.sub(r'\s+', ' ', text).strip()
    text = text.replace('\uff65', '\u30FB')
    text = re.sub(r'[\u2010-\u2015\uFF0D]', '-', text)
    return text


def normalize_material_name(name):
    if not name: return ""
    name = normalize_text(name)
    name = name.lower()
    name = name.replace('（', '(').replace('）', ')')
    name = name.replace('ｍｍ', 'mm').replace('ｍ', 'm')
    name = name.replace('×', 'x').replace('＊', '*')
    return name.strip()


def normalize_construction_method(method):
    if not method: return ""
    method = normalize_text(method)
    for old, new in [
        ("隠ぺい・コンクリート打込み", "隠ぺいコンクリート打込み"),
        ("隠ぺい\u30FBコンクリート打込み", "隠ぺいコンクリート打込み"),
        ("隠ぺいコンクリート打込み", "隠ぺいコンクリート打込み"),
        ("PF管・CD管・FEP管内", "PF管CD管FEP管内"),
        ("PF管\u30FBCD管\u30FBFEP管内", "PF管CD管FEP管内"),
        ("ピット・トラフ内", "ピットトラフ内"),
        ("ピット\u30FBトラフ内", "ピットトラフ内"),
        ("ケーブルラック配線", "ケーブルラック"),
        ("ころがし配線", "ころがし"),
        ("FEP管内配線", "FEP管内"),
        ("ラック配線", "ケーブルラック"),
        ("管内配線", "管内"),
    ]:
        if old in method:
            method = method.replace(old, new)
            break
    method = method.replace('打込みみ', '打込み')
    return method.strip()


def normalize_spec(spec):
    if not spec: return ""
    spec = normalize_material_name(spec)
    spec = re.sub(r'\s*-\s*', '-', spec)
    spec = spec.replace('幅', '').replace('地中', '').strip()
    spec = re.sub(r'([a-z])[\s-]*(\d)', r'\1\2', spec)
    return spec.strip()


def build_match_key(name, spec, method, unit):
    parts = []
    if name:   parts.append(normalize_material_name(name))
    if spec:   parts.append(normalize_spec(spec))
    if method: parts.append(normalize_construction_method(method))
    if unit:   parts.append(normalize_text(unit))
    return "|".join(parts)


def import_master(source_db_path, target_db_path):
    """既存DBから新DBにマスタデータを移行"""
    if not os.path.exists(source_db_path):
        print(f"ソースDB {source_db_path} が見つかりません")
        return False

    os.makedirs(os.path.dirname(target_db_path) if os.path.dirname(target_db_path) else ".", exist_ok=True)

    src = sqlite3.connect(source_db_path)
    src_cur = src.cursor()

    src_cur.execute("""SELECT id, source_page, category_no, field_category,
        material_name, spec_summary, remarks, construction_method, unit,
        material_unit_price, material_cost, labor_cost, expense_cost,
        composite_unit_price, removal_productivity, removal_cost, source_text_raw
    FROM estimate_master ORDER BY id""")
    rows = src_cur.fetchall()
    src.close()

    print(f"ソースDB読込: {len(rows)}件")

    tgt = sqlite3.connect(target_db_path)
    tgt_cur = tgt.cursor()

    # テーブルが存在しない場合は作成
    tgt_cur.execute("""CREATE TABLE IF NOT EXISTS estimate_master (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_page TEXT, category_no TEXT, field_category TEXT,
        material_name TEXT, spec_summary TEXT, remarks TEXT,
        construction_method TEXT, unit TEXT,
        material_unit_price REAL DEFAULT 0, material_cost REAL DEFAULT 0,
        labor_cost REAL DEFAULT 0, expense_cost REAL DEFAULT 0,
        composite_unit_price REAL DEFAULT 0, removal_productivity REAL DEFAULT 0,
        removal_cost REAL DEFAULT 0,
        normalized_name TEXT, normalized_spec TEXT, normalized_method TEXT,
        match_key TEXT, source_text TEXT,
        master_version INTEGER DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
    )""")

    # 既存データクリア
    tgt_cur.execute("DELETE FROM estimate_master")

    count = 0
    for row in rows:
        rid, src_page, cat_no, field, name, spec, notes, method, unit, \
        mat_price, mat_cost, labor, expense, composite, prod, removal, raw = row

        norm_name = normalize_material_name(name or "")
        norm_spec = normalize_spec(spec or "")
        norm_method = normalize_construction_method(method or "")
        match_key = build_match_key(name, spec, method, unit)

        tgt_cur.execute("""INSERT INTO estimate_master
            (id, source_page, category_no, field_category, material_name, spec_summary,
             remarks, construction_method, unit, material_unit_price, material_cost,
             labor_cost, expense_cost, composite_unit_price, removal_productivity,
             removal_cost, normalized_name, normalized_spec, normalized_method,
             match_key, source_text, master_version)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
            (rid, src_page, cat_no, field, name, spec, notes, method, unit,
             mat_price or 0, mat_cost or 0, labor or 0, expense or 0,
             composite or 0, prod or 0, removal or 0,
             norm_name, norm_spec, norm_method, match_key, raw or ""))
        count += 1

    tgt.commit()
    tgt.close()
    print(f"移行完了: {count}件")

    # 検証
    bad = sum(1 for r in rows if "打込みみ" in normalize_construction_method(r[7] or ""))
    print(f"打込みみバグ: {bad}件 (0=OK)")

    return True


if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else "est_data/estimation.db"
    target = sys.argv[2] if len(sys.argv) > 2 else "data/estimation.db"
    import_master(source, target)
