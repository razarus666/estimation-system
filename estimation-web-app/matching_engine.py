"""
照合エンジン（検証済みPythonロジック移植版）
8/8テストPASS確認済みのロジックをそのまま使用
"""
import re
from difflib import SequenceMatcher
from models import get_db


# ============================================================
# 正規化関数（検証済み）
# ============================================================

def normalize_text(text):
    if not text:
        return ""
    text = str(text)
    result = []
    for c in text:
        cp = ord(c)
        if 0xFF21 <= cp <= 0xFF3A or 0xFF41 <= cp <= 0xFF5A or 0xFF10 <= cp <= 0xFF19:
            result.append(chr(cp - 0xFEE0))
        elif cp == 0x3000:
            result.append(" ")
        else:
            result.append(c)
    text = "".join(result)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("\uff65", "\u30FB")
    text = re.sub(r"[\u2010-\u2015\uFF0D]", "-", text)
    return text


def normalize_material_name(name):
    if not name:
        return ""
    name = normalize_text(name)
    name = name.lower()
    name = name.replace("（", "(").replace("）", ")")
    name = name.replace("ｍｍ", "mm").replace("ｍ", "m")
    name = name.replace("×", "x").replace("＊", "*")
    return name.strip()


def normalize_construction_method(method):
    if not method:
        return ""
    method = normalize_text(method)
    method_list = [
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
    ]
    for old, new in method_list:
        if old in method:
            method = method.replace(old, new)
            break
    method = method.replace("打込みみ", "打込み")
    return method.strip()


def normalize_spec(spec):
    if not spec:
        return ""
    spec = normalize_material_name(spec)
    spec = re.sub(r"\s*-\s*", "-", spec)
    spec = spec.replace("幅", "").replace("地中", "").strip()
    spec = re.sub(r"([a-z])[\s\-]*(\d)", r"\1\2", spec)
    return spec.strip()


def build_match_key(name, spec, method, unit):
    parts = []
    if name:
        parts.append(normalize_material_name(name))
    if spec:
        parts.append(normalize_spec(spec))
    if method:
        parts.append(normalize_construction_method(method))
    if unit:
        parts.append(normalize_text(unit))
    return "|".join(parts)


# ============================================================
# あいまい一致
# ============================================================

NAME_ALIASES = {
    "ケーブルラック": "直線形ラック",
}


def token_sort_ratio(s1, s2):
    if not s1 and not s2:
        return 1.0
    if not s1 or not s2:
        return 0.0
    s1, s2 = str(s1).lower(), str(s2).lower()
    if s1 == s2:
        return 1.0
    shorter = s1 if len(s1) <= len(s2) else s2
    longer = s1 if len(s1) > len(s2) else s2
    if shorter in longer:
        return max(len(shorter) / len(longer), 0.80)
    return SequenceMatcher(
        None, " ".join(sorted(s1.split())), " ".join(sorted(s2.split()))
    ).ratio()


def spec_contains(s1, s2):
    if not s1 or not s2:
        return False
    return s1 in s2 or s2 in s1


def name_flex_match(norm_name, master_norm_name):
    if norm_name == master_norm_name:
        return True
    no_bracket = re.sub(r"\(.*?\)", "", master_norm_name).strip()
    if norm_name == no_bracket:
        return True
    if norm_name in master_norm_name or master_norm_name in norm_name:
        return True
    return False


# ============================================================
# マスタ読込・インデックス構築
# ============================================================

def load_master_data():
    conn = get_db()
    rows = conn.execute(
        """SELECT id, source_page, field_category, material_name, spec_summary,
           construction_method, unit, composite_unit_price, removal_productivity,
           removal_cost, material_unit_price, material_cost, labor_cost, expense_cost,
           source_text
        FROM estimate_master ORDER BY id"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def build_indexes(master_data):
    match_key_idx = {}
    name_method_idx = {}
    name_idx = {}
    name_no_bracket_idx = {}

    for i, rec in enumerate(master_data):
        mk = build_match_key(
            rec["material_name"], rec["spec_summary"],
            rec["construction_method"], rec["unit"]
        )
        if mk:
            match_key_idx.setdefault(mk, []).append(i)

        nn = normalize_material_name(rec["material_name"])
        nm = normalize_construction_method(rec["construction_method"])
        if nn:
            name_idx.setdefault(nn, []).append(i)
            nm_key = f"{nn}||{nm}"
            name_method_idx.setdefault(nm_key, []).append(i)
            nn_nb = re.sub(r"\(.*?\)", "", nn).strip()
            if nn_nb != nn:
                name_no_bracket_idx.setdefault(nn_nb, []).append(i)

    # エイリアス逆引き
    for alias_from, alias_to_raw in NAME_ALIASES.items():
        alias_to = normalize_material_name(alias_to_raw)
        alias_from_norm = normalize_material_name(alias_from)
        if alias_to in name_idx:
            name_idx.setdefault(alias_from_norm, []).extend(name_idx[alias_to])

    return {
        "match_key": match_key_idx,
        "name_method": name_method_idx,
        "name": name_idx,
        "name_no_bracket": name_no_bracket_idx,
    }


# ============================================================
# 照合エンジン本体
# ============================================================

def match_single_material(mat, master_data, indexes, learning_dict=None,
                          auto_adopt=0.75, fuzzy_min=0.50, max_candidates=5):
    """1材料に対して多段階照合を実行し候補リストを返す"""
    mat_name = mat.get("material_name", "") or ""
    mat_spec = mat.get("spec", "") or ""
    mat_method = mat.get("construction_method", "") or ""
    mat_unit = mat.get("unit", "") or ""

    norm_name = normalize_material_name(mat_name)
    norm_spec = normalize_spec(mat_spec)
    norm_method = normalize_construction_method(mat_method)
    query_key = build_match_key(mat_name, mat_spec, mat_method, mat_unit)

    # 学習辞書チェック
    effective_name = norm_name
    if learning_dict and norm_name in learning_dict:
        effective_name = learning_dict[norm_name]

    # エイリアス展開
    alias_name = NAME_ALIASES.get(norm_name) or NAME_ALIASES.get(effective_name)
    alias_query_key = None
    if alias_name:
        alias_query_key = build_match_key(alias_name, mat_spec, mat_method, mat_unit)

    # 学習辞書エイリアスの照合キー
    learned_query_key = None
    if effective_name != norm_name:
        learned_query_key = build_match_key(effective_name, mat_spec, mat_method, mat_unit)

    candidates = []

    # --- Stage 1: 完全一致 ---
    for qk in [query_key, alias_query_key, learned_query_key]:
        if qk and qk in indexes["match_key"]:
            for idx in indexes["match_key"][qk]:
                r = master_data[idx]
                candidates.append(_make_candidate(r, "exact", 1.0, "完全一致: 照合キー一致"))

    # --- Stage 2: 正規化名称+施工条件 ---
    if not candidates:
        search_names = [norm_name]
        if alias_name:
            search_names.append(alias_name)
        if effective_name != norm_name:
            search_names.append(effective_name)

        for nn in search_names:
            if not nn:
                continue
            nm_key = f"{nn}||{norm_method}"
            if nm_key in indexes["name_method"]:
                for idx in indexes["name_method"][nm_key]:
                    r = master_data[idx]
                    m_spec = normalize_spec(r["spec_summary"] or "")
                    spec_score = token_sort_ratio(norm_spec, m_spec) if norm_spec else 0.5
                    if spec_contains(norm_spec, m_spec):
                        spec_score = max(spec_score, 0.90)
                    conf = 0.95 * max(spec_score, 0.5)
                    candidates.append(_make_candidate(
                        r, "normalized", conf,
                        f"正規化一致: 名称+施工一致, 摘要={spec_score*100:.0f}%"
                    ))

    # --- Stage 2.5: 名称一致 ---
    if not candidates:
        all_hits = set()
        search_names2 = [norm_name]
        if alias_name:
            search_names2.append(alias_name)
        if effective_name != norm_name:
            search_names2.append(effective_name)

        for nn in search_names2:
            if not nn:
                continue
            all_hits.update(indexes["name"].get(nn, []))
            all_hits.update(indexes["name_no_bracket"].get(nn, []))

        for idx in all_hits:
            r = master_data[idx]
            master_nn = normalize_material_name(r["material_name"])

            matched = name_flex_match(norm_name, master_nn)
            if not matched and alias_name:
                matched = name_flex_match(alias_name, master_nn)
            if not matched and effective_name != norm_name:
                matched = name_flex_match(effective_name, master_nn)
            if not matched:
                continue

            m_spec = normalize_spec(r["spec_summary"] or "")
            m_method = normalize_construction_method(r["construction_method"] or "")

            spec_score = token_sort_ratio(norm_spec, m_spec) if norm_spec else 0.5
            if spec_contains(norm_spec, m_spec):
                spec_score = max(spec_score, 0.90)

            if not m_method:
                conf = 0.90 * spec_score
            elif not norm_method:
                conf = 0.90 * (spec_score * 0.7 + 0.5 * 0.3)
            else:
                method_score = token_sort_ratio(norm_method, m_method)
                conf = 0.90 * (spec_score * 0.7 + method_score * 0.3)

            candidates.append(_make_candidate(
                r, "name_match", conf,
                f"名称一致: 摘要={spec_score*100:.0f}%"
            ))

    # --- Stage 4: あいまい一致 ---
    if not candidates:
        name_matches = []
        for i, rec in enumerate(master_data):
            master_nn = normalize_material_name(rec["material_name"] or "")
            if not master_nn:
                continue
            score = token_sort_ratio(norm_name, master_nn)
            if score >= 0.40:
                name_matches.append((i, score))
        name_matches.sort(key=lambda x: -x[1])

        for idx, name_score in name_matches[:30]:
            rec = master_data[idx]
            m_method = normalize_construction_method(rec["construction_method"] or "")
            m_spec = normalize_spec(rec["spec_summary"] or "")
            method_score = token_sort_ratio(norm_method, m_method) if norm_method else 0.5
            if not m_method:
                method_score = 0.5
            spec_score = token_sort_ratio(norm_spec, m_spec) if norm_spec else 0.5
            composite = name_score * 0.40 + method_score * 0.35 + spec_score * 0.25
            if composite >= 0.45:
                conf = min(composite, 0.80)
                candidates.append(_make_candidate(
                    rec, "fuzzy", conf,
                    f"あいまい: 名称={name_score*100:.0f}%, 施工={method_score*100:.0f}%, 摘要={spec_score*100:.0f}%"
                ))

    candidates.sort(key=lambda c: -c["confidence"])
    candidates = candidates[:max_candidates]

    for i, c in enumerate(candidates):
        c["candidate_rank"] = i + 1
        c["is_adopted"] = (i == 0 and c["confidence"] >= auto_adopt)

    if not candidates:
        candidates.append({
            "master_id": None, "master_name": "", "master_spec": "",
            "master_method": "", "master_unit": "",
            "composite_unit_price": 0, "removal_productivity": 0,
            "source_page": "", "field_category": "",
            "match_type": "unmatched", "confidence": 0,
            "reason": "照合候補なし", "candidate_rank": 1, "is_adopted": False
        })

    return candidates


def _make_candidate(rec, match_type, confidence, reason):
    return {
        "master_id": rec["id"],
        "master_name": rec["material_name"] or "",
        "master_spec": rec["spec_summary"] or "",
        "master_method": rec["construction_method"] or "",
        "master_unit": rec["unit"] or "",
        "composite_unit_price": float(rec["composite_unit_price"] or 0),
        "removal_productivity": float(rec["removal_productivity"] or 0),
        "source_page": rec["source_page"] or "",
        "field_category": rec["field_category"] or "",
        "match_type": match_type,
        "confidence": min(confidence, 1.0),
        "reason": reason,
    }


# ============================================================
# 案件照合実行
# ============================================================

def run_project_matching(project_id, user_id):
    """案件全体の照合を実行"""
    conn = get_db()

    # 設定取得
    settings = {}
    for row in conn.execute("SELECT setting_key, setting_value FROM estimate_settings").fetchall():
        settings[row["setting_key"]] = row["setting_value"]
    auto_adopt = float(settings.get("auto_adopt_threshold", 0.75))
    fuzzy_min = float(settings.get("fuzzy_threshold", 0.50))
    max_cand = int(settings.get("max_candidates", 5))

    # 学習辞書読込
    learning_dict = {}
    for row in conn.execute(
        "SELECT input_name, canonical_name FROM learning_dictionary WHERE status='confirmed'"
    ).fetchall():
        learning_dict[normalize_material_name(row["input_name"])] = normalize_material_name(row["canonical_name"])

    # マスタ読込
    master_data = load_master_data()
    if not master_data:
        conn.close()
        return {"error": "積算マスタが空です"}

    indexes = build_indexes(master_data)

    # 材料リスト読込
    materials = conn.execute(
        "SELECT * FROM material_list WHERE project_id=? ORDER BY row_no", (project_id,)
    ).fetchall()
    if not materials:
        conn.close()
        return {"error": "材料リストが空です"}

    # 既存結果をクリア
    conn.execute("DELETE FROM match_results WHERE project_id=?", (project_id,))
    conn.execute("DELETE FROM estimate_details WHERE project_id=? AND is_manual_added=0", (project_id,))

    # 照合実行
    total_adopted = 0
    for mat_row in materials:
        mat = dict(mat_row)
        candidates = match_single_material(
            mat, master_data, indexes, learning_dict,
            auto_adopt, fuzzy_min, max_cand
        )

        for c in candidates:
            conn.execute("""INSERT INTO match_results
                (project_id, material_id, candidate_rank, master_id, match_type,
                 confidence, reason, is_adopted, master_name, master_spec,
                 master_method, master_unit, composite_unit_price,
                 removal_productivity, source_page, field_category)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (project_id, mat["id"], c["candidate_rank"], c["master_id"],
                 c["match_type"], c["confidence"], c["reason"],
                 1 if c["is_adopted"] else 0,
                 c["master_name"], c["master_spec"], c["master_method"],
                 c["master_unit"], c["composite_unit_price"],
                 c["removal_productivity"], c["source_page"], c["field_category"])
            )

        # 見積明細に採用分を書込み
        adopted = next((c for c in candidates if c["is_adopted"]), None)
        if adopted:
            total_adopted += 1
            qty = float(mat.get("quantity", 0) or 0)
            price = adopted["composite_unit_price"]
            prod = adopted["removal_productivity"]
            conn.execute("""INSERT INTO estimate_details
                (project_id, row_no, field_category, material_name, spec,
                 construction_method, unit, quantity, composite_unit_price,
                 amount, productivity, productivity_total, source_pdf,
                 source_page, match_type, confidence, match_reason,
                 remarks, material_id, master_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (project_id, mat["row_no"],
                 adopted["field_category"] or mat.get("field_category", ""),
                 mat["material_name"], mat["spec"],
                 mat["construction_method"], mat["unit"] or adopted["master_unit"],
                 qty, price, price * qty, prod, prod * qty,
                 "", adopted["source_page"],
                 adopted["match_type"], adopted["confidence"],
                 adopted["reason"], mat.get("remarks", ""),
                 mat["id"], adopted["master_id"])
            )
        else:
            qty = float(mat.get("quantity", 0) or 0)
            conn.execute("""INSERT INTO estimate_details
                (project_id, row_no, field_category, material_name, spec,
                 construction_method, unit, quantity, composite_unit_price,
                 amount, productivity, productivity_total, source_pdf,
                 source_page, match_type, confidence, match_reason,
                 remarks, material_id, master_id)
                VALUES (?,?,?,?,?,?,?,?,0,0,0,0,'','','unmatched',0,?,?,?,NULL)""",
                (project_id, mat["row_no"], mat.get("field_category", ""),
                 mat["material_name"], mat["spec"],
                 mat["construction_method"], mat["unit"],
                 qty,
                 candidates[0]["reason"] if candidates else "照合候補なし",
                 mat.get("remarks", ""), mat["id"])
            )

    conn.commit()
    conn.close()

    return {
        "total_materials": len(materials),
        "total_adopted": total_adopted,
        "total_unmatched": len(materials) - total_adopted,
    }
