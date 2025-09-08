# excel_merger.py
import streamlit as st
import pandas as pd
import numpy as np
from functools import reduce
from io import BytesIO
import re
from datetime import datetime
import os
from pathlib import Path
import sqlite3
import json

# Попытка импортировать AgGrid; graceful fallback к st.dataframe если недоступен
try:
    from st_aggrid import AgGrid, GridOptionsBuilder
    AGGRID_AVAILABLE = True
except Exception:
    AGGRID_AVAILABLE = False

# ---------------------- Конфигурация путей ----------------------
try:
    BASE_DIR = Path(__file__).parent.resolve()
except NameError:
    BASE_DIR = Path.cwd()

DB_PATH = BASE_DIR / "merged_history.db"
MERGED_DIR = BASE_DIR / "merged_files"
MERGED_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------- Переводы ----------------------
translations = {
    "en": {
        "title": "📊 Merge Multiple Excel Files by ID",
        "upload": "📂 Upload Excel files (.xlsx)",
        "id_select": "🔑 Select ID column and fields for each file",
        "id_help": "Choose the column to match records (ID / code / number / user, etc.)",
        "include_cols": "✅ Columns to include",
        "filters": "🔎 Filters before merge",
        "prefix": "Add filename prefix to all columns (except ID) to avoid conflicts",
        "merge_button": "🚀 Merge",
        "preview": "👀 Preview (unmatched rows at the bottom and in red)",
        "download_clean": "⬇️ Download {name}.xlsx (clean)",
        "download_colored": "⬇️ Download {name}_colored.xlsx (with highlights)",
        "download_filtered_each": "⬇️ Download per-file filtered data",
        "info_upload": "Upload at least **2 Excel files** (.xlsx) to start.",
        "info_wait": "Select ID, choose columns & filters, then click «Merge».",
        "warn_duplicates": "In file **{name}** found duplicates by selected ID: {count}. Keeping only the first.",
        "error_read": "⚠️ Could not read file {name}: {error}",
        "footer": "© All rights reserved. Made by Mashrabjon",
        "error_no_id": "File {name} has no 'id' column after normalization.",
        "error_merge": "Error while merging: {error}",
        "error_styled": "Could not create colored Excel: {error}\nTry updating pandas/openpyxl.",
        "expander": "ℹ️ Details",
        "expander_text": "- **ID** is converted to string and trimmed.\n"
                         "- Merge is **outer/inner/left/right** as chosen.\n"
                         "- `__unmatched = True` means the ID is missing in at least one file.\n"
                         "- In the clean file, helper columns are hidden; unmatched IDs go to the bottom.",
        "join_type": "🔗 Join type",
        "nan_fill": "Replace NaN with:",
        "history": "🗂️ Merged files history (persistent)",
        "metrics": "📈 Merge summary",
        "m_total": "Total rows",
        "m_matched": "Fully matched",
        "m_unmatched": "Unmatched",
        "m_files_presence": "Presence by number of files",
        "theme": "🎨 Theme",
        "light": "🌞 Light",
        "dark": "🌙 Dark",
        "clear_history": "🗑️ Clear merged history",
        "save_merged": "💾 Save merged to server (add to history)",
        "auto_save": "🔁 Auto-save merged to server"
    },
    "ru": {
        "title": "📊 Объединение нескольких Excel по ID",
        "upload": "📂 Загрузите Excel файлы (.xlsx)",
        "id_select": "🔑 Выбор ID-колонки и полей для каждого файла",
        "id_help": "Выберите колонку, по которой сопоставляем записи (ID/код/номер/пользователь и т.п.)",
        "include_cols": "✅ Колонки для включения",
        "filters": "🔎 Фильтры до объединения",
        "prefix": "Добавлять префикс с именем файла ко всем колонкам (кроме ID), чтобы избежать совпадений",
        "merge_button": "🚀 Объединить",
        "preview": "👀 Предпросмотр (несопоставленные строки внизу и красным)",
        "download_clean": "⬇️ Скачать {name}.xlsx (чистый)",
        "download_colored": "⬇️ Скачать {name}_colored.xlsx (с подсветкой)",
        "download_filtered_each": "⬇️ Скачать отфильтрованные данные по каждому файлу",
        "info_upload": "Загрузите как минимум **2 Excel-файла** (.xlsx), чтобы начать.",
        "info_wait": "Выберите ID, колонки и фильтры, затем нажмите «Объединить».",
        "warn_duplicates": "В файле **{name}** найдено дубликатов по выбранному ID: {count}. Оставляю первую запись.",
        "error_read": "⚠️ Не удалось прочитать файл {name}: {error}",
        "footer": "© Все права защищены. Сделано Машрабжон",
        "error_no_id": "В файле {name} нет колонки 'id' после нормализации.",
        "error_merge": "Ошибка при объединении: {error}",
        "error_styled": "Не удалось создать цветной Excel: {error}\nПопробуйте обновить pandas/openpyxl.",
        "expander": "ℹ️ Пояснения",
        "expander_text": "- **ID** приводится к строке и очищается от пробелов.\n"
                         "- Объединение — **outer/inner/left/right** по выбору.\n"
                         "- `__unmatched = True` означает отсутствие ID хотя бы в одном файле.\n"
                         "- В «чистом» файле служебные колонки скрыты; несопоставленные — внизу.",
        "join_type": "🔗 Тип объединения",
        "nan_fill": "Заменять NaN на:",
        "history": "🗂️ История объединённых файлов (постоянно)",
        "metrics": "📈 Сводка объединения",
        "m_total": "Всего строк",
        "m_matched": "Полностью совпавшие",
        "m_unmatched": "Несопоставленные",
        "m_files_presence": "Присутствие по числу файлов",
        "theme": "🎨 Тема",
        "light": "🌞 Светлая",
        "dark": "🌙 Тёмная",
        "clear_history": "🗑️ Очистить историю объединений",
        "save_merged": "💾 Сохранить объединённый на сервер (добавить в историю)",
        "auto_save": "🔁 Автосохранить объединённый на сервер"
    },
    "uz": {
        "title": "📊 Bir nechta Excel fayllarini ID bo‘yicha birlashtirish",
        "upload": "📂 Excel fayllarini yuklang (.xlsx)",
        "id_select": "🔑 Har bir fayl uchun ID ustuni va maydonlarni tanlang",
        "id_help": "Moslashtirish uchun ustunni tanlang (ID / kod / raqam / foydalanuvchi va h.k.)",
        "include_cols": "✅ Kiritiladigan ustunlar",
        "filters": "🔎 Birlashtirishdan oldingi filtrlar",
        "prefix": "ID dan tashqari barcha ustunlarga fayl nomi prefiksi qo‘shilsin",
        "merge_button": "🚀 Birlashtirish",
        "preview": "👀 Oldindan ko‘rish (mos kelmagan satrlar pastda va qizil)",
        "download_clean": "⬇️ {name}.xlsx (toza) yuklab olish",
        "download_colored": "⬇️ {name}_colored.xlsx (rangli) yuklab olish",
        "download_filtered_each": "⬇️ Har fayl uchun filtrlangan ma’lumotni yuklab olish",
        "info_upload": "Boshlash uchun kamida **2 ta Excel fayli** (.xlsx) yuklang.",
        "info_wait": "ID, ustunlar va filtrlarni tanlang, so‘ng «Birlashtirish».",
        "warn_duplicates": "**{name}** faylida {count} ta dublikat ID topildi. Faqat birinchi qoldi.",
        "error_read": "⚠️ {name} faylini o‘qib bo‘lmadi: {error}",
        "footer": "© Barcha huquqlar himoyalangan. Mashrabjon tomonidan yaratilgan",
        "error_no_id": "{name} faylida 'id' ustuni yo‘q.",
        "error_merge": "Birlashtirishda xato: {error}",
        "error_styled": "Rangli Excel yaratib bo‘lmadi: {error}\nPandas/openpyxl ni yangilang.",
        "expander": "ℹ️ Izohlar",
        "expander_text": "- **ID** matn ko‘rinishiga o‘tkaziladi va tozalanadi.\n"
                         "- Birlashtirish — tanlangan **outer/inner/left/right**.\n"
                         "- `__unmatched = True` — ID kamida bitta faylda yo‘q.\n"
                         "- «Toza» faylda xizmat ustunlari yashiriladi; nomutanosiblar pastda.",
        "join_type": "🔗 Birlashtirish turi",
        "nan_fill": "NaN o‘rniga:",
        "history": "🗂️ Birlashtirilgan fayllar tarixi (doimiy)",
        "metrics": "📈 Birlashtirish xulosasi",
        "m_total": "Jami qatorlar",
        "m_matched": "To‘liq mos kelgan",
        "m_unmatched": "Mos kelmagan",
        "m_files_presence": "Fayllar bo‘yicha mavjudlik",
        "theme": "🎨 Mavzu",
        "light": "🌞 Yorug‘",
        "dark": "🌙 Qorong‘i",
        "clear_history": "🗑️ Tarixni o‘chirish",
        "save_merged": "💾 Birlashtirilganni serverga saqlash (tarixga qo'shish)",
        "auto_save": "🔁 Avto-saqlash"
    },
    "ko": {
        "title": "📊 여러 Excel 파일을 ID로 병합",
        "upload": "📂 Excel 파일 업로드 (.xlsx)",
        "id_select": "🔑 각 파일의 ID 열 및 필드 선택",
        "id_help": "레코드를 일치시킬 열을 선택하세요 (ID / 코드 / 번호 / 사용자 등)",
        "include_cols": "✅ 포함할 열",
        "filters": "🔎 병합 전 필터",
        "prefix": "ID 제외 모든 열에 파일명 접두사 추가",
        "merge_button": "🚀 병합",
        "preview": "👀 미리보기 (불일치 행은 아래쪽 빨간색)",
        "download_clean": "⬇️ {name}.xlsx (클린)",
        "download_colored": "⬇️ {name}_colored.xlsx (강조)",
        "download_filtered_each": "⬇️ 파일별 필터링 데이터 다운로드",
        "info_upload": "시작하려면 최소 **2개**의 .xlsx 파일을 업로드하세요.",
        "info_wait": "ID, 열, 필터를 선택한 후 «병합».",
        "warn_duplicates": "**{name}** 파일에 선택한 ID 기준 중복 {count}개 발견. 첫 번째만 유지.",
        "error_read": "⚠️ {name} 파일을 읽을 수 없습니다: {error}",
        "footer": "© 모든 권리 보유. Mashrabjon 제작",
        "error_no_id": "{name} 파일에 'id' 열이 없습니다.",
        "error_merge": "병합 중 오류: {error}",
        "error_styled": "색상 Excel 생성 실패: {error}\nPandas/openpyxl 업데이트 권장.",
        "expander": "ℹ️ 설명",
        "expander_text": "- **ID**는 문자열로 변환 후 공백 제거.\n"
                         "- 병합은 선택한 **outer/inner/left/right**.\n"
                         "- `__unmatched = True`는 적어도 하나의 파일에 없음.\n"
                         "- 클린 파일은 보조 열 숨김; 불일치는 아래.",
        "join_type": "🔗 조인 방식",
        "nan_fill": "NaN 대체값:",
        "history": "🗂️ 병합된 파일 기록 (영구)",
        "metrics": "📈 병합 요약",
        "m_total": "총 행수",
        "m_matched": "완전 일치",
        "m_unmatched": "불일치",
        "m_files_presence": "파일 수별 존재여부",
        "theme": "🎨 테마",
        "light": "🌞 라이트",
        "dark": "🌙 다크",
        "clear_history": "🗑️ 병합 기록 삭제",
        "save_merged": "💾 병합 파일을 서버에 저장 (기록 추가)",
        "auto_save": "🔁 자동 저장"
    }
}

# ---------------------- DB (SQLite) helpers ----------------------
def init_db():
    """Создаёт таблицу merged_files если её нет."""
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS merged_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            basename TEXT NOT NULL,
            clean_path TEXT NOT NULL,
            colored_path TEXT,
            rows INTEGER,
            cols INTEGER,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def add_record_db(basename: str, clean_path: str, colored_path: str, rows: int, cols: int):
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO merged_files (basename, clean_path, colored_path, rows, cols, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (basename, clean_path, colored_path, int(rows), int(cols), datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit()
    last_id = cur.lastrowid
    conn.close()
    return last_id

def get_all_records_db():
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    cur.execute("SELECT id, basename, clean_path, colored_path, rows, cols, created_at FROM merged_files ORDER BY id DESC")
    rows = cur.fetchall()
    conn.close()
    records = []
    for r in rows:
        records.append({
            "id": int(r[0]),
            "basename": r[1],
            "clean_path": r[2],
            "colored_path": r[3],
            "rows": int(r[4]) if r[4] is not None else None,
            "cols": int(r[5]) if r[5] is not None else None,
            "created_at": r[6]
        })
    return records

def delete_record_db(record_id: int, delete_files: bool = True):
    """Удаляет запись из БД; по опции удаляет и физические файлы."""
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    cur.execute("SELECT clean_path, colored_path FROM merged_files WHERE id=?", (record_id,))
    row = cur.fetchone()
    if row:
        clean_p, colored_p = row[0], row[1]
    else:
        clean_p, colored_p = None, None
    # удаляем запись
    cur.execute("DELETE FROM merged_files WHERE id=?", (record_id,))
    conn.commit()
    conn.close()
    # удаляем файлы если нужно
    if delete_files:
        for p in (clean_p, colored_p):
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

def clear_history_db(delete_files: bool = False):
    """Очистка таблицы; опционально удаление файлов с диска."""
    if delete_files:
        recs = get_all_records_db()
        for r in recs:
            for p in (r.get("clean_path"), r.get("colored_path")):
                try:
                    if p and os.path.exists(p):
                        os.remove(p)
                except Exception:
                    pass
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    cur.execute("DELETE FROM merged_files")
    conn.commit()
    conn.close()

# Инициализируем БД
init_db()

# ---------------------- Helpers (columns/types/filters/etc) ----------------------
def normalize_colname(s: str) -> str:
    s = (s or "").strip().lower()
    return re.sub(r"[^\w]+", "", s, flags=re.UNICODE)

def guess_id_column(df: pd.DataFrame) -> str:
    priors_exact = {"id", "ид"}
    priors_common = {
        "userid", "user_id", "пользователь", "пользовател", "номер", "код",
        "clientid", "customerid", "контрагент", "табельный", "employeeid"
    }
    cols = list(df.columns)
    if not cols:
        return "id"
    norm_map = {c: normalize_colname(c) for c in cols}
    scores = {c: 0.0 for c in cols}
    for c, n in norm_map.items():
        if n in priors_exact:
            scores[c] += 5
        if n.endswith("id") or n == "id":
            scores[c] += 3
        if n in priors_common:
            scores[c] += 2
    for c in cols:
        s = df[c]
        non_null = s.notna().sum()
        if non_null == 0:
            continue
        uniq = s.dropna().astype(str).str.strip().nunique()
        uniq_ratio = uniq / max(1, non_null)
        non_null_ratio = non_null / max(1, len(df))
        scores[c] += uniq_ratio * 2 + non_null_ratio * 1.5
    best = max(scores, key=scores.get)
    return best

def to_str_id(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()

def style_unmatched(df: pd.DataFrame):
    def row_style(row):
        return ['background-color: #ffe5e5'] * len(row) if row.get('__unmatched', False) else [''] * len(row)
    return df.style.apply(row_style, axis=1)

def infer_dtype(s: pd.Series) -> str:
    if pd.api.types.is_datetime64_any_dtype(s):
        return "datetime"
    if pd.api.types.is_numeric_dtype(s):
        return "number"
    if pd.api.types.is_bool_dtype(s):
        return "bool"
    nun = s.dropna().nunique()
    if nun > 0 and nun <= 50:
        return "category"
    return "text"

def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    res = df.copy()
    for col, f in (filters or {}).items():
        if col not in res.columns:
            continue
        t = f.get("type")
        if t == "number":
            vmin, vmax = f.get("range", (None, None))
            if vmin is not None:
                res = res[res[col] >= vmin]
            if vmax is not None:
                res = res[res[col] <= vmax]
        elif t == "datetime":
            start, end = f.get("range", (None, None))
            s = pd.to_datetime(res[col], errors="coerce")
            if start is not None:
                res = res[s >= pd.to_datetime(start)]
            if end is not None:
                res = res[s <= pd.to_datetime(end)]
        elif t == "category":
            vals = f.get("values")
            if vals:
                res = res[res[col].isin(vals)]
        elif t == "bool":
            val = f.get("value", None)
            if val is not None:
                res = res[res[col] == val]
        elif t == "text":
            substr = f.get("contains", "").strip()
            if substr:
                res = res[res[col].astype(str).str.contains(substr, case=False, na=False)]
    return res

# ---------- Utilities for saving files ----------
def unique_path_for(path: Path, allow_overwrite: bool = False) -> Path:
    if allow_overwrite or not path.exists():
        return path
    base = path.stem
    suffix = path.suffix
    for i in range(1, 1000):
        candidate = path.with_name(f"{base}_{i}{suffix}")
        if not candidate.exists():
            return candidate
    return path

def save_merged_files_to_disk(basename: str, clean_df: pd.DataFrame, styled_obj, merged_df: pd.DataFrame,
                              allow_overwrite: bool = False):
    base = re.sub(r"[\\/*?:\"<>|]+", "_", basename).strip()
    if not base:
        base = f"merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    clean_path = MERGED_DIR / f"{base}.xlsx"
    colored_path = MERGED_DIR / f"{base}_colored.xlsx"
    clean_path = unique_path_for(clean_path, allow_overwrite=allow_overwrite)
    colored_path = unique_path_for(colored_path, allow_overwrite=allow_overwrite)

    # save clean
    try:
        clean_df.to_excel(clean_path, index=False, engine="openpyxl")
    except Exception as e:
        raise RuntimeError(f"Could not save clean Excel: {e}")

    # try styled -> colored, fallback to merged_df
    try:
        if styled_obj is not None:
            styled_obj.to_excel(colored_path, engine="openpyxl", index=False)
        else:
            merged_df.to_excel(colored_path, index=False, engine="openpyxl")
    except Exception:
        try:
            merged_df.to_excel(colored_path, index=False, engine="openpyxl")
        except Exception as e:
            raise RuntimeError(f"Could not save colored Excel: {e}")

    return {
        "basename": base,
        "clean_path": str(clean_path),
        "colored_path": str(colored_path),
        "rows": int(len(clean_df)),
        "cols": int(len(clean_df.columns)),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M")
    }

# ---------------------- UI ----------------------
st.set_page_config(page_title="Excel Merger", layout="wide")

# CSS
st.markdown("""
<style>
.stApp { background: #f9f9fb; }
h1 { text-align:center; color:white !important;
    background: linear-gradient(90deg,#4CAF50,#2E7D32);
    padding:12px;border-radius:10px;font-size:26px;}
[data-testid="stFileUploader"] { border: 2px dashed #4CAF50; padding: 15px; border-radius:10px; background:#f9fff9; }
.app-content-padding-bottom { padding-bottom: 90px; }
</style>
""", unsafe_allow_html=True)

# Language / theme
lang = st.sidebar.selectbox("🌐 Language / Язык / Til / 언어", options=["en","ru","uz","ko"],
                            index=1,
                            format_func=lambda x: {"en":"English","ru":"Русский","uz":"O‘zbek","ko":"한국어"}[x])
t = translations[lang]
st.title(t["title"])

theme = st.sidebar.radio(t["theme"], [t["light"], t["dark"]], index=0)
if theme == t["dark"]:
    st.markdown("<style>.stApp{background:#121212;color:white;}.stDataFrame{color:white;}</style>", unsafe_allow_html=True)

# fixed footer overlay so it's visible even if st.stop is called earlier
def render_fixed_footer():
    color = "#666" if theme != t["dark"] else "#bbb"
    st.markdown(f"""
    <div style="position:fixed; left:0; bottom:0; width:100%; text-align:center; padding:6px 0; color:{color};
                background: rgba(0,0,0,0); font-size:13px; z-index:9999;">
      {t['footer']}
    </div>
    """, unsafe_allow_html=True)
    st.markdown("<div class='app-content-padding-bottom'></div>", unsafe_allow_html=True)

render_fixed_footer()

# File uploader
uploaded = st.file_uploader(t["upload"], type=["xlsx"], accept_multiple_files=True)

# Sidebar: history from DB
with st.sidebar.expander(t["history"], expanded=True):
    recs = get_all_records_db()
    if recs:
        for rec in recs:
            st.markdown(f"**{rec['basename']}** — {rec['rows']}×{rec['cols']} ({rec['created_at']})")
            # download buttons
            cp = Path(rec["clean_path"])
            colp = Path(rec["colored_path"]) if rec.get("colored_path") else None
            if cp.exists():
                try:
                    with cp.open("rb") as fh:
                        b = fh.read()
                    st.download_button(f"⬇️ {cp.name}", data=b, file_name=cp.name,
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                       key=f"hist_dl_clean_{rec['id']}")
                except Exception as e:
                    st.warning(f"Could not prepare download for {cp.name}: {e}")
            else:
                st.caption("Clean file missing.")
            if colp and Path(colp).exists():
                try:
                    with Path(colp).open("rb") as fh:
                        b2 = fh.read()
                    st.download_button(f"⬇️ {Path(colp).name}", data=b2, file_name=Path(colp).name,
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                       key=f"hist_dl_col_{rec['id']}")
                except Exception as e:
                    st.warning(f"Could not prepare download for {colp.name}: {e}")
            else:
                st.caption("Colored file missing.")
            # delete record
            if st.button(f"🗑️ Delete {rec['basename']}", key=f"del_db_{rec['id']}"):
                delete_record_db(rec['id'], delete_files=True)
                st.success("Record deleted.")
                st.experimental_rerun()
    else:
        st.caption("-")
    if st.button(t.get("clear_history", "Clear history")):
        clear_history_db(delete_files=False)
        st.success("History cleared.")
        st.experimental_rerun()

if not (uploaded and len(uploaded) >= 2):
    st.info(t["info_upload"])
    st.stop()

# Read uploaded files (do not add to DB)
raw_dfs, file_names = [], []
for f in uploaded:
    try:
        df = pd.read_excel(f)
        raw_dfs.append(df)
        file_names.append(f.name)
    except Exception as e:
        st.error(t["error_read"].format(name=f.name, error=e))
        st.stop()

# join type
join_type = st.sidebar.selectbox(t["join_type"], ["outer","inner","left","right"], index=0)

# Form: basename + ID/columns/filters
st.subheader(t["id_select"])
default_basename = f"final_merged_{datetime.now().strftime('%Y%m%d_%H%M')}"
merge_basename = st.text_input("📁 Final merged filename (basename, no extension)", value=default_basename)
merge_basename = re.sub(r"[\\/*?:\"<>|]+", "_", (merge_basename or default_basename)).strip()
if not merge_basename:
    merge_basename = default_basename

auto_save = st.checkbox(t.get("auto_save", "🔁 Auto-save merged to server"), value=False)
allow_overwrite = st.checkbox("Overwrite existing merged files with same name", value=False)

id_cols = []
include_cols_per_file = []
filters_per_file = []

with st.form("id_select_form"):
    for i, (df, name) in enumerate(zip(raw_dfs, file_names), start=1):
        cols = list(df.columns)
        default_id = guess_id_column(df)
        col_id = st.selectbox(f"File {i}: {name}", options=cols,
                              index=cols.index(default_id) if default_id in cols else 0,
                              help=t["id_help"], key=f"id_{i}")
        id_cols.append(col_id)

        include_cols = st.multiselect(f"{t['include_cols']} — {name}", options=cols, default=cols, key=f"inc_{i}")
        if col_id not in include_cols:
            include_cols = [col_id] + include_cols
        include_cols_per_file.append(include_cols)

        with st.expander(f"{t['filters']}: {name}", expanded=False):
            local_filters = {}
            for c in include_cols:
                s = df[c]
                dtype = infer_dtype(s)
                if dtype == "number":
                    s_num = pd.to_numeric(s, errors="coerce")
                    if s_num.notna().any():
                        try:
                            vmin = float(np.nanmin(s_num))
                            vmax = float(np.nanmax(s_num))
                            rng = st.slider(f"{name} | {c}", min_value=float(vmin), max_value=float(vmax),
                                            value=(float(vmin), float(vmax)), key=f"f_num_{i}_{c}")
                            local_filters[c] = {"type":"number", "range": rng}
                        except Exception:
                            pass
                elif dtype == "datetime":
                    s_dt = pd.to_datetime(s, errors="coerce")
                    if s_dt.notna().any():
                        dmin = s_dt.min().date()
                        dmax = s_dt.max().date()
                        rng = st.date_input(f"{name} | {c}", (dmin,dmax), key=f"f_dt_{i}_{c}")
                        if isinstance(rng, tuple) and len(rng)==2:
                            local_filters[c] = {"type":"datetime","range": rng}
                elif dtype == "bool":
                    val = st.selectbox(f"{name} | {c}", options=["—", True, False], index=0, key=f"f_bool_{i}_{c}")
                    if val != "—":
                        local_filters[c] = {"type":"bool","value": val}
                elif dtype == "category":
                    opts = sorted([("—NaN—" if pd.isna(x) else str(x)) for x in s.unique()])
                    sel = st.multiselect(f"{name} | {c}", options=opts, default=[], key=f"f_cat_{i}_{c}")
                    def back(x): return np.nan if x=="—NaN—" else x
                    if sel:
                        local_filters[c] = {"type":"category","values": list(map(back, sel))}
                else:
                    txt = st.text_input(f"{name} | {c}", value="", key=f"f_txt_{i}_{c}")
                    if txt.strip():
                        local_filters[c] = {"type":"text", "contains": txt.strip()}
            filters_per_file.append(local_filters)

    add_prefix = st.checkbox(t["prefix"], value=False)
    fill_value = st.text_input(t["nan_fill"], value="-", key="nanfill")
    submitted = st.form_submit_button(t["merge_button"])

if not submitted:
    st.info(t["info_wait"])
    st.stop()

# Apply selected columns and filters
prepared_dfs = []
id_sets = []
download_buffers = []

for df, name, idc, include_cols, fdict in zip(raw_dfs, file_names, id_cols, include_cols_per_file, filters_per_file):
    work = df.copy()
    cols_to_keep = [c for c in include_cols if c in work.columns]
    work = work[cols_to_keep].copy()
    work_filtered = apply_filters(work, fdict)

    if idc not in work_filtered.columns:
        st.error(t["error_no_id"].format(name=name))
        st.stop()

    dup_count = work_filtered[idc].duplicated(keep="first").sum()
    if dup_count:
        st.warning(t["warn_duplicates"].format(name=name, count=int(dup_count)))
        work_filtered = work_filtered.drop_duplicates(subset=[idc], keep="first").copy()

    work_filtered["id"] = to_str_id(work_filtered[idc])

    if add_prefix:
        other_cols = [c for c in work_filtered.columns if c not in [idc, "id"]]
        work_filtered = work_filtered.rename(columns={c: f"{name}__{c}" for c in other_cols})

    if idc != "id":
        work_filtered = work_filtered.drop(columns=[idc], errors="ignore")

    work_filtered = work_filtered.fillna(fill_value)

    prepared_dfs.append(work_filtered)
    id_sets.append(set(work_filtered["id"].tolist()))

    buf = BytesIO()
    work_filtered.to_excel(buf, index=False)
    buf.seek(0)
    download_buffers.append((name, buf))

# verify id presence
for i, dfp in enumerate(prepared_dfs, start=1):
    if "id" not in dfp.columns:
        st.error(t["error_no_id"].format(name=file_names[i-1]))
        st.stop()

# Merge
try:
    merged = reduce(lambda l,r: pd.merge(l, r, on="id", how=join_type), prepared_dfs)
except Exception as e:
    st.error(t["error_merge"].format(error=e))
    st.stop()

# presence, unmatched and sorting
presence_cols = []
for idx, (name, ids) in enumerate(zip(file_names, id_sets), start=1):
    colname = f"__present_in_{idx}"
    merged[colname] = merged["id"].isin(ids)
    presence_cols.append(colname)

merged["__present_count"] = merged[presence_cols].sum(axis=1)
merged["__unmatched"] = merged["__present_count"] < len(prepared_dfs)
merged_sorted = merged.sort_values(by=["__unmatched","id"]).reset_index(drop=True)

# Analytics
st.subheader(t["metrics"])
c1,c2,c3 = st.columns(3)
total_rows = len(merged_sorted)
fully_matched = int((merged_sorted["__present_count"] == len(prepared_dfs)).sum())
unmatched = int(merged_sorted["__unmatched"].sum())
c1.metric(t["m_total"], f"{total_rows:,}")
c2.metric(t["m_matched"], f"{fully_matched:,}")
c3.metric(t["m_unmatched"], f"{unmatched:,}")
dist = merged_sorted["__present_count"].value_counts().sort_index()
dist_df = pd.DataFrame({"files_present_in": dist.index.astype(int), "rows": dist.values})
st.caption(t["m_files_presence"])
st.bar_chart(dist_df.set_index("files_present_in"))

# Preview & interactive
st.subheader(t["preview"])
styled = style_unmatched(merged_sorted)
st.dataframe(styled, use_container_width=True)

st.subheader("🔍 Interactive Table (filter, sort, hide columns)")
if AGGRID_AVAILABLE:
    gb = GridOptionsBuilder.from_dataframe(merged_sorted)
    gb.configure_pagination(enabled=True)
    gb.configure_default_column(editable=False, groupable=True, sortable=True, filter=True)
    gb.configure_side_bar()
    grid_options = gb.build()
    AgGrid(merged_sorted, gridOptions=grid_options, fit_columns_on_grid_load=True)
else:
    st.caption("AgGrid not available — showing basic DataFrame")
    st.dataframe(merged_sorted)

tech_cols = ["__unmatched","__present_count"] + presence_cols
clean_df = merged_sorted[[c for c in merged_sorted.columns if c not in tech_cols]].copy()

# Prepare file names/paths and download/save buttons
clean_filename = f"{merge_basename}.xlsx"
colored_filename = f"{merge_basename}_colored.xlsx"
clean_path = MERGED_DIR / clean_filename
colored_path = MERGED_DIR / colored_filename

col1, col2 = st.columns(2)
with col1:
    out1 = BytesIO()
    clean_df.to_excel(out1, index=False, engine="openpyxl")
    out1.seek(0)
    st.download_button(t["download_clean"].format(name=merge_basename), data=out1,
                       file_name=clean_filename,
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key=f"dl_clean_{merge_basename}")

    if st.button(t["save_merged"], key=f"save_{merge_basename}"):
        try:
            saved_meta = save_merged_files_to_disk(merge_basename, clean_df, styled, merged_sorted, allow_overwrite=allow_overwrite)
            # add to DB
            rec_id = add_record_db(saved_meta["basename"], saved_meta["clean_path"], saved_meta["colored_path"],
                                   saved_meta["rows"], saved_meta["cols"])
            st.success(f"Saved merged files as {saved_meta['basename']} and added to history (id={rec_id}).")
            st.experimental_rerun()
        except Exception as e:
            st.error(f"Could not save merged files: {e}")

with col2:
    out2 = BytesIO()
    try:
        if styled is not None:
            styled.to_excel(out2, engine="openpyxl", index=False)
        else:
            merged_sorted.to_excel(out2, index=False, engine="openpyxl")
        out2.seek(0)
        st.download_button(t["download_colored"].format(name=merge_basename), data=out2,
                           file_name=colored_filename,
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key=f"dl_colored_{merge_basename}")
    except Exception as e:
        try:
            out2 = BytesIO()
            merged_sorted.to_excel(out2, index=False, engine="openpyxl")
            out2.seek(0)
            st.download_button(t["download_colored"].format(name=merge_basename), data=out2,
                               file_name=colored_filename,
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               key=f"dl_colored_fallback_{merge_basename}")
            st.warning(t["error_styled"].format(error=e))
        except Exception as e2:
            st.error(t["error_styled"].format(error=e2))

# Auto-save
if auto_save:
    try:
        saved_meta = save_merged_files_to_disk(merge_basename, clean_df, styled, merged_sorted, allow_overwrite=allow_overwrite)
        # add to DB
        rec_id = add_record_db(saved_meta["basename"], saved_meta["clean_path"], saved_meta["colored_path"],
                               saved_meta["rows"], saved_meta["cols"])
        st.success(f"Auto-saved merged files as {saved_meta['basename']} and added to history (id={rec_id}).")
        st.experimental_rerun()
    except Exception as e:
        st.error(f"Auto-save failed: {e}")

# Download per-file filtered sources
with st.expander(t["download_filtered_each"], expanded=False):
    for name, buf in download_buffers:
        st.download_button(f"⬇️ {name}", data=buf, file_name=f"filtered_{name}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key=f"dlf_{name}")

with st.expander(t["expander"], expanded=False):
    st.markdown(t["expander_text"])

# Show DB-history table on main page
recs_main = get_all_records_db()
if recs_main:
    st.subheader(t["history"])
    # limited to last 50 for UI
    hist_slice = recs_main[:50]
    hist_df = pd.DataFrame(hist_slice)
    st.table(hist_df)

# bottom footer
st.markdown(f"<div style='text-align:center; padding:18px; color:#666;'>© {datetime.now().year}. {t['footer']}</div>", unsafe_allow_html=True)
