"""
_download_all_mops.py

下載 MOPS 所有年份上市/上櫃重大訊息，以 step=2 精確確認「符合條款：第 11 款」
（合併、收購、分割、股份轉換等 M&A 類），儲存至 SQLite 資料庫，支援斷點續傳。

流程：
  Step 1  → 抓取整月清單（所有公司），存入 candidates 表
  Step 2  → 逐筆打開完整內文，取得「符合條款」欄位
  確認條款 → 含「11」者寫入 announcements 表（最終結果）

執行方式：
  python _download_all_mops.py                   # 下載全部（2010~今年）
  python _download_all_mops.py --year-from 2020  # 從指定年份開始
  python _download_all_mops.py --year-to 2024    # 到指定年份為止
  python _download_all_mops.py --redo-errors     # 重新跑曾出錯的月份
  python _download_all_mops.py --no-filter       # 跳過 step=2，直接存全部公告

資料庫：mops_all.db（SQLite）
  - candidates     : step=1 的全部公告，含 step=2 確認進度
  - announcements  : 已確認為第 11 款的公告（最終結果）
  - fetch_progress : step=1 的月份完成紀錄（供斷點續傳）
  - fetch_errors   : 錯誤紀錄

注意事項：
  - 每筆公告需打一次 step=2，歷史資料量大，請預留充足執行時間
  - step=2 間隔預設 3 秒，可透過 --sleep-step2 調整
  - 遇到 FOR SECURITY / 連線失敗自動重試，Session 失效自動重建
  - 斷點續傳：中途中斷後重跑會繼續處理未完成的 candidates
"""

import argparse
import calendar
import datetime
import io
import random
import re
import sqlite3
import sys
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

# ── 設定 ─────────────────────────────────────────────────────────────────────

STR_DB_PATH          = 'mops_all.db'
STR_MOPSOV_INDEX_URL = 'https://mopsov.twse.com.tw/mops/web/index'
STR_MOPSOV_FORM_URL  = 'https://mopsov.twse.com.tw/mops/web/t05st01'
STR_MOPSOV_AJAX_URL  = 'https://mopsov.twse.com.tw/mops/web/ajax_t05st01'

STR_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer':    STR_MOPSOV_FORM_URL,
}

DICT_MOPS_CONFIG = {
    '上市': 'sii',
    '上櫃': 'otc',
}

N_YEAR_FROM_DEFAULT = 2010
N_YEAR_TO_DEFAULT   = datetime.datetime.now().year

N_RETRY_MAX         = 3      # 連線失敗最多重試次數
F_SLEEP_STEP1       = 3.0    # step=1 兩次 request 之間的間隔（秒）
F_SLEEP_STEP2       = 3.0    # step=2 兩次 request 之間的間隔（秒，可 --sleep-step2 覆蓋）
F_SLEEP_RETRY       = 10.0   # 重試前等待（秒）
F_SLEEP_SESSION     = 60.0   # Session 重建後的冷卻（秒）

# 符合條款過濾：step=2 回傳 HTML 中「符合條款」含此字串的才保留
STR_TARGET_CLAUSE   = '11'

# 欄位別名（舊版 MOPS 欄位名稱可能不同）
DICT_COL_ALIAS = {
    'date':    ( '發言日期', '公告日期', '日期', 'date' ),
    'co_id':   ( '公司代號', '股票代號', '代號', 'co_id' ),
    'co_name': ( '公司名稱', '股票名稱', '名稱', 'co_name' ),
    'subject': ( '主旨', '公告主旨', '事由', '說明', 'subject' ),
}

# ── stdout UTF-8 修正（Windows cp950 終端）────────────────────────────────────

if hasattr( sys.stdout, 'reconfigure' ):
    try:
        sys.stdout.reconfigure( encoding='utf-8', errors='replace' )
    except Exception:
        pass

# ── 資料庫初始化 ──────────────────────────────────────────────────────────────

def _migrate_db( obj_conn: sqlite3.Connection ):
    """偵測並補齊舊版 schema 缺少的欄位，讓程式不用刪 DB 就能升級。"""
    # fetch_progress：舊版沒有 step1_done / n_candidates / n_confirmed
    existing_cols = { row[1] for row in obj_conn.execute( 'PRAGMA table_info(fetch_progress)' ).fetchall() }
    list_migrations = [
        ( 'step1_done',   'ALTER TABLE fetch_progress ADD COLUMN step1_done   INTEGER NOT NULL DEFAULT 0' ),
        ( 'n_candidates', 'ALTER TABLE fetch_progress ADD COLUMN n_candidates INTEGER NOT NULL DEFAULT 0' ),
        ( 'n_confirmed',  'ALTER TABLE fetch_progress ADD COLUMN n_confirmed  INTEGER NOT NULL DEFAULT 0' ),
        ( 'co_id',        'ALTER TABLE fetch_errors   ADD COLUMN co_id        TEXT' ),
    ]
    for str_col, str_sql in list_migrations:
        if str_col not in existing_cols:
            try:
                obj_conn.execute( str_sql )
            except sqlite3.OperationalError:
                pass  # 欄位可能已存在於其他 table，忽略
    obj_conn.commit()


def init_db( str_path: str ) -> sqlite3.Connection:
    obj_conn = sqlite3.connect( str_path )
    obj_conn.execute( 'PRAGMA journal_mode=WAL' )
    obj_conn.execute( 'PRAGMA synchronous=NORMAL' )
    obj_conn.executescript( """
        -- step=1 的全部結果，含 step=2 確認進度
        CREATE TABLE IF NOT EXISTS candidates (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            market      TEXT    NOT NULL,
            year        INTEGER NOT NULL,
            month       INTEGER NOT NULL,
            co_id       TEXT    NOT NULL,
            co_name     TEXT,
            ann_date    TEXT,
            subject     TEXT,
            spoke_date  TEXT,
            spoke_time  TEXT,
            seq_no      TEXT,
            skey        TEXT,
            step2_done  INTEGER NOT NULL DEFAULT 0,  -- 0=待確認, 1=已確認
            clause      TEXT    NOT NULL DEFAULT '', -- step=2 取得的符合條款
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE ( market, co_id, spoke_date, spoke_time, seq_no )
        );

        -- 已確認為第 11 款的公告（最終結果）
        CREATE TABLE IF NOT EXISTS announcements (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            market      TEXT    NOT NULL,
            year        INTEGER NOT NULL,
            month       INTEGER NOT NULL,
            co_id       TEXT    NOT NULL,
            co_name     TEXT,
            ann_date    TEXT,
            subject     TEXT,
            clause      TEXT,
            spoke_date  TEXT,
            spoke_time  TEXT,
            seq_no      TEXT,
            skey        TEXT,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE ( market, co_id, spoke_date, spoke_time, seq_no )
        );

        -- step=1 月份完成紀錄（供斷點續傳）
        CREATE TABLE IF NOT EXISTS fetch_progress (
            market      TEXT    NOT NULL,
            year        INTEGER NOT NULL,
            month       INTEGER NOT NULL,
            step1_done  INTEGER NOT NULL DEFAULT 0,  -- 1 = step=1 已完成
            n_candidates INTEGER NOT NULL DEFAULT 0, -- step=1 抓到幾筆
            n_confirmed  INTEGER NOT NULL DEFAULT 0, -- step=2 確認幾筆第 11 款
            has_error   INTEGER NOT NULL DEFAULT 0,
            fetched_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY ( market, year, month )
        );

        CREATE TABLE IF NOT EXISTS fetch_errors (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            market      TEXT,
            year        INTEGER,
            month       INTEGER,
            co_id       TEXT,
            error_msg   TEXT,
            logged_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_cand_co_id     ON candidates (co_id);
        CREATE INDEX IF NOT EXISTS idx_cand_month     ON candidates (market, year, month);
        CREATE INDEX IF NOT EXISTS idx_cand_step2     ON candidates (step2_done);
        CREATE INDEX IF NOT EXISTS idx_ann_co_id      ON announcements (co_id);
        CREATE INDEX IF NOT EXISTS idx_ann_year       ON announcements (year, month);
    """ )
    obj_conn.commit()
    _migrate_db( obj_conn )   # 升級舊版 schema（新建 DB 時此函式為 no-op）
    return obj_conn


def is_step1_done( obj_conn: sqlite3.Connection, str_market: str, n_year: int, n_month: int ) -> bool:
    row = obj_conn.execute(
        'SELECT step1_done FROM fetch_progress WHERE market=? AND year=? AND month=?',
        ( str_market, n_year, n_month )
    ).fetchone()
    return row is not None and row[0] == 1


def is_month_fully_done( obj_conn: sqlite3.Connection, str_market: str, n_year: int, n_month: int ) -> bool:
    """step=1 完成 且 該月所有 candidates 的 step2_done=1"""
    if not is_step1_done( obj_conn, str_market, n_year, n_month ):
        return False
    n_pending = obj_conn.execute(
        'SELECT COUNT(*) FROM candidates WHERE market=? AND year=? AND month=? AND step2_done=0',
        ( str_market, n_year, n_month )
    ).fetchone()[0]
    return n_pending == 0


def mark_step1_done( obj_conn: sqlite3.Connection, str_market: str, n_year: int, n_month: int,
                     n_candidates: int, b_has_error: bool ):
    obj_conn.execute(
        """INSERT INTO fetch_progress (market, year, month, step1_done, n_candidates, has_error, fetched_at)
           VALUES (?, ?, ?, 1, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT (market, year, month) DO UPDATE SET
               step1_done=1, n_candidates=excluded.n_candidates,
               has_error=excluded.has_error, fetched_at=CURRENT_TIMESTAMP""",
        ( str_market, n_year, n_month, n_candidates, int( b_has_error ) )
    )
    obj_conn.commit()


def update_confirmed_count( obj_conn: sqlite3.Connection, str_market: str, n_year: int, n_month: int ):
    n_confirmed = obj_conn.execute(
        "SELECT COUNT(*) FROM announcements WHERE market=? AND year=? AND month=?",
        ( str_market, n_year, n_month )
    ).fetchone()[0]
    obj_conn.execute(
        "UPDATE fetch_progress SET n_confirmed=? WHERE market=? AND year=? AND month=?",
        ( n_confirmed, str_market, n_year, n_month )
    )
    obj_conn.commit()


def log_error( obj_conn: sqlite3.Connection, str_market: str, n_year: int, n_month: int,
               str_msg: str, str_co_id: str = '' ):
    obj_conn.execute(
        'INSERT INTO fetch_errors (market, year, month, co_id, error_msg) VALUES (?,?,?,?,?)',
        ( str_market, n_year, n_month, str_co_id, str_msg )
    )
    obj_conn.commit()


def insert_candidates( obj_conn: sqlite3.Connection, list_ann: List[Dict] ) -> int:
    """批次寫入 candidates（重複的 UNIQUE key 自動略過）。"""
    list_rows = []
    for d in list_ann:
        onclick = d.get( 'onclick', {} )
        list_rows.append( (
            d.get( 'market',   '' ),
            d.get( 'year',     0  ),
            d.get( 'month',    0  ),
            d.get( 'co_id',   '' ),
            d.get( 'co_name', '' ),
            d.get( 'ann_date', '' ),
            d.get( 'subject',  '' ),
            onclick.get( 'spoke_date', '' ),
            onclick.get( 'spoke_time', '' ),
            onclick.get( 'seq_no',     '' ),
            onclick.get( 'skey',       '' ),
        ) )
    obj_conn.executemany(
        """INSERT OR IGNORE INTO candidates
           (market, year, month, co_id, co_name, ann_date, subject,
            spoke_date, spoke_time, seq_no, skey)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        list_rows
    )
    obj_conn.commit()
    return len( list_rows )


def insert_announcement( obj_conn: sqlite3.Connection, dict_cand: dict, str_clause: str ):
    """將確認為第 11 款的 candidate 寫入 announcements 表。"""
    obj_conn.execute(
        """INSERT OR IGNORE INTO announcements
           (market, year, month, co_id, co_name, ann_date, subject, clause,
            spoke_date, spoke_time, seq_no, skey)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            dict_cand['market'],   dict_cand['year'],   dict_cand['month'],
            dict_cand['co_id'],    dict_cand['co_name'],
            dict_cand['ann_date'], dict_cand['subject'], str_clause,
            dict_cand['spoke_date'], dict_cand['spoke_time'],
            dict_cand['seq_no'],   dict_cand['skey'],
        )
    )
    obj_conn.commit()


def mark_candidate_step2_done( obj_conn: sqlite3.Connection, n_id: int, str_clause: str ):
    obj_conn.execute(
        'UPDATE candidates SET step2_done=1, clause=? WHERE id=?',
        ( str_clause, n_id )
    )
    obj_conn.commit()


def get_pending_candidates( obj_conn: sqlite3.Connection,
                             str_market: str, n_year: int, n_month: int ) -> List[dict]:
    """取得該月 step2_done=0 的 candidates。"""
    rows = obj_conn.execute(
        """SELECT id, market, year, month, co_id, co_name, ann_date, subject,
                  spoke_date, spoke_time, seq_no, skey
           FROM candidates
           WHERE market=? AND year=? AND month=? AND step2_done=0
           ORDER BY id""",
        ( str_market, n_year, n_month )
    ).fetchall()
    list_cols = [ 'id', 'market', 'year', 'month', 'co_id', 'co_name',
                  'ann_date', 'subject', 'spoke_date', 'spoke_time', 'seq_no', 'skey' ]
    return [ dict( zip( list_cols, row ) ) for row in rows ]

# ── Session 管理 ──────────────────────────────────────────────────────────────

def build_session() -> requests.Session:
    obj_session = requests.Session()
    obj_session.headers.update( STR_HEADERS )
    for n_attempt in range( 3 ):
        try:
            obj_session.get( STR_MOPSOV_INDEX_URL, timeout=15 )
            time.sleep( 1 )
            obj_session.get( STR_MOPSOV_FORM_URL,  timeout=15 )
            time.sleep( 1 )
            return obj_session
        except Exception as e:
            print( f'  [WARN] Session 建立失敗（第 {n_attempt+1} 次）: {e}' )
            time.sleep( F_SLEEP_RETRY )
    return obj_session

# ── onclick 解析 ──────────────────────────────────────────────────────────────

def _parse_onclick( str_onclick: str ) -> Dict[ str, str ]:
    dict_params = {}
    for obj_m in re.finditer( r'\.(\w+)\.value\s*=\s*\'([^\']*)\'', str_onclick ):
        dict_params[ obj_m.group(1) ] = obj_m.group(2)
    return dict_params


def _extract_onclicks( str_html: str ) -> List[ Dict[ str, str ] ]:
    return [ _parse_onclick( s ) for s in re.findall( r'onclick="([^"]+)"', str_html ) ]

# ── 欄位取值 ──────────────────────────────────────────────────────────────────

def _get_col( obj_row: pd.Series, tuple_names: Tuple[str, ...] ) -> str:
    for n in tuple_names:
        if n in obj_row.index and pd.notna( obj_row[ n ] ):
            return str( obj_row[ n ] ).strip()
    return ''

# ── Step=1：抓取整月清單 ──────────────────────────────────────────────────────

def fetch_month(
    obj_session: requests.Session,
    str_market:  str,
    n_year:      int,
    n_month:     int,
) -> Tuple[ List[Dict], bool ]:
    """
    抓取指定市場/年月的所有重訊標頭（不做任何篩選）。
    回傳 ( list_candidates, b_has_error )
    """
    n_roc_year = n_year - 1911
    n_days     = calendar.monthrange( n_year, n_month )[ 1 ]
    str_typek  = DICT_MOPS_CONFIG[ str_market ]

    payload = {
        'step':      '1',
        'firstin':   'ture',
        'off':       '1',
        'TYPEK':     str_typek,
        'year':      str( n_roc_year ),
        'month':     f'{n_month:02d}',
        'b_date':    '01',
        'e_date':    f'{n_days:02d}',
        'key_word':  '',
        'queryName': 'date',
        'inpuType':  'date',
        'co_id':     '',
        'keyword4':  '',
        'code1':     '',
        'TYPEK2':    '',
        'checkbtn':  '',
    }

    b_has_error = False
    for n_attempt in range( N_RETRY_MAX ):
        try:
            obj_res          = obj_session.post( STR_MOPSOV_AJAX_URL, data=payload, timeout=30 )
            obj_res.encoding = 'utf-8'
            str_html         = obj_res.text
        except Exception as e:
            print( f'\n    [WARN] step=1 連線失敗（第 {n_attempt+1} 次）: {e}' )
            b_has_error = True
            time.sleep( F_SLEEP_RETRY )
            continue

        if 'FOR SECURITY' in str_html:
            print( f'\n    [WARN] 安全機制阻擋，等待後重試...' )
            b_has_error = True
            time.sleep( F_SLEEP_SESSION )
            continue

        if '查無資料' in str_html or len( str_html ) < 300:
            return [], False

        try:
            list_dfs = pd.read_html( io.StringIO( str_html ), header=0 )
        except Exception as e:
            print( f'\n    [WARN] HTML 解析失敗: {e}' )
            b_has_error = True
            time.sleep( F_SLEEP_RETRY )
            continue

        if not list_dfs:
            return [], False

        # 找含代號/主旨的主表
        obj_df = None
        for obj_candidate in list_dfs:
            cols = [ str(c) for c in obj_candidate.columns ]
            if any( kw in c for c in cols for kw in ( '代號', '名稱', '主旨' ) ):
                obj_df = obj_candidate
                break
        if obj_df is None:
            obj_df = list_dfs[0]

        list_onclick_params = _extract_onclicks( str_html )

        list_results = []
        for n_i, ( _, obj_row ) in enumerate( obj_df.iterrows() ):
            str_co_id   = _get_col( obj_row, DICT_COL_ALIAS['co_id']   )
            str_co_name = _get_col( obj_row, DICT_COL_ALIAS['co_name'] )
            str_subject = _get_col( obj_row, DICT_COL_ALIAS['subject'] )
            str_date    = _get_col( obj_row, DICT_COL_ALIAS['date']    )
            if not str_co_id or not str_subject:
                continue

            dict_onclick = list_onclick_params[ n_i ] if n_i < len( list_onclick_params ) else {}

            list_results.append( {
                'market':   str_market,
                'year':     n_year,
                'month':    n_month,
                'co_id':    str_co_id,
                'co_name':  str_co_name,
                'ann_date': str_date,
                'subject':  str_subject,
                'onclick':  dict_onclick,
            } )

        return list_results, b_has_error

    print( f'\n    [ERROR] step=1 重試 {N_RETRY_MAX} 次均失敗，略過此月' )
    return [], True

# ── Step=2：抓取單筆公告完整內文，取「符合條款」──────────────────────────────

def extract_clause( str_html: str ) -> str:
    """從 step=2 的 HTML 中找「符合條款」欄位值，找不到回傳空字串。"""
    # 模式 1：表格 <td>符合條款</td><td>第 11 款</td>
    obj_m = re.search(
        r'符合條款\s*</td[^>]*>\s*<td[^>]*>\s*([^<]{1,80})',
        str_html, re.IGNORECASE
    )
    if obj_m:
        return obj_m.group(1).strip()

    # 模式 2：冒號後接文字（舊版格式）
    obj_m = re.search( r'符合條款[：:]\s*([^\n<]{1,80})', str_html )
    if obj_m:
        return obj_m.group(1).strip()

    return ''


def fetch_clause(
    obj_session:  requests.Session,
    dict_cand:    dict,
    f_sleep_step2: float,
) -> Tuple[ str, bool ]:
    """
    打 step=2 取得單筆公告的「符合條款」。
    回傳 ( str_clause, b_has_error )
    """
    str_typek     = DICT_MOPS_CONFIG[ dict_cand['market'] ]
    str_spoke_date = dict_cand.get( 'spoke_date', '' )
    str_spoke_time = dict_cand.get( 'spoke_time', '' )
    str_seq_no     = dict_cand.get( 'seq_no',     '' )
    str_skey       = dict_cand.get( 'skey',       '' )

    if not all( [ str_spoke_date, str_spoke_time, str_seq_no ] ):
        # onclick 參數不完整，無法打 step=2，直接標記完成（clause=空）
        return '', False

    payload = {
        'step':       '2',
        'firstin':    'ture',
        'off':        '1',
        'TYPEK':      str_typek,
        'co_id':      dict_cand['co_id'],
        'spoke_date': str_spoke_date,
        'spoke_time': str_spoke_time,
        'seq_no':     str_seq_no,
        'skey':       str_skey,
        'key_word':   '',
        'queryName':  'co_id',
        'inpuType':   'co_id',
        'keyword4':   '',
        'code1':      '',
        'TYPEK2':     '',
        'checkbtn':   '',
    }

    for n_attempt in range( N_RETRY_MAX ):
        try:
            time.sleep( random.uniform( f_sleep_step2, f_sleep_step2 + 2.0 ) )
            obj_res          = obj_session.post( STR_MOPSOV_AJAX_URL, data=payload, timeout=30 )
            obj_res.encoding = 'utf-8'
            str_html         = obj_res.text
        except Exception as e:
            print( f'\n      [WARN] step=2 連線失敗（{dict_cand["co_id"]} {str_spoke_date}，第 {n_attempt+1} 次）: {e}' )
            time.sleep( F_SLEEP_RETRY )
            continue

        if 'FOR SECURITY' in str_html:
            print( f'\n      [WARN] step=2 安全機制阻擋（{dict_cand["co_id"]}），等待...' )
            time.sleep( F_SLEEP_SESSION )
            continue

        str_clause = extract_clause( str_html )
        return str_clause, False

    print( f'\n      [ERROR] step=2 重試 {N_RETRY_MAX} 次均失敗（{dict_cand["co_id"]} {str_spoke_date}）' )
    return '', True

# ── 進度顯示 ──────────────────────────────────────────────────────────────────

def _print_summary( obj_conn: sqlite3.Connection ):
    n_ann   = obj_conn.execute( 'SELECT COUNT(*) FROM announcements' ).fetchone()[0]
    n_cand  = obj_conn.execute( 'SELECT COUNT(*) FROM candidates'    ).fetchone()[0]
    n_month = obj_conn.execute( 'SELECT COUNT(*) FROM fetch_progress WHERE step1_done=1' ).fetchone()[0]
    n_err   = obj_conn.execute( 'SELECT COUNT(*) FROM fetch_errors'  ).fetchone()[0]
    print()
    print( '=' * 60 )
    print( f'  第 11 款公告（最終結果） : {n_ann:,} 筆' )
    print( f'  candidates 總數         : {n_cand:,} 筆' )
    print( f'  已完成月份（step=1）    : {n_month} 個 (市場×月份)' )
    print( f'  錯誤紀錄               : {n_err} 筆' )
    print( '=' * 60 )

# ── 主程式 ────────────────────────────────────────────────────────────────────

def main():
    # ── 參數解析 ──────────────────────────────────────────────────────────────

    obj_parser = argparse.ArgumentParser( description='下載 MOPS 重大訊息並以 step=2 確認第 11 款' )
    obj_parser.add_argument( '--year-from',   type=int,   default=N_YEAR_FROM_DEFAULT,
                             help=f'起始年份（西元，預設 {N_YEAR_FROM_DEFAULT}）' )
    obj_parser.add_argument( '--year-to',     type=int,   default=N_YEAR_TO_DEFAULT,
                             help=f'結束年份（含，預設 {N_YEAR_TO_DEFAULT}）' )
    obj_parser.add_argument( '--db',          type=str,   default=STR_DB_PATH,
                             help=f'SQLite 資料庫路徑（預設 {STR_DB_PATH}）' )
    obj_parser.add_argument( '--sleep-step2', type=float, default=F_SLEEP_STEP2,
                             help=f'step=2 兩次 request 間隔秒數（預設 {F_SLEEP_STEP2}）' )
    obj_parser.add_argument( '--redo-errors', action='store_true',
                             help='重新抓取曾發生錯誤的月份' )
    obj_parser.add_argument( '--no-filter',   action='store_true',
                             help='跳過 step=2，直接將 step=1 全部公告存入 announcements（測試用）' )
    obj_args = obj_parser.parse_args()

    n_year_from    = obj_args.year_from
    n_year_to      = obj_args.year_to
    str_db_path    = obj_args.db
    f_sleep_step2  = obj_args.sleep_step2
    b_redo_errors  = obj_args.redo_errors
    b_skip_step2   = obj_args.no_filter

    # ── 建立工作清單（市場 × 年月）──────────────────────────────────────────

    list_tasks = []
    for n_year in range( n_year_from, n_year_to + 1 ):
        n_month_to = datetime.datetime.now().month if n_year == datetime.datetime.now().year else 12
        for n_month in range( 1, n_month_to + 1 ):
            for str_market in ( '上市', '上櫃' ):
                list_tasks.append( ( str_market, n_year, n_month ) )

    n_total_tasks  = len( list_tasks )
    str_mode       = '跳過 step=2（--no-filter）' if b_skip_step2 else f'step=2 確認第 11 款（間隔 {f_sleep_step2}s）'
    print( f'[INFO] 目標範圍：{n_year_from} ~ {n_year_to}，共 {n_total_tasks} 個工作' )
    print( f'[INFO] 模式    ：{str_mode}' )
    print( f'[INFO] 資料庫  ：{str_db_path}' )

    # ── 初始化 DB ─────────────────────────────────────────────────────────────

    obj_conn = init_db( str_db_path )

    # ── 處理 --redo-errors ────────────────────────────────────────────────────

    if b_redo_errors:
        obj_conn.execute( 'DELETE FROM fetch_progress WHERE has_error=1' )
        obj_conn.commit()
        print( '[INFO] 已清除錯誤月份紀錄，將重新抓取' )

    # ── 過濾已完成的工作 ──────────────────────────────────────────────────────

    list_pending = [
        ( m, y, mo ) for ( m, y, mo ) in list_tasks
        if not is_month_fully_done( obj_conn, m, y, mo )
    ]

    n_already = n_total_tasks - len( list_pending )
    if n_already:
        print( f'[INFO] 已完全完成 {n_already} 個工作，跳過（斷點續傳）' )
    print( f'[INFO] 待處理 {len(list_pending)} 個工作' )
    print()

    if not list_pending:
        print( '[INFO] 所有工作已完成！' )
        _print_summary( obj_conn )
        obj_conn.close()
        return

    # ── 建立 Session ──────────────────────────────────────────────────────────

    print( '[INFO] 建立 MOPS Session...' )
    obj_session = build_session()
    print( '[INFO] Session 建立完成' )
    print()

    # ── 主迴圈 ────────────────────────────────────────────────────────────────

    n_ann_total = 0
    dt_start    = datetime.datetime.now()

    for n_task_i, ( str_market, n_year, n_month ) in enumerate( list_pending ):

        # 進度
        n_done_total = n_already + n_task_i
        n_pct        = int( 100 * n_done_total / n_total_tasks )
        dt_elapsed   = datetime.datetime.now() - dt_start
        if n_task_i > 0 and dt_elapsed.total_seconds() > 0:
            f_speed = n_task_i / dt_elapsed.total_seconds()
            n_eta_s = int( ( n_total_tasks - n_done_total ) / f_speed )
            str_eta = f'{n_eta_s // 3600}h{(n_eta_s % 3600) // 60}m' if n_eta_s >= 3600 else f'{n_eta_s // 60}m{n_eta_s % 60:02d}s'
        else:
            str_eta = '?'

        print( f'[{n_pct:3d}%] {str_market} {n_year}/{n_month:02d}  ETA {str_eta}' )

        # ── Phase 1：step=1 抓整月清單（若尚未完成）──────────────────────────

        b_step1_error = False
        if not is_step1_done( obj_conn, str_market, n_year, n_month ):
            list_ann, b_step1_error = fetch_month( obj_session, str_market, n_year, n_month )

            if b_step1_error and not list_ann:
                # Session 可能過期，重建後重試
                print( f'  → step=1 失敗，重建 Session...' )
                log_error( obj_conn, str_market, n_year, n_month, 'step1 failed, rebuilding session' )
                time.sleep( F_SLEEP_SESSION )
                obj_session = build_session()
                list_ann, b_step1_error = fetch_month( obj_session, str_market, n_year, n_month )
                if b_step1_error and not list_ann:
                    log_error( obj_conn, str_market, n_year, n_month, 'step1 failed after session rebuild' )
                    print( f'  → step=1 再次失敗，略過此月\n' )
                    mark_step1_done( obj_conn, str_market, n_year, n_month, 0, True )
                    time.sleep( F_SLEEP_STEP1 )
                    continue

            insert_candidates( obj_conn, list_ann )
            mark_step1_done( obj_conn, str_market, n_year, n_month, len( list_ann ), b_step1_error )
            print( f'  step=1 完成：共 {len(list_ann):,} 筆公告存入 candidates' )

            time.sleep( F_SLEEP_STEP1 )
        else:
            n_existing = obj_conn.execute(
                'SELECT COUNT(*) FROM candidates WHERE market=? AND year=? AND month=?',
                ( str_market, n_year, n_month )
            ).fetchone()[0]
            print( f'  step=1 已完成（{n_existing:,} 筆 candidates），直接進入 step=2' )

        # ── Phase 2：step=2 逐筆確認符合條款 ──────────────────────────────────

        list_pending_cands = get_pending_candidates( obj_conn, str_market, n_year, n_month )
        n_pending_count    = len( list_pending_cands )

        if not list_pending_cands:
            print( f'  step=2 已全部完成\n' )
            continue

        if b_skip_step2:
            # --no-filter 模式：直接全部當作確認，寫入 announcements
            for dict_cand in list_pending_cands:
                insert_announcement( obj_conn, dict_cand, '' )
                mark_candidate_step2_done( obj_conn, dict_cand['id'], '' )
            update_confirmed_count( obj_conn, str_market, n_year, n_month )
            print( f'  --no-filter：略過 step=2，直接存入 {n_pending_count:,} 筆\n' )
            continue

        print( f'  step=2 開始：{n_pending_count:,} 筆待確認', end='', flush=True )

        n_confirmed_this_month = 0
        n_step2_error          = 0

        for n_cand_i, dict_cand in enumerate( list_pending_cands ):

            str_clause, b_err = fetch_clause( obj_session, dict_cand, f_sleep_step2 )

            if b_err:
                n_step2_error += 1
                log_error( obj_conn, str_market, n_year, n_month,
                           f'step2 failed: {dict_cand["subject"][:30]}',
                           dict_cand['co_id'] )
                # 不標記 step2_done，下次斷點續傳會重試
                # Session 可能需要重建
                if n_step2_error % 5 == 0:
                    print( f'\n  → 多次 step=2 失敗，重建 Session...' )
                    time.sleep( F_SLEEP_SESSION )
                    obj_session = build_session()
                continue

            mark_candidate_step2_done( obj_conn, dict_cand['id'], str_clause )

            if STR_TARGET_CLAUSE in str_clause:
                insert_announcement( obj_conn, dict_cand, str_clause )
                n_confirmed_this_month += 1
                n_ann_total            += 1
                print( f'\n    ✓ 第 11 款：{dict_cand["co_id"]} {dict_cand["co_name"]} {dict_cand["subject"][:30]}' )

            # 每 50 筆印一個點表示進度
            elif ( n_cand_i + 1 ) % 50 == 0:
                print( '.', end='', flush=True )

        update_confirmed_count( obj_conn, str_market, n_year, n_month )

        print( f'\n  step=2 完成：確認 {n_confirmed_this_month} 筆第 11 款'
               + ( f'，{n_step2_error} 筆錯誤' if n_step2_error else '' ) )
        print()

    # ── 完成 ──────────────────────────────────────────────────────────────────

    print( f'[INFO] 本次新增第 11 款公告 {n_ann_total:,} 筆' )
    _print_summary( obj_conn )
    obj_conn.close()


if __name__ == '__main__':
    main()
