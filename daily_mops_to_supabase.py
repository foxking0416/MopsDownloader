"""
daily_mops_to_supabase.py

每日爬取 MOPS 重大訊息，篩選「符合條款：第 11 款」（M&A 類），
upsert 至 Supabase 的 StockKeeper.MOPS 表。

設計目的：給 GitHub Actions cron job 每日執行。
預設抓今天 + 昨天（用 upsert 去重），確保晚上才公告的訊息也能補進來。

執行方式：
  python daily_mops_to_supabase.py                  # 預設：今天 + 昨天
  python daily_mops_to_supabase.py --days-back 3    # 抓最近 3 天

環境變數（必要）：
  SUPABASE_URL                Supabase 專案 URL（例：https://xxx.supabase.co）
  SUPABASE_KEY   Supabase service_role key（寫入需要）

目標表：StockKeeper.MOPS（unique key: market, co_id, spoke_date, spoke_time, seq_no）
"""

import argparse
import datetime
import html
import io
import os
import random
import re
import sys
import time
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests

# ── 設定 ─────────────────────────────────────────────────────────────────────

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

N_RETRY_MAX     = 3
F_SLEEP_STEP1   = 3.0
F_SLEEP_STEP2   = 3.0
F_SLEEP_RETRY   = 10.0
F_SLEEP_SESSION = 60.0

STR_TARGET_CLAUSE = '11'

# 標題（subject）包含以下任一字串就跳過，不進 step=2、不寫入 Supabase
# 之後要加關鍵字直接在這個 tuple 補即可
TUPLE_SKIP_SUBJECT_KEYWORDS = (
    '限制員工權利新股',
    '減資',
    '盈餘轉增資',
    '現金增資認股基準日',
    '公司債',
)

DICT_COL_ALIAS = {
    'date':    ( '發言日期', '公告日期', '日期', 'date' ),
    'co_id':   ( '公司代號', '股票代號', '代號', 'co_id' ),
    'co_name': ( '公司名稱', '股票名稱', '名稱', 'co_name' ),
    'subject': ( '主旨', '公告主旨', '事由', '說明', 'subject' ),
}

TZ_TAIPEI = ZoneInfo( 'Asia/Taipei' )

if hasattr( sys.stdout, 'reconfigure' ):
    try:
        sys.stdout.reconfigure( encoding='utf-8', errors='replace' )
    except Exception:
        pass


# ── Session ──────────────────────────────────────────────────────────────────

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


# ── onclick / 欄位解析 ────────────────────────────────────────────────────────

def _parse_onclick( str_onclick: str ) -> Dict[ str, str ]:
    dict_params = {}
    for obj_m in re.finditer( r'\.(\w+)\.value\s*=\s*\'([^\']*)\'', str_onclick ):
        dict_params[ obj_m.group(1) ] = obj_m.group(2)
    return dict_params


def _extract_onclicks( str_html: str ) -> List[ Dict[ str, str ] ]:
    return [ _parse_onclick( s ) for s in re.findall( r'onclick="([^"]+)"', str_html ) ]


def _get_col( obj_row: pd.Series, tuple_names: Tuple[str, ...] ) -> str:
    for n in tuple_names:
        if n in obj_row.index and pd.notna( obj_row[ n ] ):
            return str( obj_row[ n ] ).strip()
    return ''


# ── Step=1：抓單日清單 ────────────────────────────────────────────────────────

def fetch_day(
    obj_session: requests.Session,
    str_market:  str,
    obj_date:    datetime.date,
) -> List[ Dict ]:
    """
    抓取指定市場/日期的所有重訊標頭（不做篩選）。
    回傳 list_candidates。
    """
    n_roc_year = obj_date.year - 1911
    str_typek  = DICT_MOPS_CONFIG[ str_market ]

    payload = {
        'step':      '1',
        'firstin':   'ture',
        'off':       '1',
        'TYPEK':     str_typek,
        'year':      str( n_roc_year ),
        'month':     f'{obj_date.month:02d}',
        'b_date':    f'{obj_date.day:02d}',
        'e_date':    f'{obj_date.day:02d}',
        'key_word':  '',
        'queryName': 'date',
        'inpuType':  'date',
        'co_id':     '',
        'keyword4':  '',
        'code1':     '',
        'TYPEK2':    '',
        'checkbtn':  '',
    }

    for n_attempt in range( N_RETRY_MAX ):
        try:
            obj_res          = obj_session.post( STR_MOPSOV_AJAX_URL, data=payload, timeout=30 )
            obj_res.encoding = 'utf-8'
            str_html         = obj_res.text
        except Exception as e:
            print( f'    [WARN] step=1 連線失敗（第 {n_attempt+1} 次）: {e}' )
            time.sleep( F_SLEEP_RETRY )
            continue

        if 'FOR SECURITY' in str_html:
            print( f'    [WARN] 安全機制阻擋，等待後重試...' )
            time.sleep( F_SLEEP_SESSION )
            continue

        if '查無資料' in str_html or len( str_html ) < 300:
            return []

        try:
            list_dfs = pd.read_html( io.StringIO( str_html ), header=0 )
        except Exception as e:
            print( f'    [WARN] HTML 解析失敗: {e}' )
            time.sleep( F_SLEEP_RETRY )
            continue

        if not list_dfs:
            return []

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
                'year':     obj_date.year,
                'month':    obj_date.month,
                'co_id':    str_co_id,
                'co_name':  str_co_name,
                'ann_date': str_date,
                'subject':  str_subject,
                'onclick':  dict_onclick,
            } )

        return list_results

    print( f'    [ERROR] step=1 重試 {N_RETRY_MAX} 次均失敗' )
    return []


# ── Step=2：取「符合條款」+「說明」────────────────────────────────────────────

def _strip_html( str_raw: str ) -> str:
    """把 <br> 轉成換行，移除其他 tag，decode HTML entities，整理空白。"""
    str_text = re.sub( r'<br\s*/?>', '\n', str_raw, flags=re.IGNORECASE )
    str_text = re.sub( r'<[^>]+>',   '',   str_text )
    str_text = html.unescape( str_text )
    # 去掉每行首尾空白、合併連續空白行
    list_lines = [ ln.strip() for ln in str_text.splitlines() ]
    str_text = '\n'.join( ln for ln in list_lines if ln )
    return str_text.strip()


def extract_clause( str_html: str ) -> str:
    obj_m = re.search(
        r'符合條款\s*</td[^>]*>\s*<td[^>]*>\s*([^<]{1,80})',
        str_html, re.IGNORECASE
    )
    if obj_m:
        return obj_m.group(1).strip()

    obj_m = re.search( r'符合條款[：:]\s*([^\n<]{1,80})', str_html )
    if obj_m:
        return obj_m.group(1).strip()

    return ''


def extract_description( str_html: str ) -> str:
    """
    從 step=2 的 HTML 取「說明」欄位內容（重訊明細，通常含 1.~N. 條列項）。
    找不到回傳空字串。
    """
    # 模式 1：<td>說明</td><td> ... </td>（內容可能含 <br> 與多行）
    obj_m = re.search(
        r'>\s*說明\s*</td[^>]*>\s*<td[^>]*>(.*?)</td\s*>',
        str_html, re.IGNORECASE | re.DOTALL
    )
    if obj_m:
        return _strip_html( obj_m.group(1) )

    # 模式 2：冒號後接文字（舊版格式）
    obj_m = re.search( r'說明[：:]\s*(.{1,5000}?)(?=<|\n\s*\n)', str_html, re.DOTALL )
    if obj_m:
        return _strip_html( obj_m.group(1) )

    return ''


def fetch_clause_and_desc(
    obj_session:   requests.Session,
    dict_cand:     dict,
    f_sleep_step2: float,
) -> Optional[ Tuple[str, str] ]:
    """
    打 step=2 取得單筆公告的「符合條款」+「說明」。
    回傳 ( str_clause, str_description )；失敗回傳 None。
    """
    str_typek      = DICT_MOPS_CONFIG[ dict_cand['market'] ]
    dict_onclick   = dict_cand.get( 'onclick', {} )
    str_spoke_date = dict_onclick.get( 'spoke_date', '' )
    str_spoke_time = dict_onclick.get( 'spoke_time', '' )
    str_seq_no     = dict_onclick.get( 'seq_no',     '' )
    str_skey       = dict_onclick.get( 'skey',       '' )

    if not all( [ str_spoke_date, str_spoke_time, str_seq_no ] ):
        return ( '', '' )

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
            print( f'      [WARN] step=2 連線失敗（{dict_cand["co_id"]}，第 {n_attempt+1} 次）: {e}' )
            time.sleep( F_SLEEP_RETRY )
            continue

        if 'FOR SECURITY' in str_html:
            print( f'      [WARN] step=2 安全機制阻擋（{dict_cand["co_id"]}），等待...' )
            time.sleep( F_SLEEP_SESSION )
            continue

        return ( extract_clause( str_html ), extract_description( str_html ) )

    print( f'      [ERROR] step=2 重試 {N_RETRY_MAX} 次均失敗（{dict_cand["co_id"]}）' )
    return None


# ── Supabase upsert ──────────────────────────────────────────────────────────

def upsert_to_supabase(
    str_url:         str,
    str_service_key: str,
    list_rows:       List[Dict],
) -> int:
    """
    批次 upsert 到 StockKeeper.MOPS。
    使用 PostgREST：on_conflict 指定 unique constraint 欄位組合。
    """
    if not list_rows:
        return 0

    str_endpoint = f'{str_url.rstrip("/")}/rest/v1/MOPS'
    dict_headers = {
        'apikey':         str_service_key,
        'Authorization':  f'Bearer {str_service_key}',
        'Content-Type':   'application/json',
        'Content-Profile': 'StockKeeper',
        'Accept-Profile':  'StockKeeper',
        'Prefer':         'resolution=merge-duplicates,return=minimal',
    }
    params = { 'on_conflict': 'market,co_id,spoke_date,spoke_time,seq_no' }

    obj_res = requests.post(
        str_endpoint, headers=dict_headers, params=params,
        json=list_rows, timeout=60,
    )
    if obj_res.status_code >= 300:
        raise RuntimeError( f'Supabase upsert 失敗 ({obj_res.status_code}): {obj_res.text}' )
    return len( list_rows )


def candidate_to_row( dict_cand: dict, str_clause: str, str_description: str ) -> dict:
    """將 candidate 轉換為 Supabase row。"""
    onclick = dict_cand.get( 'onclick', {} )
    return {
        'market':      dict_cand['market'],
        'year':        dict_cand['year'],
        'month':       dict_cand['month'],
        'co_id':       dict_cand['co_id'],
        'co_name':     dict_cand.get( 'co_name', '' ),
        'ann_date':    dict_cand.get( 'ann_date', '' ),
        'subject':     dict_cand.get( 'subject', '' ),
        'clause':      str_clause,
        'description': str_description,
        'spoke_date':  onclick.get( 'spoke_date', '' ),
        'spoke_time':  onclick.get( 'spoke_time', '' ),
        'seq_no':      onclick.get( 'seq_no', '' ),
        'is_ma':       1,
    }


# ── 主流程 ────────────────────────────────────────────────────────────────────

def run( n_days_back: int, f_sleep_step2: float ) -> int:
    str_url = os.environ.get( 'SUPABASE_URL', '' ).strip()
    str_key = os.environ.get( 'SUPABASE_KEY', '' ).strip()
    if not str_url or not str_key:
        print( '[ERROR] 缺少環境變數 SUPABASE_URL 或 SUPABASE_KEY' )
        return 2

    obj_today = datetime.datetime.now( TZ_TAIPEI ).date()
    list_dates = [ obj_today - datetime.timedelta( days=n_offset )
                   for n_offset in range( n_days_back + 1 ) ]

    print( f'[INFO] 抓取日期範圍: {list_dates[-1]} ~ {list_dates[0]}（台北時區）' )
    print( f'[INFO] step=2 間隔  : {f_sleep_step2}s' )
    print( f'[INFO] Supabase     : {str_url}' )
    print()

    print( '[INFO] 建立 MOPS Session...' )
    obj_session = build_session()
    print( '[INFO] Session 建立完成\n' )

    n_total_cand = 0
    n_total_ma   = 0
    n_total_upsert = 0
    list_pending_rows: List[dict] = []

    for obj_date in list_dates:
        for str_market in ( '上市', '上櫃' ):
            print( f'[FETCH] {str_market} {obj_date}' )
            list_cands = fetch_day( obj_session, str_market, obj_date )
            time.sleep( F_SLEEP_STEP1 )

            n_total_cand += len( list_cands )
            print( f'  step=1: {len(list_cands):,} 筆公告' )
            if not list_cands:
                continue

            for dict_cand in list_cands:
                str_subject = dict_cand.get( 'subject', '' )
                str_hit = next(
                    ( kw for kw in TUPLE_SKIP_SUBJECT_KEYWORDS if kw in str_subject ),
                    None
                )
                if str_hit:
                    print( f'    ⊘ 跳過（標題含「{str_hit}」）：'
                           f'{dict_cand["co_id"]} {str_subject[:40]}' )
                    continue

                tuple_result = fetch_clause_and_desc( obj_session, dict_cand, f_sleep_step2 )

                if tuple_result is None:
                    # 重試後仍失敗，重建 session 後繼續
                    print( f'  → step=2 多次失敗，重建 Session...' )
                    time.sleep( F_SLEEP_SESSION )
                    obj_session = build_session()
                    continue

                str_clause, str_description = tuple_result

                if STR_TARGET_CLAUSE in str_clause:
                    list_pending_rows.append(
                        candidate_to_row( dict_cand, str_clause, str_description )
                    )
                    n_total_ma += 1
                    print( f'    ✓ 第 11 款：{dict_cand["co_id"]} '
                           f'{dict_cand.get("co_name","")} '
                           f'{dict_cand.get("subject","")[:40]}'
                           f'  (說明 {len(str_description)} 字)' )

            # 每個市場/日期完成後就 upsert 一批，避免最後一次失敗丟資料
            if list_pending_rows:
                try:
                    n_total_upsert += upsert_to_supabase( str_url, str_key, list_pending_rows )
                    print( f'  → 已 upsert {len(list_pending_rows)} 筆到 Supabase' )
                    list_pending_rows = []
                except Exception as e:
                    print( f'  [ERROR] Supabase upsert 失敗: {e}' )
                    # 不清空，下一輪再試

    # 最後一輪殘餘
    if list_pending_rows:
        try:
            n_total_upsert += upsert_to_supabase( str_url, str_key, list_pending_rows )
            print( f'[INFO] 最後 upsert {len(list_pending_rows)} 筆' )
        except Exception as e:
            print( f'[ERROR] Supabase upsert 失敗: {e}' )
            return 1

    print()
    print( '=' * 60 )
    print( f'  掃描公告總數      : {n_total_cand:,} 筆' )
    print( f'  第 11 款符合      : {n_total_ma:,} 筆' )
    print( f'  Upsert 到 Supabase: {n_total_upsert:,} 筆' )
    print( '=' * 60 )
    return 0


def main():
    obj_parser = argparse.ArgumentParser( description='每日爬取 MOPS 重大訊息（第 11 款）並寫入 Supabase' )
    obj_parser.add_argument( '--days-back',   type=int,   default=1,
                             help='往回追溯幾天（不含今天），預設 1（共抓今天+昨天）' )
    obj_parser.add_argument( '--sleep-step2', type=float, default=F_SLEEP_STEP2,
                             help=f'step=2 兩次 request 間隔秒數（預設 {F_SLEEP_STEP2}）' )
    obj_args = obj_parser.parse_args()

    return run( obj_args.days_back, obj_args.sleep_step2 )


if __name__ == '__main__':
    sys.exit( main() )
