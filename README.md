# MopsDownloader

爬取公開資訊觀測站（MOPS）重大訊息，篩選「符合條款：第 11 款」（合併、收購、分割、股份轉換等 M&A 類）。

## 檔案

| 檔案 | 用途 |
| --- | --- |
| `_download_all_mops.py` | 一次性歷史資料下載（2010~今年），存入 SQLite `mops_all.db`，支援斷點續傳。 |
| `daily_mops_to_supabase.py` | 每日增量爬蟲，直接 upsert 至 Supabase `StockKeeper.MOPS`。 |
| `.github/workflows/daily_mops.yml` | GitHub Actions cron，每天台北時間 23:30 自動執行 daily 腳本。 |

## GitHub Actions 設定步驟

1. 推上 GitHub 後，到 repo `Settings → Secrets and variables → Actions`，新增兩個 secret：
   - `SUPABASE_URL` — 例如 `https://aaaaaaaaaaaaa.supabase.co`
   - `SUPABASE_KEY` — 在 Supabase 專案 `Project Settings → API` 取得 `service_role` key（**勿外流**）
2. 確認 `StockKeeper.MOPS` 已啟用：
   - schema：`StockKeeper`
   - 表：`MOPS`
   - unique constraint：`(market, co_id, spoke_date, spoke_time, seq_no)` — 用於 upsert 去重
3. 若 Supabase API 未暴露 `StockKeeper` schema，到 `Project Settings → API → Exposed schemas` 把 `StockKeeper` 加進去。
4. 可在 Actions 頁面手動觸發 `Daily MOPS Crawler (Clause 11)` 測試一次。

## 本地測試

```bash
pip install -r requirements.txt

export SUPABASE_URL="https://xxx.supabase.co"
export SUPABASE_KEY="eyJ..."

python daily_mops_to_supabase.py                 # 今天 + 昨天
python daily_mops_to_supabase.py --days-back 3   # 最近 4 天
```

## 排程說明

- Cron：`30 15 * * *`（UTC）= 每天台北時間 23:30
- 預設抓「今天 + 昨天」兩日，配合 unique key upsert，可避免漏掉晚上才公告的重訊
- 步驟：step=1 抓當日清單 → step=2 逐筆確認「符合條款」→ 含 `11` 者寫入 Supabase
