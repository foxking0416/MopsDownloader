# StockKeeper 開發規則

## 1. UI 開發 (Qt)

### 工作流程
- 使用 Qt Designer 設計 UI，建立 `.ui` 檔
- 使用 `pyuic` 工具將 `.ui` 檔轉譯成 `.py` 檔
- 在 `.py` 檔中實現業務邏輯

### 優點
- 方便使用 Qt Designer 進行視覺化編輯和調整
- 自動生成的 UI 程式碼易於維護
- UI 邏輯與業務邏輯分離

## 2. 變數命名規則

### 型別前綴
| 型別 | 前綴 | 範例 |
|------|------|------|
| int | `n_` | `n_count`, `n_index` |
| float | `f_` | `f_price`, `f_ratio` |
| list | `list_` | `list_items`, `list_users` |
| dict | `dict_` | `dict_config`, `dict_data` |
| decimal | `d_` | `d_amount`, `d_balance` |
| 物件 | `obj_` | `obj_user`, `obj_window` |

### 命名範例
```python
n_total = 100
f_rate = 0.95
list_names = ["Alice", "Bob"]
dict_config = {"host": "localhost"}
d_total = Decimal("123.45")
obj_main_window = QMainWindow()
```

## 3. 專案結構

待補充...

## 4. Git 操作規則

- 修改或新增檔案後，可以執行 `git add` 將變更加入暫存區
- **禁止自動 commit**：不可在未經明確要求的情況下執行 `git commit`
- **禁止自動 push**：不可在未經明確要求的情況下執行 `git push`
- 只有在使用者於對話中明確提出 commit 或 push 要求時，才可執行對應操作
- commit log：盡量用中文撰寫

## 5. Supabase Table
- Schema 為 StockKeeper