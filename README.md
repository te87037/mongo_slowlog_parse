# mongo_slowlog_parse
# MongoDB 慢查詢日誌 ETL 至 Elasticsearch  
**MongoDB Slow Query Log ETL to Elasticsearch**

此工具可透過 SSH 從遠端主機讀取 MongoDB 慢查詢日誌，解析並傳送至 Elasticsearch，支援 JSON 與傳統文字格式。  
This tool fetches MongoDB slow logs via SSH, parses both JSON and traditional text formats, and sends structured data to Elasticsearch.

---

## 🔧 功能說明 | Features

- 支援多主機並行處理  
  Parallel processing of multiple remote hosts
- 自動追蹤讀取 offset，避免重複上傳  
  Automatic offset tracking to avoid duplicate ingestion
- 支援新版 JSON 及舊版文字格式慢查詢日誌  
  Supports both JSON and legacy text-formatted slow logs
- 將查詢標準化為 `query_template`，方便後續聚合分析  
  Normalizes query patterns into `query_template` for easier aggregation
- 錯誤傳送紀錄於 `es_fail.log` 以利補送  
  Failed records are logged to `es_fail.log` for re-ingestion

---

## 📂 專案結構 | Project Structure

```bash
.
├── pull.py     # 主程式 / Main script
├── host.list                # 主機清單 / Host list
├── .state/                  # Offset 儲存 / Offset state files
├── es_fail.log              # 傳送失敗記錄 / Elasticsearch failures
└── requirements.txt
