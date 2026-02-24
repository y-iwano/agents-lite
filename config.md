# 共通設定

## パラメーター
- カタログを指定します
  catalog_name = "yiwano"
- データスキーマを指定
  system_schema_name = "agents_lab2"
- ユーザースキーマを指定
  user_schema_name = "agents_lab2_yohei_iwano"
- Vector Search Endpoint を指定
  vs_endpoint = "one-env-shared-endpoint-1"
- Vector Search Index を指定
  vs_index = f"{catalog_name}.{system_schema_name}.product_docs_index"
- 要約に利用する GenAI を指定
  genai_endpoint = "databricks-gpt-oss-120b"