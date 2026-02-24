# Databricks notebook source
# MAGIC %md
# MAGIC # ハンズオンデータの準備
# MAGIC
# MAGIC サーバレスコンピュートで実行可能です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの準備

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# カタログの作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `account users`")
# スキーマの作成
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{system_schema_name}")
spark.sql(f"GRANT USE SCHEMA, SELECT ON SCHEMA {catalog_name}.{system_schema_name} TO `account users`")

# COMMAND ----------

# ボリュームの作成
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{system_schema_name}.data")

# COMMAND ----------

import shutil
import os

# ボリュームパスの定義
volume_path = f"/Volumes/{catalog_name}/{system_schema_name}/data"

# ワークスペースファイルのパス
workspace_data_path = f"{os.getcwd()}/data"

# dataフォルダ内のCSVファイルをボリュームにコピー
for fname in ["cust_service_data.csv", "policies.csv", "product_docs.csv"]:
    src = os.path.join(workspace_data_path, fname)
    dst = os.path.join(volume_path, fname)
    shutil.copyfile(src, dst)

# ボリューム上のCSVファイルを読み込み
cust_service_data_df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(f"{volume_path}/cust_service_data.csv")
)
policies_df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(f"{volume_path}/policies.csv")
)
product_docs_df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("multiline", True)
    .load(f"{volume_path}/product_docs.csv")
)

display(cust_service_data_df)
display(policies_df)
display(product_docs_df)

# Unity Catalogのテーブルに保存
cust_service_data_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{system_schema_name}.cust_service_data")
policies_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{system_schema_name}.policies")
product_docs_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{system_schema_name}.product_docs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vector Search Indexの作成
# MAGIC
# MAGIC `02_agent_eval`で使用するVector Search Indexを作成します。各ユーザーはこのVector Search Indexを使用します。

# COMMAND ----------

# ソーステーブル名
source_table = f"{catalog_name}.{system_schema_name}.product_docs"

# Change Data Feedを有効にする
spark.sql(f"""
    ALTER TABLE {source_table} 
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print(f"✅ {source_table} のチェンジデータフィードの有効化")

# 設定を確認
spark.sql(f"SHOW TBLPROPERTIES {source_table}").filter("key = 'delta.enableChangeDataFeed'").show()

# COMMAND ----------

import requests
import json
import time

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = context.apiUrl().get()
token = context.apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# パラメータ
endpoint_name = "one-env-shared-endpoint-1" # 必要に応じて変更ください
index_name = f"{catalog_name}.{system_schema_name}.product_docs_index"
source_table = f"{catalog_name}.{system_schema_name}.product_docs"

# Vector Search Endpointの存在確認と作成
endpoint_url = f"{host}/api/2.0/vector-search/endpoints/{endpoint_name}"
create_endpoint_url = f"{host}/api/2.0/vector-search/endpoints"

response = requests.get(endpoint_url, headers=headers)
if response.status_code == 200:
    print(f"✅ Vector Search Endpoint '{endpoint_name}' は既に存在します")
else:
    print(f"🔄 Vector Search Endpoint '{endpoint_name}' を作成します...")
    payload = {
        "name": endpoint_name,
        "endpoint_type": "STANDARD"
    }
    create_response = requests.post(create_endpoint_url, headers=headers, json=payload)
    if create_response.status_code in [200, 201]:
        print(f"✅ Vector Search Endpoint '{endpoint_name}' を作成しました！")
    elif create_response.status_code == 409:
        print(f"⚠️  Vector Search Endpoint '{endpoint_name}' は既に存在します (409)")
    else:
        print(f"❌ Endpoint作成エラー: {create_response.status_code}")
        print(create_response.text)

# インデックスを作成
url = f"{host}/api/2.0/vector-search/indexes"

payload = {
    "name": index_name,
    "endpoint_name": endpoint_name,
    "primary_key": "product_id",
    "index_type": "DELTA_SYNC",
    "delta_sync_index_spec": {
        "source_table": source_table,
        "pipeline_type": "TRIGGERED",
        "embedding_source_columns": [{  # columnsは配列形式
            "name": "product_doc",
            "embedding_model_endpoint_name": "databricks-gte-large-en"
        }]
    }
}

print("Vector Search Indexを作成中...")
response = requests.post(url, headers=headers, json=payload)

if response.status_code in [200, 201]:
    print("✅ インデックス作成リクエストが正常に送信されました！")
    print("📊 初回同期は自動的に開始されます...\n")
else:
    print(f"❌ インデックス作成エラー: {response.status_code}")
    print(response.text)

# COMMAND ----------

if response.status_code in [200, 201]:
    
    # インデックスの状態を監視
    def monitor_index_status(index_name, timeout_minutes=60):
        status_url = f"{host}/api/2.0/vector-search/indexes/{index_name}"
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        
        print(f"⏳ インデックスの状態を監視中...")
        print(f"タイムアウト: {timeout_minutes} 分")
        print("-" * 70)
        
        previous_count = 0
        first_check = True
        
        while time.time() - start_time < timeout_seconds:
            try:
                response = requests.get(status_url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    status = data.get("status", {})
                    spec = data.get("delta_sync_index_spec", {})
                    
                    # ステータス情報
                    state = status.get("detailed_state", "不明")
                    ready = status.get("ready", False)
                    indexed_count = status.get("indexed_row_count", 0)
                    message = status.get("message", "")
                    
                    # 最初のチェックで総行数を取得
                    if first_check:
                        total_rows = spec.get("num_rows", 0)
                        if total_rows > 0:
                            print(f"📊 インデックス対象の総行数: {total_rows:,}")
                        first_check = False
                    
                    # 進捗表示
                    elapsed = int(time.time() - start_time)
                    rows_diff = indexed_count - previous_count
                    speed = f"{rows_diff:,} 行" if rows_diff > 0 else "初期化中"
                    
                    print(f"\r⏱️  {elapsed//60}分 {elapsed%60}秒 | "
                          f"状態: {state} | "
                          f"行数: {indexed_count:,} | "
                          f"速度: {speed}/30秒 | "
                          f"準備完了: {ready}", end="")
                    
                    previous_count = indexed_count
                    
                    # 成功判定：readyがTrueかつstateがONLINE系
                    if ready == True and ("ONLINE" in state or state == "READY"):
                      print(f"\n\n✅ インデックスの準備ができました！")
                      print(f"📊 インデックス済み総行数: {indexed_count:,}")
                      print(f"⏱️  合計時間: {elapsed//60}分 {elapsed%60}秒")
                      print(f"📋 最終状態: {state}")
                      return True
                
                    # エラー判定
                    if state in ["FAILED", "ERROR"]:
                      print(f"\n\n❌ インデックス作成に失敗しました！")
                      print(f"エラー状態: {state}")
                      return False
                
                    # タイムアウト判定
                    if elapsed > timeout_seconds:
                      print(f"\n\n⏰ {timeout_minutes}分後にタイムアウトしました")
                      print(f"最終状態: {state}, 準備完了: {ready}")
                      return False
                        
                else:
                    print(f"\n⚠️  ステータス確認エラー: {response.status_code}")
                    print(response.text)
                
                time.sleep(30)  # 30秒ごとにチェック
                
            except Exception as e:
                print(f"\n⚠️  例外発生: {e}")
                time.sleep(30)
        
        print(f"\n\n⏰ {timeout_minutes}分でタイムアウトしました")
        return False
    
    # インデックスの監視を開始
    success = monitor_index_status(index_name, timeout_minutes=60)
    
    if success:
        print("\n🎉 ベクターサーチインデックスの準備ができました！")
        
        # 最終的なインデックス情報を表示
        final_url = f"{host}/api/2.0/vector-search/indexes/{index_name}"
        final_response = requests.get(final_url, headers=headers)
        if final_response.status_code == 200:
            final_data = final_response.json()
            print(f"\n📋 インデックス詳細:")
            print(f"  - 名前: {final_data.get('name')}")
            print(f"  - エンドポイント: {final_data.get('endpoint_name')}")
            print(f"  - ステータス: {final_data.get('status', {}).get('detailed_state')}")
            print(f"  - 行数: {final_data.get('status', {}).get('indexed_row_count', 0):,}")
    else:
        print("\n😞 インデックス作成が正常に完了しませんでした")
        
elif response.status_code == 409:
    print("⚠️  インデックスはすでに存在します")
    print("必要に応じて削除して再作成できます")
else:
    print(f"❌ インデックス作成エラー: {response.status_code}")
    print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC # END