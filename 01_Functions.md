# Agents lite ハンズオン用関数定義
このファイルでは、Databricks における AI Agent 開発を体験してもらうためのハンズオンで利用する関数について定義しています。

## 作成する関数一覧
- get_latest_return
- get_return_policy
- get_user_id
- get_order_history
- get_todays_date

## パラメーター
- カタログを指定します
  catalog_name = "yiwano"
- データスキーマを指定
  system_schema_name = "agents_lab"
- ユーザースキーマを指定
  user_schema_name = "agents_lab_yohei_iwano"

## 関数の定義
新たに作成する関数は全て {catalog_name}.{user_schema_name} スキーマに作成します。
### get_latest_return() 関数
{catalog_name}.{system_schema_name}.cust_service_data より最新のデータを一行取得するユーザー定義テーブル関数 (UDFT) です。
#### ユーザー定義テーブル関数のテーブル型
- purchase_date DATE
- issue_category STRING
- issue_description STRING
- name STRING

### get_return_policy() 関数
{catalog_name}.{system_schema_name}.policies より返品ポリシーを取得するユーザー定義テーブル関数 (UDFT) です。返信ポリシーは policy 列の値が 'Return Policy' のデータとして保存されています。
#### ユーザー定義テーブル関数のテーブル型
- policy           STRING,
- policy_details   STRING,
- last_updated     DATE

### get_user_id(user_name STRING) 関数
{catalog_name}.{system_schema_name}.cust_service_data より名前に基づいてuserIDを取得し、ユーザーIDを文字列として返します。name 列にユーザー名が保存されています。

### get_order_history(user_id STRING) 関数
{catalog_name}.{system_schema_name}.cust_service_data から userID に基づいて直近12ヶ月間の注文数を取得するユーザー定義テーブル関数 (UDFT) です。issue_category ごとに集計します。
#### ユーザー定義テーブル関数のテーブル型
- returns_last_12_months INT
- issue_category STRING

### get_todays_date() 関数
現在の日付を返す関数です。SQL ではなく python で記述されています。
