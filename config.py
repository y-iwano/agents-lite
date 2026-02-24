# Databricks notebook source
# カタログを指定します
catalog_name = "yiwano"

# ハンズオンで使用するVS用スキーマを指定
system_schema_name = "agents_lab"
user_schema_name = "agents_lab_yohei_iwano"

print(f"ハンズオンで使用するカタログ: {catalog_name}")
print(f"ハンズオンで使用する共有スキーマ(データを格納): {system_schema_name}")
print(f"ハンズオンで使用するユーザースキーマ(データを格納): {user_schema_name}")