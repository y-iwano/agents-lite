# Agents lite ハンズオン用 AI Agent 定義
このファイルでは、Databricks における AI Agent 開発を体験してもらうためのハンズオンで利用する AI Agent について定義しています。

## パラメーター
./config.md を参照

## AI Agent 定義
- AI Agent は 簡単な RAG を作成します。フローは次の通りです。
  1. AI Agent はユーザーからの問い合わせを受付
  1. ユーザーからの問い合わせ、genai_endpoint を利用して検索しやすい文字列に変換
  1. 上記で変換した文字列を Vector Search Index に対してハイブリッド検索します。最大３件を結果として取得
  1. 取得した結果の要約は、genai_endpoint を利用してシステムプロンプトに基づいて回答
- Vector Search Endpoint はパラメーターに記載されている vs_endpoint を利用し、Vector Search Index は vs_index を利用
- Vector Search で利用する embedding モデルは組み込みのものを利用
- AI Agent の AI フレームワークには、langchain-community と databricks-langchain を利用
- MLflow 3.x を利用し、AI Agent をトレース、デバッグできるようにする
- 要約に利用するシステムプロンプトは、"あなたはDatabricksラボのカスタマーサクセススペシャリストです。ユーザーからの製品に関する質問に対し、必要な情報はツールを使って取得し、ユーザーが製品を十分に理解できるようサポートしてください。お客様の興味を引くであろう情報を可能な限り盛り込んで、すべてのやり取りで価値を提供することを心がけてください。" とする

