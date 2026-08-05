"""Production Airflow DAG with quality gates that halt all downstream stages on failure."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from capstone_de.airflow.tasks import (
    bronze_ingestion,
    ensure_kafka_topics,
    gold_aggregate,
    quality_gate,
    rag_indexing,
    schema_enforcement,
    silver_merge,
)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="capstone_modern_data_engineering",
    description="Kafka-to-Delta Lakehouse and grounded RAG pipeline",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["capstone", "kafka", "delta", "rag"],
) as dag:
    topics = PythonOperator(task_id="ensure_kafka_topics", python_callable=ensure_kafka_topics)
    bronze = PythonOperator(task_id="bronze_ingestion", python_callable=bronze_ingestion)
    bronze_quality = PythonOperator(
        task_id="bronze_quality_gate",
        python_callable=quality_gate,
        op_kwargs={"layer": "bronze"},
    )
    silver = PythonOperator(task_id="silver_merge", python_callable=silver_merge)
    schema_check = PythonOperator(task_id="schema_enforcement", python_callable=schema_enforcement)
    silver_quality = PythonOperator(
        task_id="silver_quality_gate",
        python_callable=quality_gate,
        op_kwargs={"layer": "silver"},
    )
    gold = PythonOperator(task_id="gold_aggregate", python_callable=gold_aggregate)
    gold_quality = PythonOperator(
        task_id="gold_quality_gate",
        python_callable=quality_gate,
        op_kwargs={"layer": "gold"},
    )
    index = PythonOperator(task_id="rag_indexing", python_callable=rag_indexing)

    (
        topics
        >> bronze
        >> bronze_quality
        >> silver
        >> schema_check
        >> silver_quality
        >> gold
        >> gold_quality
        >> index
    )
