#!/usr/bin/env python3
"""Generate TAISTRA report (index.html) from Databricks.

Скелет на основі Hop Hey QBR. Тягне з Databricks:
  - Monthly metrics (з 2023-09-01, виключно завершені місяці)
  - Last 4 full weeks
  - Фінансові + операційні KPI, refunds, campaigns, acceptance/availability, top stores

Параметри партнера:
  TAISTRA      — точне значення dim_provider_v2.group_name (UPPERCASE як у БД)
  TAISTRA      — людська назва для логів і JSON
  2025-09-01   — стартова дата monthly даних (перший місяць у живе)
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from databricks import sql as dbsql

_ROOT = Path(__file__).parent


def _load_dotenv():
    """Завантажити локальний .env (НЕ комітимо в git)."""
    env_file = _ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv()


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(
            f"Missing {name}. Створіть {_ROOT / '.env'} з .env.example або експортуйте змінну.",
            file=sys.stderr,
        )
        sys.exit(1)
    return value


DATABRICKS_HOST = _require_env("DATABRICKS_HOST")
DATABRICKS_TOKEN = _require_env("DATABRICKS_TOKEN")
if DATABRICKS_TOKEN.startswith("your") or len(DATABRICKS_TOKEN) < 32:
    print(
        "DATABRICKS_TOKEN у .env — заглушка з .env.example.\n"
        "Databricks → User Settings → Developer → Access tokens → Generate new token.\n"
        "Вставте у .env: DATABRICKS_TOKEN=dapi... (без лапок).",
        file=sys.stderr,
    )
    sys.exit(1)
DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")
DATABRICKS_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

PARTNER_NAME = "TAISTRA"
PARTNER_DISPLAY = "TAISTRA"
DATA_START = "2025-09-01"

TEMPLATE_PATH = _ROOT / "template.html"
OUTPUT_PATH = _ROOT / "index.html"
DATA_PATH = _ROOT / "report_data.json"


def _connect_kwargs():
    """Додаткові kwargs. DATABRICKS_TLS_NO_VERIFY=1 — лише локально на Mac за корпоративним проксі."""
    kwargs = {}
    if os.environ.get("DATABRICKS_TLS_NO_VERIFY", "").strip().lower() in ("1", "true", "yes"):
        kwargs["_tls_no_verify"] = True
    return kwargs


def get_connection():
    extra = _connect_kwargs()
    if DATABRICKS_HTTP_PATH:
        return dbsql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
            **extra,
        )
    return dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        access_token=DATABRICKS_TOKEN,
        **extra,
    )


def run_query(cursor, query):
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def to_serializable(rows):
    out = []
    for row in rows:
        d = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
            elif hasattr(v, "as_py"):
                d[k] = v.as_py()
            elif hasattr(v, "__float__"):
                d[k] = float(v)
            elif hasattr(v, "__int__"):
                d[k] = int(v)
            else:
                d[k] = v
        out.append(d)
    return out


def _data_end():
    """Останній день попереднього завершеного місяця (виключаємо поточний місяць)."""
    today = datetime.now().date()
    first_of_current = today.replace(day=1)
    return str(first_of_current - timedelta(days=1))


def _week_boundaries():
    """4 повні тижні Mon–Sun, до останньої завершеної неділі."""
    today = datetime.now().date()
    last_sunday = today - timedelta(days=today.isoweekday())
    four_weeks_ago_monday = last_sunday - timedelta(days=27)
    return str(four_weeks_ago_monday), str(last_sunday)


DATA_END = _data_end()
WEEKLY_START, WEEKLY_END = _week_boundaries()


# ---------------------------------------------------------------------------
# SQL Queries — Monthly
# ---------------------------------------------------------------------------

FINANCIAL_MONTHLY = f"""
SELECT
    DATE_FORMAT(f.order_created_date, 'yyyy-MM') AS period,
    COUNT(*) AS orders,
    SUM(f.provider_price_before_discount) AS merchant_price_uah,
    SUM(f.provider_price_before_discount) / NULLIF(COUNT(*), 0) AS merchant_price_per_order,
    SUM(f.order_gmv) AS gmv_uah,
    SUM(f.order_gmv) / NULLIF(COUNT(*), 0) AS aov_uah,
    COUNT(DISTINCT CASE WHEN f.is_first_delivery_order THEN f.user_id END) AS users_activated,
    COUNT(DISTINCT f.user_id) AS active_users,
    SUM(f.total_refunded_amount) / NULLIF(SUM(f.order_gmv), 0) * 100 AS refund_rate_pct,
    SUM(f.order_gmv) / NULLIF(SUM(f.order_gmv_eur), 0) AS eur_uah_rate
FROM hive_metastore.ng_delivery_spark.fact_order_delivery f
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND f.order_state = 'delivered'
  AND f.order_created_date >= '{DATA_START}'
  AND f.order_created_date <= '{DATA_END}'
GROUP BY 1
ORDER BY 1
"""

FINANCIAL_WEEKLY = f"""
SELECT
    DATE_FORMAT(DATE_TRUNC('week', f.order_created_date), 'yyyy-MM-dd') AS period,
    COUNT(*) AS orders,
    SUM(f.provider_price_before_discount) AS merchant_price_uah,
    SUM(f.provider_price_before_discount) / NULLIF(COUNT(*), 0) AS merchant_price_per_order,
    SUM(f.order_gmv) AS gmv_uah,
    SUM(f.order_gmv) / NULLIF(COUNT(*), 0) AS aov_uah,
    COUNT(DISTINCT CASE WHEN f.is_first_delivery_order THEN f.user_id END) AS users_activated,
    COUNT(DISTINCT f.user_id) AS active_users,
    SUM(f.total_refunded_amount) / NULLIF(SUM(f.order_gmv), 0) * 100 AS refund_rate_pct,
    SUM(f.order_gmv) / NULLIF(SUM(f.order_gmv_eur), 0) AS eur_uah_rate
FROM hive_metastore.ng_delivery_spark.fact_order_delivery f
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND f.order_state = 'delivered'
  AND f.order_created_date >= '{WEEKLY_START}'
  AND f.order_created_date <= '{WEEKLY_END}'
GROUP BY 1
ORDER BY 1
"""

OPERATIONAL_MONTHLY = f"""
SELECT
    DATE_FORMAT(f.order_created_date, 'yyyy-MM') AS period,
    COUNT(*) AS delivered_orders,
    COUNT(DISTINCT f.provider_id) AS active_stores,
    SUM(CASE WHEN f.is_honey_order THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS honey_order_rate,
    SUM(CASE WHEN f.is_bad_order THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS bad_order_rate,
    SUM(CASE WHEN f.is_order_delivered_5_min_late THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS late_delivery_rate,
    SUM(CASE WHEN f.is_order_late_to_partner_5_min THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS late_pickup_rate,
    AVG(f.order_delivery_minutes) AS avg_delivery_minutes,
    AVG(f.courier_delivery_time_min) AS avg_courier_delivery_min
FROM hive_metastore.ng_delivery_spark.fact_order_delivery f
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND f.order_state = 'delivered'
  AND f.order_created_date >= '{DATA_START}'
  AND f.order_created_date <= '{DATA_END}'
GROUP BY 1
ORDER BY 1
"""

OPERATIONAL_WEEKLY = OPERATIONAL_MONTHLY.replace(
    "DATE_FORMAT(f.order_created_date, 'yyyy-MM') AS period",
    "DATE_FORMAT(DATE_TRUNC('week', f.order_created_date), 'yyyy-MM-dd') AS period",
).replace(
    f"AND f.order_created_date >= '{DATA_START}'\n  AND f.order_created_date <= '{DATA_END}'",
    f"AND f.order_created_date >= '{WEEKLY_START}'\n  AND f.order_created_date <= '{WEEKLY_END}'",
)

FAILED_ORDERS_MONTHLY = f"""
SELECT
    DATE_FORMAT(f.order_created_date, 'yyyy-MM') AS period,
    COUNT(*) AS total_placed,
    SUM(CASE WHEN f.order_state = 'delivered' THEN 1 ELSE 0 END) AS delivered,
    SUM(CASE WHEN f.order_state != 'delivered' THEN 1 ELSE 0 END) AS failed_total,
    SUM(CASE WHEN f.order_state != 'delivered' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS failed_rate_pct
FROM hive_metastore.ng_delivery_spark.fact_order_delivery f
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND f.order_created_date >= '{DATA_START}'
  AND f.order_created_date <= '{DATA_END}'
GROUP BY 1
ORDER BY 1
"""

FAILED_ORDERS_WEEKLY = FAILED_ORDERS_MONTHLY.replace(
    "DATE_FORMAT(f.order_created_date, 'yyyy-MM') AS period",
    "DATE_FORMAT(DATE_TRUNC('week', f.order_created_date), 'yyyy-MM-dd') AS period",
).replace(
    f"AND f.order_created_date >= '{DATA_START}'\n  AND f.order_created_date <= '{DATA_END}'",
    f"AND f.order_created_date >= '{WEEKLY_START}'\n  AND f.order_created_date <= '{WEEKLY_END}'",
)

FAILED_REASONS_MONTHLY = f"""
SELECT
    DATE_FORMAT(f.order_created_date, 'yyyy-MM') AS period,
    r.reason,
    r.actor_type,
    COUNT(*) AS cnt
FROM hive_metastore.ng_delivery_spark.delivery_order_order_resolution r
    JOIN hive_metastore.ng_delivery_spark.fact_order_delivery f ON r.order_id = f.order_id
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND f.order_created_date >= '{DATA_START}'
  AND f.order_created_date <= '{DATA_END}'
  AND f.order_state != 'delivered'
GROUP BY 1, r.reason, r.actor_type
ORDER BY 1, cnt DESC
"""

FAILED_REASONS_WEEKLY = f"""
SELECT
    DATE_FORMAT(DATE_TRUNC('week', f.order_created_date), 'yyyy-MM-dd') AS period,
    r.reason,
    r.actor_type,
    COUNT(*) AS cnt
FROM hive_metastore.ng_delivery_spark.delivery_order_order_resolution r
    JOIN hive_metastore.ng_delivery_spark.fact_order_delivery f ON r.order_id = f.order_id
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND f.order_created_date >= '{WEEKLY_START}'
  AND f.order_created_date <= '{WEEKLY_END}'
  AND f.order_state != 'delivered'
GROUP BY 1, r.reason, r.actor_type
ORDER BY 1, cnt DESC
"""

CAMPAIGNS_MONTHLY = f"""
SELECT
    DATE_FORMAT(f.order_created_date, 'yyyy-MM') AS period,
    SUM(f.demand_incentives_local) AS campaigns_discount_uah,
    SUM(
        COALESCE(f.bolt_spend_am_spend_campaign, 0)
        + COALESCE(f.bolt_spend_liquidity_campaign, 0)
        + COALESCE(f.bolt_spend_marketing_campaign, 0)
        + COALESCE(f.bolt_spend_user_lifecycle_campaign, 0)
        + COALESCE(f.bolt_spend_merchant_lifecycle_campaign, 0)
        + COALESCE(f.bolt_spend_other_campaign, 0)
    ) AS bolt_spend_eur,
    SUM(
        COALESCE(f.provider_spend_am_spend_campaign, 0)
        + COALESCE(f.provider_spend_liquidity_campaign, 0)
        + COALESCE(f.provider_spend_marketing_campaign, 0)
        + COALESCE(f.provider_spend_user_lifecycle_campaign, 0)
        + COALESCE(f.provider_spend_merchant_lifecycle_campaign, 0)
        + COALESCE(f.provider_spend_other_campaign, 0)
    ) AS merchant_spend_eur,
    COUNT(CASE WHEN f.demand_incentives_local > 0 THEN 1 END) AS campaign_orders
FROM hive_metastore.ng_delivery_spark.fact_order_delivery f
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND f.order_state = 'delivered'
  AND f.order_created_date >= '{DATA_START}'
  AND f.order_created_date <= '{DATA_END}'
GROUP BY 1
ORDER BY 1
"""

CAMPAIGNS_WEEKLY = CAMPAIGNS_MONTHLY.replace(
    "DATE_FORMAT(f.order_created_date, 'yyyy-MM') AS period",
    "DATE_FORMAT(DATE_TRUNC('week', f.order_created_date), 'yyyy-MM-dd') AS period",
).replace(
    f"AND f.order_created_date >= '{DATA_START}'\n  AND f.order_created_date <= '{DATA_END}'",
    f"AND f.order_created_date >= '{WEEKLY_START}'\n  AND f.order_created_date <= '{WEEKLY_END}'",
)

ACCEPTANCE_AVAILABILITY = f"""
SELECT
    ROUND(SUM(f.provider_acceptance_rate_value * f.provider_acceptance_rate_weight)
        / NULLIF(SUM(f.provider_acceptance_rate_weight), 0) * 100, 1) AS acceptance_rate,
    ROUND(SUM(f.provider_active_rate_value * f.provider_active_rate_weight)
        / NULLIF(SUM(f.provider_active_rate_weight), 0) * 100, 1) AS availability_rate,
    ROUND(SUM(f.provider_rating_per_order_value * f.provider_rating_per_order_weight)
        / NULLIF(SUM(f.provider_rating_per_order_weight), 0), 3) AS avg_rating
FROM hive_metastore.ng_delivery_spark.fact_provider_weekly f
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND f.metric_timestamp_local >= DATE_SUB(CURRENT_DATE(), 7)
"""

ACCEPTANCE_AVAILABILITY_MONTHLY = f"""
SELECT
    DATE_FORMAT(f.metric_timestamp_local, 'yyyy-MM') AS period,
    ROUND(SUM(f.provider_acceptance_rate_value * f.provider_acceptance_rate_weight)
        / NULLIF(SUM(f.provider_acceptance_rate_weight), 0) * 100, 1) AS acceptance_rate,
    ROUND(SUM(f.provider_active_rate_value * f.provider_active_rate_weight)
        / NULLIF(SUM(f.provider_active_rate_weight), 0) * 100, 1) AS availability_rate,
    ROUND(SUM(f.provider_rating_per_order_value * f.provider_rating_per_order_weight)
        / NULLIF(SUM(f.provider_rating_per_order_weight), 0), 3) AS avg_rating
FROM hive_metastore.ng_delivery_spark.fact_provider_weekly f
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND f.metric_timestamp_local >= '{DATA_START}'
  AND f.metric_timestamp_local <= '{DATA_END}'
GROUP BY 1
ORDER BY 1
"""

ACCEPTANCE_AVAILABILITY_WEEKLY = f"""
SELECT
    DATE_FORMAT(f.metric_timestamp_local, 'yyyy-MM-dd') AS period,
    ROUND(SUM(f.provider_acceptance_rate_value * f.provider_acceptance_rate_weight)
        / NULLIF(SUM(f.provider_acceptance_rate_weight), 0) * 100, 1) AS acceptance_rate,
    ROUND(SUM(f.provider_active_rate_value * f.provider_active_rate_weight)
        / NULLIF(SUM(f.provider_active_rate_weight), 0) * 100, 1) AS availability_rate,
    ROUND(SUM(f.provider_rating_per_order_value * f.provider_rating_per_order_weight)
        / NULLIF(SUM(f.provider_rating_per_order_weight), 0), 3) AS avg_rating
FROM hive_metastore.ng_delivery_spark.fact_provider_weekly f
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND f.metric_timestamp_local >= '{WEEKLY_START}'
  AND f.metric_timestamp_local <= '{WEEKLY_END}'
GROUP BY 1
ORDER BY 1
"""

TOP_STORES_LAST_MONTH = f"""
SELECT
    f.provider_name,
    f.city_name,
    COUNT(*) AS orders,
    SUM(f.provider_price_before_discount) AS merchant_price_uah
FROM hive_metastore.ng_delivery_spark.fact_order_delivery f
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND f.order_state = 'delivered'
  AND f.order_created_date >= DATE_FORMAT(ADD_MONTHS(DATE_TRUNC('month', CURRENT_DATE()), -1), 'yyyy-MM-dd')
  AND f.order_created_date < DATE_FORMAT(DATE_TRUNC('month', CURRENT_DATE()), 'yyyy-MM-dd')
GROUP BY 1, 2
ORDER BY merchant_price_uah DESC
LIMIT 15
"""

NETWORK_STORES = f"""
SELECT
    p.provider_id,
    p.provider_name,
    p.city_name
FROM hive_metastore.ng_delivery_spark.dim_provider_v2 p
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND p.provider_status = 'active'
  AND p.lifecycle_status = 'ready_for_work'
ORDER BY p.provider_name
LIMIT 1000
"""

STORE_WEEKLY = f"""
SELECT
    DATE_FORMAT(DATE_TRUNC('week', f.order_created_date), 'yyyy-MM-dd') AS period,
    f.provider_id,
    f.provider_name,
    f.city_name,
    COUNT(*) AS orders,
    SUM(f.provider_price_before_discount) AS merchant_price_uah,
    SUM(f.provider_price_before_discount) / NULLIF(COUNT(*), 0) AS aov_uah
FROM hive_metastore.ng_delivery_spark.fact_order_delivery f
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND p.provider_status = 'active'
  AND p.lifecycle_status = 'ready_for_work'
  AND f.order_state = 'delivered'
  AND f.order_created_date >= '{WEEKLY_START}'
  AND f.order_created_date <= '{WEEKLY_END}'
GROUP BY 1, 2, 3, 4
ORDER BY 1, orders DESC
LIMIT 5000
"""

STORE_QUALITY_WEEKLY = f"""
SELECT
    DATE_FORMAT(f.metric_timestamp_local, 'yyyy-MM-dd') AS period,
    f.provider_id,
    p.provider_name,
    ROUND(SUM(f.provider_active_rate_value * f.provider_active_rate_weight)
        / NULLIF(SUM(f.provider_active_rate_weight), 0) * 100, 1) AS availability_rate,
    ROUND(SUM(f.provider_acceptance_rate_value * f.provider_acceptance_rate_weight)
        / NULLIF(SUM(f.provider_acceptance_rate_weight), 0) * 100, 1) AS acceptance_rate,
    ROUND(SUM(f.provider_rating_per_order_value * f.provider_rating_per_order_weight)
        / NULLIF(SUM(f.provider_rating_per_order_weight), 0), 3) AS avg_rating
FROM hive_metastore.ng_delivery_spark.fact_provider_weekly f
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND p.provider_status = 'active'
  AND p.lifecycle_status = 'ready_for_work'
  AND f.metric_timestamp_local >= '{WEEKLY_START}'
  AND f.metric_timestamp_local <= '{WEEKLY_END}'
GROUP BY 1, 2, 3
ORDER BY 1, 3
LIMIT 5000
"""

STORE_RATINGS_WEEKLY = f"""
SELECT
    DATE_FORMAT(DATE_TRUNC('week', r.created_date), 'yyyy-MM-dd') AS period,
    p.provider_id,
    AVG(r.rating_value) AS avg_review_rating,
    COUNT(*) AS reviews_count,
    SUM(CASE WHEN r.comment IS NOT NULL AND LENGTH(TRIM(r.comment)) > 0 THEN 1 ELSE 0 END) AS comments_count
FROM hive_metastore.ng_delivery_spark.delivery_rating_provider_rating_history r
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON r.provider_id = p.provider_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND p.provider_status = 'active'
  AND p.lifecycle_status = 'ready_for_work'
  AND r.created_date >= '{WEEKLY_START}'
  AND r.created_date <= '{WEEKLY_END}'
  AND COALESCE(r.ignore_rating, false) = false
GROUP BY 1, 2
ORDER BY 1, 2
LIMIT 5000
"""

CUSTOMER_REVIEWS_WEEKLY = f"""
SELECT
    DATE_FORMAT(DATE_TRUNC('week', r.created_date), 'yyyy-MM-dd') AS period,
    p.provider_id,
    p.provider_name,
    p.city_name,
    r.rating_value,
    r.comment,
    CAST(r.created AS STRING) AS created_at,
    f.order_reference_id
FROM hive_metastore.ng_delivery_spark.delivery_rating_provider_rating_history r
    JOIN hive_metastore.ng_delivery_spark.dim_provider_v2 p ON r.provider_id = p.provider_id
    LEFT JOIN hive_metastore.ng_delivery_spark.fact_order_delivery f ON r.order_id = f.order_id
WHERE p.country_code = 'ua'
  AND p.group_name = '{PARTNER_NAME}'
  AND p.provider_status = 'active'
  AND p.lifecycle_status = 'ready_for_work'
  AND r.created_date >= '{WEEKLY_START}'
  AND r.created_date <= '{WEEKLY_END}'
  AND r.comment IS NOT NULL
  AND LENGTH(TRIM(r.comment)) > 0
  AND COALESCE(r.ignore_rating, false) = false
ORDER BY r.created DESC
LIMIT 2000
"""


def main():
    print(f"Partner: {PARTNER_DISPLAY} ({PARTNER_NAME})")
    print(f"Monthly window: {DATA_START} — {DATA_END}")
    print(f"Weekly window: {WEEKLY_START} — {WEEKLY_END}")
    print("Connecting to Databricks...")
    conn = get_connection()
    cursor = conn.cursor()

    print("Fetching financial data...")
    fin_m = to_serializable(run_query(cursor, FINANCIAL_MONTHLY))
    fin_w = to_serializable(run_query(cursor, FINANCIAL_WEEKLY))

    print("Fetching operational data...")
    ops_m = to_serializable(run_query(cursor, OPERATIONAL_MONTHLY))
    ops_w = to_serializable(run_query(cursor, OPERATIONAL_WEEKLY))

    print("Fetching failed orders...")
    fail_m = to_serializable(run_query(cursor, FAILED_ORDERS_MONTHLY))
    fail_w = to_serializable(run_query(cursor, FAILED_ORDERS_WEEKLY))

    print("Fetching failed order reasons...")
    fail_reasons_m = to_serializable(run_query(cursor, FAILED_REASONS_MONTHLY))
    fail_reasons_w = to_serializable(run_query(cursor, FAILED_REASONS_WEEKLY))

    print("Fetching campaign data...")
    camp_m = to_serializable(run_query(cursor, CAMPAIGNS_MONTHLY))
    camp_w = to_serializable(run_query(cursor, CAMPAIGNS_WEEKLY))

    print("Fetching acceptance/availability...")
    aa_current = to_serializable(run_query(cursor, ACCEPTANCE_AVAILABILITY))
    aa_m = to_serializable(run_query(cursor, ACCEPTANCE_AVAILABILITY_MONTHLY))
    aa_w = to_serializable(run_query(cursor, ACCEPTANCE_AVAILABILITY_WEEKLY))

    print("Fetching top stores...")
    top_stores = to_serializable(run_query(cursor, TOP_STORES_LAST_MONTH))

    print("Fetching network stores (Bolt catalogue)...")
    network_stores = to_serializable(run_query(cursor, NETWORK_STORES))
    network_store_count = len(network_stores)

    print("Fetching store-level weekly orders...")
    store_weekly = to_serializable(run_query(cursor, STORE_WEEKLY))

    print("Fetching store-level weekly quality (rating/availability)...")
    store_quality = to_serializable(run_query(cursor, STORE_QUALITY_WEEKLY))

    print("Fetching store-level weekly review counts/avg...")
    store_ratings = to_serializable(run_query(cursor, STORE_RATINGS_WEEKLY))

    print("Fetching customer text reviews...")
    customer_reviews = to_serializable(run_query(cursor, CUSTOMER_REVIEWS_WEEKLY))

    quality_map = {(q["period"], q["provider_id"]): q for q in store_quality}
    ratings_map = {(r["period"], r["provider_id"]): r for r in store_ratings}
    for entry in store_weekly:
        key = (entry["period"], entry["provider_id"])
        q = quality_map.get(key, {})
        r = ratings_map.get(key, {})
        entry["availability_rate"] = q.get("availability_rate")
        entry["acceptance_rate"] = q.get("acceptance_rate")
        entry["avg_rating"] = q.get("avg_rating")
        entry["avg_review_rating"] = r.get("avg_review_rating")
        entry["reviews_count"] = r.get("reviews_count", 0)
        entry["comments_count"] = r.get("comments_count", 0)

    # Магазин може мати 0 замовлень за тиждень, але все одно бути доступним у Bolt-каталозі
    # (Availability > 0). Створюємо синтетичні рядки store_weekly з orders=0, щоб такі точки
    # відображались у вкладці "Магазини" з показником Availability %.
    existing_keys = {(e["period"], e["provider_id"]) for e in store_weekly}
    provider_info = {s["provider_id"]: s for s in network_stores}
    for q in store_quality:
        key = (q["period"], q["provider_id"])
        if key in existing_keys:
            continue
        info = provider_info.get(q["provider_id"], {})
        r = ratings_map.get(key, {})
        store_weekly.append({
            "period": q["period"],
            "provider_id": q["provider_id"],
            "provider_name": q.get("provider_name") or info.get("provider_name"),
            "city_name": info.get("city_name"),
            "orders": 0,
            "merchant_price_uah": 0,
            "aov_uah": None,
            "availability_rate": q.get("availability_rate"),
            "acceptance_rate": q.get("acceptance_rate"),
            "avg_rating": q.get("avg_rating"),
            "avg_review_rating": r.get("avg_review_rating"),
            "reviews_count": r.get("reviews_count", 0),
            "comments_count": r.get("comments_count", 0),
        })

    cursor.close()
    conn.close()

    report_data = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "data_start": DATA_START,
        "data_end": DATA_END,
        "weekly_start": WEEKLY_START,
        "weekly_end": WEEKLY_END,
        "partner_name": PARTNER_NAME,
        "partner_display": PARTNER_DISPLAY,
        "monthly": {
            "financial": fin_m,
            "operational": ops_m,
            "failed_orders": fail_m,
            "failed_reasons": fail_reasons_m,
            "campaigns": camp_m,
            "acceptance_availability": aa_m,
        },
        "weekly": {
            "financial": fin_w,
            "operational": ops_w,
            "failed_orders": fail_w,
            "failed_reasons": fail_reasons_w,
            "campaigns": camp_w,
            "acceptance_availability": aa_w,
        },
        "acceptance_current": aa_current,
        "top_stores": top_stores,
        "network_stores": network_stores,
        "network_store_count": network_store_count,
        "store_weekly": store_weekly,
        "customer_reviews": customer_reviews,
    }

    DATA_PATH.write_text(
        json.dumps(report_data, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"Data saved to {DATA_PATH}")

    print("Generating index.html...")
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    js_data = f"const REPORT_DATA = {json.dumps(report_data, ensure_ascii=False, default=str)};"
    html = template.replace("/*__REPORT_DATA__*/", js_data)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Done! Report written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
