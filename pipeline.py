import os
import schedule
import time
import logging
import pandas as pd

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)

# ── Rutas ─────────────────────────────────────────────────
RAW_PATH = "data/raw"
PROCESSED_PATH = "data/processed"

# ── Descarga ──────────────────────────────────────────────
def download_dataset():
    logging.info("Descargando dataset...")
    os.makedirs(RAW_PATH, exist_ok=True)
    os.system(f"kaggle datasets download -d olistbr/brazilian-ecommerce -p {RAW_PATH} --unzip")
    logging.info("Dataset descargado correctamente")

# ── ETL ───────────────────────────────────────────────────
def run_etl():
    logging.info("Iniciando ETL...")
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    orders = pd.read_csv(f"{RAW_PATH}/olist_orders_dataset.csv")
    customers = pd.read_csv(f"{RAW_PATH}/olist_customers_dataset.csv")
    items = pd.read_csv(f"{RAW_PATH}/olist_order_items_dataset.csv")
    reviews = pd.read_csv(f"{RAW_PATH}/olist_order_reviews_dataset.csv")
    products = pd.read_csv(f"{RAW_PATH}/olist_products_dataset.csv")
    translations = pd.read_csv(f"{RAW_PATH}/product_category_name_translation.csv")

    orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"])
    orders["order_delivered_customer_date"] = pd.to_datetime(orders["order_delivered_customer_date"])
    orders["order_estimated_delivery_date"] = pd.to_datetime(orders["order_estimated_delivery_date"])

    df = orders.merge(customers, on="customer_id") \
               .merge(items, on="order_id") \
               .merge(products[["product_id", "product_category_name"]], on="product_id", how="left") \
               .merge(translations, on="product_category_name", how="left") \
               .merge(reviews[["order_id", "review_score"]].drop_duplicates(subset="order_id"), on="order_id", how="left")

    df = df.dropna(subset=["order_delivered_customer_date"])
    df["entrega_a_tiempo"] = df["order_delivered_customer_date"] <= df["order_estimated_delivery_date"]
    df["price"] = df["price"].astype("float64")

    df.to_excel(f"{PROCESSED_PATH}/orders_clean.xlsx", index=False)
    logging.info(f"ETL completado: {len(df)} registros exportados")

# ── Métricas ──────────────────────────────────────────────
def run_metrics():
    logging.info("Calculando métricas...")
    df = pd.read_excel(f"{PROCESSED_PATH}/orders_clean.xlsx")
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])

    # RFM
    fecha_ref = df["order_purchase_timestamp"].max()

    rfm = df.groupby("customer_unique_id").agg(
        recency=("order_purchase_timestamp", lambda x: (fecha_ref - x.max()).days),
        frequency=("order_id", "nunique"),
        monetary=("price", "sum")
    ).reset_index()

    rfm["r_score"] = pd.qcut(rfm["recency"], 3, labels=[3, 2, 1])
    rfm["f_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 3, labels=[1, 2, 3])
    rfm["m_score"] = pd.qcut(rfm["monetary"], 3, labels=[1, 2, 3])
    rfm["rfm_score"] = rfm["r_score"].astype(int) + rfm["f_score"].astype(int) + rfm["m_score"].astype(int)
    rfm["segmento"] = pd.cut(rfm["rfm_score"], bins=[2, 4, 6, 9],
                              labels=["En Riesgo", "Leales", "Campeones"])

    rfm["monetary"] = rfm["monetary"].round(2)
    rfm.to_excel(f"{PROCESSED_PATH}/rfm.xlsx", index=False)

    # SLA por estado
    sla = df.groupby("customer_state").agg(
        total_ordenes=("order_id", "nunique"),
        entregas_a_tiempo=("entrega_a_tiempo", "sum")
    ).reset_index()

    sla["pct_a_tiempo"] = (sla["entregas_a_tiempo"] / sla["total_ordenes"] * 100).round(1)
    sla.to_excel(f"{PROCESSED_PATH}/sla_por_estado.xlsx", index=False)

    # Ventas mensuales
    df["mes"] = df["order_purchase_timestamp"].dt.to_period("M").dt.to_timestamp()
    ventas = df.groupby("mes").agg(
        ordenes=("order_id", "nunique"),
        revenue=("price", "sum")
    ).reset_index()
    ventas["revenue"] = ventas["revenue"].round(2)
    ventas.to_excel(f"{PROCESSED_PATH}/ventas_mensuales.xlsx", index=False)
    logging.info("Métricas calculadas y exportadas")

# ── Orquestador ───────────────────────────────────────────
def run_pipeline():
    logging.info("Iniciando pipeline...")
    try:
        download_dataset()
        run_etl()
        run_metrics()
        logging.info("Pipeline completado exitosamente")
    except Exception as e:
        logging.error(f"Error en pipeline: {e}")

schedule.every().day.at("08:00").do(run_pipeline)

if __name__ == "__main__":
    run_pipeline()
    while True:
        schedule.run_pending()
        time.sleep(60)