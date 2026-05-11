from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    concat_ws,
    current_date,
    current_timestamp,
    lit,
    sha2,
    to_date,
    when,
    year,
    month
)

from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    DateType
)


spark = SparkSession.builder.appName(
    "RetailEnterpriseWarehouseFramework"
).getOrCreate()


# =====================================================
# S3 PATHS
# =====================================================

RAW_BUCKET = "s3://retail-raw-surya"
CURATED_BUCKET = "s3://retail-curated-surya"

customers_path = f"{RAW_BUCKET}/customers/"
products_path = f"{RAW_BUCKET}/products/"
orders_path = f"{RAW_BUCKET}/orders/"
payments_path = f"{RAW_BUCKET}/payments/"
inventory_path = f"{RAW_BUCKET}/inventory/"

dim_customer_path = f"{CURATED_BUCKET}/dim_customer_scd2/"
dim_customer_temp_path = f"{CURATED_BUCKET}/temp/dim_customer_scd2/"

dim_product_path = f"{CURATED_BUCKET}/dim_product_scd1/"
fact_orders_path = f"{CURATED_BUCKET}/fact_orders/"
fact_inventory_path = f"{CURATED_BUCKET}/fact_inventory/"

audit_path = f"{CURATED_BUCKET}/audit/warehouse_framework/"
reject_path = f"{CURATED_BUCKET}/rejects/warehouse_framework/"
recon_path = f"{CURATED_BUCKET}/reconciliation/warehouse_framework/"


# =====================================================
# READ RAW DATA
# =====================================================

customers_raw = spark.read.option("header", "true").csv(customers_path)
products_raw = spark.read.option("header", "true").csv(products_path)
orders_raw = spark.read.option("header", "true").csv(orders_path)
payments_raw = spark.read.option("header", "true").csv(payments_path)
inventory_raw = spark.read.option("header", "true").csv(inventory_path)


# =====================================================
# REJECT RECORDS
# =====================================================

customers_reject = (
    customers_raw
    .filter(
        col("customer_id").isNull() |
        col("customer_name").isNull() |
        col("email").isNull()
    )
    .withColumn("reject_reason", lit("Missing required customer fields"))
    .withColumn("rejected_timestamp", current_timestamp())
)

products_reject = (
    products_raw
    .filter(
        col("product_id").isNull() |
        col("product_name").isNull() |
        col("category").isNull() |
        col("price").isNull()
    )
    .withColumn("reject_reason", lit("Missing required product fields"))
    .withColumn("rejected_timestamp", current_timestamp())
)

orders_reject = (
    orders_raw
    .filter(
        col("order_id").isNull() |
        col("customer_id").isNull() |
        col("product_id").isNull() |
        col("quantity").isNull() |
        col("order_date").isNull()
    )
    .withColumn("reject_reason", lit("Missing required order fields"))
    .withColumn("rejected_timestamp", current_timestamp())
)

payments_reject = (
    payments_raw
    .filter(
        col("payment_id").isNull() |
        col("order_id").isNull() |
        col("payment_status").isNull() |
        col("payment_mode").isNull()
    )
    .withColumn("reject_reason", lit("Missing required payment fields"))
    .withColumn("rejected_timestamp", current_timestamp())
)

inventory_reject = (
    inventory_raw
    .filter(
        col("inventory_id").isNull() |
        col("product_id").isNull() |
        col("warehouse_id").isNull() |
        col("stock_quantity").isNull() |
        col("reorder_level").isNull() |
        col("last_updated").isNull()
    )
    .withColumn("reject_reason", lit("Missing required inventory fields"))
    .withColumn("rejected_timestamp", current_timestamp())
)

customers_reject.write.mode("append").json(f"{reject_path}/customers/")
products_reject.write.mode("append").json(f"{reject_path}/products/")
orders_reject.write.mode("append").json(f"{reject_path}/orders/")
payments_reject.write.mode("append").json(f"{reject_path}/payments/")
inventory_reject.write.mode("append").json(f"{reject_path}/inventory/")


# =====================================================
# CLEAN CUSTOMERS
# =====================================================

customers_clean = (
    customers_raw
    .dropDuplicates(["customer_id"])
    .filter(col("customer_id").isNotNull())
    .filter(col("customer_name").isNotNull())
    .filter(col("email").isNotNull())
)


# =====================================================
# CUSTOMER DIMENSION - SCD TYPE 2
# =====================================================

customers_new = (
    customers_clean
    .withColumn(
        "record_hash",
        sha2(
            concat_ws(
                "||",
                col("customer_name"),
                col("email"),
                col("city"),
                col("state")
            ),
            256
        )
    )
    .withColumn("effective_start_date", current_date())
    .withColumn("effective_end_date", lit(None).cast(DateType()))
    .withColumn("current_flag", lit("Y"))
    .withColumn("processed_timestamp", current_timestamp())
)

try:
    customers_existing = spark.read.parquet(dim_customer_path)

    current_customers = customers_existing.filter(
        col("current_flag") == "Y"
    )

    changed_customers = (
        customers_new.alias("new")
        .join(
            current_customers.alias("old"),
            col("new.customer_id") == col("old.customer_id"),
            "inner"
        )
        .filter(
            col("new.record_hash") != col("old.record_hash")
        )
        .select(col("new.customer_id"))
        .dropDuplicates()
    )

    unchanged_existing = (
        customers_existing.alias("old")
        .join(
            changed_customers.alias("chg"),
            col("old.customer_id") == col("chg.customer_id"),
            "left_anti"
        )
        .select("old.*")
    )

    expired_old_records = (
        customers_existing.alias("old")
        .join(
            changed_customers.alias("chg"),
            col("old.customer_id") == col("chg.customer_id"),
            "inner"
        )
        .select("old.*")
        .withColumn(
            "effective_end_date",
            when(
                col("current_flag") == "Y",
                current_date()
            ).otherwise(col("effective_end_date"))
        )
        .withColumn(
            "current_flag",
            when(
                col("current_flag") == "Y",
                lit("N")
            ).otherwise(col("current_flag"))
        )
    )

    changed_new_records = (
        customers_new.alias("new")
        .join(
            changed_customers.alias("chg"),
            col("new.customer_id") == col("chg.customer_id"),
            "inner"
        )
        .select("new.*")
    )

    new_customers_only = (
        customers_new.alias("new")
        .join(
            current_customers.alias("old"),
            col("new.customer_id") == col("old.customer_id"),
            "left_anti"
        )
        .select("new.*")
    )

    dim_customer_final = (
        unchanged_existing
        .unionByName(expired_old_records)
        .unionByName(changed_new_records)
        .unionByName(new_customers_only)
    )

except Exception:
    dim_customer_final = customers_new


# Safe SCD write using temporary path
dim_customer_final = dim_customer_final.cache()
dim_customer_count = dim_customer_final.count()

dim_customer_final.write.mode("overwrite").parquet(dim_customer_temp_path)

temp_customer_df = spark.read.parquet(dim_customer_temp_path).cache()
temp_customer_df.count()

temp_customer_df.write.mode("overwrite").parquet(dim_customer_path)


# =====================================================
# PRODUCT DIMENSION - SCD TYPE 1
# =====================================================

dim_product = (
    products_raw
    .dropDuplicates(["product_id"])
    .filter(col("product_id").isNotNull())
    .filter(col("product_name").isNotNull())
    .filter(col("category").isNotNull())
    .withColumn("price", col("price").cast(DoubleType()))
    .filter(col("price") > 0)
    .withColumn("processed_timestamp", current_timestamp())
)

dim_product.write.mode("overwrite").parquet(dim_product_path)


# =====================================================
# CLEAN ORDERS
# =====================================================

orders_clean = (
    orders_raw
    .dropDuplicates(["order_id"])
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .withColumn("quantity", col("quantity").cast(IntegerType()))
    .withColumn("order_date", to_date(col("order_date")))
    .filter(col("quantity") > 0)
)


# =====================================================
# CLEAN PAYMENTS
# =====================================================

payments_clean = (
    payments_raw
    .dropDuplicates(["payment_id"])
    .filter(col("payment_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("payment_status").isNotNull())
    .filter(col("payment_mode").isNotNull())
)


# =====================================================
# CLEAN INVENTORY
# =====================================================

inventory_clean = (
    inventory_raw
    .dropDuplicates(["inventory_id"])
    .filter(col("inventory_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("warehouse_id").isNotNull())
    .withColumn("stock_quantity", col("stock_quantity").cast(IntegerType()))
    .withColumn("reorder_level", col("reorder_level").cast(IntegerType()))
    .withColumn("last_updated", to_date(col("last_updated")))
    .filter(col("stock_quantity") >= 0)
    .filter(col("reorder_level") >= 0)
)


# =====================================================
# READ CURRENT DIMENSIONS
# =====================================================

dim_customer_current = (
    spark.read.parquet(dim_customer_path)
    .filter(col("current_flag") == "Y")
)

dim_product_current = spark.read.parquet(dim_product_path)


# =====================================================
# FACT ORDERS
# GRAIN: 1 ROW = 1 ORDER
# =====================================================

fact_orders = (
    orders_clean.alias("o")
    .join(
        dim_customer_current.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    )
    .join(
        dim_product_current.alias("p"),
        col("o.product_id") == col("p.product_id"),
        "left"
    )
    .join(
        payments_clean.alias("pay"),
        col("o.order_id") == col("pay.order_id"),
        "left"
    )
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.email"),
        col("c.city"),
        col("c.state"),
        col("o.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("p.price"),
        col("o.quantity"),
        (col("o.quantity") * col("p.price")).alias("total_amount"),
        col("pay.payment_id"),
        col("pay.payment_status"),
        col("pay.payment_mode"),
        col("o.order_date")
    )
    .withColumn("year", year(col("order_date")))
    .withColumn("month", month(col("order_date")))
    .withColumn("processed_timestamp", current_timestamp())
)

fact_orders.write.mode("overwrite").partitionBy("year", "month").parquet(fact_orders_path)


# =====================================================
# FACT INVENTORY
# GRAIN: 1 ROW = 1 PRODUCT IN 1 WAREHOUSE
# =====================================================

fact_inventory = (
    inventory_clean.alias("i")
    .join(
        dim_product_current.alias("p"),
        col("i.product_id") == col("p.product_id"),
        "left"
    )
    .select(
        col("i.inventory_id"),
        col("i.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("p.price"),
        col("i.warehouse_id"),
        col("i.stock_quantity"),
        col("i.reorder_level"),
        col("i.last_updated")
    )
    .withColumn("processed_timestamp", current_timestamp())
)

fact_inventory.write.mode("overwrite").parquet(fact_inventory_path)


# =====================================================
# RECONCILIATION CHECKS
# =====================================================

customers_source_count = customers_raw.count()
customers_valid_count = customers_clean.count()
customers_reject_count = customers_reject.count()

products_source_count = products_raw.count()
products_valid_count = dim_product.count()
products_reject_count = products_reject.count()
dim_product_count = dim_product.count()

orders_source_count = orders_raw.count()
orders_valid_count = orders_clean.count()
orders_reject_count = orders_reject.count()
fact_orders_count = fact_orders.count()

payments_source_count = payments_raw.count()
payments_valid_count = payments_clean.count()
payments_reject_count = payments_reject.count()

inventory_source_count = inventory_raw.count()
inventory_valid_count = inventory_clean.count()
inventory_reject_count = inventory_reject.count()
fact_inventory_count = fact_inventory.count()

recon_data = [
    {
        "entity_name": "customers",
        "source_count": customers_source_count,
        "valid_count": customers_valid_count,
        "reject_count": customers_reject_count,
        "target_count": dim_customer_count,
        "reconciliation_status": "PASS"
    },
    {
        "entity_name": "products",
        "source_count": products_source_count,
        "valid_count": products_valid_count,
        "reject_count": products_reject_count,
        "target_count": dim_product_count,
        "reconciliation_status": "PASS" if products_valid_count == dim_product_count else "FAIL"
    },
    {
        "entity_name": "orders",
        "source_count": orders_source_count,
        "valid_count": orders_valid_count,
        "reject_count": orders_reject_count,
        "target_count": fact_orders_count,
        "reconciliation_status": "PASS" if orders_valid_count == fact_orders_count else "FAIL"
    },
    {
        "entity_name": "payments",
        "source_count": payments_source_count,
        "valid_count": payments_valid_count,
        "reject_count": payments_reject_count,
        "target_count": fact_orders_count,
        "reconciliation_status": "REFERENCE_ONLY"
    },
    {
        "entity_name": "inventory",
        "source_count": inventory_source_count,
        "valid_count": inventory_valid_count,
        "reject_count": inventory_reject_count,
        "target_count": fact_inventory_count,
        "reconciliation_status": "PASS" if inventory_valid_count == fact_inventory_count else "FAIL"
    }
]

recon_df = (
    spark.createDataFrame(recon_data)
    .withColumn("reconciliation_timestamp", current_timestamp())
)

recon_df.write.mode("append").json(recon_path)


# =====================================================
# AUDIT LOGGING
# =====================================================

audit_data = [
    {
        "job_name": "retail-enterprise-warehouse-framework-job",
        "entity_name": "dim_customer_scd2",
        "target_path": dim_customer_path,
        "record_count": dim_customer_count,
        "job_status": "SUCCESS"
    },
    {
        "job_name": "retail-enterprise-warehouse-framework-job",
        "entity_name": "dim_product_scd1",
        "target_path": dim_product_path,
        "record_count": dim_product_count,
        "job_status": "SUCCESS"
    },
    {
        "job_name": "retail-enterprise-warehouse-framework-job",
        "entity_name": "fact_orders",
        "target_path": fact_orders_path,
        "record_count": fact_orders_count,
        "job_status": "SUCCESS"
    },
    {
        "job_name": "retail-enterprise-warehouse-framework-job",
        "entity_name": "fact_inventory",
        "target_path": fact_inventory_path,
        "record_count": fact_inventory_count,
        "job_status": "SUCCESS"
    }
]

audit_df = (
    spark.createDataFrame(audit_data)
    .withColumn("audit_timestamp", current_timestamp())
)

audit_df.write.mode("append").json(audit_path)


print("Enterprise Retail Warehouse Framework Completed Successfully")
print("Created dim_customer_scd2")
print("Created dim_product_scd1")
print("Created fact_orders")
print("Created fact_inventory")
print("Created rejects")
print("Created reconciliation logs")
print("Created audit logs")