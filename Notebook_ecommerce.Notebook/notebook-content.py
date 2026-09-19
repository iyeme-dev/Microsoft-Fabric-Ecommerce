# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "921de468-ef2a-4852-92f0-18552c875b6a",
# META       "default_lakehouse_name": "ecommerce_lakehouse",
# META       "default_lakehouse_workspace_id": "47b0f787-351b-4245-bead-f5c7f854fc67",
# META       "known_lakehouses": [
# META         {
# META           "id": "921de468-ef2a-4852-92f0-18552c875b6a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# # Ecommerce Medallion Architecture
# 
# This notebook transforms ecommerce data through the Bronze, Silver and Gold layers.
# 
# ## Flow
# 
# Bronze Parquet Files → Bronze Delta Tables → Silver Cleaned Tables → Gold Customer 360

# MARKDOWN ********************

# # 1. READ BRONZE DATA AND CREATE DATAFRAMES

# CELL ********************

from pyspark.sql.functions import (
    col,
    lower,
    trim,
    initcap,
    when,
    to_date,
    regexp_replace
)
from pyspark.sql.types import DoubleType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers_raw = spark.read.parquet("Files/Bronze/customers.parquet")

orders_raw = spark.read.parquet("Files/Bronze/orders.parquet")

payments_raw = spark.read.parquet("Files/Bronze/payments.parquet")

support_raw = spark.read.parquet("Files/Bronze/support_tickets.parquet")

web_raw = spark.read.parquet("Files/Bronze/web_activities.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(customers_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Customers

# CELL ********************

display(customers_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Orders

# CELL ********************

display(orders_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Payments

# CELL ********************

display(payments_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Support

# CELL ********************

display(support_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Web activity

# CELL ********************

display(web_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2. CREATE BRONZE DELTA TABLES


# MARKDOWN ********************


# CELL ********************

customers_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customers")

orders_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("orders")

payments_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("payments")

support_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("support")

web_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("web")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 3. SILVER LAYER - CLEAN AND STANDARDISE THE DATA

# MARKDOWN ********************

# ## Customer Data - Clean

# CELL ********************

customers = spark.table("customers")

customers_clean = (
    customers

    # Standardise email
    .withColumn(
        "email",
        lower(trim(col("email")))
    )

    # Standardise customer names
    .withColumn(
        "name",
        initcap(trim(col("name")))
    )

    # Standardise gender values
    .withColumn(
        "gender",
        when(
            lower(trim(col("gender"))).isin("f", "female"),
            "Female"
        )
        .when(
            lower(trim(col("gender"))).isin("m", "male"),
            "Male"
        )
        .otherwise("Other")
    )

    # Convert DOB into a proper date
    .withColumn(
        "dob",
        to_date(
            regexp_replace(col("dob"), "/", "-")
        )
    )

    # Standardise location
    .withColumn(
        "location",
        initcap(trim(col("location")))
    )

    # Remove duplicate customers
    .dropDuplicates(["customer_id"])

    # Customer ID must exist
    .dropna(subset=["customer_id"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(customers_clean.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Orders Data - Clean

# MARKDOWN ********************


# CELL ********************

orders = spark.table("orders")

orders_clean = (
    orders

    .withColumn(
        "order_date",
        when(
            col("order_date").rlike(r"^\d{4}/\d{2}/\d{2}$"),
            to_date(col("order_date"), "yyyy/MM/dd")
        )
        .when(
            col("order_date").rlike(r"^\d{2}-\d{2}-\d{4}$"),
            to_date(col("order_date"), "dd-MM-yyyy")
        )
        .when(
            col("order_date").rlike(r"^\d{8}$"),
            to_date(col("order_date"), "yyyyMMdd")
        )
        .otherwise(
            to_date(col("order_date"))
        )
    )

    # Convert amount to a numeric value
    .withColumn(
        "amount",
        col("amount").cast(DoubleType())
    )

    # Negative order amounts are treated as invalid
    .withColumn(
        "amount",
        when(col("amount") < 0, None)
        .otherwise(col("amount"))
    )

    # Standardise status
    .withColumn(
        "status",
        initcap(trim(col("status")))
    )

    # Remove duplicate orders
    .dropDuplicates(["order_id"])

    # Important fields must exist
    .dropna(
        subset=[
            "order_id",
            "customer_id",
            "order_date"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(orders_clean.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Payments Data - Clean

# CELL ********************

payments = spark.table("payments")

payments_clean = (
    payments

    # Standardise payment date
    .withColumn(
        "payment_date",
        to_date(
            regexp_replace(col("payment_date"), "/", "-")
        )
    )

    # Standardise payment method
    .withColumn(
        "payment_method",
        initcap(trim(col("payment_method")))
    )

    .replace(
        {
            "Creditcard": "Credit Card",
            "Credit Card": "Credit Card"
        },
        subset=["payment_method"]
    )

    # Standardise payment status
    .withColumn(
        "payment_status",
        initcap(trim(col("payment_status")))
    )

    # Convert amount to numeric
    .withColumn(
        "amount",
        col("amount").cast(DoubleType())
    )

    # Negative payment amounts become NULL
    .withColumn(
        "amount",
        when(col("amount") < 0, None)
        .otherwise(col("amount"))
    )

    .dropDuplicates(["payment_id"])

    .dropna(
        subset=[
            "payment_id",
            "customer_id",
            "payment_date"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(payments_clean.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payments_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_payments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Support Ticket Data - Clean

# CELL ********************

support = spark.table("support")

support_clean = (
    support

    # Standardise ticket dates
    .withColumn(
        "ticket_date",
        to_date(
            regexp_replace(col("ticket_date"), "/", "-")
        )
    )

    # Clean issue type
    .withColumn(
        "issue_type",
        initcap(trim(col("issue_type")))
    )

    # Clean resolution status
    .withColumn(
        "resolution_status",
        initcap(trim(col("resolution_status")))
    )

    # Replace placeholder values with NULL
    .replace(
        {
            "Na": None,
            "N/A": None,
            "": None
        },
        subset=[
            "issue_type",
            "resolution_status"
        ]
    )

    .dropDuplicates(["ticket_id"])

    .dropna(
        subset=[
            "ticket_id",
            "customer_id",
            "ticket_date"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(support_clean.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

support_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_support")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Web Activity Data - Clean

# MARKDOWN ********************


# CELL ********************

web = spark.table("web")

web_clean = (
    web

    # Standardise session date
    .withColumn(
        "session_time",
        to_date(
            regexp_replace(col("session_time"), "/", "-")
        )
    )

    # Standardise page path
    .withColumn(
        "page_viewed",
        lower(trim(col("page_viewed")))
    )

    # Standardise device type
    .withColumn(
        "device_type",
        initcap(trim(col("device_type")))
    )

    .dropDuplicates(["session_id"])

    .dropna(
        subset=[
            "session_id",
            "customer_id",
            "session_time",
            "page_viewed"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(web_clean.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

web_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_web")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_tables = {
    "customers": spark.table("silver_customers"),
    "orders": spark.table("silver_orders"),
    "payments": spark.table("silver_payments"),
    "support": spark.table("silver_support"),
    "web": spark.table("silver_web")
}

for table_name, df in silver_tables.items():
    print(f"{table_name}: {df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 4. GOLD LAYER - CUSTOMER 360

# CELL ********************

c = spark.table("silver_customers").alias("c")
o = spark.table("silver_orders").alias("o")
p = spark.table("silver_payments").alias("p")
s = spark.table("silver_support").alias("s")
w = spark.table("silver_web").alias("w")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build Customer 360

# CELL ********************

customer360 = (
    c

    .join(
        o,
        col("c.customer_id") == col("o.customer_id"),
        "left"
    )

    .join(
        p,
        col("c.customer_id") == col("p.customer_id"),
        "left"
    )

    .join(
        s,
        col("c.customer_id") == col("s.customer_id"),
        "left"
    )

    .join(
        w,
        col("c.customer_id") == col("w.customer_id"),
        "left"
    )

    .select(

        # CUSTOMER
        col("c.customer_id").alias("customer_id"),
        col("c.name").alias("name"),
        col("c.email").alias("email"),
        col("c.gender").alias("gender"),
        col("c.dob").alias("dob"),
        col("c.location").alias("location"),

        # ORDER
        col("o.order_id").alias("order_id"),
        col("o.order_date").alias("order_date"),
        col("o.amount").alias("order_amount"),
        col("o.status").alias("order_status"),

        # PAYMENT
        col("p.payment_method").alias("payment_method"),
        col("p.payment_status").alias("payment_status"),
        col("p.amount").alias("payment_amount"),

        # SUPPORT
        col("s.ticket_id").alias("ticket_id"),
        col("s.issue_type").alias("issue_type"),
        col("s.ticket_date").alias("ticket_date"),
        col("s.resolution_status").alias("resolution_status"),

        # WEB
        col("w.page_viewed").alias("page_viewed"),
        col("w.device_type").alias("device_type"),
        col("w.session_time").alias("session_time")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(customer360.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer360.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_customer360")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Confirm the Gold table

# CELL ********************

gold_customer360 = spark.table("gold_customer360")

display(gold_customer360.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Gold table rows:", gold_customer360.count())
print("Gold table columns:", len(gold_customer360.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_customer360.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Customer Summary

# CELL ********************

customer_summary = (
    customer360

    .groupBy(
        "customer_id",
        "name",
        "email",
        "gender",
        "dob",
        "location"
    )

    .agg(
        countDistinct("order_id").alias("total_orders"),

        sum("order_amount").alias("total_order_value"),

        avg("order_amount").alias("average_order_value"),

        sum("payment_amount").alias("total_payments"),

        countDistinct("ticket_id").alias("support_tickets"),

        countDistinct("page_viewed").alias("pages_viewed"),

        max("order_date").alias("last_order_date"),

        max("session_time").alias("last_web_activity")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(customer_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_customer_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
