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

