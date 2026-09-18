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

from pyspark.sql.functions import *
from pyspark.sql.types import *

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

