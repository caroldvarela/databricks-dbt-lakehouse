"""
===========================================================
 Silver Layer Pipeline - Delta Live Tables (DLT)
===========================================================

This script defines the Silver layer of the project using 
Delta Live Tables. Based on the raw data in Bronze:

Processed Dimensions:
    - Products
    - Customers
    - Stores
    - Salespersons
    - Campaigns

Rescue columns are removed, data types are standardized, 
    and a `modified_date` column is added. 
Streaming tables are created with change control 
    (SCD Type 1) using `dlt.create_auto_cdc_flow`.

Facts:
    - Sales

Foreign keys and amounts are normalized.  
Data quality rules are applied with `@dlt.expect_all_or_drop` 
       (non-null values, positive amounts, key integrity).

Result: Clean, standardized, and reliable Silver tables, 
ready for dimensional modeling in the Gold layer.
"""

from pyspark.sql.functions import * 
from pyspark.sql.types import * 
import dlt 

###################################################################### 
# DIM_PRODUCTS 
###################################################################### 
@dlt.view( name="trans_products" ) 
def trans_products(): 
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/products/data/") 
    df = df.drop("_rescued_data") \
        .withColumn("product_sk", col("product_sk").cast(IntegerType())) \
        .withColumn("product_id", col("product_id").cast(StringType())) \
        .withColumn("product_name", col("product_name").cast(StringType())) \
        .withColumn("category", col("category").cast(StringType())) \
        .withColumn("brand", col("brand").cast(StringType())) \
        .withColumn("origin_location", col("origin_location").cast(StringType())) \
        .withColumn("modified_date", current_timestamp()) 
    return df 

dlt.create_streaming_table("silver_products") 
dlt.create_auto_cdc_flow( 
    target="silver_products", 
    source="trans_products", 
    keys=["product_sk"], 
    sequence_by=col("modified_date"), 
    stored_as_scd_type=1 
)

###################################################################### 
# DIM_CUSTOMERS 
###################################################################### 
@dlt.view( name="trans_customers" ) 
def trans_customers(): 
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data/") 
    df = df.drop("_rescued_data") \
        .withColumn("customer_sk", col("customer_sk").cast(IntegerType())) \
        .withColumn("customer_id", col("customer_id").cast(StringType())) \
        .withColumn("first_name", col("first_name").cast(StringType())) \
        .withColumn("last_name", col("last_name").cast(StringType())) \
        .withColumn("email", col("email").cast(StringType())) \
        .withColumn("residential_location", col("residential_location").cast(StringType())) \
        .withColumn("customer_segment", col("customer_segment").cast(StringType())) \
        .withColumn("modified_date", current_timestamp()) 
    return df 

dlt.create_streaming_table("silver_customers") 
dlt.create_auto_cdc_flow( 
    target="silver_customers", 
    source="trans_customers", 
    keys=["customer_sk"], 
    sequence_by=col("modified_date"), 
    stored_as_scd_type=1 
)

###################################################################### 
# DIM_STORES 
###################################################################### 
@dlt.view( name="trans_stores" ) 
def trans_stores(): 
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/stores/data/") 
    df = df.drop("_rescued_data") \
        .withColumn("store_sk", col("store_sk").cast(IntegerType())) \
        .withColumn("store_id", col("store_id").cast(StringType())) \
        .withColumn("store_name", col("store_name").cast(StringType())) \
        .withColumn("store_type", col("store_type").cast(StringType())) \
        .withColumn("store_location", col("store_location").cast(StringType())) \
        .withColumn("store_manager_sk", col("store_manager_sk").cast(IntegerType())) \
        .withColumn("modified_date", current_timestamp()) 
    return df 

dlt.create_streaming_table("silver_stores") 
dlt.create_auto_cdc_flow( 
    target="silver_stores", 
    source="trans_stores", 
    keys=["store_sk"], 
    sequence_by=col("modified_date"), 
    stored_as_scd_type=1 
)

###################################################################### 
# DIM_SALESPERSONS 
###################################################################### 
@dlt.view( name="trans_salespersons" ) 
def trans_salespersons(): 
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/salespersons/data/") 
    df = df.drop("_rescued_data") \
        .withColumn("salesperson_sk", col("salesperson_sk").cast(IntegerType())) \
        .withColumn("salesperson_id", col("salesperson_id").cast(StringType())) \
        .withColumn("salesperson_name", col("salesperson_name").cast(StringType())) \
        .withColumn("salesperson_role", col("salesperson_role").cast(StringType())) \
        .withColumn("modified_date", current_timestamp()) 
    return df 

dlt.create_streaming_table("silver_salespersons") 
dlt.create_auto_cdc_flow( 
    target="silver_salespersons", 
    source="trans_salespersons", 
    keys=["salesperson_sk"], 
    sequence_by=col("modified_date"), 
    stored_as_scd_type=1 
)

###################################################################### 
# DIM_CAMPAIGNS 
###################################################################### 
@dlt.view( name="trans_campaigns" ) 
def trans_campaigns(): 
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/campaigns/data/") 
    df = df.drop("_rescued_data") \
        .withColumn("campaign_sk", col("campaign_sk").cast(IntegerType())) \
        .withColumn("campaign_id", col("campaign_id").cast(StringType())) \
        .withColumn("campaign_name", col("campaign_name").cast(StringType())) \
        .withColumn("start_date_sk", col("start_date_sk").cast(IntegerType())) \
        .withColumn("end_date_sk", col("end_date_sk").cast(IntegerType())) \
        .withColumn("campaign_budget", col("campaign_budget").cast(DoubleType())) \
        .withColumn("modified_date", current_timestamp()) 
    return df 

dlt.create_streaming_table("silver_campaigns") 
dlt.create_auto_cdc_flow( 
    target="silver_campaigns", 
    source="trans_campaigns", 
    keys=["campaign_sk"], 
    sequence_by=col("modified_date"), 
    stored_as_scd_type=1 
)

###################################################################### 
# FACT_SALES 
###################################################################### 

@dlt.view( name="trans_sales" ) 
def trans_sales(): 
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/sales/data/") 
    df = df.withColumn("sales_sk", col("sales_sk").cast(IntegerType()))\
        .withColumn("customer_sk", col("customer_sk").cast(IntegerType()))\
        .withColumn("product_sk", col("product_sk").cast(IntegerType()))\
        .withColumn("store_sk", col("store_sk").cast(IntegerType()))\
        .withColumn("salesperson_sk", col("salesperson_sk").cast(IntegerType()))\
        .withColumn("campaign_sk", col("campaign_sk").cast(IntegerType()))\
        .withColumn("total_amount", col("total_amount").cast(DoubleType()))\
        .withColumn("date_sk", col("date_sk").cast(IntegerType()))\
        .withColumn("time_sk", col("time_sk").cast(IntegerType()))\
        .withColumn("modified_date", current_timestamp()) \
        .drop("_rescued_data") 
    return df 

rules = { 
    "sales_sk_not_null": "sales_sk IS NOT NULL", 
    "sales_id_not_null": "sales_id IS NOT NULL", 
    "customer_sk_not_null": "customer_sk IS NOT NULL", 
    "product_sk_not_null": "product_sk IS NOT NULL", 
    "store_sk_not_null": "store_sk IS NOT NULL", 
    "salesperson_sk_not_null": "salesperson_sk IS NOT NULL", 
    "campaign_sk_not_null": "campaign_sk IS NOT NULL", 
    "date_sk_not_null": "date_sk IS NOT NULL", 
    "time_sk_not_null": "time_sk IS NOT NULL", 
    "total_amount_not_null": "total_amount IS NOT NULL", 
    "total_amount_positive": "total_amount RLIKE '^[0-9]+(\\.[0-9]{1,2})?$' AND CAST(total_amount AS DOUBLE) > 0" 
} 

@dlt.table( name="silver_sales" ) 
@dlt.expect_all_or_drop(rules) 
def silver_sales(): 
    df = spark.readStream.table("trans_sales") 
    return df
