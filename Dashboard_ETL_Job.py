import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    new_dfc = dfc.select(list(dfc.keys())[0]).repartition(1)
    return(DynamicFrameCollection({"CustomTransform0": new_dfc}, glueContext))
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Product
Product_node1722916926607 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://sales-customer-dashboard/data/product/Products_utf8.csv"], "recurse": True}, transformation_ctx="Product_node1722916926607")

# Script generated for node Customer
Customer_node1722539750004 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://sales-customer-dashboard/data/customer/Customers_utf8.csv"], "recurse": True}, transformation_ctx="Customer_node1722539750004")

# Script generated for node Order
Order_node1722540245501 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://sales-customer-dashboard/data/order/Orders_utf8.csv"]}, transformation_ctx="Order_node1722540245501")

# Script generated for node Location
Location_node1722916992929 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://sales-customer-dashboard/data/location/Location_utf8.csv"], "recurse": True}, transformation_ctx="Location_node1722916992929")

# Script generated for node Product (Change Schema)
ProductChangeSchema_node1722916954168 = ApplyMapping.apply(frame=Product_node1722916926607, mappings=[("product id", "string", "product_id", "string"), ("category", "string", "category", "string"), ("sub-category", "string", "sub_category", "string"), ("product name", "string", "product_name", "string")], transformation_ctx="ProductChangeSchema_node1722916954168")

# Script generated for node Customer (Change Schema)
CustomerChangeSchema_node1722546110782 = ApplyMapping.apply(frame=Customer_node1722539750004, mappings=[("customer id", "string", "customer_id", "varchar"), ("customer name", "string", "customer_name", "string")], transformation_ctx="CustomerChangeSchema_node1722546110782")

# Script generated for node Order (Change Schema)
OrderChangeSchema_node1722540319097 = ApplyMapping.apply(frame=Order_node1722540245501, mappings=[("order id", "string", "order_id", "varchar"), ("order date", "string", "order_date", "date"), ("customer id", "string", "customer_id", "varchar"), ("segment", "string", "segment", "string"), ("postal code", "string", "postal_code", "string"), ("product id", "string", "product_id", "string"), ("sales", "string", "sales", "float"), ("quantity", "string", "quantity", "int"), ("discount", "string", "discount", "float"), ("profit", "string", "profit", "float")], transformation_ctx="OrderChangeSchema_node1722540319097")

# Script generated for node Change Schema
ChangeSchema_node1722917021605 = ApplyMapping.apply(frame=Location_node1722916992929, mappings=[("postal code", "string", "postal_code", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("region", "string", "region", "string")], transformation_ctx="ChangeSchema_node1722917021605")

# Script generated for node SQL Query
SqlQuery811 = '''
WITH first_order AS (SELECT customer_id, order_date,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date ASC) AS rank
FROM o)

SELECT c.customer_id, c.customer_name, fo.order_date AS first_order_date
FROM c LEFT JOIN first_order fo
ON c.customer_id = fo.customer_id
WHERE fo.rank = 1
'''
SQLQuery_node1722546169017 = sparkSqlQuery(glueContext, query = SqlQuery811, mapping = {"o":OrderChangeSchema_node1722540319097, "c":CustomerChangeSchema_node1722546110782}, transformation_ctx = "SQLQuery_node1722546169017")

# Script generated for node Custom Transform
CustomTransform_node1723080137686 = MyTransform(glueContext, DynamicFrameCollection({"SQLQuery_node1722546169017": SQLQuery_node1722546169017}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1723080159757 = SelectFromCollection.apply(dfc=CustomTransform_node1723080137686, key=list(CustomTransform_node1723080137686.keys())[0], transformation_ctx="SelectFromCollection_node1723080159757")

# Script generated for node Product_etl
Product_etl_node1722916975856 = glueContext.write_dynamic_frame.from_options(frame=ProductChangeSchema_node1722916954168, connection_type="s3", format="glueparquet", connection_options={"path": "s3://sales-customer-dashboard/output_data/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Product_etl_node1722916975856")

# Script generated for node Order_etl
Order_etl_node1722916883837 = glueContext.write_dynamic_frame.from_options(frame=OrderChangeSchema_node1722540319097, connection_type="s3", format="glueparquet", connection_options={"path": "s3://sales-customer-dashboard/output_data/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Order_etl_node1722916883837")

# Script generated for node Location_etl
Location_etl_node1722917033018 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1722917021605, connection_type="s3", format="glueparquet", connection_options={"path": "s3://sales-customer-dashboard/output_data/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Location_etl_node1722917033018")

# Script generated for node Customer_etl
Customer_etl_node1722727550044 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1723080159757, connection_type="s3", format="glueparquet", connection_options={"path": "s3://sales-customer-dashboard/output_data/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Customer_etl_node1722727550044")

job.commit()