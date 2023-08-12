from pyspark.sql import functions as F

amazon_shipment

amazon_shipment.groupBy(
    F.date_format('shipment_date', 'yyyy-MM').alias('month')
).agg(
    F.countDistinct('shipment_id', 'sub_id').alias('id')
).select(['month', 'id']).toPandas()
