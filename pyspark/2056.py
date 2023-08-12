from pyspark.sql import functions as F
from pyspark.sql.types import StringType

amazon_shipment

amazon_shipment.groupBy(
    F.date_format('shipment_date', 'yyyy-MM').alias('month')
).agg(
    F.countDistinct('shipment_id', 'sub_id').alias('cnt')
).select(['month', 'cnt']).toPandas()
