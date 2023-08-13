import pyspark
from pyspark.sql import functions as f

billboard_top_100_year_end = billboard_top_100_year_end.filter((f.col('year')==2010) & (f.col('year_rank').between(1,10)))

billboard_top_100_year_end.select('year_rank', 'group_name', 'song_name').dropDuplicates(subset=['song_name']).toPandas().head(10)