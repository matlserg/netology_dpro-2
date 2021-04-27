from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, round

spark = SparkSession \
    .builder \
    .appName("advanced_spark_ht_1") \
    .getOrCreate()

csv_df = spark.read.option('header', True).option('inferSchema', True).csv('owid-covid-data.csv')
df = csv_df.filter(csv_df['date'] == '2021-03-31')\
            .select('iso_code',
                    'location',
                    round(col('total_cases_per_million') / 10_000, 2).alias('recovered_percent '))\
            .orderBy(col('total_cases_per_million').desc())
df.show(15)
