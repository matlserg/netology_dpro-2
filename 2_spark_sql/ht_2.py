from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, desc, round

spark = SparkSession \
    .builder \
    .appName("advanced_spark_ht_2") \
    .getOrCreate()

csv_df = spark.read.option('header', True).option('inferSchema', True).csv('owid-covid-data.csv')

df_select = csv_df.filter((csv_df['date'] <= '2021-03-31') & (csv_df['date'] >= '2021-03-25'))\
            .filter(~csv_df.iso_code.contains('OWID'))\
            .select('iso_code', 'location', 'new_cases')

df = df_select.groupBy('iso_code', 'location').agg(sum("new_cases").alias('sum_new_cases'))\
    .orderBy(col('sum_new_cases').desc())

df.show(10)
