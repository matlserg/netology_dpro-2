from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("advanced_spark_ht_2") \
    .getOrCreate()

csv_df = spark.read.option('header', True).option('inferSchema', True).csv('owid-covid-data.csv')

df_select = csv_df.filter((csv_df['date'] <= '2021-03-31') & (csv_df['date'] >= '2021-03-25'))\
            .filter(csv_df.iso_code == 'RUS')\
            .select('iso_code', 'date', col('new_cases').alias('new_cases_today'))


df = df_select.withColumn('new_cases_yesterday',
                          lag('new_cases_today', 1).over(Window.partitionBy("iso_code").orderBy("date")))\
            .select('date',
                    'new_cases_yesterday',
                    'new_cases_today',
                    (col('new_cases_today') - col('new_cases_yesterday')).alias('new_cases_delta'))

df.show()


"""
+----------+-------------------+---------------+---------------+
|      date|new_cases_yesterday|new_cases_today|new_cases_delta|
+----------+-------------------+---------------+---------------+
|2021-03-25|               null|         9128.0|           null|
|2021-03-26|             9128.0|         9073.0|          -55.0|
|2021-03-27|             9073.0|         8783.0|         -290.0|
|2021-03-28|             8783.0|         8979.0|          196.0|
|2021-03-29|             8979.0|         8589.0|         -390.0|
|2021-03-30|             8589.0|         8162.0|         -427.0|
|2021-03-31|             8162.0|         8156.0|           -6.0|
+----------+-------------------+---------------+---------------+
"""