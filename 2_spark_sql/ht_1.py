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

"""
+--------+-------------+------------------+
|iso_code|     location|recovered_percent |
+--------+-------------+------------------+
|     AND|      Andorra|             15.54|
|     MNE|   Montenegro|             14.52|
|     CZE|      Czechia|             14.31|
|     SMR|   San Marino|             13.94|
|     SVN|     Slovenia|             10.37|
|     LUX|   Luxembourg|              9.85|
|     ISR|       Israel|              9.63|
|     USA|United States|               9.2|
|     SRB|       Serbia|              8.83|
|     BHR|      Bahrain|              8.49|
|     PAN|       Panama|              8.23|
|     PRT|     Portugal|              8.06|
|     EST|      Estonia|              8.02|
|     SWE|       Sweden|              7.97|
|     LTU|    Lithuania|              7.94|
+--------+-------------+------------------+
"""