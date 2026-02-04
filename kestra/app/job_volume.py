from pyspark.sql import SparkSession
# 1️⃣ Ініціалізуємо Spark
spark = SparkSession.builder.appName("read-transform").getOrCreate()
# 2️⃣ Створюємо DataFrame (наприклад числа від 0 до 9)
df = spark.range(0, 10).withColumnRenamed("id", "number")
# 3️⃣ Пишемо у Parquet у змонтовану папку /data
df.write.mode("overwrite").parquet("/data/numbers_parquet")
print("✅ Parquet file created at /data/numbers_parquet")
# 4️⃣ Читаємо назад той же Parquet
df2 = spark.read.parquet("/data/numbers_parquet")
df2.show()
# 5️⃣ Закриваємо Spark
spark.stop()