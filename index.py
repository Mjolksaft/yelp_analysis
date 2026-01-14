from shlex import split
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, size, split
import pyspark.sql.functions as F

# Start Spark session
spark = SparkSession.builder \
    .appName("Yelp Analysis") \
    .config("spark.executor.memory", "5g") \
    .config("spark.driver.memory", "5g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .master("local[*]") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.defaultFS", "hdfs://localhost:9000")
print("Spark session started successfully!")

review_df = spark.read.json("/yelp_data/review.json")
cleaned_reviews_df = spark.read.parquet("/yelp_data/cleaned/reviews")

# Cleaning ************************************

# print("number of rows in review:", review_df.count())
# print("number of rows in review:", cleaned_reviews_df.count())

# cleaned_reviews_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in cleaned_reviews_df.columns]).show()
# cleaned_reviews_df.groupBy("review_id").count().filter("count > 1").show()

# cleaned_reviews_df.show(5, truncate=100)


# business_df = spark.read.json("/yelp_data/business.json")
# user_df     = spark.read.json("/yelp_data/user.json")
# checkin_df  = spark.read.json("/yelp_data/checkin.json")
# tip_df      = spark.read.json("/yelp_data/tip.json")



# # 1️⃣ Drop nulls in critical columns
# review_cleaned = review_df.dropna(
#     subset=[
#         "business_id",
#         "review_id",
#         "stars",
#         "text",
#         "user_id"
#     ]
# )

# # 2️⃣ Drop duplicate reviews
# review_cleaned = review_cleaned.dropDuplicates(["review_id"])

# # 3️⃣ Filter invalid star ratings
# review_cleaned = review_cleaned.filter(
#     (col("stars") >= 1) & (col("stars") <= 5)
# )

# # 4️⃣ Clean text using Spark built-ins (no UDFs)
# review_cleaned = review_cleaned.withColumn(
#     "clean_text",
#     trim(
#         regexp_replace(
#             regexp_replace(lower(col("text")), r"http\S+", ""),  # remove URLs
#             r"[^a-z\s]", ""                                      # remove symbols/emojis
#         )
#     )
# )

# # 5️⃣ Remove very short reviews
# review_cleaned = review_cleaned.filter(
#     F.length("clean_text") > 20
# )

# # 6️⃣ Convert date column to proper date type
# review_cleaned = review_cleaned.withColumn(
#     "review_date",
#     to_date(col("date"))
# )

# # 7️⃣ Save cleaned reviews back to HDFS
# review_cleaned.write \
#     .mode("overwrite") \
#     .parquet("/yelp_data/cleaned/reviews")

# print("Reviews cleaned and saved successfully!")




#################
## exploration ##
#################

# print("Total cleaned reviews:", cleaned_reviews_df.count())
# cleaned_reviews_df.printSchema()
# cleaned_reviews_df.show(5, truncate=80)


# # shows stars distribution
# cleaned_reviews_df.groupBy("stars") \
#     .count() \
#     .orderBy("stars") \
#     .show()

# # Top 10 users by number of reviews
# cleaned_reviews_df.groupBy("user_id") \
#     .count() \
#     .orderBy(col("count").desc()) \
#     .show(10)


# # Top 10 businesses by number of reviews
# cleaned_reviews_df.groupBy("business_id") \
#     .count() \
#     .orderBy(col("count").desc()) \
#     .show(10)


# # Reviews over time (by year)


# reviews_with_len = cleaned_reviews_df.withColumn(
#     "text_length",
#     length("clean_text")
# )

# # Average character length by star rating
# reviews_with_len.groupBy("stars") \
#     .agg(
#         F.avg("text_length").alias("avg_text_length"),
#         F.count("*").alias("count")
#     ) \
#     .orderBy("stars") \
#     .show()


reviews_with_len = cleaned_reviews_df.withColumn(
    "word_count",
    size(split(col("clean_text"), " "))
)

# Average word length by star rating
reviews_with_len.groupBy("stars") \
    .agg(
        F.avg("word_count").alias("avg_word_count"),
        F.count("*").alias("count")
    ) \
    .orderBy("stars") \
    .show()