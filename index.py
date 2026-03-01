from shlex import split
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, size, split
from pyspark import StorageLevel
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

df = spark.read.parquet("/yelp_data/cleaned/reviews")
print("Total cleaned reviews:", df.count())
df.printSchema()


# nulls = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
# nulls.show(truncate=False)

# dups = df.groupBy("review_id").count().filter("count > 1")
# print("Duplicate review_id rows:", dups.count())


# stars_dist = df.groupBy("stars").count().orderBy("stars")
# stars_dist.show()
# stars_dist.write.mode("overwrite").parquet("/yelp_data/results/stars_distribution")

# business_df = spark.read.json("/yelp_data/business.json") \
#     .select("business_id", "name", "city", "stars")

# top_businesses = df.groupBy("business_id").count().orderBy(F.col("count").desc()).limit(10)
# top_businesses.show(truncate=False)
# top_businesses.write.mode("overwrite").parquet("/yelp_data/results/top_10_businesses")

# top_businesses_named = top_businesses.join(
#     business_df,
#     on="business_id",
#     how="left"
# )

# top_businesses_named = top_businesses_named.select(
#     "name",
#     "city",
#     "stars",
#     "count"
# ).orderBy(F.col("count").desc())

# top_businesses_named.show(truncate=False)

# top_businesses_named.write.mode("overwrite") \
#     .parquet("/yelp_data/results/top_10_businesses_named")

top_rated = df.groupBy("business_id") \
    .agg(
        F.avg("stars").alias("avg_rating"),
        F.count("*").alias("n_reviews")
    ) \
    .filter(F.col("n_reviews") >= 1000) \
    .orderBy(F.col("avg_rating").desc()) \
    .limit(10)

top_rated.show(truncate=False)

business_df = spark.read.json("hdfs://localhost:9000/yelp_data/business.json") \
    .select("business_id", "name", "city", "stars")

top_rated_named = top_rated.join(
    business_df,
    on="business_id",
    how="left"
).select(
    "name",
    "city",
    F.round("avg_rating", 3).alias("avg_rating"),
    "n_reviews"
).orderBy(F.col("avg_rating").desc())

top_rated_named.show(truncate=False)

top_rated_named.write.mode("overwrite") \
    .parquet("/yelp_data/results/top_rated_min1000_reviews")

# top_users = df.groupBy("user_id").count().orderBy(F.col("count").desc()).limit(10)
# top_users.show(truncate=False)
# top_users.write.mode("overwrite").parquet("/yelp_data/results/top_10_users")

reviews = df.withColumn(
    "clean_text_norm",
    F.trim(F.regexp_replace(F.col("clean_text"), r"\s+", " "))
).withColumn(
    "char_count",
    F.length(F.col("clean_text_norm"))
).withColumn(
    "rating_group",
    F.when(F.col("stars").isin([1,2]), "low")
     .when(F.col("stars").isin([4,5]), "high")
     .otherwise("mid")
)

reviews_lh = reviews.filter(F.col("rating_group").isin(["low","high"])) \
    .select("stars","rating_group","char_count","clean_text_norm")


# char_stats = reviews_lh.groupBy("rating_group").agg(
#     F.avg("char_count").alias("avg_chars"),
#     F.expr("percentile_approx(char_count, 0.5)").alias("median_chars"),
#     F.stddev("char_count").alias("std_chars"),
#     F.count("*").alias("n")
# ).orderBy("rating_group")

# char_stats.show(truncate=False)
# char_stats.write.mode("overwrite").parquet("/yelp_data/results/charcount_low_high")

# char_by_star = reviews_lh.groupBy("stars").agg(
#     F.avg("char_count").alias("avg_chars"),
#     F.expr("percentile_approx(char_count, 0.5)").alias("median_chars"),
#     F.stddev("char_count").alias("std_chars"),
#     F.count("*").alias("n")
# ).orderBy("stars")

# char_by_star.show(truncate=False)
# char_by_star.write.mode("overwrite").parquet("/yelp_data/results/charcount_by_star")

# N = 100000  # per group (try 100k first)

# low_sample = reviews_lh.filter("rating_group='low'") \
#     .select("rating_group","clean_text_norm").limit(N)

# high_sample = reviews_lh.filter("rating_group='high'") \
#     .select("rating_group","clean_text_norm").limit(N)

# sample = low_sample.unionByName(high_sample)
# print("Sample rows:", sample.count())



# tokens = sample.select(
#     "rating_group",
#     F.explode(F.split("clean_text_norm", r"\s+")).alias("token")
# ).filter(
#     (F.length("token") > 2) &
#     (~F.col("token").rlike("^[0-9]+$"))
# )

# stopwords_list = [
#     "i","me","my","myself","we","our","ours","ourselves",
#     "you","your","yours","yourself","yourselves","he","him","his",
#     "himself","she","her","hers","herself","it","its","itself","they",
#     "them","their","theirs","themselves","what","which","who","whom",
#     "this","that","these","those","am","is","are","was","were","be",
#     "been","being","have","has","had","having","do","does","did","doing",
#     "a","an","the","and","but","if","or","because","as","until","while",
#     "of","at","by","for","with","about","against","between","into","through",
#     "during","before","after","above","below","to","from","up","down",
#     "in","out","on","off","over","under","again","further","then","once",
#     "here","there","when","where","why","how","all","any","both","each",
#     "few","more","most","other","some","such","no","nor","not","only",
#     "own","same","so","than","too","very","s","t","can","will","just",
#     "don","should","now"
# ]

# tokens = tokens.filter(~F.col("token").isin(stopwords_list))

# top_words = tokens.groupBy("rating_group","token").count()

# top_low = top_words.filter("rating_group='low'") \
#     .orderBy(F.col("count").desc()).limit(30)

# top_high = top_words.filter("rating_group='high'") \
#     .orderBy(F.col("count").desc()).limit(30)

# top_low.show(30, truncate=False)
# top_high.show(30, truncate=False)

# top_low.write.mode("overwrite").parquet("/yelp_data/results/top_words_low_sample")
# top_high.write.mode("overwrite").parquet("/yelp_data/results/top_words_high_sample")


# complaint_words = ["refund","rude","dirty","worst","disappointed","terrible","horrible","never","cold","slow",
#                    "manager","waste","overpriced","sick","awful","poor","bland","burnt","waited","broken"]

# positive_words  = ["amazing","great","love","perfect","friendly","awesome","excellent","best","recommend",
#                    "delicious","fresh","happy","fast","clean","nice","wonderful"]

# # Count token hits per group (token-level)
# complaint_hits = tokens.filter(F.col("token").isin(complaint_words)) \
#     .groupBy("rating_group").count() \
#     .withColumnRenamed("count", "complaint_word_hits")

# positive_hits = tokens.filter(F.col("token").isin(positive_words)) \
#     .groupBy("rating_group").count() \
#     .withColumnRenamed("count", "positive_word_hits")

# # Review counts per group (review-level)
# review_counts = sample.groupBy("rating_group").count() \
#     .withColumnRenamed("count", "n_reviews")

# lex_summary = review_counts.join(complaint_hits, "rating_group", "left") \
#     .join(positive_hits, "rating_group", "left") \
#     .fillna(0) \
#     .withColumn("complaint_hits_per_review", F.col("complaint_word_hits") / F.col("n_reviews")) \
#     .withColumn("positive_hits_per_review", F.col("positive_word_hits") / F.col("n_reviews")) \
#     .orderBy("rating_group")

# lex_summary.show(truncate=False)
# lex_summary.write.mode("overwrite").parquet("/yelp_data/results/lexicon_summary_sample")


BASE = "hdfs://localhost:9000/yelp_data/results"

def show_parquet(name, path, order_col=None, desc=False, n=50):
    print("\n" + "="*100)
    print(f"{name}")
    print("="*100)
    try:
        df = spark.read.parquet(path)
        if order_col:
            df = df.orderBy(F.col(order_col).desc() if desc else F.col(order_col))
        df.show(n, truncate=False)
    except Exception as e:
        print(f"Could not load {path}")
        print(str(e)[:300])

show_parquet(
    "Star Distribution",
    f"{BASE}/stars_distribution",
    order_col="stars"
)

show_parquet(
    "Character Count - Low vs High",
    f"{BASE}/charcount_low_high",
    order_col="rating_group"
)

show_parquet(
    "Character Count - By Star",
    f"{BASE}/charcount_by_star",
    order_col="stars"
)

show_parquet(
    "Top 10 Businesses by Review Count",
    f"{BASE}/top_10_businesses_named",
    order_col="count",
    desc=True,
    n=10
)

show_parquet(
    "Top 10 Users by Review Count",
    f"{BASE}/top_10_users",
    order_col="count",
    desc=True,
    n=10
)

show_parquet(
    "Top 30 Words - LOW Rated Reviews (Sample)",
    f"{BASE}/top_words_low_sample",
    order_col="count",
    desc=True,
    n=30
)


show_parquet(
    "Top 30 Words - HIGH Rated Reviews (Sample)",
    f"{BASE}/top_words_high_sample",
    order_col="count",
    desc=True,
    n=30
)

show_parquet(
    "Lexicon Summary - Complaint vs Positive Words",
    f"{BASE}/lexicon_summary_sample",
    order_col="rating_group"
)