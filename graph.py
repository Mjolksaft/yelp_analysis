from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

BASE = "hdfs://localhost:9000/yelp_data/results"

stars_df = spark.read.parquet(f"{BASE}/stars_distribution") \
    .orderBy("stars")

stars_pd = stars_df.toPandas()

plt.figure()
plt.bar(stars_pd["stars"].astype(str), stars_pd["count"])
plt.title("Star Rating Distribution")
plt.xlabel("Stars")
plt.ylabel("Number of Reviews")
plt.tight_layout()
plt.savefig("figure1_star_distribution.png", dpi=200)
plt.show()


char_df = spark.read.parquet(f"{BASE}/charcount_low_high") \
    .orderBy("rating_group")

char_pd = char_df.toPandas()

plt.figure()
plt.bar(char_pd["rating_group"], char_pd["avg_chars"])
plt.title("Average Review Length (Characters)")
plt.xlabel("Rating Group")
plt.ylabel("Average Characters")
plt.tight_layout()
plt.savefig("figure2_avg_char_length.png", dpi=200)
plt.show()


low_words_df = spark.read.parquet(f"{BASE}/top_words_low_sample") \
    .orderBy(F.col("count").desc()) \
    .limit(10)

low_pd = low_words_df.toPandas()

plt.figure()
plt.bar(low_pd["token"], low_pd["count"])
plt.title("Top 10 Words – Low-Rated Reviews")
plt.xlabel("Word")
plt.ylabel("Frequency")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("figure3_low_words.png", dpi=200)
plt.show()

high_words_df = spark.read.parquet(f"{BASE}/top_words_high_sample") \
    .orderBy(F.col("count").desc()) \
    .limit(10)

high_pd = high_words_df.toPandas()

plt.figure()
plt.bar(high_pd["token"], high_pd["count"])
plt.title("Top 10 Words - High-Rated Reviews")
plt.xlabel("Word")
plt.ylabel("Frequency")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("figure4_high_words.png", dpi=200)
plt.show()


lex_df = spark.read.parquet(f"{BASE}/lexicon_summary_sample") \
    .orderBy("rating_group")

lex_pd = lex_df.toPandas()

x = range(len(lex_pd))

plt.figure()
plt.bar([i - 0.2 for i in x], lex_pd["complaint_hits_per_review"], width=0.4, label="Complaint / Review")
plt.bar([i + 0.2 for i in x], lex_pd["positive_hits_per_review"], width=0.4, label="Positive / Review")

plt.xticks(list(x), lex_pd["rating_group"])
plt.title("Lexicon Frequency per Review")
plt.xlabel("Rating Group")
plt.ylabel("Hits per Review")
plt.legend()
plt.tight_layout()
plt.savefig("figure5_lexicon_comparison.png", dpi=200)
plt.show()

print("All figures saved successfully.")