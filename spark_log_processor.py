"export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, split, count, desc, when, lit, sum

from log_generator import LOG_FILE_PATH
import time

def column_contains_string(df, column_name):
    # Try to cast the column to double. If it succeeds, it's not a string.
    # If it fails, it might be a string (or null).
    string_check = df.select(
        sum(
            when(
                col(column_name).cast("double").isNull() & col(column_name).isNotNull(), 1
            ).otherwise(0)).alias("string_count")
        ).collect()[0]["string_count"]

    return string_check > 0

def main():
    """161 seconds"""
    spark = SparkSession.builder.appName("LogProcessor").getOrCreate()

    logs = spark.read.text(LOG_FILE_PATH)

    split_col = split(logs['value'], '::')

    df = logs.select(
        split_col.getItem(0).alias('timestamp'),
        split_col.getItem(1).alias('ip'),
        split_col.getItem(2).alias('user'),
        split_col.getItem(3).alias('action'),
        split_col.getItem(4).alias('response_time')
    )

    action_count = df.groupBy('user') \
        .agg(count('*').alias('count')) \
        .orderBy(col('count').desc()) \
        .limit(10)
    action_count.show()

    avg_action_time = df.groupBy('action') \
        .agg({'response_time': 'avg'}) \
        .withColumnRenamed('avg(response_time)', 'avg_time')
    avg_action_time.show()

    n_unique_ips = df.select('ip').distinct().count()
    print(f"Number of unique IPs: {n_unique_ips}")

def main2():
    """170 seconds"""
    spark = SparkSession.builder.appName("LogProcessor").getOrCreate()

    logs = spark.read.text(LOG_FILE_PATH)

    split_col = split(logs['value'], '::')

    # Create a dataframe with the split columns
    df = logs.select(
        split_col.getItem(0).alias('timestamp'),
        split_col.getItem(1).alias('ip'),
        split_col.getItem(2).alias('user'),
        split_col.getItem(3).alias('action'),
        split_col.getItem(4).alias('response_time')
    )
    # print(f"Has strings: {column_contains_string(df, 'response_time')}")

    # Perform combined aggregations
    combined_agg = df.groupBy('user', 'action').agg(
        count('*').alias('user_action_count'),
        # sum(col('response_time').cast('float')).alias('response_time')
        sum("response_time").alias('total_duration')
    )

    # Derive top users
    top_users = combined_agg.groupBy('user') \
        .agg(sum('user_action_count').alias('total_count')) \
        .orderBy(desc('total_count')) \
        .limit(10)

    # Derive average duration by action
    avg_duration_by_action = combined_agg.groupBy('action') \
        .agg(
            (sum('total_duration') / sum('user_action_count')).alias('avg_duration')
        ) \
        .orderBy('action')
    
    top_users.show()
    avg_duration_by_action.show()
    n_unique_ips = df.select('ip').distinct().count()
    print(f"Number of unique IPs: {n_unique_ips}")



if __name__ == "__main__":
    t0 = time.time()
    main2()
    print(f"Finish processing in {time.time() - t0:.2f} seconds")