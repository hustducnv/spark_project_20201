from pyspark.sql import SparkSession


def read_streaming_prediction(path):
    """
    read streaming data from 'path' and print out console
    for testing
    """
    spark = (SparkSession
             .builder
             .appName('Read Streaming Predictions')
             .getOrCreate()
             )

    sc = spark.sparkContext
    df_tmp = spark.read.parquet(path)

    df = spark.readStream.schema(df_tmp.schema).parquet(path)
    df = df.select('review', 'star_rating', 'prediction')

    writer = df.writeStream.format("console").outputMode("update").trigger(processingTime='5 seconds')

    streaming_query = writer.start()
    streaming_query.awaitTermination()


if __name__ == '__main__':
    import os
    import sys
    result_dir = sys.argv[1]
    print('Streaming data from: {}'.format(result_dir))
    read_streaming_prediction(result_dir)
