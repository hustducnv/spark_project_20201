from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import *
import pyspark.sql.functions as F


def realtime_prediction(model_path, source_dir, checkpoint_dir, result_dir):
    spark = (SparkSession
             .builder
             .appName('Sentiment')
             .getOrCreate()
             )
    sc = spark.sparkContext

    # 1. Define input source
    # define schema
    schema = 'film_id STRING, comment_id STRING, title STRING, content STRING, user_id STRING, date STRING, star_rating INT'
    schema_2 = StructType([
        StructField('fiml_id', StringType(), False),  # nullable = False
        StructField('comment_id', StringType(), False),
        StructField('title', StringType(), False),
        StructField('content', StringType(), False),
        StructField('user_id', StringType(), False),
        StructField('date', StringType(), False),
        StructField('star_rating', IntegerType(), False),
    ])

    review_df = (spark
                 .readStream
                 .csv(source_dir, header=True, multiLine=True, schema=schema_2)
                 # .json(json_file_test, schema=schema, multiLine=True)
                 )

    # 2. Transform data
    model = PipelineModel.load(model_path)

    review_df = review_df.withColumn('review', F.concat('title', F.lit(' '), 'content'))
    predictons = model.transform(review_df)

    # 3. Define output sink and output mode
    writer = (predictons
              .writeStream
              .format('parquet')
              .option('path', result_dir)
              .outputMode('append')
              .trigger(processingTime='5 seconds')
              .option('checkpointLocation', checkpoint_dir)
              )

    # 4. Start
    streaming_query = writer.start()
    streaming_query.awaitTermination()


if __name__ == '__main__':
    import os
    import sys

    model_path = sys.argv[1]
    print(model_path)
    realtime_review_dir = sys.argv[2]
    print(realtime_review_dir)
    result_dir = sys.argv[3]
    print(result_dir)
    checkpoint_dir = sys.argv[4]
    print(checkpoint_dir)
    realtime_prediction(model_path, realtime_review_dir, checkpoint_dir, result_dir)
