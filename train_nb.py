from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.feature import (
    RegexTokenizer, StopWordsRemover,
    CountVectorizer, IDF, HashingTF,
    VectorAssembler
)
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics, BinaryClassificationMetrics
import os
from time import time
import numpy as np


def train_model(csv_file, save_dir):
    spark = (SparkSession
             .builder
             .appName('Sentiment')
             .getOrCreate()
             )
    sc = spark.sparkContext

    # 1. READ DATA
    print('1. Reading data', '#'*100)
    # define schema
    schema = 'film_id STRING, comment_id STRING, title STRING, content STRING, user_id STRING, date STRING, star_rating INT'
    schema_2 = StructType([
        StructField('film_id', StringType(), False),  # nullable = False
        StructField('comment_id', StringType(), False),
        StructField('title', StringType(), False),
        StructField('content', StringType(), False),
        StructField('user_id', StringType(), False),
        StructField('date', StringType(), False),
        StructField('star_rating', IntegerType(), False),
    ])
    # NOTE: phải đảm bảo thứ tự schema giống trong file csv. nếu không chắc thì không dùng schema, để nó tự infer

    review_df = (spark
                 .read
                 .csv(csv_file, header=True, multiLine=True, schema=schema_2)
                 # .json(json_file_test, schema=schema, multiLine=True)
                 )



    # 2. DATA PRE-PROCESSING
    print('2. DATA PRE-PROCESSING', '#'*100)
    # 2.1 remove rating  == 5, 6
    # new_df = review_df.filter((review_df.star_rating > 6) | (review_df.star_rating < 5))
    new_df = review_df

    # 2.2 map rating - label
    map_rating_label_function = F.udf(lambda x: 0 if x <= 5 else 1, IntegerType())
    new_df = new_df.withColumn('label', map_rating_label_function('star_rating'))

    # 2.3 concatenate title and content = comment
    new_df = new_df.withColumn('review', F.concat('title', F.lit(' '), 'content'))

    # 2.4 select columns
    data = new_df.select('comment_id', 'review', 'star_rating', 'label')
    data = data.dropna()

    # statistic
    label_count = data.select('comment_id', 'label').groupBy('label').count()
    print('after pre-processing: ')
    label_count.show()



    # 3. PREPARING DATA FOR TRAINING AND TESTING:
    print('3. PREPARING DATA FOR TRAINING AND TESTING', '#'*100)
    label_count = label_count.toPandas().set_index('label')
    count_1 = label_count.loc[1]    # number of positive reviews_full
    count_0 = label_count.loc[0]    # number of negative reviews_full
    label_count = [count_0, count_1]

    # 3.1 stratified sampling
    if np.argmin(label_count) == 0:
        fractions = {0: 1, 1: (count_0/count_1)}
    else:
        fractions = {0: (count_1/count_0)}
    data_new = data.sampleBy(
        'label',
        fractions=fractions,
        seed=0
    )
    print('after sampling:')
    data_new.select('comment_id', 'label').groupBy('label').count().show()

    # 3.2 train test split
    df_train, df_test = data_new.randomSplit([0.8, 0.2], seed=0)
    # df_train = df_train.filter((F.col('star_rating') < 4) | (F.col('star_rating') > 7))
    print('df_train: ')
    df_train.select('comment_id', 'label').groupBy('label').count().show()
    print('df_test: ')
    df_test.select('comment_id', 'label').groupBy('label').count().show()



    # 4. BUILD MODEL PIPELINE
    print('4. BUILD MODEL PIPELINE', '#'*100)
    pipeline = build_model_pipeline()



    # 5. TRAINING
    print('5. TRAINING', '#'*100)
    start_time = time()
    model = pipeline.fit(df_train)
    print('DONE! Training time: {}'.format(time()-start_time))



    # 6. TESTING
    start_time = time()
    print('6. TESTING', '#'*100)
    predictions = model.transform(df_test)
    print('DONE! inference time: {}'.format(time() - start_time))
    predictions.select('star_rating', 'label', 'probability', 'prediction').show(30, truncate=False)

    evaluator = MulticlassClassificationEvaluator(
        predictionCol='prediction',
        labelCol='label',
        probabilityCol='probability',
        metricName='accuracy'
    )
    acc = evaluator.evaluate(predictions)
    print('ACCURACY SCORE: {}'.format(acc))


    # 7. SAVING PIPELINE MODEL
    save_path = os.path.join(save_dir, str(time()))
    model.write().overwrite().save(save_path)
    print('saved model to {}'.format(save_path))

    spark.stop()


def build_model_pipeline():
    """
    TF (term frequency): number of times the word occurs in a sepcific document
    DF (document frequency): number of times a word coccurs in collection of documents
    TF-IDF (TF - inverse DF): measures the significace of a word in a document
    """

    # 1. tokenize words, convert word to lowercase
    tokenizer = RegexTokenizer(
        inputCol='review',
        outputCol='review_tokens_uf',
        pattern='\\s+|[(),.!?\";]',
        toLowercase=True
    )

    # 2. remove stopwords
    stopwords_remover = StopWordsRemover(
        stopWords=StopWordsRemover.loadDefaultStopWords('english'),
        inputCol='review_tokens_uf',
        outputCol='review_tokens'
    )

    # 3. TF
    # cv = CountVectorizer(
    #     inputCol='review_tokens',
    #     outputCol='tf',
    #     vocabSize=200000
    # )
    cv = HashingTF(
        inputCol='review_tokens',
        outputCol='tf'
    )

    # 4. IDF
    idf = IDF(
        inputCol='tf',
        outputCol='features'
    )

    # 5. NB
    nb = NaiveBayes()

    pipeline = Pipeline(stages=[tokenizer, stopwords_remover, cv, idf, nb])

    return pipeline


if __name__ == '__main__':
    base_dir = os.path.dirname(__file__)
    print(base_dir)
    save_dir = os.path.join(base_dir, 'model', 'NB')
    data_path = "/home/ducnv/code/SPARK/data/reviews_train/"
    train_model(data_path, save_dir)