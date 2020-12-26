from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from preprocessing_utils import *
import sys


def preprocessing(file, film_type, save_dir=None):

    """

    :param file: path to json file
    :param save_dir: path to save result (parquet)
    :param film_type: type of film (features, tv_series,..)
    :return: Spark DataFrame
    """

    columns = ['film_id', 'url', 'title', 'type', 'release_date', 'genres', 'plot_keywords',
               'imdb_rating', 'rating_count', 'metascore', 'popularity', 'reviews', 'runtime',
               'directors', 'writers', 'main_cast_members', 'production_co', 'countries', 'languages',
               'filming_locations', 'mpaa', 'story_line', 'budget', 'cumulative_world_wide_gross',
               'opening_weekend_USA', 'known_as', 'aspect_ratio']


    # 1. READ DATA
    # print('1. READ DATA', '#'*50)
    spark = (SparkSession
             .builder
             .appName('Data Preprocessing')
             .getOrCreate()
             )
    sc = spark.sparkContext

    df = spark.read.json(file, multiLine=True)
    print('read {} records'.format(df.count()))



    # 2. PREPROCESSING
    sf_map_release_date = F.udf(map_str_to_datetime, DateType())
    sf_map_str_to_list = F.udf(map_str_to_list, ArrayType(StringType()))
    sf_map_budget = F.udf(map_budget, IntegerType())
    sf_map_url_to_id = F.udf(map_url_to_id, StringType())
    sf_map_runtime = F.udf(map_runtime, IntegerType())

    new_df = df.withColumn('film_id', sf_map_url_to_id('url'))

    new_df = new_df.withColumn('type', F.lit(film_type))

    # $50.990.000 -> 50990000
    new_df = (new_df
              .withColumn('new_release_date', sf_map_release_date('release_date'))
              .drop('release_date')
              .withColumnRenamed('new_release_date', 'release_date')
              )

    new_df = (new_df
              .withColumn('new_budget', sf_map_budget('budget'))
              .drop('budget')
              .withColumnRenamed('new_budget', 'budget')
              )

    new_df = (new_df
              .withColumn('new_cumulative_world_wide_gross', sf_map_budget('cumulative_world_wide_gross'))
              .drop('cumulative_world_wide_gross')
              .withColumnRenamed('new_cumulative_world_wide_gross', 'cumulative_world_wide_gross')
              )

    new_df = (new_df
              .withColumn('new_opening_weekend_USA', sf_map_budget('opening_weekend_USA'))
              .drop('opening_weekend_USA')
              .withColumnRenamed('new_opening_weekend_USA', 'opening_weekend_USA')
              )



    # string to list
    new_df = (new_df
              .withColumn('new_countries', sf_map_str_to_list('countries'))
              .drop('countries')
              .withColumnRenamed('new_countries', 'countries')
              )

    new_df = (new_df
              .withColumn('new_directors', sf_map_str_to_list('directors'))
              .drop('directors')
              .withColumnRenamed('new_directors', 'directors')
              )

    new_df = (new_df
              .withColumn('new_writers', sf_map_str_to_list('writers'))
              .drop('writers')
              .withColumnRenamed('new_writers', 'writers')
              )

    new_df = (new_df
              .withColumn('new_genres', sf_map_str_to_list('genres'))
              .drop('genres')
              .withColumnRenamed('new_genres', 'genres')
              )

    new_df = (new_df
              .withColumn('new_production_co', sf_map_str_to_list('production_co'))
              .drop('production_co')
              .withColumnRenamed('new_production_co', 'production_co')
              )

    new_df = (new_df
              .withColumn('new_main_cast_members', sf_map_str_to_list('main_cast_members'))
              .drop('main_cast_members')
              .withColumnRenamed('new_main_cast_members', 'main_cast_members')
              )

    new_df = (new_df
              .withColumn('new_languages', sf_map_str_to_list('languages'))
              .drop('languages')
              .withColumnRenamed('new_languages', 'languages')
              )

    new_df = (new_df
              .withColumn('new_filming_locations', sf_map_str_to_list('filming_locations'))
              .drop('filming_locations')
              .withColumnRenamed('new_filming_locations', 'filming_locations')
              )



    # cast type
    new_df = (new_df
              .withColumn('new_imdb_rating', F.col('imdb_rating').cast(FloatType()))
              .drop('imdb_rating')
              .withColumnRenamed('new_imdb_rating', 'imdb_rating')
              )
    try:
        new_df = (new_df
                  .withColumn('new_metascore', F.col('metascore').cast(IntegerType()))
                  .drop('metascore')
                  .withColumnRenamed('new_metascore', 'metascore')
                  )
    except:
        columns.remove('metascore')
        print('Does not has "metascore"!')

    try:
        new_df = (new_df
                  .withColumn('new_popularity', F.col('popularity').cast(IntegerType()))
                  .drop('popularity')
                  .withColumnRenamed('new_popularity', 'popularity')
                  )
    except:
        columns.remove('popularity')
        print('Does not has "popularity"!')

    new_df = (new_df
              .withColumn('new_rating_count', F.col('rating_count').cast(IntegerType()))
              .drop('rating_count')
              .withColumnRenamed('new_rating_count', 'rating_count')
              )

    try:
        new_df = (new_df
                  .withColumn('new_reviews', F.col('reviews').cast(IntegerType()))
                  .drop('reviews')
                  .withColumnRenamed('new_reviews', 'reviews')
                  )
    except:
        columns.remove('reviews')
        print('Does not has "reveiws"!')


    # runtime: 90 mins -> 90
    new_df = (new_df
              .withColumn('new_runtime', sf_map_runtime('runtime'))
              .drop('runtime')
              .withColumnRenamed('new_runtime', 'runtime')
              )

    # arrange
    final_df = new_df.select(columns)


    # SAVE TO PARQUET
    if save_dir is not None:
        final_df.write.parquet(save_dir, mode='append')
        print('save data to {}'.format(save_dir))

    return final_df


if __name__ == '__main__':
    import os
    from time import time
    data_dir = sys.argv[1]
    save_dir = sys.argv[2]
    files = os.listdir(data_dir)

    for _file in files:
        if _file == 'short_film.json':
            continue
        start_time = time()
        print('preprocessing: {}'.format(_file), '#'*100)
        path = os.path.join(data_dir, _file)
        film_type = _file.replace('.json', '')
        preprocessing(path, film_type, save_dir)
        print('Done! Time: {}'.format(time() - start_time), '#'*10)