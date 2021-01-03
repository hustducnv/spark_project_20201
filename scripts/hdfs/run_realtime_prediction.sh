#!/bin/bash
python /home/ducnv/code/SPARK/realtime_prediction.py \
hdfs://spark-master:9000/user/LargeMuch/ML/models/1609654873.442228 \
hdfs://spark-master:9000/user/LargeMuch/ML/realtime_reviews  \
hdfs://spark-master:9000/user/LargeMuch/ML/predictions/results \
hdfs://spark-master:9000/user/LargeMuch/ML/predictions/checkpoint