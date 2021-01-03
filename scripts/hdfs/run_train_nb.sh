#!/bin/bash
spark-submit /home/ducnv/code/SPARK/train_nb.py \
hdfs://spark-master:9000/user/LargeMuch/data_final/reviews_full \
hdfs://spark-master:9000/user/LargeMuch/ML/models