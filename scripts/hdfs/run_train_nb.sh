#!/bin/bash
spark-submit /home/ducnv/code/SPARK/train_nb.py \
hdfs://172.20.10.6:9000/user/LargeMuch/data_final/reviews_full/{review_0_100k.csv,review_100k_200k.csv,review_200k_300k.csv,review_300k_400k.csv} \
hdfs://172.20.10.6:9000/user/LargeMuch/models