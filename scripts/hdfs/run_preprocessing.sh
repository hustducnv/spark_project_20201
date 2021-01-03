#!/bin/bash
spark-submit /home/ducnv/code/SPARK/preprocessing.py \
hdfs://spark-master:9000/user/LargeMuch/data_final/full_film \
hdfs://spark-master:9000/user/LargeMuch/cleaned_data