#!/bin/bash
spark-submit /home/ducnv/code/SPARK/preprocessing.py \
hdfs://172.20.10.6:9000/user/LargeMuch/data_final/full_film \
hdfs://172.20.10.6:9000/user/LargeMuch/cleaned_data