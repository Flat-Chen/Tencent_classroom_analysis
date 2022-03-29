#!/bin/bash

export PYSPARK_PYTHON=/home/pyenv/pyspark/bin/python  
export PYSPARK_DRIVER_PYTHON=/home/pyenv/pyspark/bin/python

spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 1g \
--driver-cores 2 \
--num-executors 3 \
--executor-memory 1G \
--executor-cores 2 \
--jars hdfs://node01:9000/jar/mysql-connector-java-8.0.16.jar \
--conf spark.yarn.maxAppAttempts=1 \
/home/etl_all.py
