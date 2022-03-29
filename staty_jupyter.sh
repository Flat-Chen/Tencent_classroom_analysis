#!/bin/bash
export PYSPARK_PYTHON=/home/pyenv/pyspark/bin/python  
export PYSPARK_DRIVER_PYTHON=/home/pyenv/pyspark/bin/python
source /root/anaconda3/etc/profile.d/conda.sh
conda activate pyspark
rm -rf nohup.out
nohup jupyter lab --allow-root &
sleep 3
cat nohup.out |grep token
