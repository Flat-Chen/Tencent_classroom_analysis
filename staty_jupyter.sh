#!/bin/bash
export PYSPARK_PYTHON=/home/pyenv/pyspark/bin/python  
export PYSPARK_DRIVER_PYTHON=/home/pyenv/pyspark/bin/python
source /root/anaconda3/etc/profile.d/conda.sh
conda activate pyspark
nohup jupyter lab --allow-root &
